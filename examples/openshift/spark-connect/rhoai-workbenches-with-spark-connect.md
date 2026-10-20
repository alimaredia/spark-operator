## Running Spark Jobs from RHOAI Workbenches

This guide walks through connecting an RHOAI workbench to a Spark Connect server so you can run Spark workloads directly from a Jupyter notebook.

The server runs in **its own namespace**, not in `redhat-ods-applications`. Throughout this guide that namespace is `spark-workloads` — substitute your own. This is the configuration most users will have, and it is the one where the Spark job RBAC and a consumer NetworkPolicy are yours to create. See [Running in the operator's namespace](#running-in-the-operators-namespace) for what changes if you do put the server in `redhat-ods-applications`.

### Prerequisites

- RHOAI 3.5 or newer
- DataScienceCluster (DSC) configured with both the Spark Operator and Workbenches set to Managed
- A namespace for the Spark Connect server, and permission to create NetworkPolicies in it

### Create the namespace and Spark job RBAC

The ServiceAccount, Role, and RoleBinding that Spark driver pods need ship only into `redhat-ods-applications`. The Spark Connect server acts as the driver and uses them to create and manage its own executor pods, so they have to exist in the server's namespace too:

```bash
oc new-project spark-workloads
oc apply -k config/spark-rbac -n spark-workloads
```

That creates `ServiceAccount/spark-operator-spark`, `Role/spark-role`, and `RoleBinding/spark-role-binding`. Without them the server pod starts but cannot launch executors. It does **not** create any NetworkPolicy — that is the next section.

If you are not working from a clone of this repository, copy them out of the operator namespace instead:

```bash
oc get serviceaccount spark-operator-spark -n redhat-ods-applications -o yaml
oc get role spark-role -n redhat-ods-applications -o yaml
oc get rolebinding spark-role-binding -n redhat-ods-applications -o yaml
```

Strip `namespace`, `resourceVersion`, `uid`, and `creationTimestamp` from each before re-applying them with `-n spark-workloads`.

### Allow workbenches to reach the server

On RHOAI/ODH the operator creates operand NetworkPolicies (the `OperandNetworkPolicy` feature gate). These leave the Spark Connect server **default-deny for ingress** and admit only intra-Spark traffic — executors can reach it, a workbench cannot. Workbenches live in their own data-science-project namespaces, so a workbench reaching the server's gRPC port (`15002`) is **cross-namespace** traffic you have to admit explicitly, with a policy in the **server's** namespace:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-connect-from-workbenches
  namespace: spark-workloads          # the SparkConnect server's namespace
spec:
  podSelector:
    matchLabels: {spark-role: connect-server}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: {matchLabels: {opendatahub.io/dashboard: "true"}}
          podSelector: {matchLabels: {opendatahub.io/workbenches: "true"}}
      ports:
        - {protocol: TCP, port: 15002}
```

```bash
oc apply -f spark-connect-networkpolicy.yaml
```

Both selectors in that single `from` peer are required together, and they are deliberately narrow: the `namespaceSelector` matches RHOAI data science projects, and the `podSelector` matches only workbench pods inside them. An unlabelled pod in a data science project is still denied.

Apply it before or after creating the server — the CNI re-evaluates continuously. The operator neither manages nor garbage-collects this policy. NetworkPolicies are additive, with no deny, so it only *adds* allows and leaves the operator's own policies alone; add an `ownerReference` to the SparkConnect if you want kube GC to remove it with the server. See [network-policy-pod-inventory.md](../docs/network-policy-pod-inventory.md) for the full model, including why consumer ingress is user-managed.

**If your client runs in the server's own namespace** — a script or notebook co-located with it rather than a dashboard workbench — the cross-namespace policy above will not admit it. Apply the same-namespace form as well — the two are additive:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-operator-allow-consumers
  namespace: spark-workloads
spec:
  podSelector:
    matchExpressions:
      - {key: spark-role, operator: In, values: [driver, connect-server]}
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector: {}                 # any pod in the same namespace
      ports:
        - {protocol: TCP, port: 4040}     # Spark UI
        - {protocol: TCP, port: 15002}    # Spark Connect gRPC
```

### Create a Spark Connect Server

Create the SparkConnect resource in your namespace:

```bash
oc apply -f spark-connect-server-rhoai.yaml
```

Confirm the server pod is running before continuing:

```bash
oc get pods -n spark-workloads -l spark-role=connect-server
```

### Create a Service for the Spark Connect Server

The Spark Connect pod IP changes every time the pod restarts. To provide a stable endpoint, create a Service that routes traffic to the current pod automatically. This gives workbenches a fixed DNS name (`spark-connect-service.spark-workloads.svc.cluster.local`) so they don't need to update their connection string after restarts.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: spark-connect-service
  namespace: spark-workloads
  labels:
    app.kubernetes.io/part-of: sparkoperator
spec:
  type: ClusterIP
  selector:
    sparkoperator.k8s.io/connect-name: spark-connect
    spark-role: connect-server
  ports:
    - name: spark-connect
      port: 15002
      targetPort: 15002
      protocol: TCP
```

```bash
oc apply -f spark-connect-service.yaml
```

A Service does not bypass NetworkPolicy — traffic is evaluated against the backing pod, so the policy from the previous section is still what admits the workbench.

### Run a Spark Workload from a Workbench

Create a workbench from the RHOAI dashboard by navigating to your Data Science Project and clicking **Create workbench**. A minimal Jupyter CPU image (e.g., "Jupyter | Minimal | CPU | Python 3.12") is sufficient for Spark Connect workloads.

Spark Connect supports DataFrames, Spark SQL, and parts of Structured Streaming and MLlib from a Jupyter notebook. DataFrames are the best fit for notebooks since they offer a familiar tabular interface similar to pandas.

First, install PySpark and its dependencies. The PySpark version must match the Spark version used in the Spark Connect server image. After running this cell, you may need to restart the notebook kernel for the packages to take effect:

```
!pip install pyspark==4.0.1 pandas pyarrow grpcio grpcio-status zstandard
```

Then connect to the Spark Connect server and run a workload. The hostname is the Service's fully-qualified name, so it includes the **server's** namespace, not the workbench's:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect-service.spark-workloads.svc.cluster.local:15002").getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Claire", 66)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.filter(df.Age > 40).show()
```

```
+------+---+
|  Name|Age|
+------+---+
|   Bob| 45|
|Claire| 66|
+------+---+
```

### Debugging Tips

To check the Spark Connect server logs run:

```bash
oc logs spark-connect-server -n spark-workloads
```

To debug connectivity issues, open a shell on the workbench pod. First, find the workbench pod name and namespace:

```bash
oc get pods --all-namespaces -l opendatahub.io/workbenches=true
```

Then exec into the pod for any other further debugging you might need to do:

```bash
oc rsh -n <project-namespace> <workbench-pod-name>
```

A hang on `getOrCreate()` or on the first query, rather than a connection refusal, is the signature of a NetworkPolicy drop. Check that the policy exists in the **server's** namespace and that the workbench's namespace and pod carry both labels the policy selects:

```bash
oc get networkpolicy -n spark-workloads
oc get namespace <project-namespace> -o jsonpath='{.metadata.labels}'
oc get pod -n <project-namespace> <workbench-pod-name> -o jsonpath='{.metadata.labels}'
```

To confirm the port is reachable at all, from the workbench shell:

```bash
curl -v telnet://spark-connect-service.spark-workloads.svc.cluster.local:15002
```

### Running in the operator's namespace

If you put the server in `redhat-ods-applications` instead, the NetworkPolicy step above is a **no-op**: the RHOAI/ODH overlays ship `spark-operator-allow-internal` (`config/overlays/{rhoai,odh}/networkpolicy.yaml`), which selects every operand pod in that namespace and already admits `15002` both from any pod in the namespace and from workbench pods in dashboard namespaces — the same rule, plus more. Ingress there is wider than the operand policies alone would make it, so a server in that namespace is reachable without any policy of your own.

That is also why this guide does not use it as the example: a walkthrough run there appears to work with no NetworkPolicy, which tells you nothing about whether your own namespace is configured correctly.
