## Running Spark Jobs from RHOAI Workbenches

This guide walks through connecting an RHOAI workbench to a Spark Connect server so you can run Spark workloads directly from a Jupyter notebook.

### Prerequisites

- RHOAI 3.5 or newer
- DataScienceCluster (DSC) configured with both the Spark Operator and Workbenches set to Managed

### Create a Spark Connect Server

Create a SparkConnect resource in the `redhat-ods-applications` namespace:

```bash
oc apply -f spark-connect-server-rhoai.yaml
```

By default, the required ServiceAccount, Role, and RoleBinding exist only in `redhat-ods-applications`. To deploy a SparkConnect resource in a different namespace, you must recreate those there:

```bash
oc get serviceaccount spark-operator-spark -n redhat-ods-applications -o yaml
oc get role spark-operator-role -n redhat-ods-applications -o yaml
oc get rolebinding spark-operator-rolebinding -n redhat-ods-applications -o yaml
```

**NetworkPolicy.** On RHOAI/ODH the operator creates operand NetworkPolicies (the `OperandNetworkPolicy` feature gate), which leave the Spark Connect server **default-deny for ingress** and admit only intra-Spark traffic — so executors can reach it, but a workbench cannot. Workbenches live in their own data-science-project namespaces, so reaching the server's gRPC port (`15002`) is **cross-namespace** and must be allowed by a NetworkPolicy you create **in the SparkConnect server's namespace**:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-connect-from-workbenches
  namespace: redhat-ods-applications   # the SparkConnect server's namespace
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

This mirrors the workbench rule that the legacy `spark-operator-allow-internal` policy provided; see [network-policy-pod-inventory.md](../docs/network-policy-pod-inventory.md) for the full model (including why consumer ingress is user-managed). If you run the server in a namespace other than `redhat-ods-applications`, create this NetworkPolicy there too.

### Create a Service for the Spark Connect Server

The Spark Connect pod IP changes every time the pod restarts. To provide a stable endpoint, create a Service that routes traffic to the current pod automatically. This gives workbenches a fixed DNS name (`spark-connect-service.redhat-ods-applications.svc.cluster.local`) so they don't need to update their connection string after restarts.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: spark-connect-service
  namespace: redhat-ods-applications
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

### Run a Spark Workload from a Workbench

Create a workbench from the RHOAI dashboard by navigating to your Data Science Project and clicking **Create workbench**. A minimal Jupyter CPU image (e.g., "Jupyter | Minimal | CPU | Python 3.12") is sufficient for Spark Connect workloads.

Spark Connect supports DataFrames, Spark SQL, and parts of Structured Streaming and MLlib from a Jupyter notebook. DataFrames are the best fit for notebooks since they offer a familiar tabular interface similar to pandas.

First, install PySpark and its dependencies. The PySpark version must match the Spark version used in the Spark Connect server image. After running this cell, you may need to restart the notebook kernel for the packages to take effect:

```
!pip install pyspark==4.0.1 pandas pyarrow grpcio grpcio-status zstandard
```

Then connect to the Spark Connect server and run a workload:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect-service.redhat-ods-applications.svc.cluster.local:15002").getOrCreate()

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
oc logs spark-connect-server -n redhat-ods-applications
```

To debug connectivity issues, open a shell on the workbench pod. First, find the workbench pod name and namespace:

```bash
oc get pods --all-namespaces -l opendatahub.io/workbenches=true
```

Then exec into the pod for any other further debugging you might need to do:

```bash
oc rsh -n <project-namespace> <workbench-pod-name>
```
