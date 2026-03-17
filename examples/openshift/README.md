# Kubeflow Spark Operator on Openshift

This documentation details how to install the Kubeflow Spark Operator (KSO) on Openshift, and goes through an example of running a Spark workload that converts documents using [docling](https://docling-project.github.io/).

## 1. Spark Operator Architecture

The Spark Operator consists of:

1.  A **SparkApplication Controller** that watches for events (Create, Update, Delete) of `SparkApplication` resources across configured namespaces.
2.  A **Submission Runner**: When a `SparkApplication` is created, the operator run the `spark-submit` command and executes it inside a simplified "submission" pod or internally.
3.  **Spark Pod Monitor**: Watches the status of the Driver and Executor pods and updates the `.status` field of the `SparkApplication` resource.
4.  **Mutating Admission Webhook**: An optional but recommended component that intercepts pod creation requests. It injects Spark-specific configuration (like mounting ConfigMaps or Volumes) into the Driver and Executor pods before they are scheduled.

KSO provides a CustomResourceDefinition called a SparkApplication which defines a Spark job, and a controller that handles the lifecycle of Spark driver and executor pods.

KSO does not run jobs on a standalone Spark cluster, rather it uses Spark's Kubernetes scheduler back end. When a SparkApplication is submitted, spark-submit points to the Kubernetes cluster and creates the spark driver pod for the application which in turn creates the executor pods. KSO expects users to bring their own Spark runtime images. KSO supports Spark 2.3 and up.

## 2. Installation on OpenShift

> **Pre-requisite:** This section requires **Cluster Admin** privileges. You must install the operator once so that users can submit `SparkApplication` CRDs.

### Prerequisites
*   OpenShift CLI (`oc`) configured
*   Cluster Admin privileges
*   Git (to clone the repository)

### Installation Steps

The operator is installed using Kustomize manifests located in `config/`.

#### 1. Clone the Repository

```bash
git clone https://github.com/opendatahub-io/spark-operator.git
cd spark-operator
```

#### 2. Login to OpenShift

```bash
# Log in to your Red Hat OpenShift cluster
oc login 
```

#### 3. Install the Operator

```bash
oc apply -k config/default/ --server-side=true
```

> **Note:** The `--server-side=true` flag is required because the CRDs are large and exceed Kubernetes annotation size limits for client-side apply.

This creates:
- Operator namespace with controller and webhook deployments
- 3 CRDs (SparkApplication, ScheduledSparkApplication, SparkConnect)
- Comprehensive RBAC configuration
- Spark job ServiceAccount for driver pods

> **Version Note:** The Kustomize manifests use Spark Operator v2.4.0 which supports Spark 3.5.x. The document processing spark application in this guide uses Spark 3.5.7 with Java 17 for Python 3.10 compatibility (required by docling). See the [version matrix](https://github.com/kubeflow/spark-operator?tab=readme-ov-file#version-matrix) for details.
> **Multiple Namespaces:** By default, the operator watches all namespaces (empty `--namespaces=` flag). To watch specific namespaces, modify the `--namespaces` argument in `config/manager/manager.yaml` to `--namespaces=ns1,ns2,ns3`.

#### 4. Verify Installation

```bash
oc get pods -n spark-operator -l app.kubernetes.io/name=spark-operator

# Expected output:
# NAME                                        READY   STATUS    RESTARTS   AGE
# spark-operator-controller-xxx               1/1     Running   0          1m
# spark-operator-webhook-xxx                  1/1     Running   0          1m
```

#### 5. Verify Security Context (OpenShift)

Confirm that the `restricted-v2` SCC is assigned:

```bash
oc describe pod -n spark-operator -l app.kubernetes.io/component=controller | grep -i openshift.io/scc
# Expected: openshift.io/scc: restricted-v2
```

Verify the container runs with a non-root UID:

```bash
POD=$(oc get pod -n spark-operator -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.name}')
oc exec -n spark-operator $POD -- id
# Expected: uid=1000xxx gid=0(root) groups=0(root),1000xxx
```

## 3. SparkApplication CRD

The **SparkApplication** Custom Resource Definition (CRD) is the core abstraction provided by the operator. It allows you to define Spark applications declaratively using Kubernetes YAML manifests, similar to how you define Deployments or Pods.

Key fields in the `SparkApplication` spec include:

*   **`type`**: The language of the application (`Python`).
*   **`mode`**: Deployment mode (`cluster` or `client`). In `cluster` mode, the driver runs in a pod.
*   **`image`**: The container image to use for the driver and executors.
*   **`mainApplicationFile`**: The entry point path (e.g., `local:///app/scripts/run_spark_job.py`).
*   **`sparkVersion`**: The version of Spark to use (must match the image).
*   **`restartPolicy`**: Handling of failures (`Never`, `OnFailure`, `Always`).
*   **`driver` / `executor`**: Resource requests (cores, memory), labels, service accounts, and **security contexts**.
*   **`volumes` / `volumeMounts`**: PVCs for input and output data.

Here is an example snippet from `examples/openshift/k8s/docling-spark-app.yaml`:

```yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: docling-spark-job
  namespace: spark-operator  # One of the namespaces the operator is configured to watch
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: quay.io/rishasin/docling-spark:multi-output
  imagePullPolicy: Always
  mainApplicationFile: local:///app/scripts/run_spark_job.py
  arguments:
    - "--input-dir"
    - "/app/assets"
    - "--output-file"
    - "/app/output/"
  sparkVersion: "3.5.7"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    memory: "4g"
    serviceAccount: spark-operator-spark
    securityContext: {}
  executor:
    cores: 1
    instances: 2
    memory: "4g"
    securityContext: {}
```

> **Note:** See `examples/openshift/k8s/docling-spark-app.yaml` for the complete configuration 

## 4. About Docling-Spark Application

The `docling-spark` application demonstrates a production-grade pattern for processing documents at scale using:
*   **Docling**: For advanced document layout analysis and understanding.
*   **Apache Spark**: For distributed processing across the cluster.
*   **Kubeflow Spark Operator**: For native Kubernetes lifecycle management.
*   **PVC-based Storage**: Input and output data stored on persistent volumes.

### How It Works
1.  Upload PDFs to the **input PVC**.
2.  **Spark Operator** launches a Driver Pod.
3.  **Driver** reads files from input PVC, distributes work to Executor Pods.
4.  **Executors** process PDFs in parallel (OCR, Layout Analysis, Table Extraction).
5.  **Driver** collects results and writes to **output PVC**.
6.  Download results from output PVC anytime.

## 5. Deploying the Docling-Spark Application

This section uses the **pre-built image** `quay.io/rishasin/docling-spark:latest` which contains the Docling + PySpark application with all dependencies. The manifest `examples/openshift/k8s/docling-spark-app.yaml` is already configured to use this image.

> **Important:** The Spark job runs in the **same namespace** as the operator. The deploy script uses the `spark-operator-spark` ServiceAccount that is created during operator installation.

> **Building Custom Images?** If you need custom dependencies or want to use your own container registry, see [Building Custom Spark Images for OpenShift](./BuildingCustomSparkImages.md) for best practices on OpenShift compatibility and build instructions.

### Step 1: Upload Your PDFs

```bash
# Make the script executable (first time only)
chmod +x examples/openshift/k8s/deploy.sh

# Upload your PDF files to the input PVC
./examples/openshift/k8s/deploy.sh upload ./path/to/your/pdfs/
```


### Step 2: Run the Spark Job
The deploy script creates PVCs and submits the SparkApplication in the operator namespace:

```bash
./examples/openshift/k8s/deploy.sh
```

Expected output:

```text
==============================================
  Deploying Docling + PySpark
==============================================
  Namespace: spark-operator
==============================================

[INFO] 1. Verifying namespace exists...
[OK] Namespace 'spark-operator' exists

[INFO] 2. Ensuring PVCs exist...
persistentvolumeclaim/docling-input created
persistentvolumeclaim/docling-output created

3. Submitting Spark Application...
sparkapplication.sparkoperator.k8s.io/docling-spark-job created

[OK] Deployment complete!

📊 Check status:
   oc get sparkapplications -n spark-operator
   oc get pods -n spark-operator -w

📝 View logs:
   oc logs -f docling-spark-job-driver -n spark-operator

🌐 Access Spark UI (when driver is running):
   oc port-forward -n spark-operator svc/docling-spark-job-ui-svc 4040:4040
   Open: http://localhost:4040
```

> **Note:** On subsequent runs, you'll see `unchanged` instead of `created` for resources that already exist.

### Step 3: Monitor the Job

#### Watch pods
```bash
# adjust namespace based on your deployment
oc get pods -n spark-operator -w
```

Expected output (pods lifecycle):

```text
NAME                                        READY   STATUS              AGE
docling-spark-job-driver                    0/1     Pending             0s
docling-spark-job-driver                    0/1     ContainerCreating   0s
docling-spark-job-driver                    1/1     Running             2s
doclingsparkjob-xxx-exec-1                  0/1     Pending             0s
doclingsparkjob-xxx-exec-1                  0/1     ContainerCreating   0s
doclingsparkjob-xxx-exec-1                  1/1     Running             3s
doclingsparkjob-xxx-exec-2                  1/1     Running             3s
...
doclingsparkjob-xxx-exec-1                  0/1     Completed           83s
doclingsparkjob-xxx-exec-2                  0/1     Completed           83s
docling-spark-job-driver                    0/1     Completed           100s
```

#### Application Logs
Once the Driver pod is created, check its logs for Spark-specific initialization and application output:

```bash
oc logs docling-spark-job-driver -n spark-operator
oc logs docling-spark-job-exec-1 -n spark-operator
```

#### SparkApplication Status
Inspect the status of the CRD to see if the operator encountered validation errors or submission failures:

```bash
# Adjust namespace based on your deployment
oc describe sparkapplication docling-spark-job -n spark-operator
```

### Step 4: Download the Results

First, delete the SparkApplication to release the output PVC:
```bash
# Adjust namespace based on your deployment
oc delete sparkapplication docling-spark-job -n spark-operator
```

Expected output:

```text
sparkapplication.sparkoperator.k8s.io "docling-spark-job" deleted
```

Download results from the output PVC:
```bash
./examples/openshift/k8s/deploy.sh download ./examples/openshift/output/
```

Expected output:

```text
==============================================
  Downloading results from Output PVC
==============================================

[INFO] Creating helper pod 'pvc-downloader'...
pod/pvc-downloader created
[INFO] Waiting for pod to be ready...
pod/pvc-downloader condition met
[OK] Helper pod ready
[INFO] Files on output PVC:
-rw-rw-r--. 1 1000840000 1000840000 2140488 Dec 15 03:55 summary.jsonl
[INFO] Copying files to './output/'...

[INFO] Downloaded files:
-rw-r--r--  1 user  staff  2140488 Dec 15 03:55 summary.jsonl
[OK] Download complete!
[INFO] Deleting helper pod 'pvc-downloader'...
pod "pvc-downloader" deleted
```

View results:
```bash
cat ./output/summary.jsonl
```

## 6: Access Spark UI (Optional)

While the driver is running:
```bash
# Adjust namespace based on your deployment
oc port-forward -n spark-operator svc/docling-spark-job-ui-svc 4040:4040
# Open: http://localhost:4040
```

## 7: Cleanup

```bash
# Delete the SparkApplication
oc delete sparkapplication docling-spark-job -n spark-operator

# Delete PVCs (WARNING: This deletes all data!)
oc delete pvc docling-input docling-output -n spark-operator

# Delete the operator
oc delete -k config/default/
```
