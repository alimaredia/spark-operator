# Accessing Spark UI via Port-Forward

This guide demonstrates how to access the Spark Application UI for local testing and development using `oc port-forward` or `kubectl port-forward`.

## When to Use Port-Forward

Port-forwarding is ideal for:
- **Development and testing** - Quick local access without network configuration
- **Debugging individual jobs** - Inspect running applications on your workstation
- **Environments without Routes** - When OpenShift Routes or Ingress are not configured

For production deployments with shareable HTTPS URLs, see [the Route guide](../route/README.md).

> **Note:** Port-forward works even when the operator's operand NetworkPolicies
> are enabled (RHOAI/ODH). Port-forward traffic is proxied by the API server
> through the node (kubelet), which NetworkPolicy always permits, so **no extra
> NetworkPolicy is required** — unlike Route access, which is cross-namespace. See
> [network-policy-pod-inventory.md](../../docs/network-policy-pod-inventory.md).

## Prerequisites

- A running SparkApplication submitted via Kubeflow Spark Operator
- `oc` CLI configured and logged in to your OpenShift cluster
- Access to the namespace where the Spark driver is running

## Accessing the Spark UI

### Step 1: Identify the Driver Pod

Find your Spark driver pod:

```bash
oc get pods -l spark-role=driver

# Example output:
# NAME                        READY   STATUS    RESTARTS   AGE
# spark-pi-driver             1/1     Running   0          2m
```

### Step 2: Port-Forward to the Driver Pod

The Spark UI runs on port 4040 inside the driver pod. Forward it to your local machine:

```bash
oc port-forward spark-pi-driver 4040:4040
```

Expected output:
```
Forwarding from 127.0.0.1:4040 -> 4040
Forwarding from [::1]:4040 -> 4040
```

**Note:** Keep this terminal open while accessing the UI.

### Step 3: Access the Spark UI

Open your browser and navigate to:

```
http://localhost:4040
```

You should see the Spark Application UI showing:
- **Jobs** - Overview of submitted jobs
- **Stages** - Detailed stage execution and task progress
- **Storage** - Cached RDDs and DataFrames
- **Environment** - Spark configuration and runtime properties
- **Executors** - Executor status, resource usage, and logs
- **SQL** - SQL query execution plans (if using Spark SQL)

### Step 4: Stop Port-Forwarding

When finished, press `Ctrl+C` in the terminal to stop the port-forward.

## Using a Different Local Port

If port 4040 is already in use, forward to a different local port:

```bash
oc port-forward spark-pi-driver 8080:4040
```

Then access the UI at `http://localhost:8080`.

## Accessing from a Different Namespace

If the Spark driver is in a different namespace, specify it with `-n`:

```bash
oc port-forward -n team-a-workbenches spark-pi-driver 4040:4040
```

## Limitations

Port-forwarding has several limitations for production use:

| Limitation | Impact | Production Alternative |
|------------|--------|------------------------|
| **Local only** | Only accessible from your workstation | Use OpenShift Routes for team access |
| **Requires active terminal** | Connection drops when terminal closes or network disconnects | Routes remain accessible |
| **No authentication** | Anyone on localhost can access | Routes support TLS and authentication |
| **Manual process** | Must identify pod and run command each time | Routes auto-update with DNS |

For production deployments, see [the Route guide](../route/README.md) for persistent HTTPS access.

## Troubleshooting

### Pod Not Found
```bash
# List all pods with spark-role label
oc get pods -l spark-role=driver --all-namespaces

# Describe SparkApplication to see driver pod status
oc describe sparkapplication spark-pi
```

### Port Already in Use
```
error: unable to listen on port 4040: Listeners failed to create with the following errors: [unable to create listener: Error listen tcp4 127.0.0.1:4040: bind: address already in use]
```

**Solution:** Use a different local port:
```bash
oc port-forward spark-pi-driver 4041:4040
```

### Connection Refused
If you see `connection refused` in the browser:
- Verify the driver pod is in `Running` state: `oc get pod spark-pi-driver`
- Check that Spark UI is enabled in the SparkApplication spec (enabled by default)
- Ensure the driver container has started the Spark UI server (check logs: `oc logs spark-pi-driver`)

### UI Shows "Application Not Found"
If the Spark UI loads but shows no application data, the driver may still be initializing. Wait a few seconds and refresh.

## Example: Monitoring a Running Job

Complete workflow from job submission to UI access:

```bash
# 1. Submit a SparkApplication
oc apply -f examples/spark-pi.yaml

# 2. Wait for driver pod to start
oc wait --for=condition=Ready pod -l spark-role=driver,spark-app-name=spark-pi --timeout=120s

# 3. Get driver pod name
DRIVER_POD=$(oc get pod -l spark-role=driver,spark-app-name=spark-pi -o jsonpath='{.items[0].metadata.name}')

# 4. Start port-forward
oc port-forward $DRIVER_POD 4040:4040 &

# 5. Open browser to http://localhost:4040

# 6. Stop port-forward when done
kill %1
```

## Next Steps

- [Configure OpenShift Routes for production access](../route/README.md)
- [Deploy Spark History Server for completed job analysis](SparkHistoryServer.md)
- [Configure event logging for historical analysis](EventLogging.md)
