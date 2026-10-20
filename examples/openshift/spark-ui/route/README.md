# Accessing the Spark UI via OpenShift Routes

## Overview

The Spark Web UI provides real-time monitoring of Spark jobs, stages, storage,
and executors. On OpenShift, you can expose the Spark UI through Routes with
HTTPS for persistent, shareable access without requiring local port-forwarding.

## Prerequisites

- OpenShift 4.x cluster
- Spark Operator installed and running
- Your cluster's apps domain (get it with: `oc get ingress.config.openshift.io cluster -o jsonpath='{.spec.domain}'`)

> **NetworkPolicy requirement (RHOAI/ODH).** When the operator's operand
> NetworkPolicies are enabled (the `OperandNetworkPolicy` feature gate, on in the
> RHOAI/ODH overlays), the driver pod is **default-deny for ingress** and only
> admits intra-Spark traffic. The OpenShift router reaching the driver's `4040`
> from the `openshift-ingress` namespace is **cross-namespace**, so the Route
> will not connect until you add a NetworkPolicy admitting it. No NetworkPolicy is
> shipped for consumers, so create this yourself in the workload namespace, once
> (alongside your RBAC):
>
> ```yaml
> apiVersion: networking.k8s.io/v1
> kind: NetworkPolicy
> metadata:
>   name: spark-ui-from-router
>   namespace: spark-test            # your workload namespace
> spec:
>   podSelector:
>     matchLabels: {spark-role: driver}
>   policyTypes: [Ingress]
>   ingress:
>     - from:
>         - namespaceSelector:
>             matchLabels: {network.openshift.io/policy-group: ingress}
>       ports:
>         - {protocol: TCP, port: 4040}   # or your custom spark.ui.port
> ```
>
> See [network-policy-pod-inventory.md](../../docs/network-policy-pod-inventory.md)
> for the full model. If operand NetworkPolicies are not enabled, no extra policy
> is needed.

## Example Configuration

First, get your cluster's apps domain:

```bash
CLUSTER_DOMAIN=$(oc get ingress.config.openshift.io cluster -o jsonpath='{.spec.domain}')
echo "Your cluster domain: $CLUSTER_DOMAIN"
# Example output: apps.my-cluster.abc123.p1.openshiftapps.com
```

Then configure your SparkApplication with the domain:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi-route-test
  namespace: spark-test
spec:
  type: Scala
  mode: cluster
  image: quay.io/opendatahub/data-processing:Spark-v4.0.1
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
  arguments: ["100000"]  # Long-running for UI access (adjust based on cluster resources)
  sparkVersion: "4.0.1"
  
  restartPolicy:
    type: Never
  
  driver:
    cores: 1
    memory: "1000m"
    serviceAccount: spark-operator-spark
  
  executor:
    cores: 1
    instances: 2
    memory: "1000m"
  
  # Driver ingress configuration for route creation
  driverIngressOptions:
    - servicePort: 4040
      servicePortName: "spark-driver-ui-port"
      ingressURLFormat: "{{$appName}}-{{$appNamespace}}.apps.my-cluster.abc123.p1.openshiftapps.com"  # Replace with your cluster domain
      ingressAnnotations:
        route.openshift.io/termination: "edge"
```

**Note:** Replace `apps.my-cluster.abc123.p1.openshiftapps.com` with your actual cluster domain from the command above.

---

> ⚠️ **SECURITY WARNING - Routes are Publicly Accessible by Default**
>
> Routes created with this configuration are **publicly accessible** without authentication. Anyone with the route URL can access the Spark UI, which exposes:
> - Job configuration and spark-submit parameters
> - Environment variables (may include sensitive configuration)
> - Application logs and error messages  
> - Executor details, resource usage, and performance metrics
> - SQL query plans (may contain table/column names)
>
> **For production deployments with sensitive data:**
> - Configure authentication (OAuth proxy, OpenShift RBAC integration)
> - Use NetworkPolicies to restrict access
> - Consider internal-only routes (no external access)
> - Consult your OpenShift administrator for security best practices
>
> See [OpenShift Route Security Documentation](https://docs.openshift.com/container-platform/latest/networking/routes/route-configuration.html) for securing routes.
>
> **For development/testing only:** Use [port-forwarding](../port-forward/) instead of routes for local-only access.

---

## Deploying the Application

Create a YAML file with your SparkApplication configuration (see examples above), then apply it:

```bash
oc apply -f spark-application.yaml
```

Wait for the driver pod to start:

```bash
oc get pods -n <namespace> -l spark-role=driver -w
```

Once the driver pod is `Running`, the route will be accessible.

## Accessing the Spark UI

Get the route URL:

```bash
ROUTE_URL=$(oc get route -n <namespace> -l sparkoperator.k8s.io/app-name=<app-name> -o jsonpath='{.items[0].spec.host}')
echo "https://$ROUTE_URL"
```

Access via browser:
```
https://<route-url>
```

Or test with curl:
```bash
curl -k https://$ROUTE_URL
```

## How It Works

1. **Operator creates Service** — Exposes driver pod port 4040
   - Service name: `<app-name>-driver-4040`
   - Port name: `spark-driver-ui-port`

2. **Operator creates Ingress** — With OpenShift route annotations
   - Ingress name: `<app-name>-ing-4040`
   - Annotations include `route.openshift.io/termination: edge`

3. **OpenShift creates Route** — Automatically from Ingress
   - Route hostname matches `ingressURLFormat` template
   - TLS edge termination enabled
   - Routes HTTPS traffic to Service → Driver Pod

## Configuration Reference

### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| `servicePort` | Spark UI port (usually 4040) | `4040` |
| `servicePortName` | Port name for the Service | `"spark-driver-ui-port"` |
| `ingressURLFormat` | Route hostname template | `"{{$appName}}-{{$appNamespace}}.apps.example.com"` |

### Template Variables

The `ingressURLFormat` supports these variables:

- `{{$appName}}` — SparkApplication name
- `{{$appNamespace}}` — Namespace
- `{{$appId}}` — Spark application ID

## When to Use Routes vs Port-Forwarding

| Method | Use Case | Accessibility | Persistence |
|--------|----------|---------------|-------------|
| Port-forward | Local dev/testing | localhost only | Session-based |
| Route (this guide) | Production, shared access | Cluster-wide HTTPS URL | Persistent |

**Use Routes when:**
- Multiple users need access to the Spark UI
- You need HTTPS/TLS security
- The application runs for extended periods
- You want bookmarkable, shareable URLs

**Use Port-forward when:**
- Quick local debugging
- Temporary one-time access
- No external access requirements

See [spark-ui-port-forwarding.md](spark-ui-port-forwarding.md) for port-forward details.

## Troubleshooting

### Route shows "Application is not available"

**Cause:** Driver pod completed or not yet ready.

**Solution:**
```bash
# Check if driver is running
oc get pods -n <namespace> -l spark-role=driver

# If completed, the UI is no longer available
# For testing, use a long-running job (see example above)
```

### Route hostname already claimed

**Cause:** Another route is using the same hostname.

**Solution:** Use a unique hostname in `ingressURLFormat`:
```yaml
ingressURLFormat: "spark-<unique-name>-{{$appNamespace}}.apps.example.com"
```

### Ingress created but Route missing

**Cause:** Missing OpenShift route annotations.

**Solution:** Ensure `ingressAnnotations` includes:
```yaml
ingressAnnotations:
  route.openshift.io/termination: "edge"
```

## Security Considerations

### TLS Termination

Routes use `edge` termination where TLS terminates at the OpenShift router:

```yaml
route.openshift.io/termination: "edge"
```

This is required because Spark UI runs on HTTP (port 4040) without built-in TLS support.

### Spark UI Exposure

Routes are publicly accessible by default. The Spark UI exposes:
- Job configuration and environment variables
- Application logs and error messages
- Executor details and resource usage

For sensitive workloads, consider using [port-forwarding](spark-ui-port-forwarding.md) instead of routes.

## Additional Resources

- [spark-ui-port-forwarding.md](spark-ui-port-forwarding.md) — Port-forward method
- [OpenShift Routes Documentation](https://docs.openshift.com/container-platform/latest/networking/routes/route-configuration.html)