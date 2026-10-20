# Spark Observability on OpenShift - Overview

This document provides a high-level overview of Spark job observability capabilities for OpenShift AI.

## Feature Summary

**Purpose:** Enable data engineers and platform operators to monitor and debug Spark jobs running via the Kubeflow Spark Operator on OpenShift AI.

**Two capabilities:**

1. **Live Monitoring** - Access Spark Application UI while jobs are running
2. **Post-Mortem Analysis** - Use Spark History Server to investigate completed/failed jobs

---

## Live Monitoring - Spark Application UI

Access the built-in Spark UI for running jobs to view:
- Real-time stage progress and task execution
- Executor metrics (CPU, memory, shuffle)
- SQL query plans (for Spark SQL workloads)
- Event timeline and DAG visualization

### Access Methods

| Method | Use Case | Documentation |
|--------|----------|---------------|
| **OpenShift Route** | Production - shareable HTTPS URLs | [Route Access Guide](../spark-ui/route/) |
| **Port-Forward** | Development/testing - quick local access | [Port-Forward Guide](../spark-ui/port-forward/) |

**Recommendation:** Route-based access for production, port-forward for development.

> **NetworkPolicy note (RHOAI/ODH).** When operand NetworkPolicies are enabled
> (the `OperandNetworkPolicy` feature gate), driver pods are default-deny for
> ingress. **Route** access and **cross-namespace Prometheus scraping** of driver
> metrics (served on the UI port) then require a NetworkPolicy admitting the
> router / monitoring tier; **port-forward** needs none. See
> [network-policy-pod-inventory.md](./network-policy-pod-inventory.md).

---

## Post-Mortem Analysis - Spark History Server

Deploy a persistent History Server to view completed job details after driver pods terminate.

### Storage Backend Options

History Server requires persistent storage for event logs. Choose based on your infrastructure:

#### S3-Compatible Object Storage

**What it is:** Any S3-compatible object storage endpoint (AWS S3, OpenShift Data Foundation, cloud providers, enterprise storage vendors)

**Advantages:**
- ✅ Concurrent access from multiple namespaces and applications
- ✅ Unlimited storage scalability
- ✅ Works with any S3-compatible provider
- ✅ Industry-standard protocol

**Considerations:**
- Different S3 providers have different performance, cost, and availability characteristics
- Requires network connectivity to S3 endpoint (use providers that support your connectivity requirements)
- Credentials management required

**Documentation:** [S3 Setup Guide](../spark-history-server/s3/)

**Examples of S3-compatible storage:**
- AWS S3 (cloud)
- OpenShift Data Foundation / Ceph RGW (on-cluster)
- Enterprise storage vendors (NetApp, Pure Storage, Dell EMC)
- Cloud provider object storage (Google Cloud Storage, Azure Blob with S3 compatibility)

---

#### ReadWriteMany (RWX) PVC

**What it is:** Kubernetes Persistent Volume Claim with ReadWriteMany access mode

**Advantages:**
- ✅ Simple setup - no credentials or endpoints to configure
- ✅ Works with any RWX-capable storage provider
- ✅ Native Kubernetes resource

**Considerations:**
- Requires storage provider that supports ReadWriteMany access mode
- Not all Kubernetes storage classes support RWX (check your storage provider)
- Typically namespace-scoped (see Multi-Namespace Considerations below)

**Documentation:** [PVC Setup Guide](../spark-history-server/pvc/)

**Examples of RWX-capable storage:**
- OpenShift Data Foundation / CephFS
- NFS
- Enterprise storage vendors (NetApp, Portworx, IBM Storage Fusion)
- Cloud file storage (AWS EFS, Azure Files, Google Filestore)

---

### Multi-Namespace Considerations

**S3 approach:** Multiple namespaces can write to the same S3 bucket. A single History Server can read logs from applications across all namespaces.

**PVC approach:** PVCs are namespace-scoped. For multi-namespace deployments, you'll need either:
- Separate History Server per namespace, or
- Namespace-shared storage configuration (consult your storage provider documentation)

---

### Disconnected / Air-Gapped Environments

Both the Spark Operator and History Server work in disconnected/air-gapped environments:
- **Neither component requires internet connectivity** by default
- S3 approach: Use on-cluster storage (e.g., OpenShift Data Foundation) or enterprise appliances
- PVC approach: Works with any local RWX storage provider

The only external dependency is container image registry access (can be mirrored to disconnected registry).

---

## Documentation Inventory

### Live Monitoring
- ✅ **[Route Access](../spark-ui/route/)** - Production HTTPS access
- ✅ **[Port-Forward Access](../spark-ui/port-forward/)** - Development/testing

### Post-Mortem (History Server)
- ✅ **[S3 Setup](../spark-history-server/s3/)** - S3-compatible object storage
- ✅ **[PVC Setup](../spark-history-server/pvc/)** - ReadWriteMany PVC storage

---

## Validated Configurations

The following configurations have been tested hands-on on ROSA:

**Live Monitoring:**
- ✅ Route Access
- ✅ Port-Forward Access

**History Server Storage:**
- ✅ S3-compatible storage (AWS S3)
- ✅ PVC storage (RWO gp3-csi; pattern applies to any RWX provider)

**Security:**
- ✅ Route public accessibility verified and documented

---

## References

- **RFE:** RHAIRFE-1478
- **Kubeflow Spark Operator Docs:** https://www.kubeflow.org/docs/components/spark-operator/
