# Kueue + Spark Operator Integration: User Guide

> **Maturity: Alpha** — The Kueue SparkApplication integration is alpha-quality
> (upstream Kueue v0.17+). APIs, behavior, and configuration may change in
> future releases. Review the [known limitations](#known-limitations) before
> submitting workloads.

This guide is for **data engineers and Spark users** who submit
SparkApplications on an OpenShift cluster where Kueue quota management is
enabled. It covers how to submit jobs through Kueue, understand the admission
lifecycle, configure priorities, and design jobs that are resilient to
suspend/resume.

Your cluster administrator should have already completed the setup described
in the [Administrator Guide](kueue-spark-admin-guide.md).

## Contents

- [Overview](#overview)
- [Submitting a SparkApplication with Kueue](#submitting-a-sparkapplication-with-kueue)
- [Understanding the Admission Lifecycle](#understanding-the-admission-lifecycle)
- [Monitoring Your Job](#monitoring-your-job)
- [Configuring Job Priorities](#configuring-job-priorities)
- [Quota and Queueing Behavior](#quota-and-queueing-behavior)
- [Designing for Suspend/Resume](#designing-for-suspendresume)
- [Known Limitations](#known-limitations)
- [Troubleshooting](#troubleshooting)

---

## Overview

When Kueue is enabled on your cluster, it acts as a gatekeeper for
SparkApplication workloads. Instead of running immediately, your Spark job
is placed in a queue and admitted only when sufficient quota is available.
This ensures fair resource sharing across teams and prevents any single user
from monopolizing the cluster.

From your perspective, the only change is adding a single label to your
SparkApplication. Everything else — driver creation, executor scheduling,
job monitoring — works the same as before.

## Submitting a SparkApplication with Kueue

### The Queue Label

To have Kueue manage your SparkApplication, add the
`kueue.x-k8s.io/queue-name` label with the name of the LocalQueue your
administrator has created:

```yaml
metadata:
  labels:
    kueue.x-k8s.io/queue-name: spark-lq
```

### Complete Example

Here is a complete SparkApplication that runs Spark Pi with Kueue admission:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi-kueue
  namespace: spark-test
  labels:
    kueue.x-k8s.io/queue-name: spark-lq
spec:
  type: Scala
  mode: cluster
  image: ghcr.io/apache/spark-docker/spark:3.5.4
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.5.4.jar
  arguments:
  - "1000"
  sparkVersion: "3.5.4"
  restartPolicy:
    type: Never
  volumes:
  - name: spark-work-dir
    emptyDir: {}
  driver:
    cores: 1
    memory: 512m
    serviceAccount: spark-operator-spark
    securityContext: {}
    volumeMounts:
    - name: spark-work-dir
      mountPath: /opt/spark/work-dir
  executor:
    instances: 1
    cores: 1
    memory: 512m
    securityContext: {}
    volumeMounts:
    - name: spark-work-dir
      mountPath: /opt/spark/work-dir
```

Submit it:

```bash
oc apply -f examples/openshift/kueue/spark-pi-kueue.yaml
```

### Without Kueue

If you omit the `kueue.x-k8s.io/queue-name` label, the SparkApplication
bypasses Kueue entirely and is submitted directly by the Spark Operator with
no quota enforcement. This is the standard behavior for non-queued workloads.

## Understanding the Admission Lifecycle

When you submit a Kueue-managed SparkApplication, it goes through these states:

```
Submit ──► Suspended ──► Resuming ──► New ──► Submitted ──► Running ──► Completed
              │                                                            │
              │ (Kueue admits                                              │
              │  when quota                                       Quota reclaimed
              │  available)                                       by Kueue
              │
         If quota full:
         waits in queue
```

### Step-by-Step

| Step | What Happens | Visible State |
|------|-------------|---------------|
| 1 | You submit the SparkApplication | `spec.suspend` is set to `true` by Kueue's webhook |
| 2 | Kueue creates a Workload object | SparkApplication shows state: `Suspended` |
| 3 | Kueue checks quota availability | Workload condition: `QuotaReserved` or `Pending` |
| 4 | Quota available — Kueue admits the Workload | Kueue sets `spec.suspend=false` |
| 5 | Spark Operator picks up the unsuspended app | State transitions: `Resuming` → `New` → `Submitted` |
| 6 | Driver pod is created, then executor pods | State: `Running` |
| 7 | Job completes | State: `Completed`, quota is reclaimed |

If quota is **not** available at step 3, your job remains in `Suspended` state
until other jobs complete and free up resources.

## Monitoring Your Job

### Watch SparkApplication Status

```bash
oc get sparkapplication <name> -n <namespace> -w
```

### Check Kueue Workload Objects

Each Kueue-managed SparkApplication has a corresponding Workload. Inspect it
to understand the admission status:

```bash
# List all workloads
oc get workloads -n <namespace>

# Detailed view with conditions
oc describe workload <workload-name> -n <namespace>
```

Key Workload conditions:

| Condition | Meaning |
|-----------|---------|
| `QuotaReserved=True` | Quota has been reserved for this workload |
| `Admitted=True` | The workload has been admitted and is running |
| `Admitted=False` | The workload is waiting for quota |
| `Finished=True` | The workload has completed |

### Check Events

Kueue emits Kubernetes events for lifecycle transitions:

```bash
oc get events -n <namespace> --sort-by='.lastTimestamp' | tail -20
```

Look for events with reasons like `Suspended`, `Started`, `QuotaReserved`,
`Admitted`, `Evicted`, `Preempted`, and `Completed`.

### Check Pods

```bash
oc get pods -n <namespace> | grep <app-name>
```

## Configuring Job Priorities

### Setting Priority on a SparkApplication

If your administrator has configured `WorkloadPriorityClass` resources, you
can assign a priority to your SparkApplication using a label:

```yaml
metadata:
  labels:
    kueue.x-k8s.io/queue-name: spark-lq
    kueue.x-k8s.io/priority-class: high-priority
```

### How Priority Affects Admission

When multiple jobs are waiting in the queue:

1. **Higher-priority jobs are admitted first** — a job with priority value 1000
   is admitted before one with value 100 when quota becomes available
2. **Preemption** — if a higher-priority job is submitted and quota is
   exhausted, Kueue may suspend (preempt) a running lower-priority job to
   make room

### What Happens When Your Job Is Preempted

If your running job is preempted by a higher-priority workload:

1. Kueue sets `spec.suspend=true` on your SparkApplication
2. The Spark Operator **deletes all driver and executor pods**
3. Your job enters `Suspended` state and returns to the queue
4. When quota becomes available again, Kueue re-admits your job
5. Your job **restarts from scratch** — a new driver pod is created with a
   new submission ID

> **Important:** Preemption is **not** a graceful pause. All in-memory data
> and intermediate results are lost. See
> [Designing for Suspend/Resume](#designing-for-suspendresume).

### Checking Your Priority

To see which `WorkloadPriorityClass` resources are available:

```bash
oc get workloadpriorityclasses
```

Ask your administrator which priority class is appropriate for your workload.

## Quota and Queueing Behavior

### When Quota Is Exhausted

If the queue's quota is fully consumed by other running jobs, your
SparkApplication remains in `Suspended` state. It is not rejected — it simply
waits. You can see pending workloads with:

```bash
oc get workloads -n <namespace>
```

### How Quota Is Reclaimed

When a running SparkApplication completes (or fails), Kueue reclaims its quota.
The next pending workload in the queue is then admitted automatically. No
action is needed on your part.

### Fair Sharing

If your administrator has configured multi-tenant Cohorts, Kueue distributes
resources fairly across teams. This means:

- Your team can temporarily borrow unused quota from other teams
- If another team needs their quota back, your borrowing workload may be
  preempted (suspended)
- Over time, each team receives their fair share of resources

## Designing for Suspend/Resume

### Why This Matters

When Kueue suspends your SparkApplication — whether due to preemption, quota
reclamation, or fair sharing — **all pods are deleted and all in-memory state
is lost**. This is a fundamental aspect of how Kueue integrates with the Spark
Operator.

When your job is re-admitted, it starts completely from scratch with a new
driver pod and new executor pods.

### Design Principles

1. **Make jobs idempotent** — your job should produce the same correct result
   whether it runs once or is restarted multiple times

2. **Use persistent storage for outputs** — write results to S3, HDFS, or a
   PersistentVolumeClaim, not to local executor storage

3. **Use overwrite mode** — when writing outputs, use `mode("overwrite")`
   rather than `mode("append")` to avoid duplicates on restart:

   ```python
   df.write.mode("overwrite").parquet("s3a://bucket/output/")
   ```

4. **Enable checkpointing for long jobs** — for jobs that process large
   datasets, use Spark checkpointing to save intermediate state:

   ```python
   spark.sparkContext.setCheckpointDir("s3a://bucket/checkpoints/")
   df.checkpoint()
   ```

5. **Break large jobs into stages** — instead of one monolithic job, submit
   multiple smaller SparkApplications that each write their output to
   persistent storage. If one stage is preempted, only that stage needs to
   restart.

### Example: Resilient Output Pattern

```python
output_path = "s3a://my-bucket/results/daily-report/"

df_result = (
    spark.read.parquet("s3a://my-bucket/input/")
    .filter(col("date") == "2025-01-15")
    .groupBy("category")
    .agg(sum("revenue").alias("total_revenue"))
)

df_result.write.mode("overwrite").parquet(output_path)
```

This job can be safely restarted at any point: it reads from a stable input,
performs a deterministic transformation, and overwrites the output location.

## Known Limitations

### Dynamic Allocation Not Supported

Do **not** set `dynamicAllocation.enabled=true` on SparkApplications submitted
through Kueue. Kueue needs to know the exact resource footprint at admission
time. Dynamic allocation changes the executor count at runtime, which is
incompatible with Kueue's quota model.

```yaml
# DO NOT USE with Kueue
spec:
  dynamicAllocation:
    enabled: true       # Not supported
    minExecutors: 1
    maxExecutors: 10
```

Instead, set a fixed executor count:

```yaml
spec:
  executor:
    instances: 2        # Fixed count — compatible with Kueue
```

### Suspend/Resume Deletes All Pods

When your job is suspended (preempted), all driver and executor pods are
deleted. There is no graceful shutdown — the process is equivalent to
`kubectl delete pod`. Any data in memory or local ephemeral storage is lost.

### No Partial Suspension

Kueue cannot suspend individual executors. It's all-or-nothing: either the
entire SparkApplication is running, or it is fully suspended with zero pods.

### Alpha Maturity

This integration is alpha-quality. Behavior may change in future releases.
Test thoroughly in non-production environments before relying on it for
critical workloads.

## Troubleshooting

### My Job Is Stuck in `Suspended`

**Check 1: Is there a Workload object?**

```bash
oc get workloads -n <namespace> | grep <app-name>
```

If no Workload exists, the Kueue controller may lack RBAC permissions. Ask
your administrator to check the Kueue controller logs.

**Check 2: Is quota available?**

```bash
oc get clusterqueue -o wide
```

Look at the `PENDING WORKLOADS` and resource utilization columns. If the queue
is full, your job is waiting for other jobs to complete.

**Check 3: Is the Workload admitted?**

```bash
oc describe workload <workload-name> -n <namespace>
```

Look at the `Conditions` section. If `Admitted=False`, check the `message`
field for the reason (e.g., insufficient quota, namespace selector mismatch).

### My Job Was Running but Disappeared

Your job was likely preempted by a higher-priority workload. Check:

```bash
# Look for Evicted/Preempted events
oc get events -n <namespace> --sort-by='.lastTimestamp' | grep -i "evict\|preempt\|suspend"

# Check the SparkApplication status
oc get sparkapplication <name> -n <namespace> -o yaml | grep -A5 appState
```

If the job was preempted, it will return to `Suspended` state and be
re-admitted when quota is available.

### My Job Completed but Shows Wrong Results After Restart

If your job was preempted and restarted, it may have produced partial or
duplicate results. Ensure your job follows the
[design principles](#design-principles):

- Use `mode("overwrite")` for output writes
- Make transformations idempotent
- Don't rely on `mode("append")` for jobs that may restart

### Event Visibility

To understand what happened to your job, inspect events and Workload status:

```bash
# All events for your app
oc get events -n <namespace> --field-selector involvedObject.name=<app-name>

# Workload conditions (detailed admission history)
oc get workload <workload-name> -n <namespace> -o jsonpath='{.status.conditions}' | python3 -m json.tool

# Kueue-specific events
oc get events -n <namespace> --sort-by='.lastTimestamp' | grep -iE "admit|suspend|quota|preempt|evict"
```

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| SparkApplication runs without being suspended | Missing `kueue.x-k8s.io/queue-name` label | Add the label to `metadata.labels` |
| SparkApplication runs without being suspended | Namespace not labeled | Ask admin to run `oc label namespace <ns> kueue.openshift.io/managed=true` |
| `Workload not found` in events | Kueue RBAC missing | Ask admin to apply SparkApplication finalizer RBAC |
| Job suspended indefinitely | Quota exhausted | Wait for other jobs to complete, or ask admin to increase quota |
| `AccessDeniedException: /opt/spark/work-dir` | Missing writable volume | Add an `emptyDir` volume mounted at `/opt/spark/work-dir` |
| Dynamic allocation errors | `dynamicAllocation.enabled=true` | Remove dynamic allocation config; use fixed `executor.instances` |

### Getting Help

If the troubleshooting steps above don't resolve your issue:

1. Share the output of `oc describe sparkapplication <name> -n <namespace>`
2. Share the output of `oc describe workload <workload-name> -n <namespace>`
3. Share relevant events: `oc get events -n <namespace> --sort-by='.lastTimestamp' | tail -30`
4. Contact your cluster administrator with these details
