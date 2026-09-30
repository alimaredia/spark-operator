# Driver-submission resource coverage

What Kubernetes objects a Spark **cluster-mode** driver submission can create, and
which of them the pure-Go `sparkdrivercreator` reproduces. Scoped to Spark 4.0.4's
`KubernetesDriverBuilder` feature steps plus the objects `KubernetesClientApplication.run`
owns. See [`../../test/oracle`](../../test/oracle) for the differential test that
pins the reproduced ones, and the workspace-root `approaches-comparison.md` for the
deny-list rationale.

The governing principle: every object the native creator does **not** reproduce is
one whose creation depends on **submit-host local state** (local credential/keytab
files, an ambient Kerberos TGT, live token minting, a host `$HADOOP_CONF_DIR`) —
state a pure-Go operator controller pod structurally lacks. Those same modes also
have **no typed CRD field and no `--conf` the operator will emit**, so they are
unreachable through the operator's CRD/webhook surface regardless.

## Table A — resources a driver submission could conceivably create

| # | Resource (API object) | Spark feature step / origin | Reachable via operator? | Native creator |
|---|------------------------|-----------------------------|-------------------------|----------------|
| 1 | Driver **Pod** | `BasicDriverFeatureStep` (all steps mutate it) | ✅ always | ✅ ported |
| 2 | Driver **ConfigMap** (`spark.properties`) | `KubernetesClientApplication.run` | ✅ always | ✅ ported |
| 3 | Driver **Service** | `DriverServiceFeatureStep` | ✅ always | ✅ ported |
| 4 | Executor pod-template **ConfigMap** (`<prefix>-driver-podspec-conf-map`) | `PodTemplateConfigMapStep` | ✅ always (operator synthesizes an executor template) | ✅ ported |
| 5 | On-demand **PersistentVolumeClaim(s)** (`<prefix>-driver-pvc-<i>`) | `MountVolumesFeatureStep` | ✅ via `spec.sparkConf` passthrough (typed volume source carries only claimName+readOnly) | ✅ ported |
| 6 | Driver **credential Secret** (oauth token / client cert / key) | `DriverKubernetesCredentialsFeatureStep` | 🚫 webhook denies all `authenticate.*oauthToken*File*` confs | 🚫 deny-listed |
| 7 | Kerberos **delegation-token Secret** | `KerberosConfDriverFeatureStep` | 🚫 no CRD field; needs host keytab / krb5.conf / ambient TGT | 🚫 deny-listed |
| 8 | Kerberos **krb5.conf ConfigMap** | `KerberosConfDriverFeatureStep` | 🚫 same as above | 🚫 deny-listed |
| 9 | Hadoop config **ConfigMap** (built from local `$HADOOP_CONF_DIR`) | `HadoopConfDriverFeatureStep` (local-directory mode) | 🚫 no conf enables it; needs host files | 🚫 deny-listed |

### Pod mutations that reference (but do not create) objects

These are not new resources — they mount pre-existing objects, so they appear only
as driver-pod mutations, all reproduced by the native creator:

- **Referenced Secrets** — `spec.driver.secrets` / `spec.driver.envSecretKeyRefs`
  (`MountSecretsFeatureStep` / `EnvSecretsFeatureStep`); Secret must pre-exist.
- **Typed credential env** — GCPServiceAccount / HadoopDelegationToken secret types
  (`driverSecretOption`) add `GOOGLE_APPLICATION_CREDENTIALS` / `HADOOP_TOKEN_FILE_LOCATION`.
- **Referenced Hadoop ConfigMap** — `spark.kubernetes.hadoop.configMapName`
  (`HadoopConfDriverFeatureStep` pre-existing-ConfigMap mode); ConfigMap must pre-exist.
- **Non-PVC driver volumes** — emptyDir / hostPath / nfs (`MountVolumesFeatureStep`).

## Table B — resources unreachable through the operator's CRD/webhook surface

The subset of Table A the native creator omits, with the concrete reason each is
unreachable. None is a coverage gap: there is no supported input that produces it.

| # | Resource | Why it is unreachable | Blocking mechanism |
|---|----------|------------------------|--------------------|
| 6 | Driver credential Secret | User cannot set the token/cert/key-file confs that trigger secret minting | `internal/webhook/sparkconf_validator.go` hard-denies every `spark.kubernetes.authenticate[.driver\|.executor].oauthToken[File]` conf ("authentication credentials are managed by the operator") — only the `serviceAccountName` sub-path survives |
| 7 | Kerberos delegation-token Secret | No typed CRD field exists; even via `spec.sparkConf`, minting reads a local keytab / krb5.conf / ambient TGT the controller pod does not have | No `keytab`/`principal`/`krb5` field in `api/v1beta2/sparkapplication_types.go`; no submit-host local state |
| 8 | Kerberos krb5.conf ConfigMap | Same as #7 — built alongside the delegation-token Secret | Same as #7 |
| 9 | Hadoop config ConfigMap (local-dir mode) | `spec.hadoopConfigMap` is mounted directly by the mutating webhook (not translated to `spark.kubernetes.hadoop.configMapName`); `spec.hadoopConf` maps to `--conf spark.hadoop.*`; neither drives the local-dir mode, which also needs host files | `submission.go` never emits `configMapName`; `addHadoopConfigMap` in `internal/webhook/sparkpod_defaulter.go` mounts the referenced ConfigMap itself |

For the referenced (reachable) counterpart of #9 —
`spark.kubernetes.hadoop.configMapName` via `spec.sparkConf` passthrough — see the
`sparkpi-hadoopconf` oracle case; that pre-existing-ConfigMap mode **is** ported.
