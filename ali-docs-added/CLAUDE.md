# Spark REST Submitter — Project Guide

## Goal

Eliminate **all build-time dependency on Spark/PySpark** from a company's Kubeflow Spark
Operator product, so that the company's build pipelines ship **only non-Spark code** (Go
operator + Helm/CRDs), while Spark is provided **entirely at runtime by the user-supplied
Spark container image** — exactly the way driver and executor pods already get their Spark.

Concretely, the endgame is:

- **Operator**: pure Go (Kubeflow Spark Operator in REST-submitter mode). No Spark, no JVM,
  no PySpark in its image. It only makes HTTP calls. to a custom resource for a Spark-native REST submission server for Kubernetes cluster. 
- **Company build footprint**: Go operator + Helm chart / CRD only. Nothing Spark-linked is
  built or vendored.
- **Spark (and its CVEs)**: 100% the customer's image, patched on the customer's schedule —
  same trust model already used for driver/executor images.

The core motivation is **supply-chain / CVE reduction**: today the Kubeflow Spark Operator
image bundles Spark/PySpark (to shell out to `spark-submit`), which drags in a large CVE
surface. Moving submission out of the operator — and making the submitter part of Spark that
runs in the user's image — removes that surface from everything the company builds.

## Repositories in this workspace

| Directory | What it is | Role in this effort |
|-----------|-----------|---------------------|
| `spark/` | Apache Spark source (checkout is `master`, `5.0.0-SNAPSHOT`) | Where the upstream changes go (the two tracks below) |
| `spark-operator/` | Kubeflow Spark Operator | The Go operator; recently added an alpha REST-submitter option that POSTs `spark-submit` args to a submitter service |
| `k8s-spark-submitter/` | Existing standalone REST submitter service (Scala/Java 17, targets Spark 4.0.1) | Proof-of-concept / reference implementation of the submitter; the thing the operator's alpha REST-submitter talks to |

</content>
</invoke>
