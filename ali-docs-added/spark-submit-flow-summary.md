# How `spark-submit` Works (Kubernetes cluster mode)

A code-grounded walkthrough of the `spark-submit` executable in Apache Spark, from the moment
it receives command-line arguments (e.g. from the Kubeflow Spark Operator) until it launches a
Spark driver on Kubernetes. Companion to `k8s-spark-submitter-summary.md`; the final section maps
this flow onto what the REST submitter reuses.

Line references point at the source under `spark/`.

## Context

- **Repo:** `spark/` — checkout is `master`.
- **SPARK_VERSION:** `5.0.0-SNAPSHOT` (`pom.xml:29`; runtime value via `SparkBuildInfo.spark_version`,
  `core/.../org/apache/spark/package.scala:46`).
- **Relevant modules:**
  - `launcher/` — pure-Java launcher library (`org.apache.spark.launcher`).
  - `core/` — `org.apache.spark.deploy.SparkSubmit` and friends.
  - `resource-managers/kubernetes/core/` — the K8s submission client
    (`org.apache.spark.deploy.k8s.*`), a **separate Maven module** whose jar must be on the
    classpath for K8s support.

### The shape of the thing (read this first)

`spark-submit` runs across **two JVMs**: a throwaway launcher JVM that *builds* the real `java`
command, and the actual `SparkSubmit` JVM that runs it. And for Kubernetes **cluster** mode, the
"job launch" is not the user's application at all — it's an in-process handoff to
`KubernetesClientApplication`, which talks to the Kubernetes API to create a **driver pod**. The
user's real driver then runs remotely inside that pod; this local process exits after submission
(unless configured to wait).

## The flow, step by step

### 1. Shell entry point — two nested scripts
- `bin/spark-submit:27` — `exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"`.
  A thin wrapper that resolves `SPARK_HOME`, sets `PYTHONHASHSEED=0`, and prepends the target class.
- `bin/spark-class` assembles the JVM command:
  - Sources env, locates `java`, computes `LAUNCH_CLASSPATH` from `$SPARK_HOME/jars/*`
    (`bin/spark-class:26-56`).
  - Runs a **bootstrap 128MB JVM** whose only job is to print the real command
    (`bin/spark-class:72-74`): `"$RUNNER" -Xmx128m ... -cp "$LAUNCH_CLASSPATH" org.apache.spark.launcher.Main "$@"`.
  - Reads the launcher's NUL-separated tokens into a bash `CMD` array (`:82-99`), checks the
    appended exit code (`:100-117`), then `exec "${CMD[@]}"` (`:124`).

Net effect: the shell asks `launcher.Main` to generate the exact
`java ... org.apache.spark.deploy.SparkSubmit ...` command, then execs it.

### 2. Launcher layer — builds the java command (`launcher/`)
- `launcher/.../Main.java:51` — `main()` pops the first arg as the class name (`:55`). Since it's
  `SparkSubmit` (`:60`), it builds a `SparkSubmitCommandBuilder(args)` (`:62`), calls
  `buildCommand()` (`:63`), and prints the tokens NUL-separated for bash (`:94-104`). On bad args
  it falls back to `MainClassOptionParser` to still emit a usable help command (`:64-84`).
- `SparkSubmitCommandBuilder`:
  - Constructor parses submit args via an inner `OptionParser`
    (`SparkSubmitCommandBuilder.java:123-159`, parser `:545-639`) into fields (`--master`,
    `--deploy-mode`, `--class`, `--conf`, …).
  - `buildCommand()` (`:172-192`) → `buildSparkSubmitCommand()` (`:312-368`), which:
    - Notes `isClientMode()` is **false** for `--deploy-mode cluster` (`:483-488`), so it does
      **not** add `-Xmx`/driver JVM opts to this process — the driver runs remotely in the pod.
    - Builds `java -cp <classpath>` (`AbstractCommandBuilder.java:102-129`), adds
      `SPARK_SUBMIT_OPTS` + default module-opens (`:332`, `:363`), appends the literal
      `org.apache.spark.deploy.SparkSubmit` (`:365`), then the normalized submit args
      (`:366`, built `:194-310`).

Output: the concrete `java ... SparkSubmit --master k8s://... --deploy-mode cluster --class ...
<appjar> <appargs>` command that `spark-class` execs.

### 3. `SparkSubmit` — parse & dispatch (`core/.../deploy/SparkSubmit.scala`)
- `main` → `new SparkSubmit().doSubmit(args)` (`:1097`). The class is `private[spark]` (`:63`).
- `doSubmit` (`:70-100`): `parseArguments` → `new SparkSubmitArguments(...)` (`:102-104`), set up
  logging, then dispatch on `appArgs.action` (`:94-99`):
  - `SUBMIT` → `submit` (`:95`)
  - `KILL` → `kill` (`:96`, impl `:109-124`)
  - `REQUEST_STATUS` → `requestStatus` (`:97`, impl `:129-139`)
  - `PRINT_VERSION` → `printVersion`
- Action enum `SparkSubmitAction` (`:52-55`). For K8s, `kill`/`requestStatus` route to
  `K8SSparkSubmitOperation` via `SparkSubmitUtils.getSubmitOperations(args.master)` (ServiceLoader;
  `:120-122`, `:135-137`).

### 4. `prepareSubmitEnvironment` — the core decision method (`:243`)
`private[deploy] def prepareSubmitEnvironment(args, conf=None): (Seq[String], Seq[String], SparkConf, String)`.
Returns `(childArgs, childClasspath, sparkConf, childMainClass)` (`:926`).
- Derives cluster manager from the master string (`:254-267`): `k8s://...` → `KUBERNETES`
  (`:260`, via `SparkMasterRegex.isK8s`, `SparkContext.scala:3834-3841`; constant `KUBERNETES = 16`
  at `SparkSubmit.scala:1109`). Validates the K8s classes are present by loading
  `KUBERNETES_CLUSTER_SUBMIT_CLASS` (`:290-294`).
- Deploy mode `"cluster"` → `CLUSTER` (`:272`); `isKubernetesCluster = KUBERNETES && CLUSTER`
  (`:329`).
- **The crux — child main class for K8s cluster** (`:838-864`):
  ```
  childMainClass = KUBERNETES_CLUSTER_SUBMIT_CLASS
                 = "org.apache.spark.deploy.k8s.submit.KubernetesClientApplication"  (:839, :1130)
  ```
  The user's app becomes an **argument** in a flag protocol (`:840-863`):
  - JVM: `--primary-java-resource <res> --main-class <userMainClass>` (`:848-851`)
  - Python: `--primary-py-file ... --main-class org.apache.spark.deploy.PythonRunner` (`:841-843`)
  - R: `--primary-r-file ... --main-class org.apache.spark.deploy.RRunner` (`:844-846`)
  - each user arg → `--arg <arg>` (`:855-859`); `--proxy-user` appended if set (`:861-863`).
- (Contrast: YARN cluster → `YARN_CLUSTER_SUBMIT_CLASS` `:819`; standalone → REST/legacy
  `:790-794`; client mode → the user's own `args.mainClass` `:727`.)
- Finishes by folding `--conf`/properties into `sparkConf`, resolving path configs, stamping
  `spark.app.submitTime` (`:910-915`), and returning the tuple (`:926`).

### 5. `runMain` — reflectively launch the child (`:964-1046`)
- Calls `prepareSubmitEnvironment(args)` and destructures the tuple (`:965`).
- Builds a classloader, adds each `childClasspath` jar (`:984-987`).
- `mainClass = Utils.classForName(childMainClass)` (`:992`) — loads `KubernetesClientApplication`.
- Wraps it (`:1014-1018`): because `KubernetesClientApplication` implements the `SparkApplication`
  trait (`SparkApplication.scala:27-31`), it's instantiated via its no-arg constructor; otherwise a
  static-`main` class would be wrapped in `JavaMainApplication` (`:39-55`).
- `app.start(childArgs.toArray, sparkConf)` (`:1033`) — **in-process, same JVM**.

### 6. The Kubernetes submission client
File: `resource-managers/kubernetes/core/.../submit/KubernetesClientApplication.scala`
- `KubernetesClientApplication` (`:221`, `private[spark]`): `start()` (`:223-226`) parses argv with
  `ClientArguments.fromCommandLineArgs` (`:224`, parser `:51-85`) into
  `ClientArguments(mainAppResource, mainClass, driverArgs, proxyUser)` (`:45-49`), then `run()`.
- `run()` (`:228-260`):
  - App id `spark-<uuid>` via `KubernetesConf.getKubernetesAppId()` (`:233`).
  - `KubernetesConf.createDriverConf(...)` → `KubernetesDriverConf` (`:234-240`).
  - Strips `k8s://` (`:243`), creates the fabric8 client via
    `SparkKubernetesClientFactory.createKubernetesClient(..., ClientType.Submission)` (`:246-252`),
    wrapped in `Utils.tryWithResource`.
  - `new Client(kubernetesConf, new KubernetesDriverBuilder(), kubernetesClient, watcher).run()`
    (`:253-258`).
- `Client.run()` (`:104-211`, `Client` is `private[spark]` `:98`):
  1. `builder.buildFromFeatures(conf, kubernetesClient)` → `KubernetesDriverSpec` (`:105`).
  2. Build the driver **ConfigMap** (`spark.properties`): name via
     `KubernetesClientUtils.configMapNameDriver` (`:106`), files via `buildSparkConfDirFilesMap`
     (`:107-108`), then `buildConfigMap` incl. namespace entry (`:109-110`).
  3. Add `SPARK_CONF_DIR` env + a ConfigMap-backed volume mount to the driver (`:114-135`).
  4. **Create resources via fabric8**, ordered, with rollback:
     - pre-pod resources server-side-applied (`:139-148`);
     - **create the driver pod**: `kubernetesClient.pods().inNamespace(...).resource(resolvedDriverPod).create()` (`:153-154`);
     - re-apply pre-resources with the pod as **owner reference** (`:162-171`);
     - apply `driverKubernetesResources ++ Seq(configMap)`, owner-referenced (`:174-182`).
  5. If `spark.kubernetes.submission.waitAppCompletion`, attach a `LoggingPodStatusWatcher` and loop
     to completion (`:184-205`); else log and return (`:206-210`). Submission id is
     `namespace:driverPodName` (`:214-216`).

### 7. Feature steps — how the driver pod spec is built
File: `resource-managers/kubernetes/core/.../submit/KubernetesDriverBuilder.scala`
- `KubernetesDriverBuilder` is public `@Stable @DeveloperApi @Since("3.0.0")` (`:33-41`), documented
  as used by the "Spark K8s operator".
- `buildFromFeatures(conf, client)` (`:39-110`):
  - Initial pod from `spark.kubernetes.driver.podTemplateFile` if set, else `SparkPod.initialPod()`
    (`:42-50`).
  - Loads user feature steps from `KUBERNETES_DRIVER_POD_FEATURE_STEPS` reflectively (`:52-74`).
  - Built-in step list (`:76-88`): `BasicDriverFeatureStep`, `DriverKubernetesCredentialsFeatureStep`,
    `DriverServiceFeatureStep`, `NetworkPolicyFeatureStep`, `MountSecretsFeatureStep`,
    `EnvSecretsFeatureStep`, `MountVolumesFeatureStep`, `DriverCommandFeatureStep`,
    `HadoopConfDriverFeatureStep`, `KerberosConfDriverFeatureStep`, `PodTemplateConfigMapStep`,
    `LocalDirsFeatureStep`, `++ userFeatures`, minus any excluded (`:90-91`).
  - Folds each step over a `KubernetesDriverSpec` (`:93-109`); each contributes `configurePod`,
    `getAdditionalPodSystemProperties`, `getAdditionalPreKubernetesResources`,
    `getAdditionalKubernetesResources` (trait `features/KubernetesFeatureConfigStep.scala:33-85`).
- Output: a fully-resolved driver pod + container plus side resources — a driver `Service`, optional
  `NetworkPolicy`, secret mounts/env, volumes, Hadoop/Kerberos config, pod-template ConfigMap, local
  dirs, and the driver launch command. `DriverCommandFeatureStep` (`:40-59`) picks that command by
  the `MainAppResource` subtype (`JavaMainAppResource`/`PythonMainAppResource`/`RMainAppResource`).

## How this maps to `k8s-spark-submitter`

The REST submitter reuses these exact Spark seams, split at a deliberate line. The public
`@DeveloperApi` K8s builder/utils are meant to be reused as-is; the arg-prep and `Client` pieces are
`private[spark]`/`private[deploy]`, which is precisely **why the submitter places its own classes
inside `org.apache.spark.*` packages** — that package placement is the only way to reach
`prepareSubmitEnvironment` and the private `Client` internals.

| Spark's own flow (`spark-submit`) | What `k8s-spark-submitter` does |
|---|---|
| `prepareSubmitEnvironment` (`private[deploy]`) picks childMainClass + builds child args | `K8sSparkSubmitArgsParser` (pkg `org.apache.spark.deploy`) calls the same `SparkSubmit().prepareSubmitEnvironment` + `ClientArguments.fromCommandLineArgs` |
| `KubernetesClientApplication.start()` → `Client.run()` (`private[spark]`) | `K8sSparkClient` (pkg `org.apache.spark.deploy.k8s.submit`) **reimplements** `Client.run()` — Spark's `Client` uses a ConfigMap-naming singleton that assumes one submission per JVM (breaks in a long-lived server) |
| `KubernetesDriverBuilder.buildFromFeatures` (public `@DeveloperApi`) | Called **as-is** — the supported reuse API |
| `KubernetesClientUtils.buildConfigMap` etc. (public `@DeveloperApi`) | Called **as-is** |
| fabric8 create-resources sequence inside `Client.run()` (`:139-182`) | Reimplemented as `createResources` — the generic apply boundary |

The `createResources` step flagged as generalizable maps directly to `Client.run()`'s fabric8 phases
(`KubernetesClientApplication.scala:139-182`): the same "create driver pod, then apply owned
resources with the pod as GC owner" pattern — the piece that could be reimplemented in Go against a
Go Kubernetes client, with no Spark dependency.

### The public reuse surface (supported)
`KubernetesDriverBuilder.buildFromFeatures`, `KubernetesConf.createDriverConf` (has a Java-friendly
overload with nullable `proxyUser`, `KubernetesConf.scala:343-351`), `KubernetesClientUtils`
(with `...Java` variants), `KubernetesFeatureConfigStep`, `SparkPod`, `KubernetesDriverSpec` — all
`@DeveloperApi`.

### The private slice (requires package placement or reflection)
`SparkSubmit.prepareSubmitEnvironment` (`private[deploy]`), the whole `SparkSubmit` class
(`private[spark]`), `ClientArguments` / `ClientArguments.fromCommandLineArgs` (`private[spark]`),
`Client` and `KubernetesClientApplication` (`private[spark]`), and `K8SSparkSubmitOperation`
(`private[spark]`, for kill/status).

## Key files

| File | Role |
|------|------|
| `bin/spark-submit`, `bin/spark-class` | Shell entry; build & exec the JVM command |
| `launcher/.../Main.java` | Launcher entry; prints the final java command |
| `launcher/.../SparkSubmitCommandBuilder.java` | Assembles the `java ... SparkSubmit ...` command |
| `core/.../deploy/SparkSubmit.scala` | `main` → `doSubmit` → `prepareSubmitEnvironment` → `runMain` |
| `core/.../deploy/SparkApplication.scala` | The `SparkApplication` trait `runMain` invokes |
| `resource-managers/kubernetes/core/.../submit/KubernetesClientApplication.scala` | `KubernetesClientApplication` + `Client` (driver pod creation) |
| `resource-managers/kubernetes/core/.../submit/KubernetesDriverBuilder.scala` | Feature-step pipeline building the driver spec |
| `resource-managers/kubernetes/core/.../submit/KubernetesClientUtils.scala` | ConfigMap / spark.properties helpers |
| `resource-managers/kubernetes/core/.../features/KubernetesFeatureConfigStep.scala` | The feature-step contract |
