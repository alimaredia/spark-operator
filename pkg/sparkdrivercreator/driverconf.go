/*
Copyright 2025 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkdrivercreator

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os/user"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// Defaults ported from Spark's k8s Config/Constants for the target versions.
// These mirror the values BasicDriverFeatureStep reads out of KubernetesDriverConf
// when the user leaves them unset.
const (
	defaultDriverCores      = int32(1)    // spark.driver.cores
	defaultDriverMemoryMiB  = int64(1024) // spark.driver.memory (1g)
	driverMinMemOverheadMiB = int64(384)  // spark.driver.minMemoryOverhead
	// Default memory overhead factors from Spark's BasicDriverFeatureStep: JVM
	// apps use 0.1, non-JVM (Python/R) apps 0.4, unless the user overrides
	// spark.kubernetes.memoryOverheadFactor.
	jvmMemoryOverheadFactor    = 0.1
	nonJVMMemoryOverheadFactor = 0.4
	defaultDriverContainerNm   = "spark-kubernetes-driver"
	defaultImagePullPolicy     = corev1.PullIfNotPresent
	driverPodNameSuffix        = "-driver"
	driverServiceSuffix        = "-driver-svc"
	kubernetesDNSLabelMaxSize  = 63

	// defaultMaster is a representative in-cluster API-server URL. Stock
	// spark-submit computes spark.master from the API-server env at submit time;
	// the oracle normalizes it away, so the exact value is immaterial to Build.
	defaultMaster = "k8s://https://kubernetes.default.svc:443"
)

// Port defaults. NOTE: the connect-server port is version-gated. On Spark 4.0.x
// it defaults to 0 (the port is only emitted when connect is explicitly enabled),
// so the port != 0 filter in ports() drops it and the 4.0.4 golden has no
// spark-connect port. Spark 4.2.0's DriverServiceFeatureStep publishes it by
// default at 15002, so connectPortDefault() returns that when the declared
// version exposes it (see versionExposesConnectPort).
const (
	defaultDriverPort         = 7078
	defaultBlockManagerPort   = 7079
	defaultUIPort             = 4040
	defaultConnectPort        = 0
	defaultConnectPortEnabled = 15002
)

// connectAppProtocol is the appProtocol Spark's DriverServiceFeatureStep stamps
// on the spark-connect *service* port. The matching container port carries no
// appProtocol (only protocol: TCP), so it lives on portSpec but is applied by the
// service builder alone.
const connectAppProtocol = "grpc"

// SparkConf keys BasicDriverFeatureStep consults for the container ports.
const (
	confDriverPort       = "spark.driver.port"
	confDriverBMPort     = "spark.driver.blockManager.port"
	confBlockManagerPort = "spark.blockManager.port"
	confUIPort           = "spark.ui.port"
	confConnectPort      = "spark.connect.grpc.binding.port"
)

// Python application entry point and interpreter conf keys. Spark forces the
// driver main class to PythonRunner for a k8s cluster-mode Python app (see
// SparkSubmit's isKubernetesCluster branch), and DriverCommandFeatureStep sets
// the PySpark interpreter env vars from these confs.
const (
	pythonRunnerMainClass       = "org.apache.spark.deploy.PythonRunner"
	confPysparkPython           = "spark.pyspark.python"
	confPysparkDriverPython     = "spark.pyspark.driver.python"
	confDriverMemOverheadFactor = "spark.driver.memoryOverheadFactor"
	envPysparkPython            = "PYSPARK_PYTHON"
	envPysparkDriverPython      = "PYSPARK_DRIVER_PYTHON"
)

// appNameSanitizeRe matches everything not allowed in a DNS-label-ish name.
var appNameSanitizeRe = regexp.MustCompile(`[^a-z0-9\-]`)

// dashCollapseRe collapses runs of '-' left behind by sanitization.
var dashCollapseRe = regexp.MustCompile(`-+`)

// driverConf is the reproducible subset of Spark's KubernetesDriverConf: the
// resolved inputs the feature steps read, derived once from the SparkApplication
// plus Spark's defaults. It exists so each step (BasicDriverFeatureStep first)
// reads from a single resolved source instead of re-deriving from the CRD.
type driverConf struct {
	// Identity.
	appName            string // spark.app.name — the SparkApplication's name
	appID              string // spark-<32hex>, randomized (normalized away in the oracle)
	resourceNamePrefix string // <appName>-<16hex>, randomized (normalized away)
	sparkVersion       string // value of the spark-version label (the runtime Spark version)
	configMapName      string // spark-drv-<16hex>-conf-map, randomized (normalized away)
	localDir           string // /var/data/spark-<uuid>, randomized (normalized away)
	serviceName        string // <resourceNamePrefix>-driver-svc, randomized (normalized away)

	// driverPodName is the driver pod's name. Stock spark-submit defaults it to
	// "<resourceNamePrefix>-driver" (randomized), but honors an explicit
	// spark.kubernetes.driver.pod.name override. The Kubeflow operator always sets
	// that override to a deterministic "<app>-driver" so the controller can track
	// the pod by name — so unlike the service/ConfigMap names (which stay derived
	// from the random prefix), this may be a fixed value.
	driverPodName string

	// Application entry point. resourceType is spark.kubernetes.resource.type
	// (java|python|r), derived from the SparkApplication's type. For Python apps
	// mainClass is forced to PythonRunner (Spark does this in spark-submit) and the
	// PySpark interpreter env vars are resolved from conf; both are empty otherwise.
	resourceType        string
	mainClass           string
	mainAppResource     string
	appArgs             []string
	pysparkPython       string // PYSPARK_PYTHON env value ("" = unset, env var omitted)
	pysparkDriverPython string // PYSPARK_DRIVER_PYTHON env value ("" = unset)

	// Image.
	image           string
	imagePullPolicy corev1.PullPolicy

	// CPU. coresRequest is a raw quantity string (Spark keeps it as a string so
	// fractional/milli values like "100m" round-trip); limitCores is optional.
	// coresSet / coresRequestSet track whether the user supplied the value: the
	// conf map emits spark.driver.cores / spark.kubernetes.driver.request.cores
	// only for user-supplied values (spark-submit only serializes set confs).
	cores           int32
	coresSet        bool
	coresRequest    string
	coresRequestSet bool
	limitCores      string

	// Memory (MiB). memoryWithOverheadMiB is what the pod requests/limits.
	// memory is the raw user string (e.g. "1g"); empty when unset, so the conf
	// map emits spark.driver.memory only when the user supplied it.
	memory                string
	memoryMiB             int64
	memoryOverheadMiB     int64
	memoryWithOverheadMiB int64
	// memoryOverheadFactor is the resolved default overhead factor Spark serializes
	// as spark.kubernetes.memoryOverheadFactor: 0.1 for JVM, 0.4 for non-JVM apps,
	// or the user's spark.kubernetes.memoryOverheadFactor override.
	memoryOverheadFactor float64

	// Metadata / scheduling.
	labels        map[string]string
	annotations   map[string]string
	environment   []corev1.EnvVar // ordered custom driver env vars
	nodeSelector  map[string]string
	schedulerName string

	// Identity of the effective user; SPARK_USER is set from proxyUser when set,
	// else the submitting OS user.
	proxyUser string

	// serviceAccount is spark.kubernetes.authenticate.driver.serviceAccountName
	// (driver.serviceAccount); empty means unset (field omitted from the pod).
	serviceAccount string

	// driverLabelsCustom are the user-supplied driver labels (driver.labels),
	// serialized as spark.kubernetes.driver.label.<k>. The preset spark-* labels
	// are added to the pod directly and never appear as conf.
	driverLabelsCustom map[string]string

	// master is spark.master. Stock spark-submit derives it from the API server
	// env at submit time; the oracle normalizes it away, so a representative
	// default suffices for a reproducible build.
	master string

	// namespace the driver resources live in.
	namespace string

	// sparkConf is the raw user conf, consulted for port overrides.
	sparkConf map[string]string

	// Executor pod template (classic operator path). hasExecPodTemplate gates the
	// PodTemplateConfigMapStep port: when set, Build emits the executor
	// pod-template ConfigMap, mounts it on the driver, and adds the executor
	// podTemplateFile/podTemplateContainerName confs. podSpecConfigMapName is the
	// "<prefix>-driver-podspec-conf-map" ConfigMap name; execPodTemplateYAML is the
	// template serialized exactly as the operator writes it (sigs.k8s.io/yaml), so
	// it lands in the ConfigMap byte-for-byte.
	hasExecPodTemplate           bool
	podSpecConfigMapName         string
	execPodTemplateYAML          string
	execPodTemplateContainerName string

	// Driver volumes (spark.kubernetes.driver.volumes.*), resolved once in Spark's
	// order. Today only persistentVolumeClaim is ported (the on-demand PVC path);
	// see mount_volumes.go. pvcAccessMode is the access mode on-demand PVCs get
	// (ReadWriteOncePod unless the legacy conf flips it to ReadWriteOnce).
	driverVolumes []driverVolume
	pvcAccessMode corev1.PersistentVolumeAccessMode

	// Secrets referenced by the driver (see mount_secrets.go). Neither creates an
	// API object — both reference pre-existing Secrets. driverSecrets are mounted as
	// volumes (spark.kubernetes.driver.secrets.*); driverEnvSecrets are env vars
	// sourced via secretKeyRef (spark.kubernetes.driver.secretKeyRef.*).
	driverSecrets    []secretMount
	driverEnvSecrets []envSecret

	// hadoopConfigMapName is a pre-existing ConfigMap to mount as the Hadoop config
	// dir on the driver (spark.kubernetes.hadoop.configMapName). Empty unless set; the
	// HADOOP_CONF_DIR local-directory mode of HadoopConfDriverFeatureStep is a
	// submit-host artifact and is not reproduced. See hadoop_conf.go.
	hadoopConfigMapName string
}

// newDriverConf resolves a SparkApplication into a driverConf, applying Spark's
// defaults for anything the user left unset. It generates the randomized app id
// and resource-name prefix the same shape Spark does; the oracle normalizes both
// sides' random values to placeholders, so they need not be deterministic.
func newDriverConf(app *v1beta2.SparkApplication, opts BuildOptions) (*driverConf, error) {
	appName := app.Name
	driver := app.Spec.Driver

	image, err := resolveImage(app)
	if err != nil {
		return nil, err
	}

	resourceType, err := resolveResourceType(app.Spec.Type)
	if err != nil {
		return nil, err
	}

	memMiB := defaultDriverMemoryMiB
	memory := ""
	if driver.Memory != nil && *driver.Memory != "" {
		memory = *driver.Memory
		memMiB, err = parseMemoryMiB(*driver.Memory)
		if err != nil {
			return nil, fmt.Errorf("driver memory: %w", err)
		}
	}
	overheadFactor := resolveDefaultOverheadFactor(resourceType, app.Spec.SparkConf)
	overheadMiB, err := resolveMemoryOverheadMiB(driver, app.Spec.SparkConf, memMiB, overheadFactor)
	if err != nil {
		return nil, err
	}

	cores := defaultDriverCores
	coresSet := false
	if driver.Cores != nil {
		cores = *driver.Cores
		coresSet = true
	}
	coresRequest := strconv.Itoa(int(cores))
	coresRequestSet := false
	if driver.CoreRequest != nil && *driver.CoreRequest != "" {
		coresRequest = *driver.CoreRequest
		coresRequestSet = true
	}
	limitCores := ""
	if driver.CoreLimit != nil {
		limitCores = *driver.CoreLimit
	}

	// The resource-name prefix is generated once and shared: the service is
	// "<prefix>-driver-svc" and the ConfigMap "spark-drv-<id>-conf-map", both
	// derived from it. The driver pod also defaults to "<prefix>-driver", but an
	// explicit spark.kubernetes.driver.pod.name (or driver.podName) overrides only
	// the pod name — the service/ConfigMap keep the random prefix.
	resourceNamePrefix := generateResourceNamePrefix(appName)

	c := &driverConf{
		appName:               appName,
		appID:                 generateAppID(),
		resourceNamePrefix:    resourceNamePrefix,
		sparkVersion:          app.Spec.SparkVersion,
		configMapName:         generateConfigMapName(),
		localDir:              generateLocalDir(),
		serviceName:           generateDriverServiceName(resourceNamePrefix),
		podSpecConfigMapName:  resourceNamePrefix + podSpecConfigMapSuffix,
		driverPodName:         resolveDriverPodName(driver, app.Spec.SparkConf, resourceNamePrefix),
		image:                 image,
		imagePullPolicy:       defaultImagePullPolicy,
		cores:                 cores,
		coresSet:              coresSet,
		coresRequest:          coresRequest,
		coresRequestSet:       coresRequestSet,
		limitCores:            limitCores,
		memory:                memory,
		memoryMiB:             memMiB,
		memoryOverheadMiB:     overheadMiB,
		memoryWithOverheadMiB: memMiB + overheadMiB,
		memoryOverheadFactor:  overheadFactor,
		resourceType:          resourceType,
		annotations:           copyStringMap(driver.Annotations),
		environment:           driverEnv(driver),
		nodeSelector:          driverNodeSelector(app),
		driverLabelsCustom:    copyStringMap(driver.Labels),
		master:                defaultMaster,
		sparkConf:             copyStringMap(app.Spec.SparkConf),
		namespace:             app.Namespace,
		appArgs:               append([]string(nil), app.Spec.Arguments...),
	}
	// Resolve the driver main class. For a Python app Spark forces
	// org.apache.spark.deploy.PythonRunner (SparkSubmit's isKubernetesCluster
	// branch emits --main-class PythonRunner regardless of any user main class),
	// and DriverCommandFeatureStep resolves the PySpark interpreter env vars.
	if resourceType == resourceTypePython {
		c.mainClass = pythonRunnerMainClass
		c.pysparkPython, c.pysparkDriverPython = resolvePysparkEnv(c.sparkConf)
	} else if app.Spec.MainClass != nil {
		c.mainClass = *app.Spec.MainClass
	}
	if app.Spec.MainApplicationFile != nil {
		c.mainAppResource = *app.Spec.MainApplicationFile
	}
	// Labels need the resolved appID/sparkVersion, so build them from c.
	c.labels = driverLabels(c, driver.Labels)

	if driver.SchedulerName != nil {
		c.schedulerName = *driver.SchedulerName
	}
	if app.Spec.ProxyUser != nil {
		c.proxyUser = *app.Spec.ProxyUser
	}
	if driver.ServiceAccount != nil {
		c.serviceAccount = *driver.ServiceAccount
	}

	// Resolve the executor pod template (classic operator path). It is serialized
	// with the same encoder the operator uses to write the file (sigs.k8s.io/yaml,
	// via util.WriteObjectToFile), so PodTemplateConfigMapStep — which embeds the
	// file text verbatim — yields byte-identical ConfigMap data.
	if opts.ExecutorPodTemplate != nil {
		data, err := yaml.Marshal(opts.ExecutorPodTemplate)
		if err != nil {
			return nil, fmt.Errorf("marshaling executor pod template: %w", err)
		}
		c.hasExecPodTemplate = true
		c.execPodTemplateYAML = string(data)
		c.execPodTemplateContainerName = defaultExecutorContainerNm
	}

	// Resolve driver volumes from conf, mirroring MountVolumesFeatureStep. Only
	// on-demand PVCs are ported so far; other volume types error out rather than
	// being silently dropped.
	volumes, err := parseDriverVolumes(c.sparkConf, resourceNamePrefix)
	if err != nil {
		return nil, err
	}
	c.driverVolumes = volumes
	c.pvcAccessMode = pvcAccessModeDefault
	if c.sparkConf[confLegacyPVCAccess] == "true" {
		c.pvcAccessMode = pvcAccessModeLegacy
	}

	// Resolve referenced secrets (mounted volumes + secretKeyRef env vars). Neither
	// creates an object; both only mutate the driver pod. See mount_secrets.go. The
	// typed CRD secret fields are first folded into the conf (mirroring the operator's
	// --conf translation) so the feature steps and the spark.properties passthrough
	// share one source of truth.
	if len(driver.Secrets) > 0 || len(driver.EnvSecretKeyRefs) > 0 {
		if c.sparkConf == nil {
			c.sparkConf = map[string]string{}
		}
		addDriverSecretConf(c.sparkConf, driver)
		// Typed secrets (GCPServiceAccount/HadoopDelegationToken) also inject a
		// credential env var. Append it to the custom driver env so BasicDriver emits
		// the pod env var and the spark.properties passthrough emits the driverEnv line
		// (mirroring the operator's driverSecretOption). Lands after any driver.env
		// entries, before SPARK_DRIVER_BIND_ADDRESS — matching Spark's env order.
		c.environment = append(c.environment, driverSecretCredentialEnv(driver)...)
	}
	c.driverSecrets = parseDriverSecrets(c.sparkConf)
	c.driverEnvSecrets, err = parseDriverEnvSecrets(c.sparkConf)
	if err != nil {
		return nil, err
	}

	// Resolve a pre-existing Hadoop config ConfigMap to mount on the driver
	// (HadoopConfDriverFeatureStep). See hadoop_conf.go.
	c.hadoopConfigMapName = c.sparkConf[confHadoopConfigMapName]

	return c, nil
}

// resolveImage returns the driver image, honoring the driver-level override then
// the app-level image, matching Spark's DRIVER_CONTAINER_IMAGE resolution.
func resolveImage(app *v1beta2.SparkApplication) (string, error) {
	if img := app.Spec.Driver.Image; img != nil && *img != "" {
		return *img, nil
	}
	if img := app.Spec.Image; img != nil && *img != "" {
		return *img, nil
	}
	return "", fmt.Errorf("sparkdrivercreator: must specify a driver container image")
}

// resolveResourceType maps the SparkApplication type to Spark's
// spark.kubernetes.resource.type. Scala/Java (and the unset default) are "java";
// Python is "python". R (SparkR) is deliberately and permanently unsupported: it
// is deprecated in Apache Spark 4.0.0 and slated for removal upstream (see
// spark/docs/sparkr.md), so the native submitter will not reproduce it. R apps are
// rejected here rather than silently mislaunched as a JVM app.
func resolveResourceType(appType v1beta2.SparkApplicationType) (string, error) {
	switch appType {
	case v1beta2.SparkApplicationTypeJava, v1beta2.SparkApplicationTypeScala, "":
		return resourceTypeJava, nil
	case v1beta2.SparkApplicationTypePython:
		return resourceTypePython, nil
	case v1beta2.SparkApplicationTypeR:
		return "", fmt.Errorf("sparkdrivercreator: R (SparkR) applications are not supported by the native submitter (SparkR is deprecated in Spark 4.0.0 and slated for removal upstream)")
	default:
		return "", fmt.Errorf("sparkdrivercreator: unsupported application type %q", appType)
	}
}

// resolvePysparkEnv mirrors DriverCommandFeatureStep.configureForPython's
// interpreter resolution: PYSPARK_PYTHON from spark.pyspark.python, and
// PYSPARK_DRIVER_PYTHON from spark.pyspark.driver.python falling back to
// spark.pyspark.python. Unlike Spark it does not consult the submitter's OS
// environment — the native path must be reproducible from the CRD alone — so an
// unset interpreter yields "" and the env var is omitted.
func resolvePysparkEnv(sparkConf map[string]string) (pysparkPython, pysparkDriverPython string) {
	pysparkPython = sparkConf[confPysparkPython]
	pysparkDriverPython = sparkConf[confPysparkDriverPython]
	if pysparkDriverPython == "" {
		pysparkDriverPython = pysparkPython
	}
	return pysparkPython, pysparkDriverPython
}

// resolveDefaultOverheadFactor returns Spark BasicDriverFeatureStep's
// defaultOverheadFactor — the value serialized as spark.kubernetes.memoryOverheadFactor:
// the user's spark.kubernetes.memoryOverheadFactor override if set, else 0.1 for
// JVM apps and 0.4 for non-JVM (Python/R) apps.
func resolveDefaultOverheadFactor(resourceType string, sparkConf map[string]string) float64 {
	if v, ok := sparkConf[confMemoryOverheadFactor]; ok && v != "" {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
	}
	if resourceType == resourceTypeJava {
		return jvmMemoryOverheadFactor
	}
	return nonJVMMemoryOverheadFactor
}

// resolveMemoryOverheadMiB mirrors BasicDriverFeatureStep's overhead math: an
// explicit spark.driver.memoryOverhead wins; otherwise max(factor*mem, minimum),
// where the factor prefers spark.driver.memoryOverheadFactor over the resolved
// default (0.1 JVM / 0.4 non-JVM). Spark truncates factor*mem to an int.
func resolveMemoryOverheadMiB(driver v1beta2.DriverSpec, sparkConf map[string]string, memMiB int64, defaultFactor float64) (int64, error) {
	if driver.MemoryOverhead != nil && *driver.MemoryOverhead != "" {
		v, err := parseMemoryMiB(*driver.MemoryOverhead)
		if err != nil {
			return 0, fmt.Errorf("driver memoryOverhead: %w", err)
		}
		return v, nil
	}
	factor := defaultFactor
	if v, ok := sparkConf[confDriverMemOverheadFactor]; ok && v != "" {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			factor = f
		}
	}
	factored := int64(factor * float64(memMiB))
	if factored > driverMinMemOverheadMiB {
		return factored, nil
	}
	return driverMinMemOverheadMiB, nil
}

// driverLabels builds the driver pod labels the way KubernetesConf does: custom
// driver labels first, then the reserved preset labels (which win). Spark forbids
// custom labels from colliding with the presets; we simply let presets override.
func driverLabels(c *driverConf, custom map[string]string) map[string]string {
	labels := copyStringMap(custom)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[labelSparkVersion] = c.sparkVersion
	labels[labelSparkAppSelector] = c.appID
	labels[labelSparkAppName] = appNameLabel(c.appName)
	labels[labelSparkRole] = sparkRoleDriver
	return labels
}

// driverEnv resolves the ordered custom driver env vars. It uses the CRD's
// typed env list (the modern field) directly, preserving order.
func driverEnv(driver v1beta2.DriverSpec) []corev1.EnvVar {
	if len(driver.Env) == 0 {
		return nil
	}
	out := make([]corev1.EnvVar, len(driver.Env))
	copy(out, driver.Env)
	return out
}

// driverNodeSelector merges the app-level node selector with the driver-level
// one, the driver-level taking precedence (matching Spark's global then
// driver-specific prefixes).
func driverNodeSelector(app *v1beta2.SparkApplication) map[string]string {
	out := map[string]string{}
	for k, v := range app.Spec.NodeSelector {
		out[k] = v
	}
	for k, v := range app.Spec.Driver.NodeSelector {
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// currentUserName returns the submitting OS user, mirroring
// Utils.getCurrentUserName(). The oracle normalizes SPARK_USER to a placeholder,
// so the exact value only matters for real submissions.
func currentUserName() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return "spark"
}

// appNameLabel sanitizes an app name into a DNS-label-safe value, matching
// KubernetesConf.getAppNameLabel.
func appNameLabel(appName string) string {
	s := strings.ToLower(strings.TrimSpace(appName))
	s = appNameSanitizeRe.ReplaceAllString(s, "-")
	s = dashCollapseRe.ReplaceAllString(s, "-")
	if len(s) > kubernetesDNSLabelMaxSize {
		s = s[:kubernetesDNSLabelMaxSize]
	}
	return strings.Trim(s, "-")
}

// generateAppID returns a Spark-shaped app id: "spark-" + 32 lowercase hex.
func generateAppID() string {
	return "spark-" + randHex(16)
}

// generateResourceNamePrefix returns "<sanitized appName>-<16 hex>", matching
// KubernetesConf.getResourceNamePrefix's shape (sanitized name + uniqueID).
func generateResourceNamePrefix(appName string) string {
	name := appNameLabel(appName)
	prefix := name + "-" + randHex(8)
	// Spark also guards against a leading digit; keep parity.
	if len(prefix) > 0 && prefix[0] >= '0' && prefix[0] <= '9' {
		prefix = "x" + prefix[1:]
	}
	return prefix
}

// generateConfigMapName returns "spark-drv-<16hex>-conf-map", matching
// KubernetesClientUtils.configMapNameDriver (spark-drv-<uniqueID>-conf-map).
func generateConfigMapName() string {
	return "spark-drv-" + randHex(8) + "-conf-map"
}

// generateLocalDir returns the default local dir Spark's LocalDirsFeatureStep
// mounts when the user configures none: /var/data/spark-<uuid>.
func generateLocalDir() string {
	return "/var/data/spark-" + randUUID()
}

// resolveDriverPodName mirrors Spark's driver pod naming: an explicit
// spark.kubernetes.driver.pod.name wins, otherwise the pod is
// "<resourceNamePrefix>-driver". The CRD's driver.podName is honored first because
// the operator's util.GetDriverPodName translates it into that conf before submit;
// checking it here keeps the pure-Go builder correct even without the adapter.
// Only the pod name changes — the service and ConfigMap keep the random prefix.
func resolveDriverPodName(driver v1beta2.DriverSpec, sparkConf map[string]string, resourceNamePrefix string) string {
	if driver.PodName != nil && *driver.PodName != "" {
		return *driver.PodName
	}
	if name := sparkConf[confDriverPodName]; name != "" {
		return name
	}
	return resourceNamePrefix + driverPodNameSuffix
}

// generateDriverServiceName mirrors KubernetesConf.driverServiceName: the driver
// service is "<resourceNamePrefix>-driver-svc" unless that exceeds the 63-char
// DNS label limit, in which case Spark falls back to a "spark-<uniqueID>-driver-svc"
// form. The fallback only triggers for very long app names (none in the oracle
// matrix); it is included for parity.
func generateDriverServiceName(resourceNamePrefix string) string {
	preferred := resourceNamePrefix + driverServiceSuffix
	if len(preferred) <= kubernetesDNSLabelMaxSize {
		return preferred
	}
	return "spark-" + randHex(8) + driverServiceSuffix
}

// randUUID returns a random RFC-4122-shaped UUID (8-4-4-4-12 hex). Only the
// shape matters here — the value is normalized to a placeholder in the oracle.
func randUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		for i := range b {
			b[i] = 0
		}
	}
	s := hex.EncodeToString(b)
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand should not fail; fall back to a fixed value rather than panic.
		for i := range b {
			b[i] = 0
		}
	}
	return hex.EncodeToString(b)
}

// parseMemoryMiB parses a Spark memory string (e.g. "1g", "512m", "1024") into
// MiB, mirroring JavaUtils.byteStringAsMb: no suffix means MiB, binary units.
func parseMemoryMiB(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty memory value")
	}
	i := 0
	for i < len(s) && (s[i] == '+' || s[i] == '-' || s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	num, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory value %q: %w", s, err)
	}
	unit := strings.ToLower(strings.TrimSpace(s[i:]))
	var mult float64 // multiplier to MiB
	switch unit {
	case "", "m", "mb", "mib":
		mult = 1
	case "b":
		mult = 1.0 / (1024 * 1024)
	case "k", "kb", "kib":
		mult = 1.0 / 1024
	case "g", "gb", "gib":
		mult = 1024
	case "t", "tb", "tib":
		mult = 1024 * 1024
	case "p", "pb", "pib":
		mult = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown memory unit %q in %q", unit, s)
	}
	return int64(num * mult), nil
}

// intConf reads an integer spark conf value, returning def if unset or unparseable.
func (c *driverConf) intConf(key string, def int) int {
	v, ok := c.sparkConf[key]
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return def
	}
	return n
}

// blockManagerPort mirrors Spark's precedence: spark.driver.blockManager.port,
// falling back to spark.blockManager.port, then the default.
func (c *driverConf) blockManagerPort() int {
	if v, ok := c.sparkConf[confDriverBMPort]; ok && v != "" {
		return c.intConf(confDriverBMPort, defaultBlockManagerPort)
	}
	return c.intConf(confBlockManagerPort, defaultBlockManagerPort)
}

// portSpec is a resolved (name, port) triple shared by the container ports and
// the driver service ports — the oracle proves the two sets are identical. The
// appProtocol is service-only (the spark-connect grpc port); the container port
// ignores it.
type portSpec struct {
	name        string
	port        int32
	appProtocol string
}

// ports resolves the driver's ports in Spark's order, dropping any that resolve
// to 0 (an invalid port) — which is how the spark-connect port stays off on Spark
// 4.0.x and is published (at 15002) only on versions that expose it.
func (c *driverConf) ports() []portSpec {
	candidates := []portSpec{
		{name: portNameDriverRPC, port: int32(c.intConf(confDriverPort, defaultDriverPort))},
		{name: portNameBlockManager, port: int32(c.blockManagerPort())},
		{name: portNameUI, port: int32(c.intConf(confUIPort, defaultUIPort))},
		{name: portNameConnectServer, port: int32(c.intConf(confConnectPort, c.connectPortDefault())), appProtocol: connectAppProtocol},
	}
	out := make([]portSpec, 0, len(candidates))
	for _, p := range candidates {
		if p.port == 0 {
			continue
		}
		out = append(out, p)
	}
	return out
}

// connectPortDefault returns the spark-connect port default for this submission's
// declared Spark version: 15002 on versions that publish it by default (so it
// lands on both the driver service and container), 0 otherwise (so the port != 0
// filter in ports() drops it, leaving 4.0.4 output unchanged).
func (c *driverConf) connectPortDefault() int {
	if versionExposesConnectPort(c.sparkVersion) {
		return defaultConnectPortEnabled
	}
	return defaultConnectPort
}

func copyStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
