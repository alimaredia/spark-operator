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
	"math"
	"os/user"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// Defaults ported from Spark's k8s Config/Constants for the target versions.
// These mirror the values BasicDriverFeatureStep reads out of KubernetesDriverConf
// when the user leaves them unset.
const (
	defaultDriverCores        = int32(1)    // spark.driver.cores
	defaultDriverMemoryMiB    = int64(1024) // spark.driver.memory (1g)
	driverMinMemOverheadMiB   = int64(384)  // spark.driver.minMemoryOverhead
	memoryOverheadFactor      = 0.1         // spark.driver.memoryOverheadFactor (JVM apps)
	defaultDriverContainerNm  = "spark-kubernetes-driver"
	defaultImagePullPolicy    = corev1.PullIfNotPresent
	driverPodNameSuffix       = "-driver"
	kubernetesDNSLabelMaxSize = 63
)

// Port defaults. NOTE: connect-server port defaults to 0 on Spark 4.0.x (the
// port is only emitted when connect is enabled), unlike master where it is
// 15002. The golden for 4.0.4 has no spark-connect port, so we default it to 0
// and let the port != 0 filter drop it. Revisit per-version when the matrix grows.
const (
	defaultDriverPort       = 7078
	defaultBlockManagerPort = 7079
	defaultUIPort           = 4040
	defaultConnectPort      = 0
)

// SparkConf keys BasicDriverFeatureStep consults for the container ports.
const (
	confDriverPort       = "spark.driver.port"
	confDriverBMPort     = "spark.driver.blockManager.port"
	confBlockManagerPort = "spark.blockManager.port"
	confUIPort           = "spark.ui.port"
	confConnectPort      = "spark.connect.grpc.binding.port"
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

	// Image.
	image           string
	imagePullPolicy corev1.PullPolicy

	// CPU. coresRequest is a raw quantity string (Spark keeps it as a string so
	// fractional/milli values like "100m" round-trip); limitCores is optional.
	cores        int32
	coresRequest string
	limitCores   string

	// Memory (MiB). memoryWithOverheadMiB is what the pod requests/limits.
	memoryMiB             int64
	memoryOverheadMiB     int64
	memoryWithOverheadMiB int64

	// Metadata / scheduling.
	labels        map[string]string
	annotations   map[string]string
	environment   []corev1.EnvVar // ordered custom driver env vars
	nodeSelector  map[string]string
	schedulerName string

	// Identity of the effective user; SPARK_USER is set from proxyUser when set,
	// else the submitting OS user.
	proxyUser string

	// sparkConf is the raw user conf, consulted for port overrides.
	sparkConf map[string]string
}

// newDriverConf resolves a SparkApplication into a driverConf, applying Spark's
// defaults for anything the user left unset. It generates the randomized app id
// and resource-name prefix the same shape Spark does; the oracle normalizes both
// sides' random values to placeholders, so they need not be deterministic.
func newDriverConf(app *v1beta2.SparkApplication) (*driverConf, error) {
	appName := app.Name
	driver := app.Spec.Driver

	image, err := resolveImage(app)
	if err != nil {
		return nil, err
	}

	memMiB := defaultDriverMemoryMiB
	if driver.Memory != nil && *driver.Memory != "" {
		memMiB, err = parseMemoryMiB(*driver.Memory)
		if err != nil {
			return nil, fmt.Errorf("driver memory: %w", err)
		}
	}
	overheadMiB, err := resolveMemoryOverheadMiB(driver, memMiB)
	if err != nil {
		return nil, err
	}

	cores := defaultDriverCores
	if driver.Cores != nil {
		cores = *driver.Cores
	}
	coresRequest := strconv.Itoa(int(cores))
	if driver.CoreRequest != nil && *driver.CoreRequest != "" {
		coresRequest = *driver.CoreRequest
	}
	limitCores := ""
	if driver.CoreLimit != nil {
		limitCores = *driver.CoreLimit
	}

	c := &driverConf{
		appName:               appName,
		appID:                 generateAppID(),
		resourceNamePrefix:    generateResourceNamePrefix(appName),
		sparkVersion:          app.Spec.SparkVersion,
		image:                 image,
		imagePullPolicy:       defaultImagePullPolicy,
		cores:                 cores,
		coresRequest:          coresRequest,
		limitCores:            limitCores,
		memoryMiB:             memMiB,
		memoryOverheadMiB:     overheadMiB,
		memoryWithOverheadMiB: memMiB + overheadMiB,
		annotations:           copyStringMap(driver.Annotations),
		environment:           driverEnv(driver),
		nodeSelector:          driverNodeSelector(app),
		sparkConf:             copyStringMap(app.Spec.SparkConf),
	}
	// Labels need the resolved appID/sparkVersion, so build them from c.
	c.labels = driverLabels(c, driver.Labels)

	if driver.SchedulerName != nil {
		c.schedulerName = *driver.SchedulerName
	}
	if app.Spec.ProxyUser != nil {
		c.proxyUser = *app.Spec.ProxyUser
	}
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

// resolveMemoryOverheadMiB mirrors BasicDriverFeatureStep's overhead math for
// JVM apps: an explicit memoryOverhead wins; otherwise max(factor*mem, minimum).
func resolveMemoryOverheadMiB(driver v1beta2.DriverSpec, memMiB int64) (int64, error) {
	if driver.MemoryOverhead != nil && *driver.MemoryOverhead != "" {
		v, err := parseMemoryMiB(*driver.MemoryOverhead)
		if err != nil {
			return 0, fmt.Errorf("driver memoryOverhead: %w", err)
		}
		return v, nil
	}
	factored := int64(math.Floor(memoryOverheadFactor * float64(memMiB)))
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
