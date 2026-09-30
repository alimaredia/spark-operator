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

package sparkapplication

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/sparkdrivercreator"
)

// VersionGateContainerName is the name of the init container the native submitter
// prepends to the driver pod to verify the image's Spark version. Exported so
// tests (and the controller's status surfacing) can identify it.
const VersionGateContainerName = "spark-version-gate"

const (
	envDeclaredSparkVersion  = "DECLARED_SPARK_VERSION"
	envSupportedSparkVersion = "SUPPORTED_SPARK_VERSIONS"
)

// versionGateScript detects the Spark version actually baked into the driver image
// and fails (exit 1) — with a human-readable reason written to the container's
// termination-message file so the controller can surface it — unless the image's
// real version both matches the declared spec.sparkVersion and is one the native
// creator is oracle-verified for.
//
// Detection is JVM-free and cheap: it reads $SPARK_HOME/RELEASE (the version line
// stock Spark images ship), falling back to the spark-core jar name. It never runs
// spark-submit (which would start a JVM). Because it runs as an init container in
// the driver pod (restartPolicy: Never), a non-zero exit is terminal and the Spark
// driver process never starts on a mismatching image.
const versionGateScript = `set -u
home="${SPARK_HOME:-/opt/spark}"
fail() {
	printf '%s\n' "$1" > /dev/termination-log 2>/dev/null || true
	printf '%s\n' "$1" >&2
	exit 1
}
actual=""
if [ -r "$home/RELEASE" ]; then
	actual="$(head -n1 "$home/RELEASE" | awk '{print $2}')"
fi
if [ -z "$actual" ] && [ -d "$home/jars" ]; then
	actual="$(ls "$home/jars" | sed -n 's/^spark-core_[0-9][0-9.]*-\([0-9][0-9.]*\)\.jar$/\1/p' | head -n1)"
fi
[ -n "$actual" ] || fail "spark-version-gate: cannot determine the Spark version in this image (no readable $home/RELEASE and no spark-core jar); declared spec.sparkVersion=$DECLARED_SPARK_VERSION"
if [ "$actual" != "$DECLARED_SPARK_VERSION" ]; then
	fail "spark-version-gate: image Spark version ($actual) does not match spec.sparkVersion ($DECLARED_SPARK_VERSION)"
fi
case " $SUPPORTED_SPARK_VERSIONS " in
	*" $actual "*) ;;
	*) fail "spark-version-gate: Spark version $actual is not supported by the native submitter (supported: $SUPPORTED_SPARK_VERSIONS)" ;;
esac
printf 'spark-version-gate: OK (Spark %s matches spec.sparkVersion and is supported)\n' "$actual"
`

// withVersionGate prepends the spark-version-gate init container to the driver pod.
//
// The native creator builds the driver pod from the *declared* spec.sparkVersion,
// which is a hand-typed CRD field that nothing cross-checks against the container
// image. This gate is the honest backstop: it runs the driver's own image and
// refuses to start the Spark driver if the image's real Spark version does not
// match the declaration or is not one the native creator is oracle-verified for.
// Because the driver pod uses restartPolicy: Never, a gate failure is terminal and
// no Spark workload (driver or executors) ever runs on a mismatching image.
//
// It reuses the driver container's image and pull policy so it adds no extra image
// pull, and rides the scheduling/pull the driver pod incurs anyway — so the check
// costs effectively nothing on a correctly-configured application.
func withVersionGate(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	declared := app.Spec.SparkVersion
	if declared == "" {
		return fmt.Errorf("spark version gate: spec.sparkVersion is empty; cannot verify the image's Spark version")
	}

	image, pullPolicy, err := driverContainerImage(pod)
	if err != nil {
		return err
	}

	gate := corev1.Container{
		Name:            VersionGateContainerName,
		Image:           image,
		ImagePullPolicy: pullPolicy,
		Command:         []string{"/bin/sh", "-c", versionGateScript},
		Env: []corev1.EnvVar{
			{Name: envDeclaredSparkVersion, Value: declared},
			{Name: envSupportedSparkVersion, Value: strings.Join(sparkdrivercreator.SupportedSparkVersions, " ")},
		},
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
	}

	// Prepend so the gate runs before any user-supplied driver init containers,
	// short-circuiting the pod before it does any real work on a bad image.
	pod.Spec.InitContainers = append([]corev1.Container{gate}, pod.Spec.InitContainers...)
	return nil
}

// driverContainerImage returns the image and pull policy of the driver container
// in the built pod, so the gate can run the exact same image.
func driverContainerImage(pod *corev1.Pod) (string, corev1.PullPolicy, error) {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == common.SparkDriverContainerName {
			return pod.Spec.Containers[i].Image, pod.Spec.Containers[i].ImagePullPolicy, nil
		}
	}
	return "", "", fmt.Errorf("spark version gate: driver container %q not found in built pod", common.SparkDriverContainerName)
}
