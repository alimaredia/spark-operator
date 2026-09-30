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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/sparkdrivercreator"
)

// findContainerEnv returns the value of env var name in the container, or "".
func findContainerEnv(c corev1.Container, name string) string {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func TestNativeSparkSubmitter_Submit_InjectsVersionGate(t *testing.T) {
	c := uidAssigningClient(t)
	app := nativeTestApp() // SparkVersion "4.0.0", driver image "spark:4.0.0"

	require.NoError(t, NewNativeSparkSubmitter(c).Submit(context.Background(), app))

	pod := &corev1.Pod{}
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: "sparkpi-driver"}, pod))

	require.NotEmpty(t, pod.Spec.InitContainers, "expected the version-gate init container")
	gate := pod.Spec.InitContainers[0]
	assert.Equal(t, VersionGateContainerName, gate.Name, "gate must be the first init container")

	// Runs the driver's own image (no extra pull) and pins the versions to check.
	var driverImage string
	for _, ct := range pod.Spec.Containers {
		if ct.Name == common.SparkDriverContainerName {
			driverImage = ct.Image
		}
	}
	require.NotEmpty(t, driverImage)
	assert.Equal(t, driverImage, gate.Image)
	assert.Equal(t, app.Spec.SparkVersion, findContainerEnv(gate, envDeclaredSparkVersion))
	assert.Equal(t,
		strings.Join(sparkdrivercreator.SupportedSparkVersions, " "),
		findContainerEnv(gate, envSupportedSparkVersion))

	// It's a shell gate that writes its reason to the termination-message file.
	require.Len(t, gate.Command, 3)
	assert.Equal(t, "/bin/sh", gate.Command[0])
	assert.Equal(t, "-c", gate.Command[1])
	assert.Contains(t, gate.Command[2], "termination-log")
	assert.Equal(t, corev1.TerminationMessageReadFile, gate.TerminationMessagePolicy)
}

func TestNativeSparkSubmitter_Submit_EmptyVersionRejected(t *testing.T) {
	c := uidAssigningClient(t)
	app := nativeTestApp()
	app.Spec.SparkVersion = ""

	err := NewNativeSparkSubmitter(c).Submit(context.Background(), app)
	require.Error(t, err, "empty spec.sparkVersion must fail submission")
	assert.Contains(t, err.Error(), "sparkVersion")

	// The driver pod must not have been created on a rejected submission.
	pod := &corev1.Pod{}
	err = c.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: "sparkpi-driver"}, pod)
	assert.Error(t, err, "driver pod should not exist when the version gate rejects submission")
}

func TestWithVersionGate_PrependsBeforeUserInitContainers(t *testing.T) {
	app := nativeTestApp()
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: common.SparkDriverContainerName, Image: "spark:4.0.0", ImagePullPolicy: corev1.PullIfNotPresent},
			},
			InitContainers: []corev1.Container{
				{Name: "user-init", Image: "busybox"},
			},
		},
	}

	require.NoError(t, withVersionGate(pod, app))

	require.Len(t, pod.Spec.InitContainers, 2)
	assert.Equal(t, VersionGateContainerName, pod.Spec.InitContainers[0].Name)
	assert.Equal(t, "user-init", pod.Spec.InitContainers[1].Name)
	// Pull policy is inherited from the driver container.
	assert.Equal(t, corev1.PullIfNotPresent, pod.Spec.InitContainers[0].ImagePullPolicy)
}

func TestWithVersionGate_NoDriverContainer(t *testing.T) {
	app := nativeTestApp()
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "not-the-driver", Image: "x"}},
	}}
	err := withVersionGate(pod, app)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "driver container")
}
