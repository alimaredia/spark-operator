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
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// These unit tests pin the fields BasicDriverFeatureStep owns, in isolation from
// the rest of the pipeline (LocalDirs / DriverCommand / conf volume are separate
// steps). The full-pod byte-for-byte check against real spark-submit lives in
// test/oracle and only goes green once every step lands.

var (
	appIDShapeRe  = regexp.MustCompile(`^spark-[0-9a-f]{32}$`)
	prefixShapeRe = regexp.MustCompile(`^sparkpi-[0-9a-f]{16}$`)
)

// runBasic builds the driverConf and runs only BasicDriverFeatureStep from the
// empty initial pod, returning the resulting pod + main container.
func runBasic(t *testing.T, app *v1beta2.SparkApplication) (*corev1.Pod, *corev1.Container, *driverConf) {
	t.Helper()
	conf, err := newDriverConf(app, BuildOptions{})
	require.NoError(t, err)
	out := newBasicDriverFeatureStep(conf).configurePod(initialSparkPod())
	return out.pod, out.container, conf
}

func minimalApp() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "sparkpi", Namespace: "default"},
		Spec: v1beta2.SparkApplicationSpec{
			SparkVersion:        "4.0.4",
			Image:               ptr.To("spark:4.0.4"),
			MainClass:           ptr.To("org.apache.spark.examples.SparkPi"),
			MainApplicationFile: ptr.To("local:///opt/spark/examples/jars/spark-examples_2.13-4.0.4.jar"),
			SparkConf: map[string]string{
				"spark.kubernetes.submission.waitAppCompletion": "false",
			},
		},
	}
}

func TestBasicDriverFeatureStep_Minimal(t *testing.T) {
	pod, container, conf := runBasic(t, minimalApp())

	// Identity: randomized but Spark-shaped, and consistent across pod/labels/env.
	assert.Regexp(t, prefixShapeRe, conf.resourceNamePrefix)
	assert.Regexp(t, appIDShapeRe, conf.appID)
	assert.Equal(t, conf.resourceNamePrefix+"-driver", pod.Name)

	// Container basics.
	assert.Equal(t, defaultDriverContainerNm, container.Name)
	assert.Equal(t, "spark:4.0.4", container.Image)
	assert.Equal(t, corev1.PullIfNotPresent, container.ImagePullPolicy)
	assert.Nil(t, container.SecurityContext, "no securityContext on Spark 4.0.x by default")

	// Ports: the three always-on ports; spark-connect (port 0) omitted.
	assert.Equal(t, []corev1.ContainerPort{
		{Name: portNameDriverRPC, ContainerPort: 7078, Protocol: corev1.ProtocolTCP},
		{Name: portNameBlockManager, ContainerPort: 7079, Protocol: corev1.ProtocolTCP},
		{Name: portNameUI, ContainerPort: 4040, Protocol: corev1.ProtocolTCP},
	}, container.Ports)

	// Core env, in Spark's order. SPARK_USER value is host-dependent; only its
	// presence/position is pinned here (the oracle normalizes the value).
	require.Len(t, container.Env, 3)
	assert.Equal(t, envSparkUser, container.Env[0].Name)
	assert.NotEmpty(t, container.Env[0].Value)
	assert.Equal(t, corev1.EnvVar{Name: envApplicationID, Value: conf.appID}, container.Env[1])
	assert.Equal(t, corev1.EnvVar{
		Name: envDriverBindAddress,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "status.podIP"},
		},
	}, container.Env[2])

	// Resources: cpu request only; memory-with-overhead as request and limit.
	// 1g default + max(0.1*1024, 384) = 1024 + 384 = 1408Mi.
	assert.Equal(t, resource.MustParse("1"), container.Resources.Requests[corev1.ResourceCPU])
	assert.Equal(t, resource.MustParse("1408Mi"), container.Resources.Requests[corev1.ResourceMemory])
	assert.Equal(t, resource.MustParse("1408Mi"), container.Resources.Limits[corev1.ResourceMemory])
	_, hasCPULimit := container.Resources.Limits[corev1.ResourceCPU]
	assert.False(t, hasCPULimit, "no cpu limit unless coreLimit is set")

	// Pod-level.
	assert.Equal(t, corev1.RestartPolicyNever, pod.Spec.RestartPolicy)
	assert.Empty(t, pod.Spec.NodeSelector)
	assert.Empty(t, pod.Spec.SchedulerName)

	// Preset labels — Spark's bare keys, not the operator's namespaced ones.
	assert.Equal(t, map[string]string{
		labelSparkVersion:     "4.0.4",
		labelSparkAppSelector: conf.appID,
		labelSparkAppName:     "sparkpi",
		labelSparkRole:        sparkRoleDriver,
	}, pod.Labels)
}

func TestBasicDriverFeatureStep_Overrides(t *testing.T) {
	app := minimalApp()
	app.Spec.Driver = v1beta2.DriverSpec{
		SparkPodSpec: v1beta2.SparkPodSpec{
			Cores:          ptr.To[int32](1),
			Memory:         ptr.To("1g"),
			ServiceAccount: ptr.To("spark"),
			Labels:         map[string]string{"env": "prod"},
			Env:            []corev1.EnvVar{{Name: "MY_ENV", Value: "my-value"}},
			NodeSelector:   map[string]string{"disktype": "ssd"},
		},
	}

	pod, container, conf := runBasic(t, app)

	// Custom driver label merged with presets (presets win on collision).
	assert.Equal(t, "prod", pod.Labels["env"])
	assert.Equal(t, "sparkpi", pod.Labels[labelSparkAppName])
	assert.Equal(t, conf.appID, pod.Labels[labelSparkAppSelector])

	// Custom node selector propagates.
	assert.Equal(t, map[string]string{"disktype": "ssd"}, pod.Spec.NodeSelector)

	// Custom env sits between SPARK_APPLICATION_ID and SPARK_DRIVER_BIND_ADDRESS.
	require.Len(t, container.Env, 4)
	assert.Equal(t, envSparkUser, container.Env[0].Name)
	assert.Equal(t, corev1.EnvVar{Name: envApplicationID, Value: conf.appID}, container.Env[1])
	assert.Equal(t, corev1.EnvVar{Name: "MY_ENV", Value: "my-value"}, container.Env[2])
	assert.Equal(t, envDriverBindAddress, container.Env[3].Name)

	// Same 1408Mi / cpu "1" as the minimal case (cores:1, memory:1g == defaults).
	assert.Equal(t, resource.MustParse("1"), container.Resources.Requests[corev1.ResourceCPU])
	assert.Equal(t, resource.MustParse("1408Mi"), container.Resources.Requests[corev1.ResourceMemory])
}

func TestNewDriverConf_MemoryOverhead(t *testing.T) {
	cases := []struct {
		name        string
		memory      string
		overhead    *string
		wantMem     int64
		wantOverhd  int64
		wantWithOvh int64
	}{
		{"default 1g -> min overhead 384", "1g", nil, 1024, 384, 1408},
		{"4g -> factor 0.1 wins (410)", "4g", nil, 4096, 409, 4505},
		{"explicit overhead honored", "1g", ptr.To("512m"), 1024, 512, 1536},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := minimalApp()
			app.Spec.Driver = v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{
				Memory:         ptr.To(tc.memory),
				MemoryOverhead: tc.overhead,
			}}
			conf, err := newDriverConf(app, BuildOptions{})
			require.NoError(t, err)
			assert.Equal(t, tc.wantMem, conf.memoryMiB)
			assert.Equal(t, tc.wantOverhd, conf.memoryOverheadMiB)
			assert.Equal(t, tc.wantWithOvh, conf.memoryWithOverheadMiB)
		})
	}
}

func TestParseMemoryMiB(t *testing.T) {
	cases := map[string]int64{
		"1g":       1024,
		"1024m":    1024,
		"1024":     1024,
		"512m":     512,
		"2g":       2048,
		"1048576k": 1024,
	}
	for in, want := range cases {
		got, err := parseMemoryMiB(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
}
