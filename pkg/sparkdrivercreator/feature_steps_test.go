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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// These unit tests pin the steps that run after BasicDriverFeatureStep in
// isolation. The full byte-for-byte pod check lives in test/oracle.

func TestLocalDirsFeatureStep(t *testing.T) {
	conf, err := newDriverConf(minimalApp(), BuildOptions{})
	require.NoError(t, err)

	out := newLocalDirsFeatureStep(conf).configurePod(initialSparkPod())

	require.Len(t, out.pod.Spec.Volumes, 1)
	assert.Equal(t, "spark-local-dir-1", out.pod.Spec.Volumes[0].Name)
	require.NotNil(t, out.pod.Spec.Volumes[0].EmptyDir, "local dir should be an emptyDir")

	require.Len(t, out.container.VolumeMounts, 1)
	assert.Equal(t, "spark-local-dir-1", out.container.VolumeMounts[0].Name)
	assert.Equal(t, conf.localDir, out.container.VolumeMounts[0].MountPath)

	require.Len(t, out.container.Env, 1)
	assert.Equal(t, corev1.EnvVar{Name: envSparkLocalDirs, Value: conf.localDir}, out.container.Env[0])
}

func TestDriverCommandFeatureStep_Java(t *testing.T) {
	conf, err := newDriverConf(minimalApp(), BuildOptions{})
	require.NoError(t, err)

	out := newDriverCommandFeatureStep(conf).configurePod(initialSparkPod())

	assert.Equal(t, []string{
		"driver",
		"--properties-file", sparkConfPath,
		"--class", "org.apache.spark.examples.SparkPi",
		"local:///opt/spark/examples/jars/spark-examples_2.13-4.0.4.jar",
	}, out.container.Args)
}

func TestDriverCommandFeatureStep_ProxyUserAndArgs(t *testing.T) {
	app := minimalApp()
	app.Spec.ProxyUser = ptr.To("alice")
	app.Spec.Arguments = []string{"100"}
	conf, err := newDriverConf(app, BuildOptions{})
	require.NoError(t, err)

	out := newDriverCommandFeatureStep(conf).configurePod(initialSparkPod())

	assert.Equal(t, []string{
		"driver",
		"--proxy-user", "alice",
		"--properties-file", sparkConfPath,
		"--class", "org.apache.spark.examples.SparkPi",
		"local:///opt/spark/examples/jars/spark-examples_2.13-4.0.4.jar",
		"100",
	}, out.container.Args)
}

func TestDriverKubernetesCredentialsFeatureStep(t *testing.T) {
	t.Run("unset leaves both fields empty", func(t *testing.T) {
		conf, err := newDriverConf(minimalApp(), BuildOptions{})
		require.NoError(t, err)
		out := newDriverKubernetesCredentialsFeatureStep(conf).configurePod(initialSparkPod())
		assert.Empty(t, out.pod.Spec.ServiceAccountName)
		assert.Empty(t, out.pod.Spec.DeprecatedServiceAccount)
	})

	t.Run("set populates both the modern and deprecated fields", func(t *testing.T) {
		app := minimalApp()
		app.Spec.Driver = v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{
			ServiceAccount: ptr.To("spark"),
		}}
		conf, err := newDriverConf(app, BuildOptions{})
		require.NoError(t, err)
		out := newDriverKubernetesCredentialsFeatureStep(conf).configurePod(initialSparkPod())
		assert.Equal(t, "spark", out.pod.Spec.ServiceAccountName)
		assert.Equal(t, "spark", out.pod.Spec.DeprecatedServiceAccount)
	})
}

func TestAttachConfVolume(t *testing.T) {
	conf, err := newDriverConf(minimalApp(), BuildOptions{})
	require.NoError(t, err)

	out := attachConfVolume(initialSparkPod(), conf)

	require.Len(t, out.container.Env, 1)
	assert.Equal(t, corev1.EnvVar{Name: envSparkConfDir, Value: sparkConfDirPath}, out.container.Env[0])

	require.Len(t, out.container.VolumeMounts, 1)
	assert.Equal(t, confVolumeName, out.container.VolumeMounts[0].Name)
	assert.Equal(t, sparkConfDirPath, out.container.VolumeMounts[0].MountPath)

	require.Len(t, out.pod.Spec.Volumes, 1)
	vol := out.pod.Spec.Volumes[0]
	assert.Equal(t, confVolumeName, vol.Name)
	require.NotNil(t, vol.ConfigMap)
	assert.Equal(t, conf.configMapName, vol.ConfigMap.Name)
	assert.Equal(t, []corev1.KeyToPath{
		{Key: sparkPropertiesKey, Path: sparkPropertiesKey, Mode: ptr.To(confFileMode)},
	}, vol.ConfigMap.Items)
}
