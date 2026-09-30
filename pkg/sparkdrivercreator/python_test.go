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

// pythonApp returns a minimal cluster-mode PySpark application: type Python and
// no main class (Spark forces PythonRunner for k8s cluster Python apps).
func pythonApp() *v1beta2.SparkApplication {
	app := minimalApp()
	app.Spec.Type = v1beta2.SparkApplicationTypePython
	app.Spec.MainClass = nil
	app.Spec.MainApplicationFile = ptr.To("local:///opt/spark/examples/src/main/python/pi.py")
	return app
}

func TestResolveResourceType(t *testing.T) {
	cases := []struct {
		appType v1beta2.SparkApplicationType
		want    string
		wantErr bool
	}{
		{v1beta2.SparkApplicationTypeScala, resourceTypeJava, false},
		{v1beta2.SparkApplicationTypeJava, resourceTypeJava, false},
		{"", resourceTypeJava, false}, // unset defaults to java
		{v1beta2.SparkApplicationTypePython, resourceTypePython, false},
		{v1beta2.SparkApplicationTypeR, "", true}, // SparkR deprecated upstream; permanently unsupported
		{"Elixir", "", true},                      // unknown
	}
	for _, tc := range cases {
		t.Run(string(tc.appType), func(t *testing.T) {
			got, err := resolveResourceType(tc.appType)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestResolveDefaultOverheadFactor(t *testing.T) {
	assert.Equal(t, 0.1, resolveDefaultOverheadFactor(resourceTypeJava, nil))
	assert.Equal(t, 0.4, resolveDefaultOverheadFactor(resourceTypePython, nil))
	// A user override wins for either app type.
	conf := map[string]string{confMemoryOverheadFactor: "0.25"}
	assert.Equal(t, 0.25, resolveDefaultOverheadFactor(resourceTypeJava, conf))
	assert.Equal(t, 0.25, resolveDefaultOverheadFactor(resourceTypePython, conf))
}

// TestNewDriverConf_Python pins the Python resolution: PythonRunner main class,
// python resource type, and the non-JVM 0.4 overhead factor (which drives a
// larger driver memory request than the JVM default).
func TestNewDriverConf_Python(t *testing.T) {
	conf, err := newDriverConf(pythonApp(), BuildOptions{})
	require.NoError(t, err)

	assert.Equal(t, resourceTypePython, conf.resourceType)
	assert.Equal(t, pythonRunnerMainClass, conf.mainClass)
	assert.Equal(t, "local:///opt/spark/examples/src/main/python/pi.py", conf.mainAppResource)
	assert.Equal(t, 0.4, conf.memoryOverheadFactor)
	// 1g default + max(0.4*1024=409, 384) = 1024 + 409 = 1433Mi.
	assert.Equal(t, int64(409), conf.memoryOverheadMiB)
	assert.Equal(t, int64(1433), conf.memoryWithOverheadMiB)
	// No interpreter confs -> no PySpark env resolved.
	assert.Empty(t, conf.pysparkPython)
	assert.Empty(t, conf.pysparkDriverPython)
}

func TestBuild_PythonApp_ConfigMapAndArgs(t *testing.T) {
	res, err := New().Build(pythonApp(), BuildOptions{})
	require.NoError(t, err)

	props := res.ConfigMap.Data[sparkPropertiesKey]
	assert.Contains(t, props, "\nspark.kubernetes.resource.type=python\n")
	assert.Contains(t, props, "\nspark.kubernetes.memoryOverheadFactor=0.4\n")
	// The primary .py resource is NOT auto-added to spark.jars: SparkSubmit only
	// adds the primary resource to spark.jars for JVM apps ("for python and R files,
	// the primary resource is already distributed as a regular file"). See the
	// sparkpi-python oracle golden, which has no spark.jars line.
	assert.NotContains(t, props, "\nspark.jars=")

	// The driver launches via PythonRunner against the .py primary resource.
	container := res.Pod.Spec.Containers[0]
	assert.Equal(t, []string{
		"driver",
		"--properties-file", sparkConfPath,
		"--class", pythonRunnerMainClass,
		"local:///opt/spark/examples/src/main/python/pi.py",
	}, container.Args)

	// No interpreter conf set -> no PYSPARK_* env vars.
	for _, e := range container.Env {
		assert.NotEqual(t, envPysparkPython, e.Name)
		assert.NotEqual(t, envPysparkDriverPython, e.Name)
	}
}

// TestDriverCommandFeatureStep_PythonEnv verifies the interpreter env vars are
// emitted from conf, with PYSPARK_DRIVER_PYTHON falling back to spark.pyspark.python.
func TestDriverCommandFeatureStep_PythonEnv(t *testing.T) {
	app := pythonApp()
	app.Spec.SparkConf[confPysparkPython] = "python3"
	conf, err := newDriverConf(app, BuildOptions{})
	require.NoError(t, err)

	out := newDriverCommandFeatureStep(conf).configurePod(initialSparkPod())

	assert.Contains(t, out.container.Env, corev1.EnvVar{Name: envPysparkPython, Value: "python3"})
	// driver.python unset -> falls back to spark.pyspark.python.
	assert.Contains(t, out.container.Env, corev1.EnvVar{Name: envPysparkDriverPython, Value: "python3"})
}

// TestBuild_PythonEnvOrder pins that a Python app's PYSPARK_* env vars precede
// SPARK_LOCAL_DIRS, matching Spark's feature-step order (DriverCommand before
// LocalDirs).
func TestBuild_PythonEnvOrder(t *testing.T) {
	app := pythonApp()
	app.Spec.SparkConf[confPysparkPython] = "python3"
	res, err := New().Build(app, BuildOptions{})
	require.NoError(t, err)

	container := res.Pod.Spec.Containers[0]
	pysparkIdx, localDirsIdx := -1, -1
	for i, e := range container.Env {
		switch e.Name {
		case envPysparkPython:
			pysparkIdx = i
		case envSparkLocalDirs:
			localDirsIdx = i
		}
	}
	require.NotEqual(t, -1, pysparkIdx, "PYSPARK_PYTHON env must be present")
	require.NotEqual(t, -1, localDirsIdx, "SPARK_LOCAL_DIRS env must be present")
	assert.Less(t, pysparkIdx, localDirsIdx, "PYSPARK_* must precede SPARK_LOCAL_DIRS")
}

func TestNewDriverConf_RejectsR(t *testing.T) {
	app := minimalApp()
	app.Spec.Type = v1beta2.SparkApplicationTypeR
	_, err := newDriverConf(app, BuildOptions{})
	assert.Error(t, err)
}
