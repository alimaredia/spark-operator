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

import corev1 "k8s.io/api/core/v1"

// sparkConfPath is where the conf volume's spark.properties lands in the driver
// container; DriverCommandFeatureStep points --properties-file at it.
const sparkConfPath = "/opt/spark/conf/spark.properties"

// driverCommandFeatureStep ports Spark's DriverCommandFeatureStep: it sets the
// driver container's args to the spark-internal "driver" launcher invocation.
// The Java and Python branches share the same argument shape (the main class is
// resolved upstream — PythonRunner for Python), and the Python branch also adds
// the PySpark interpreter env vars. R apps are rejected before Build.
type driverCommandFeatureStep struct {
	conf *driverConf
}

func newDriverCommandFeatureStep(conf *driverConf) *driverCommandFeatureStep {
	return &driverCommandFeatureStep{conf: conf}
}

func (s *driverCommandFeatureStep) configurePod(in sparkPod) sparkPod {
	c := s.conf
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	// Order mirrors DriverCommandFeatureStep.configureForJava:
	//   driver [--proxy-user <user>] --properties-file <path> --class <class>
	//   <primaryResource> [appArgs...]
	args := []string{"driver"}
	if c.proxyUser != "" {
		args = append(args, "--proxy-user", c.proxyUser)
	}
	args = append(args, "--properties-file", sparkConfPath)
	args = append(args, "--class", c.mainClass)
	// renameMainAppResource leaves a local:// (or other non-uploadable) resource
	// untouched for both the Java (shouldUploadLocal=false) and Python
	// (shouldUploadLocal=true) branches, so we emit it verbatim.
	args = append(args, c.mainAppResource)
	args = append(args, c.appArgs...)

	container.Args = args

	// Python apps: configureForPython also sets the PySpark interpreter env vars,
	// skipping any that resolve to empty (KubernetesUtils.buildEnvVars drops nulls).
	if c.resourceType == resourceTypePython {
		container.Env = append(container.Env, pysparkEnvVars(c)...)
	}

	return sparkPod{pod: pod, container: container}
}

// pysparkEnvVars returns the PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON env vars for a
// Python driver, omitting any whose interpreter path is unset.
func pysparkEnvVars(c *driverConf) []corev1.EnvVar {
	var env []corev1.EnvVar
	if c.pysparkPython != "" {
		env = append(env, corev1.EnvVar{Name: envPysparkPython, Value: c.pysparkPython})
	}
	if c.pysparkDriverPython != "" {
		env = append(env, corev1.EnvVar{Name: envPysparkDriverPython, Value: c.pysparkDriverPython})
	}
	return env
}
