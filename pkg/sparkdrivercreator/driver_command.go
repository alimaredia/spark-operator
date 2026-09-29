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

// sparkConfPath is where the conf volume's spark.properties lands in the driver
// container; DriverCommandFeatureStep points --properties-file at it.
const sparkConfPath = "/opt/spark/conf/spark.properties"

// driverCommandFeatureStep ports the JVM branch of Spark's
// DriverCommandFeatureStep: it sets the driver container's args to the
// spark-internal "driver" launcher invocation. Python/R apps take a different
// branch and are out of scope (rejected by the deny-list), so only the JVM
// resource form is produced here.
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
	// renameMainAppResource with shouldUploadLocal=false leaves a local:/// (or
	// other non-uploadable) resource untouched, so we emit it verbatim.
	args = append(args, c.mainAppResource)
	args = append(args, c.appArgs...)

	container.Args = args

	return sparkPod{pod: pod, container: container}
}
