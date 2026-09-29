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

// driverKubernetesCredentialsFeatureStep ports the reproducible slice of Spark's
// DriverKubernetesCredentialsFeatureStep: mounting/creating credential secrets is
// out of scope (deny-listed), so the only pod mutation left is setting the driver
// service account when one is configured (buildPodWithServiceAccount).
type driverKubernetesCredentialsFeatureStep struct {
	conf *driverConf
}

func newDriverKubernetesCredentialsFeatureStep(conf *driverConf) *driverKubernetesCredentialsFeatureStep {
	return &driverKubernetesCredentialsFeatureStep{conf: conf}
}

func (s *driverKubernetesCredentialsFeatureStep) configurePod(in sparkPod) sparkPod {
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	if s.conf.serviceAccount != "" {
		// Spark's buildPodWithServiceAccount sets both the modern field and the
		// deprecated alias (fabric8 withServiceAccount/withServiceAccountName), so
		// the pod carries both to match the golden byte-for-byte.
		pod.Spec.ServiceAccountName = s.conf.serviceAccount
		pod.Spec.DeprecatedServiceAccount = s.conf.serviceAccount
	}

	return sparkPod{pod: pod, container: container}
}
