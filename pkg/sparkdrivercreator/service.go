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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	serviceAPIVersion = "v1"
	serviceKind       = "Service"

	// Defaults from Spark's Config.scala for the driver headless service.
	driverServiceIPFamilyPolicy = "SingleStack"
	driverServiceIPFamily       = "IPv4"
)

// buildDriverService ports Spark's DriverServiceFeatureStep.getAdditionalKubernetesResources:
// the headless (clusterIP=None) service that gives the driver a stable hostname
// executors resolve. It carries the spark-app-selector label, selects the driver
// pod by the four preset labels, and exposes the resolved driver ports.
//
// The owner reference is set to the driver pod, but WITHOUT a uid: the pod's uid
// is assigned by the API server at creation time, so the submitter adapter fills
// it in after creating the pod (exactly as Spark's Client.run does via
// addOwnerReference). The oracle normalizes the uid on both sides.
func buildDriverService(conf *driverConf) *corev1.Service {
	ipFamilyPolicy := corev1.IPFamilyPolicy(driverServiceIPFamilyPolicy)

	svc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: serviceAPIVersion, Kind: serviceKind},
		ObjectMeta: metav1.ObjectMeta{
			Name: conf.serviceName,
			Labels: map[string]string{
				labelSparkAppSelector: conf.appID,
			},
			OwnerReferences: driverPodOwnerReference(conf),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:      corev1.ClusterIPNone,
			IPFamilies:     []corev1.IPFamily{corev1.IPFamily(driverServiceIPFamily)},
			IPFamilyPolicy: &ipFamilyPolicy,
			Selector:       copyStringMap(conf.labels),
			Ports:          driverServicePorts(conf),
		},
	}
	return svc
}

// driverServicePorts maps the shared resolved port set to service ports. Spark
// leaves protocol unset (defaulting to TCP) and points targetPort at the same
// numeric port as the published port.
func driverServicePorts(conf *driverConf) []corev1.ServicePort {
	var ports []corev1.ServicePort
	for _, p := range conf.ports() {
		ports = append(ports, corev1.ServicePort{
			Name:       p.name,
			Port:       p.port,
			TargetPort: intstr.FromInt32(p.port),
		})
	}
	return ports
}
