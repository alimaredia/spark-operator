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
	"k8s.io/utils/ptr"
)

const (
	configMapAPIVersion = "v1"
	configMapKind       = "ConfigMap"
)

// buildDriverConfigMap ports the inline ConfigMap assembly in
// KubernetesClientApplication.run: it serializes the resolved system properties
// into spark.properties (via KubernetesClientUtils.buildStringFromPropertiesMap)
// and adds the namespace as its own data key (KUBERNETES_NAMESPACE), then wraps
// them in an immutable ConfigMap owned by the driver pod.
//
// Like the Service, the owner reference carries no uid — see
// driverPodOwnerReference. The ConfigMap name is randomized (normalized away in
// the oracle).
func buildDriverConfigMap(conf *driverConf) *corev1.ConfigMap {
	props := buildSystemProperties(conf)
	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: configMapAPIVersion, Kind: configMapKind},
		ObjectMeta: metav1.ObjectMeta{
			Name:            conf.configMapName,
			Namespace:       conf.namespace,
			OwnerReferences: driverPodOwnerReference(conf),
		},
		Immutable: ptr.To(true),
		Data: map[string]string{
			confNamespace:      conf.namespace,
			sparkPropertiesKey: serializeProperties(conf.configMapName, props),
		},
	}
}
