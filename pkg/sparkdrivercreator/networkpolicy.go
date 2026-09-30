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
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	networkPolicyAPIVersion = "networking.k8s.io/v1"
	networkPolicyKind       = "NetworkPolicy"

	// driverNetworkPolicySuffix is appended to the resource-name prefix to form
	// the NetworkPolicy name, mirroring Spark's NetworkPolicyFeatureStep
	// ("<resourceNamePrefix>-policy"). It carries the prefix — like the driver
	// Service — so the oracle normalizes it away.
	driverNetworkPolicySuffix = "-policy"

	// sparkRoleExecutor is Spark's spark-role label value for executor pods. The
	// NetworkPolicy targets executors (unlike the driver-role resources), so it is
	// defined here rather than alongside sparkRoleDriver.
	sparkRoleExecutor = "executor"
)

// buildDriverNetworkPolicy ports Spark's NetworkPolicyFeatureStep (SPARK-55653):
// a NetworkPolicy that restricts ingress to the application's executor pods to
// traffic originating from pods that share the same spark-app-selector. Spark
// creates it on every submit starting in 4.2.0.
//
// It is version-gated: the user requires it to appear ONLY when the
// SparkApplication's declared spec.sparkVersion produces it, so this returns nil
// for earlier supported versions (4.0.4), keeping their output byte-for-byte
// unchanged. See versionCreatesDriverNetworkPolicy.
//
// Like the driver Service/ConfigMap, it is owned by the driver pod with the uid
// left unset: the pod's uid is server-assigned at creation time, so the submitter
// adapter fills it in after creating the pod (mirroring Spark's Client.run ->
// addOwnerReference). The oracle normalizes the uid on both sides.
func buildDriverNetworkPolicy(conf *driverConf) *networkingv1.NetworkPolicy {
	if !versionCreatesDriverNetworkPolicy(conf.sparkVersion) {
		return nil
	}
	return &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{APIVersion: networkPolicyAPIVersion, Kind: networkPolicyKind},
		ObjectMeta: metav1.ObjectMeta{
			Name: conf.resourceNamePrefix + driverNetworkPolicySuffix,
			// Spark's NetworkPolicyFeatureStep stamps the namespace into the object
			// body (like the driver ConfigMap, unlike the driver Service). Mirror it
			// so the golden matches; the submitter adapter still applies into it.
			Namespace: conf.namespace,
			Labels: map[string]string{
				labelSparkAppSelector: conf.appID,
			},
			OwnerReferences: driverPodOwnerReference(conf),
		},
		Spec: networkingv1.NetworkPolicySpec{
			// Applies to this application's executor pods.
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					labelSparkAppSelector: conf.appID,
					labelSparkRole:        sparkRoleExecutor,
				},
			},
			// Allow ingress only from pods of the same application (driver and peer
			// executors). PolicyTypes is left unset: with an ingress rule present,
			// Kubernetes defaults it to ["Ingress"], matching what Spark emits.
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{
						{
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									labelSparkAppSelector: conf.appID,
								},
							},
						},
					},
				},
			},
		},
	}
}
