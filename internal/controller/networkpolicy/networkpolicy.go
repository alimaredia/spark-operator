/*
Copyright 2026 The Kubeflow authors.

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

// Package networkpolicy manages the blanket NetworkPolicy created for Spark
// operand pods (driver, executor, and Spark Connect server/executor). A single
// policy per namespace - selecting operands by the launched-by-spark-operator
// label - covers every Spark workload in that namespace, instead of one policy
// per application instance.
package networkpolicy

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

const (
	// OperandPolicyName is the fixed name of the blanket NetworkPolicy created
	// once per namespace that contains Spark operand pods.
	OperandPolicyName = "spark-operator-operands"

	// sparkConnectGRPCPort is the Spark Connect gRPC server port.
	sparkConnectGRPCPort = 15002
)

// Ensure creates or updates the blanket operand NetworkPolicy in the given
// namespace and records owner as one of its owners.
//
// A single policy selects all operand pods via the launched-by-spark-operator
// label, so it covers every SparkApplication / SparkConnect driver, executor,
// and server pod in the namespace. Each workload that has operands in the
// namespace is added as a non-controller owner reference, so the Kubernetes
// garbage collector deletes the policy automatically once the last such
// workload in the namespace is removed - no explicit last-app cleanup needed.
//
// Ensure is idempotent: re-adding an existing owner is a no-op, and the spec is
// static, so a steady-state namespace issues no writes.
func Ensure(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, namespace string) error {
	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: OperandPolicyName, Namespace: namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, np, func() error {
		if np.Labels == nil {
			np.Labels = map[string]string{}
		}
		np.Labels[common.LabelCreatedBySparkOperator] = "true"
		np.Spec = operandPolicySpec()
		// Non-controller owner reference: a dependent may have many owners, and
		// kube GC removes the policy only once all of them are gone.
		return controllerutil.SetOwnerReference(owner, np, scheme)
	})
	return err
}

// operandPolicySpec returns the spec shared by every operand policy.
//
// Ingress: intra-Spark traffic from any operand pod in the namespace on all
// ports (Spark uses random RPC/block-manager ports), plus the Spark UI (4040)
// and Spark Connect gRPC (15002) from any pod in the same namespace. External
// UI access is via `kubectl port-forward`, which is node-sourced and always
// permitted, so it needs no rule. Egress: allow-all (operand pods reach the API
// server, DNS, and user-specified external endpoints that cannot be enumerated).
func operandPolicySpec() networkingv1.NetworkPolicySpec {
	tcp := corev1.ProtocolTCP
	uiPort := intstr.FromInt(int(common.DefaultSparkWebUIPort))
	grpcPort := intstr.FromInt(sparkConnectGRPCPort)
	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{
			MatchLabels: map[string]string{common.LabelLaunchedBySparkOperator: "true"},
		},
		PolicyTypes: []networkingv1.PolicyType{
			networkingv1.PolicyTypeIngress,
			networkingv1.PolicyTypeEgress,
		},
		Ingress: []networkingv1.NetworkPolicyIngressRule{
			{
				// Intra-Spark: any operand pod in the namespace, all ports.
				From: []networkingv1.NetworkPolicyPeer{
					{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{common.LabelLaunchedBySparkOperator: "true"}}},
				},
			},
			{
				// Consumer-facing ports from any pod in the same namespace.
				From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}},
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &tcp, Port: &uiPort},
					{Protocol: &tcp, Port: &grpcPort},
				},
			},
		},
		Egress: []networkingv1.NetworkPolicyEgressRule{{}}, // allow-all
	}
}
