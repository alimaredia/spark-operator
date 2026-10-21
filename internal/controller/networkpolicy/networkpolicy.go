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

// Package networkpolicy manages the NetworkPolicies created for Spark operand
// pods (driver, executor, and Spark Connect server/executor).
//
// Two policies are shared per namespace (one object each, selected by label):
//   - an egress allow-all policy covering every operand pod, and
//   - an ingress policy covering the non-executor pods (driver, Connect server)
//     for intra-Spark traffic and the Spark UI / Connect gRPC consumer ports.
//
// Executor ingress is handled by a per-workload policy that matches Spark's
// NetworkPolicyFeatureStep (SPARK-55653): executor ingress is restricted to
// pods of the same application. Keeping executors on a per-app ingress policy
// (rather than the namespace blanket) means that when Spark 4.2+ creates its own
// executor policy, the two are identical same-app rules - so they do not
// additively widen each other's isolation.
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
	// OperandEgressPolicyName is the per-namespace egress policy covering all operand pods.
	OperandEgressPolicyName = "spark-operator-operand-egress"
	// OperandIngressPolicyName is the per-namespace ingress policy covering the
	// non-executor operand pods (driver, Connect server).
	OperandIngressPolicyName = "spark-operator-operand-ingress"

	// executorPolicySuffix is appended to the workload name for its executor policy.
	executorPolicySuffix = "-executor"

	// driverUIPolicySuffix is appended to the app name for the per-app driver UI
	// policy created only when the app overrides spark.ui.port.
	driverUIPolicySuffix = "-driver-ui"
)

// EnsureForSparkApplication converges the operand NetworkPolicies for a
// SparkApplication: the shared egress and non-executor ingress policies (owned
// by the app), plus a per-app executor ingress policy keyed on the Spark
// application ID - identical to Spark's NetworkPolicyFeatureStep. appID is
// app.Status.SparkApplicationID; when empty (driver not yet observed) the
// executor policy is skipped and created on a later reconcile. Covers
// SparkApplications created by a ScheduledSparkApplication too, since those
// reconcile through the SparkApplication controller.
//
// uiTargetPort is the driver's actual Spark UI port (from spark.ui.port). The
// shared ingress blanket only admits the default 4040, so when the app overrides
// the port a small per-app policy is created to admit the custom UI/metrics port.
func EnsureForSparkApplication(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, appID string, uiTargetPort int32) error {
	if err := ensureSharedPolicies(ctx, c, scheme, owner); err != nil {
		return err
	}
	// Custom UI port override: the blanket ingress admits only DefaultSparkWebUIPort.
	if uiTargetPort != common.DefaultSparkWebUIPort {
		if err := ensurePolicy(ctx, c, scheme, owner, owner.GetName()+driverUIPolicySuffix,
			driverUIIngressSpec(owner.GetName(), uiTargetPort)); err != nil {
			return err
		}
	}
	if appID == "" {
		return nil
	}
	return ensurePolicy(ctx, c, scheme, owner, owner.GetName()+executorPolicySuffix,
		executorIngressSpec(common.LabelSparkApplicationSelector, appID))
}

// EnsureForSparkConnect converges the operand NetworkPolicies for a SparkConnect:
// the shared egress and non-executor ingress policies (owned by the SparkConnect),
// plus a per-Connect executor ingress policy keyed on the connect-name label.
// (connect-name selects the same pod set as the server's spark-app-selector, so
// it is equivalent to SPARK-55653 for the Connect app and does not widen it.)
func EnsureForSparkConnect(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object) error {
	if err := ensureSharedPolicies(ctx, c, scheme, owner); err != nil {
		return err
	}
	return ensurePolicy(ctx, c, scheme, owner, owner.GetName()+executorPolicySuffix,
		executorIngressSpec(common.LabelSparkConnectName, owner.GetName()))
}

// ensureSharedPolicies converges the two per-namespace blanket policies and adds
// owner as one of their owners (kube GC removes them when the last owner in the
// namespace is deleted).
func ensureSharedPolicies(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object) error {
	if err := ensurePolicy(ctx, c, scheme, owner, OperandEgressPolicyName, egressSpec()); err != nil {
		return err
	}
	return ensurePolicy(ctx, c, scheme, owner, OperandIngressPolicyName, nonExecutorIngressSpec())
}

// ensurePolicy creates or updates the named NetworkPolicy in the owner's
// namespace with the given spec, recording owner as a non-controller owner. It
// is idempotent: unchanged spec + already-present owner => no write.
func ensurePolicy(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, name string, spec networkingv1.NetworkPolicySpec) error {
	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: owner.GetNamespace()},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, np, func() error {
		if np.Labels == nil {
			np.Labels = map[string]string{}
		}
		np.Labels[common.LabelCreatedBySparkOperator] = "true"
		np.Spec = spec
		return controllerutil.SetOwnerReference(owner, np, scheme)
	})
	return err
}

// egressSpec: allow-all egress for every operand pod (operands reach the API
// server, DNS, and external data/dependency endpoints that cannot be enumerated).
func egressSpec() networkingv1.NetworkPolicySpec {
	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{
			MatchLabels: map[string]string{common.LabelLaunchedBySparkOperator: "true"},
		},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
		Egress:      []networkingv1.NetworkPolicyEgressRule{{}},
	}
}

// nonExecutorIngressSpec: ingress for the driver and Connect server pods -
// intra-Spark traffic from any operand in the namespace (so executors can reach
// them, on Spark's random RPC/block-manager ports), plus the Spark UI (4040,
// which also serves driver metrics) and Spark Connect gRPC (15002) from any
// namespace. Executors are intentionally excluded - their ingress is the per-app
// policy from executorIngressSpec.
func nonExecutorIngressSpec() networkingv1.NetworkPolicySpec {
	tcp := corev1.ProtocolTCP
	uiPort := intstr.FromInt(int(common.DefaultSparkWebUIPort))
	grpcPort := intstr.FromInt(common.SparkConnectServerPort)
	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{
			MatchLabels: map[string]string{common.LabelLaunchedBySparkOperator: "true"},
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      common.LabelSparkRole,
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{common.SparkRoleDriver, common.SparkRoleConnectServer},
			}},
		},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		Ingress: []networkingv1.NetworkPolicyIngressRule{
			{
				From: []networkingv1.NetworkPolicyPeer{
					{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{common.LabelLaunchedBySparkOperator: "true"}}},
				},
			},
			{
				From: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{}}},
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &tcp, Port: &uiPort},
					{Protocol: &tcp, Port: &grpcPort},
				},
			},
		},
	}
}

// executorIngressSpec: ingress to an application's executor pods restricted to
// pods of the same application, keyed on the given grouping label
// (spark-app-selector for SparkApplication, connect-name for SparkConnect).
// Ingress-only, matching Spark's NetworkPolicyFeatureStep (SPARK-55653);
// executor egress is covered by the shared egress policy.
func executorIngressSpec(groupLabelKey, groupLabelValue string) networkingv1.NetworkPolicySpec {
	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{
			MatchLabels: map[string]string{
				groupLabelKey:         groupLabelValue,
				common.LabelSparkRole: common.SparkRoleExecutor,
			},
		},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		Ingress: []networkingv1.NetworkPolicyIngressRule{
			{
				From: []networkingv1.NetworkPolicyPeer{
					{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{groupLabelKey: groupLabelValue}}},
				},
			},
		},
	}
}

// driverUIIngressSpec admits a SparkApplication driver's non-default Spark UI
// port (which also serves driver metrics) from any namespace, mirroring the
// blanket ingress rule for the default 4040. Selects the app's driver by the
// operator-owned app-name label.
func driverUIIngressSpec(appName string, uiPort int32) networkingv1.NetworkPolicySpec {
	tcp := corev1.ProtocolTCP
	port := intstr.FromInt(int(uiPort))
	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{
			MatchLabels: map[string]string{
				common.LabelSparkAppName: appName,
				common.LabelSparkRole:    common.SparkRoleDriver,
			},
		},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		Ingress: []networkingv1.NetworkPolicyIngressRule{
			{
				From:  []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{}}},
				Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &port}},
			},
		},
	}
}
