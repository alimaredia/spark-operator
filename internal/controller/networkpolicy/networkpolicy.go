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
// The operator ships only the allows required for a Spark job to run:
//   - a per-namespace egress allow-all policy covering every operand pod, and
//   - a per-namespace ingress policy covering the non-executor pods (driver,
//     Connect server) for intra-Spark traffic (executors reaching the driver /
//     server on Spark's random RPC/block-manager ports), and
//   - a per-workload executor ingress policy restricting executor ingress to
//     pods of the same application - identical to Spark's NetworkPolicyFeatureStep
//     (SPARK-55653), so when Spark 4.2+ creates its own executor policy the two
//     are the same same-app rule and do not additively widen each other.
//
// Consumer-facing ingress (Spark UI, Spark Connect gRPC, metrics scraping from
// other namespaces) is intentionally NOT opened by the operator: selecting the
// pods with the policies above denies all other ingress by default, and cluster
// admins / users expose those ports with their own additive NetworkPolicy in the
// workload namespace (the same way they provision RBAC there). kubectl
// port-forward keeps working without any extra policy - it is node-sourced into
// the pod's own netns, which NetworkPolicy does not govern - as does
// same-namespace operand traffic. A non-operand pod in the same namespace, such
// as a notebook or a Spark Connect client, is denied on 4040/15002 until such a
// policy is applied.
package networkpolicy

import (
	"context"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/features"
)

const (
	// OperandEgressPolicyName is the per-namespace egress policy covering all operand pods.
	OperandEgressPolicyName = "spark-operator-operand-egress"
	// OperandIngressPolicyName is the per-namespace ingress policy covering the
	// non-executor operand pods (driver, Connect server).
	OperandIngressPolicyName = "spark-operator-operand-ingress"

	// sparkApplicationExecutorPolicySuffix and sparkConnectExecutorPolicySuffix
	// are appended to the workload name for its executor policy. The suffix is
	// per-kind because a SparkApplication and a SparkConnect may share a name in
	// one namespace: with a single shared suffix both reconcilers converge the
	// same object with different specs (spark-app-selector vs connect-name),
	// overwriting each other on every pass. Since nonExecutorIngressSpec excludes
	// executors, whichever workload currently loses has its executor pods
	// selected by no policy at all - silently fail-open, with no error surfaced.
	sparkApplicationExecutorPolicySuffix = "-sparkapp-executor"
	sparkConnectExecutorPolicySuffix     = "-sparkconnect-executor"
)

// EnsureForSparkApplication converges the operand NetworkPolicies for a
// SparkApplication: the shared egress and non-executor ingress policies (owned
// by the app), plus a per-app executor ingress policy keyed on the Spark
// application ID - identical to Spark's NetworkPolicyFeatureStep. appID is
// app.Status.SparkApplicationID; when empty (driver not yet observed) the
// executor policy is skipped and created on a later reconcile. Covers
// SparkApplications created by a ScheduledSparkApplication too, since those
// reconcile through the SparkApplication controller.
func EnsureForSparkApplication(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, appID string) error {
	if !features.Enabled(features.OperandNetworkPolicy) {
		return nil
	}
	if err := ensureSharedPolicies(ctx, c, scheme, owner); err != nil {
		return err
	}
	if appID == "" {
		return nil
	}
	return ensurePolicy(ctx, c, scheme, owner, owner.GetName()+sparkApplicationExecutorPolicySuffix,
		executorIngressSpec(common.LabelSparkApplicationSelector, appID))
}

// EnsureForSparkConnect converges the operand NetworkPolicies for a SparkConnect:
// the shared egress and non-executor ingress policies (owned by the SparkConnect),
// plus a per-Connect executor ingress policy keyed on the connect-name label.
// (connect-name selects the same pod set as the server's spark-app-selector, so
// it is equivalent to SPARK-55653 for the Connect app and does not widen it.)
func EnsureForSparkConnect(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object) error {
	if !features.Enabled(features.OperandNetworkPolicy) {
		return nil
	}
	if err := ensureSharedPolicies(ctx, c, scheme, owner); err != nil {
		return err
	}
	return ensurePolicy(ctx, c, scheme, owner, owner.GetName()+sparkConnectExecutorPolicySuffix,
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
// intra-Spark traffic from any operand pod in the namespace (so executors can
// reach them on Spark's random RPC/block-manager ports), all ports. Consumer
// ports (UI, Connect gRPC, metrics) are intentionally not opened here; selecting
// the pods denies all other ingress by default, and those are exposed by a
// user-managed additive policy. Executors are excluded - their ingress is the
// per-app policy from executorIngressSpec.
func nonExecutorIngressSpec() networkingv1.NetworkPolicySpec {
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
