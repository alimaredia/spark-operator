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

package networkpolicy

import (
	"context"
	"errors"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/features"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func app(name, uid string) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(uid)},
	}
}

func conn(name, uid string) *v1alpha1.SparkConnect {
	return &v1alpha1.SparkConnect{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(uid)},
	}
}

func TestEgressSpecIsAllowAll(t *testing.T) {
	spec := egressSpec()
	if spec.PodSelector.MatchLabels[common.LabelLaunchedBySparkOperator] != "true" {
		t.Errorf("egress podSelector = %+v, want launched-by-spark-operator=true", spec.PodSelector)
	}
	if len(spec.PolicyTypes) != 1 || spec.PolicyTypes[0] != networkingv1.PolicyTypeEgress {
		t.Errorf("egress policyTypes = %v, want [Egress]", spec.PolicyTypes)
	}
	if len(spec.Egress) != 1 || len(spec.Egress[0].To) != 0 || len(spec.Egress[0].Ports) != 0 {
		t.Errorf("egress = %+v, want single allow-all rule", spec.Egress)
	}
}

func TestNonExecutorIngressSpec(t *testing.T) {
	spec := nonExecutorIngressSpec()

	// Selects driver + connect-server, not executors.
	if len(spec.PodSelector.MatchExpressions) != 1 {
		t.Fatalf("podSelector matchExpressions = %+v, want one spark-role In rule", spec.PodSelector.MatchExpressions)
	}
	req := spec.PodSelector.MatchExpressions[0]
	if req.Key != common.LabelSparkRole || req.Operator != metav1.LabelSelectorOpIn {
		t.Errorf("podSelector expr = %+v", req)
	}
	wantRoles := map[string]bool{common.SparkRoleDriver: true, common.SparkRoleConnectServer: true}
	for _, v := range req.Values {
		if !wantRoles[v] {
			t.Errorf("unexpected role %q in selector", v)
		}
	}
	if len(req.Values) != 2 {
		t.Errorf("roles = %v, want driver and connect-server", req.Values)
	}

	if len(spec.PolicyTypes) != 1 || spec.PolicyTypes[0] != networkingv1.PolicyTypeIngress {
		t.Errorf("policyTypes = %v, want [Ingress]", spec.PolicyTypes)
	}
	// Only the intra-Spark rule: all ports, from any operand in the namespace.
	// No consumer-port rule (UI/gRPC are user-managed).
	if len(spec.Ingress) != 1 {
		t.Fatalf("ingress rules = %d, want 1 (intra-Spark only)", len(spec.Ingress))
	}
	if len(spec.Ingress[0].Ports) != 0 ||
		spec.Ingress[0].From[0].PodSelector.MatchLabels[common.LabelLaunchedBySparkOperator] != "true" {
		t.Errorf("intra-Spark rule = %+v", spec.Ingress[0])
	}
	for _, peer := range spec.Ingress[0].From {
		if peer.NamespaceSelector != nil {
			t.Errorf("ingress must not admit cross-namespace peers, got: %+v", peer)
		}
	}
}

// TestExecutorIngressSpecMatchesSparkFeatureStep verifies the executor policy is
// the SPARK-55653 shape: same-app ingress only, no egress, no port restriction.
func TestExecutorIngressSpecMatchesSparkFeatureStep(t *testing.T) {
	spec := executorIngressSpec(common.LabelSparkApplicationSelector, "spark-abc123")

	if spec.PodSelector.MatchLabels[common.LabelSparkApplicationSelector] != "spark-abc123" ||
		spec.PodSelector.MatchLabels[common.LabelSparkRole] != common.SparkRoleExecutor {
		t.Errorf("podSelector = %+v, want spark-app-selector + spark-role=executor", spec.PodSelector)
	}
	if len(spec.PolicyTypes) != 1 || spec.PolicyTypes[0] != networkingv1.PolicyTypeIngress {
		t.Errorf("policyTypes = %v, want [Ingress] only (no Egress)", spec.PolicyTypes)
	}
	if len(spec.Egress) != 0 {
		t.Errorf("egress = %+v, want none (ingress-only)", spec.Egress)
	}
	if len(spec.Ingress) != 1 || len(spec.Ingress[0].Ports) != 0 {
		t.Fatalf("ingress = %+v, want one port-less rule", spec.Ingress)
	}
	if spec.Ingress[0].From[0].PodSelector.MatchLabels[common.LabelSparkApplicationSelector] != "spark-abc123" {
		t.Errorf("ingress from = %+v, want same-app spark-app-selector", spec.Ingress[0].From)
	}
}

func TestEnsureForSparkApplicationCreatesPoliciesAndAccumulatesOwners(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.OperandNetworkPolicy, true)
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := context.Background()

	// First app: creates the shared policies + its executor policy.
	if err := EnsureForSparkApplication(ctx, c, scheme, app("app1", "uid-1"), "spark-app1"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{OperandEgressPolicyName, OperandIngressPolicyName, "app1" + executorPolicySuffix} {
		np := &networkingv1.NetworkPolicy{}
		if err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: "ns"}, np); err != nil {
			t.Fatalf("policy %s not created: %v", name, err)
		}
		if len(np.OwnerReferences) != 1 {
			t.Errorf("policy %s owners = %d, want 1", name, len(np.OwnerReferences))
		}
	}

	// Second app: shares the blanket policies (2 owners) but gets its own executor policy.
	if err := EnsureForSparkApplication(ctx, c, scheme, app("app2", "uid-2"), "spark-app2"); err != nil {
		t.Fatal(err)
	}
	shared := &networkingv1.NetworkPolicy{}
	_ = c.Get(ctx, client.ObjectKey{Name: OperandEgressPolicyName, Namespace: "ns"}, shared)
	if len(shared.OwnerReferences) != 2 {
		t.Errorf("shared egress owners = %d, want 2", len(shared.OwnerReferences))
	}
	exec2 := &networkingv1.NetworkPolicy{}
	if err := c.Get(ctx, client.ObjectKey{Name: "app2" + executorPolicySuffix, Namespace: "ns"}, exec2); err != nil {
		t.Errorf("app2 executor policy not created: %v", err)
	}
}

func TestEnsureForSparkApplicationSkipsExecutorPolicyWithoutAppID(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.OperandNetworkPolicy, true)
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := context.Background()

	if err := EnsureForSparkApplication(ctx, c, scheme, app("app1", "uid-1"), ""); err != nil {
		t.Fatal(err)
	}
	// Shared policies exist; executor policy is skipped until the appID is known.
	if err := c.Get(ctx, client.ObjectKey{Name: OperandEgressPolicyName, Namespace: "ns"}, &networkingv1.NetworkPolicy{}); err != nil {
		t.Errorf("shared egress should exist: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Name: "app1" + executorPolicySuffix, Namespace: "ns"}, &networkingv1.NetworkPolicy{}); err == nil {
		t.Error("executor policy should not be created without an appID")
	}
}

func TestEnsureForSparkConnectCreatesPolicies(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.OperandNetworkPolicy, true)
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := context.Background()

	if err := EnsureForSparkConnect(ctx, c, scheme, conn("conn1", "uid-1")); err != nil {
		t.Fatal(err)
	}

	// Shared blanket policies + the per-Connect executor policy are created, each owned by the SparkConnect.
	for _, name := range []string{OperandEgressPolicyName, OperandIngressPolicyName, "conn1" + executorPolicySuffix} {
		np := &networkingv1.NetworkPolicy{}
		if err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: "ns"}, np); err != nil {
			t.Fatalf("policy %s not created: %v", name, err)
		}
		if len(np.OwnerReferences) != 1 || np.OwnerReferences[0].Kind != "SparkConnect" {
			t.Errorf("policy %s owners = %+v, want one SparkConnect owner", name, np.OwnerReferences)
		}
	}

	// The executor policy is keyed on connect-name (not spark-app-selector).
	exec := &networkingv1.NetworkPolicy{}
	_ = c.Get(ctx, client.ObjectKey{Name: "conn1" + executorPolicySuffix, Namespace: "ns"}, exec)
	if exec.Spec.PodSelector.MatchLabels[common.LabelSparkConnectName] != "conn1" ||
		exec.Spec.PodSelector.MatchLabels[common.LabelSparkRole] != common.SparkRoleExecutor {
		t.Errorf("executor podSelector = %+v, want connect-name=conn1 + spark-role=executor", exec.Spec.PodSelector)
	}
	if exec.Spec.Ingress[0].From[0].PodSelector.MatchLabels[common.LabelSparkConnectName] != "conn1" {
		t.Errorf("executor ingress from = %+v, want connect-name=conn1", exec.Spec.Ingress[0].From)
	}
}

// failingCreateClient returns a client whose Create always fails, to exercise
// the error-return paths.
func failingCreateClient(t *testing.T, scheme *runtime.Scheme) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
			return errors.New("boom")
		},
	}).Build()
}

func TestEnsureForSparkApplicationReturnsErrorOnFailure(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.OperandNetworkPolicy, true)
	scheme := newScheme(t)
	c := failingCreateClient(t, scheme)
	if err := EnsureForSparkApplication(context.Background(), c, scheme, app("app1", "uid-1"), "spark-app1"); err == nil {
		t.Fatal("expected an error when policy creation fails")
	}
}

func TestEnsureForSparkConnectReturnsErrorOnFailure(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.OperandNetworkPolicy, true)
	scheme := newScheme(t)
	c := failingCreateClient(t, scheme)
	if err := EnsureForSparkConnect(context.Background(), c, scheme, conn("conn1", "uid-1")); err == nil {
		t.Fatal("expected an error when policy creation fails")
	}
}

// TestEnsureNoOpWhenGateDisabled verifies the feature gate (off by default)
// makes the Ensure functions no-ops that create nothing.
func TestEnsureNoOpWhenGateDisabled(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.OperandNetworkPolicy, false)
	scheme := newScheme(t)
	ctx := context.Background()

	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	if err := EnsureForSparkApplication(ctx, c, scheme, app("app1", "uid-1"), "spark-app1"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureForSparkConnect(ctx, c, scheme, conn("conn1", "uid-1")); err != nil {
		t.Fatal(err)
	}
	nps := &networkingv1.NetworkPolicyList{}
	if err := c.List(ctx, nps); err != nil {
		t.Fatal(err)
	}
	if len(nps.Items) != 0 {
		t.Fatalf("expected no NetworkPolicies when the gate is disabled, got %d", len(nps.Items))
	}
}
