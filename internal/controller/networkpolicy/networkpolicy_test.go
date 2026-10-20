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
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
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
	return scheme
}

func app(name, uid string) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(uid)},
	}
}

func TestOperandPolicySpec(t *testing.T) {
	spec := operandPolicySpec()

	if spec.PodSelector.MatchLabels[common.LabelLaunchedBySparkOperator] != "true" {
		t.Errorf("podSelector = %+v, want launched-by-spark-operator=true", spec.PodSelector)
	}
	hasIngress, hasEgress := false, false
	for _, pt := range spec.PolicyTypes {
		hasIngress = hasIngress || pt == networkingv1.PolicyTypeIngress
		hasEgress = hasEgress || pt == networkingv1.PolicyTypeEgress
	}
	if !hasIngress || !hasEgress {
		t.Errorf("policyTypes = %v, want both Ingress and Egress", spec.PolicyTypes)
	}
	if len(spec.Ingress) != 2 {
		t.Fatalf("ingress rules = %d, want 2", len(spec.Ingress))
	}
	// Rule 0: intra-Spark, all ports.
	if len(spec.Ingress[0].Ports) != 0 ||
		spec.Ingress[0].From[0].PodSelector.MatchLabels[common.LabelLaunchedBySparkOperator] != "true" {
		t.Errorf("intra-Spark rule = %+v", spec.Ingress[0])
	}
	// Rule 1: consumer ports (UI + Connect gRPC) from same namespace.
	ports := map[int]bool{}
	for _, p := range spec.Ingress[1].Ports {
		ports[p.Port.IntValue()] = true
	}
	if !ports[int(common.DefaultSparkWebUIPort)] || !ports[sparkConnectGRPCPort] {
		t.Errorf("consumer ports = %v, want %d and %d", ports, common.DefaultSparkWebUIPort, sparkConnectGRPCPort)
	}
	// Egress allow-all: a single empty rule.
	if len(spec.Egress) != 1 || len(spec.Egress[0].To) != 0 || len(spec.Egress[0].Ports) != 0 {
		t.Errorf("egress = %+v, want single allow-all rule", spec.Egress)
	}
}

func TestEnsureCreatesAndAccumulatesOwners(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := context.Background()
	key := client.ObjectKey{Name: OperandPolicyName, Namespace: "ns"}

	// First workload creates the policy with one owner.
	if err := Ensure(ctx, c, scheme, app("app1", "uid-1"), "ns"); err != nil {
		t.Fatal(err)
	}
	np := &networkingv1.NetworkPolicy{}
	if err := c.Get(ctx, key, np); err != nil {
		t.Fatalf("policy not created: %v", err)
	}
	if len(np.OwnerReferences) != 1 {
		t.Fatalf("owners = %d, want 1", len(np.OwnerReferences))
	}

	// Second workload is added as an additional owner.
	if err := Ensure(ctx, c, scheme, app("app2", "uid-2"), "ns"); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(ctx, key, np)
	if len(np.OwnerReferences) != 2 {
		t.Fatalf("owners = %d, want 2", len(np.OwnerReferences))
	}

	// Re-ensuring an existing owner is idempotent (no duplicate).
	if err := Ensure(ctx, c, scheme, app("app1", "uid-1"), "ns"); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(ctx, key, np)
	if len(np.OwnerReferences) != 2 {
		t.Fatalf("owners after re-ensure = %d, want 2", len(np.OwnerReferences))
	}
}
