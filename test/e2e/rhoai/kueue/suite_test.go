//go:build openshift
// +build openshift

/*
Copyright 2024 The Kubeflow authors.

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

package e2e_kueue_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

const (
	ReleaseNamespace = "spark-operator"
	PollInterval     = 1 * time.Second
	WaitTimeout      = 5 * time.Minute
)

var (
	cfg       *rest.Config
	testEnv   *envtest.Environment
	k8sClient client.Client
)

func TestKueueIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kueue + SparkApplication Integration Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("Bootstrapping Kueue test environment")

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		UseExistingCluster:    ptr.To(true),
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	Expect(v1alpha1.AddToScheme(scheme.Scheme)).NotTo(HaveOccurred())
	Expect(v1beta2.AddToScheme(scheme.Scheme)).NotTo(HaveOccurred())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	createCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	By("Ensuring namespace exists: " + ReleaseNamespace)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ReleaseNamespace}}
	err = k8sClient.Create(createCtx, ns)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace %s", ReleaseNamespace)
	}

	// The workload namespace is separate from the operator's: Spark workloads do
	// not run where the operator runs. It carries Kueue's managed label, without
	// which Kueue's webhook never intercepts the SparkApplications under test and
	// every admission assertion here would fail.
	By("Ensuring workload namespace exists: " + KueueTestNamespace)
	workloadNS := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   KueueTestNamespace,
			Labels: map[string]string{"kueue.openshift.io/managed": "true"},
		},
	}
	err = k8sClient.Create(createCtx, workloadNS)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace %s", KueueTestNamespace)
	}

	By("Kueue test environment setup complete")
})

var _ = AfterSuite(func() {
	By("Tearing down Kueue test environment")

	if testEnv != nil {
		err := testEnv.Stop()
		Expect(err).ToNot(HaveOccurred())
	}
})
