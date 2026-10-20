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

package rhoai_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	// +kubebuilder:scaffold:imports
)

const (
	ReleaseName   = "spark-operator"
	TestNamespace = "spark-test"

	PollInterval = 1 * time.Second
	WaitTimeout  = 5 * time.Minute

	InstallMethodHelm         = "helm"
	InstallMethodKustomize    = "kustomize"
	InstallMethodPreinstalled = "preinstalled"
)

var (
	cfg              *rest.Config
	testEnv          *envtest.Environment
	k8sClient        client.Client
	clientset        *kubernetes.Clientset
	installMethod    string
	repoRoot         string
	origParamsEnv    []byte
	sparkAppImage    string
	ReleaseNamespace string

	mutatingWebhookName   string
	validatingWebhookName string
)

func TestSparkOperatorRHOAI(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Spark Operator RHOAI Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	var err error

	repoRoot = filepath.Join("..", "..", "..")

	installMethod = os.Getenv("INSTALL_METHOD")
	if installMethod == "" {
		installMethod = InstallMethodKustomize
	}
	ReleaseNamespace = os.Getenv("RELEASE_NAMESPACE")
	if ReleaseNamespace == "" {
		// The Kustomize install uses the RHOAI overlay, which stamps all
		// resources into redhat-ods-applications.
		ReleaseNamespace = "redhat-ods-applications"
	}
	sparkAppImage = os.Getenv("SPARK_APP_IMAGE")
	logf.Log.Info("Using install method", "method", installMethod)
	if sparkAppImage != "" {
		logf.Log.Info("Overriding Spark app image", "image", sparkAppImage)
	}

	switch installMethod {
	case InstallMethodKustomize, InstallMethodPreinstalled:
		mutatingWebhookName = "mutating-webhook-configuration"
		validatingWebhookName = "validating-webhook-configuration"
	default:
		mutatingWebhookName = "spark-operator-webhook"
		validatingWebhookName = "spark-operator-webhook"
	}

	By("Bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(repoRoot, "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: filepath.Join(repoRoot, "bin", "k8s",
			fmt.Sprintf("1.33.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
		UseExistingCluster: ptr.To(true),
	}

	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	Expect(v1alpha1.AddToScheme(scheme.Scheme)).NotTo(HaveOccurred())
	Expect(v1beta2.AddToScheme(scheme.Scheme)).NotTo(HaveOccurred())
	// +kubebuilder:scaffold:scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	clientset, err = kubernetes.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
	Expect(clientset).NotTo(BeNil())

	if installMethod != InstallMethodPreinstalled {
		By("Ensuring clean state for release namespace")
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ReleaseNamespace}}
		existingNs := &corev1.Namespace{}
		err = k8sClient.Get(context.TODO(), types.NamespacedName{Name: ReleaseNamespace}, existingNs)

		if err == nil {
			By(fmt.Sprintf("Namespace %s already exists, deleting stale namespace", ReleaseNamespace))
			Expect(k8sClient.Delete(context.TODO(), existingNs)).NotTo(HaveOccurred())

			By("Waiting for namespace to be fully deleted")
			Eventually(func() bool {
				err := k8sClient.Get(context.TODO(), types.NamespacedName{Name: ReleaseNamespace}, &corev1.Namespace{})
				return apierrors.IsNotFound(err)
			}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(BeTrue())

			By("Creating fresh namespace")
			Expect(k8sClient.Create(context.TODO(), namespace)).NotTo(HaveOccurred())
		} else if apierrors.IsNotFound(err) {
			By("Creating release namespace")
			Expect(k8sClient.Create(context.TODO(), namespace)).NotTo(HaveOccurred())
		} else {
			Expect(err).NotTo(HaveOccurred(), "Failed to check namespace existence")
		}
	} else {
		logf.Log.Info("Operator is preinstalled, skipping operator namespace management", "namespace", ReleaseNamespace)
	}

	switch installMethod {
	case InstallMethodKustomize:
		installWithKustomize()
	case InstallMethodPreinstalled:
		logf.Log.Info("Operator already installed, skipping install")
	default:
		installWithHelm()
	}

	ensureKindCompatibleOperatorIngress(context.Background())

	By("Waiting for the webhooks to be ready")
	mutatingWebhookKey := types.NamespacedName{Name: mutatingWebhookName}
	validatingWebhookKey := types.NamespacedName{Name: validatingWebhookName}
	Expect(waitForMutatingWebhookReady(context.Background(), mutatingWebhookKey)).NotTo(HaveOccurred())
	Expect(waitForValidatingWebhookReady(context.Background(), validatingWebhookKey)).NotTo(HaveOccurred())
	// TODO: Remove this when there is a better way to ensure the webhooks are ready before running the e2e tests.
	time.Sleep(10 * time.Second)

	if installMethod == InstallMethodKustomize || installMethod == InstallMethodPreinstalled {
		By("Patching webhook namespaceSelectors to include test namespace")
		patchWebhookNamespaceSelectors(mutatingWebhookKey, validatingWebhookKey)
	}

	By("Ensuring clean state for test namespace")
	testNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: TestNamespace}}
	existingTestNs := &corev1.Namespace{}
	err = k8sClient.Get(context.TODO(), types.NamespacedName{Name: TestNamespace}, existingTestNs)

	if err == nil {
		By(fmt.Sprintf("Test namespace %s already exists, deleting stale namespace", TestNamespace))
		Expect(k8sClient.Delete(context.TODO(), existingTestNs)).NotTo(HaveOccurred())

		By("Waiting for test namespace to be fully deleted")
		Eventually(func() bool {
			err := k8sClient.Get(context.TODO(), types.NamespacedName{Name: TestNamespace}, &corev1.Namespace{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(BeTrue())

		By("Creating fresh test namespace")
		Expect(k8sClient.Create(context.TODO(), testNamespace)).NotTo(HaveOccurred())
	} else if apierrors.IsNotFound(err) {
		By("Creating test namespace")
		Expect(k8sClient.Create(context.TODO(), testNamespace)).NotTo(HaveOccurred())
	} else {
		Expect(err).NotTo(HaveOccurred(), "Failed to check test namespace existence")
	}

	if installMethod == InstallMethodKustomize || installMethod == InstallMethodPreinstalled {
		By("Applying Spark job RBAC to test namespace")
		sparkRBACDir := filepath.Join(repoRoot, "config", "spark-rbac")
		rbacCmd := exec.Command("kubectl", "apply", "-k", sparkRBACDir, "-n", TestNamespace, "--server-side", "--force-conflicts")
		output, err := rbacCmd.CombinedOutput()
		logf.Log.Info("kubectl apply -k spark-rbac output", "output", string(output))
		Expect(err).NotTo(HaveOccurred(), "Failed to apply Spark job RBAC to test namespace")
	} else {
		By("Copying Spark service account and RBAC from operator namespace to test namespace")
		srcSA := &corev1.ServiceAccount{}
		Expect(k8sClient.Get(context.TODO(), types.NamespacedName{
			Name: "spark-operator-spark", Namespace: ReleaseNamespace,
		}, srcSA)).NotTo(HaveOccurred(), "Failed to get spark ServiceAccount from operator namespace %s", ReleaseNamespace)
		srcSA.Namespace = TestNamespace
		srcSA.ResourceVersion = ""
		srcSA.UID = ""
		srcSA.CreationTimestamp = metav1.Time{}
		srcSA.OwnerReferences = nil
		srcSA.Finalizers = nil
		srcSA.ManagedFields = nil
		srcSA.Secrets = nil
		Expect(k8sClient.Create(context.TODO(), srcSA)).NotTo(HaveOccurred())

		srcRole := &rbacv1.Role{}
		Expect(k8sClient.Get(context.TODO(), types.NamespacedName{
			Name: "spark-operator-role", Namespace: ReleaseNamespace,
		}, srcRole)).NotTo(HaveOccurred(), "Failed to get spark Role from operator namespace %s", ReleaseNamespace)
		srcRole.Namespace = TestNamespace
		srcRole.ResourceVersion = ""
		srcRole.UID = ""
		srcRole.CreationTimestamp = metav1.Time{}
		srcRole.OwnerReferences = nil
		srcRole.Finalizers = nil
		srcRole.ManagedFields = nil
		Expect(k8sClient.Create(context.TODO(), srcRole)).NotTo(HaveOccurred())

		srcRB := &rbacv1.RoleBinding{}
		Expect(k8sClient.Get(context.TODO(), types.NamespacedName{
			Name: "spark-operator-rolebinding", Namespace: ReleaseNamespace,
		}, srcRB)).NotTo(HaveOccurred(), "Failed to get spark RoleBinding from operator namespace %s", ReleaseNamespace)
		srcRB.Namespace = TestNamespace
		srcRB.ResourceVersion = ""
		srcRB.UID = ""
		srcRB.CreationTimestamp = metav1.Time{}
		srcRB.OwnerReferences = nil
		srcRB.Finalizers = nil
		srcRB.ManagedFields = nil
		for i := range srcRB.Subjects {
			if srcRB.Subjects[i].Kind == "ServiceAccount" {
				srcRB.Subjects[i].Namespace = TestNamespace
			}
		}
		Expect(k8sClient.Create(context.TODO(), srcRB)).NotTo(HaveOccurred())
	}

	// The RHOAI/ODH overlays enable operand NetworkPolicies, which leave
	// operand pods default-deny for consumer ingress. Expose the Spark UI
	// (4040) and Spark Connect gRPC (15002) to same-namespace consumers in the
	// test namespace - the namespace-enablement step a user performs alongside
	// RBAC. (Selecting by label means it works before the workload pods exist.)
	By("Creating the consumer NetworkPolicy in the test namespace")
	consumerNP := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "spark-operator-allow-consumers", Namespace: TestNamespace},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "spark-role",
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{"driver", "connect-server"},
				}},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}},
				Ports: []networkingv1.NetworkPolicyPort{
					{Port: ptr.To(intstr.FromInt(4040))},
					{Port: ptr.To(intstr.FromInt(15002))},
				},
			}},
		},
	}
	Expect(k8sClient.Create(context.TODO(), consumerNP)).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if origParamsEnv != nil {
		kustomizeDir := filepath.Join(repoRoot, "config", "overlays", "rhoai")
		paramsEnvPath := filepath.Join(kustomizeDir, "params.env")
		Expect(os.WriteFile(paramsEnvPath, origParamsEnv, 0644)).NotTo(HaveOccurred())
	}

	cleanup := os.Getenv("CLEANUP")
	if strings.EqualFold(cleanup, "false") {
		logf.Log.Info("CLEANUP=false, skipping uninstall")
	} else {
		if installMethod == InstallMethodKustomize || installMethod == InstallMethodPreinstalled {
			By("Cleaning up Spark job RBAC from test namespace")
			sparkRBACDir := filepath.Join(repoRoot, "config", "spark-rbac")
			rbacDelCmd := exec.Command("kubectl", "delete", "-k", sparkRBACDir, "-n", TestNamespace, "--ignore-not-found", "--timeout=60s")
			rbacDelCmd.Stdout = GinkgoWriter
			rbacDelCmd.Stderr = GinkgoWriter
			_ = rbacDelCmd.Run()
		}

		By("Deleting test namespace")
		testNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: TestNamespace}}
		if err := k8sClient.Delete(context.TODO(), testNamespace); err != nil {
			logf.Log.Info("Test namespace deletion skipped (may already be deleted)", "namespace", TestNamespace, "error", err)
		}

		switch installMethod {
		case InstallMethodKustomize:
			uninstallKustomize()
		case InstallMethodPreinstalled:
			logf.Log.Info("Operator was preinstalled, skipping uninstall")
		default:
			uninstallHelm()

			By("Deleting release namespace")
			namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ReleaseNamespace}}
			Expect(k8sClient.Delete(context.TODO(), namespace)).NotTo(HaveOccurred())
		}
	}

	By("Tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
})

// ---------------------------------------------------------------------------
// Helm install / uninstall
// ---------------------------------------------------------------------------

func installWithHelm() {
	By("Installing the Spark operator helm chart")
	envSettings := cli.New()
	envSettings.SetNamespace(ReleaseNamespace)
	actionConfig := &action.Configuration{}
	Expect(actionConfig.Init(envSettings.RESTClientGetter(), envSettings.Namespace(), os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) {
		logf.Log.Info(fmt.Sprintf(format, v...))
	})).NotTo(HaveOccurred())
	installAction := action.NewInstall(actionConfig)
	Expect(installAction).NotTo(BeNil())
	installAction.ReleaseName = ReleaseName
	installAction.Namespace = envSettings.Namespace()
	installAction.Wait = true
	installAction.Timeout = WaitTimeout
	chartPath := filepath.Join(repoRoot, "charts", "spark-operator-chart")
	chart, err := loader.Load(chartPath)
	Expect(err).NotTo(HaveOccurred())
	Expect(chart).NotTo(BeNil())
	values, err := chartutil.ReadValuesFile(filepath.Join(chartPath, "ci", "ci-values.yaml"))
	Expect(err).NotTo(HaveOccurred())
	Expect(values).NotTo(BeNil())
	release, err := installAction.Run(chart, values)
	Expect(err).NotTo(HaveOccurred())
	Expect(release).NotTo(BeNil())
}

func uninstallHelm() {
	By("Uninstalling the Spark operator helm chart")
	envSettings := cli.New()
	envSettings.SetNamespace(ReleaseNamespace)
	actionConfig := &action.Configuration{}
	Expect(actionConfig.Init(envSettings.RESTClientGetter(), envSettings.Namespace(), os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) {
		logf.Log.Info(fmt.Sprintf(format, v...))
	})).NotTo(HaveOccurred())
	uninstallAction := action.NewUninstall(actionConfig)
	Expect(uninstallAction).NotTo(BeNil())
	uninstallAction.Wait = true
	uninstallAction.Timeout = WaitTimeout
	resp, err := uninstallAction.Run(ReleaseName)
	Expect(err).To(BeNil())
	Expect(resp).NotTo(BeNil())
}

// ---------------------------------------------------------------------------
// Kustomize install / uninstall
// ---------------------------------------------------------------------------

func installWithKustomize() {
	By("Installing the Spark operator using Kustomize manifests")
	kustomizeDir := filepath.Join(repoRoot, "config", "overlays", "rhoai")

	operatorImage := os.Getenv("SPARK_OPERATOR_IMAGE")
	if operatorImage != "" {
		By(fmt.Sprintf("Overriding operator image to %s", operatorImage))
		overrideParamsEnvImage(kustomizeDir, operatorImage)
	}

	ensurePodMonitorCRDForKIND()

	By("Applying Kustomize manifests")
	output, err := runCommand("kubectl", "apply", "-k", kustomizeDir, "--server-side=true", "--force-conflicts")
	logf.Log.Info("kubectl apply -k output", "output", output)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply kustomize manifests: %s", output)

	By("Waiting for operator deployments to be available")
	output, err = runCommand("kubectl", "wait", "--for=condition=Available", "deployment",
		"-l", "app.kubernetes.io/name=spark-operator",
		"-n", ReleaseNamespace,
		fmt.Sprintf("--timeout=%s", WaitTimeout))
	logf.Log.Info("kubectl wait output", "output", output)
	Expect(err).NotTo(HaveOccurred(), "Deployments did not become available: %s", output)
}

func uninstallKustomize() {
	By("Uninstalling the Spark operator Kustomize manifests")
	kustomizeDir := filepath.Join(repoRoot, "config", "overlays", "rhoai")
	output, err := runCommand("kubectl", "delete", "-k", kustomizeDir, "--ignore-not-found=true")
	logf.Log.Info("kubectl delete -k output", "output", output)
	Expect(err).NotTo(HaveOccurred(), "Failed to delete kustomize manifests: %s", output)
}

// ensurePodMonitorCRDForKIND installs a minimal podmonitors.monitoring.coreos.com
// CRD on non-OpenShift clusters. The RHOAI overlay ships a PodMonitor, but KIND
// has no Prometheus Operator, so `kubectl apply -k` would otherwise fail with
// "no matches for kind PodMonitor". On OpenShift the real CRD already exists.
func ensurePodMonitorCRDForKIND() {
	if runningOnOpenShift() {
		return
	}

	By("Installing a minimal PodMonitor CRD for KIND (no Prometheus Operator present)")
	const podMonitorCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: podmonitors.monitoring.coreos.com
spec:
  group: monitoring.coreos.com
  names:
    kind: PodMonitor
    listKind: PodMonitorList
    plural: podmonitors
    singular: podmonitor
  scope: Namespaced
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          x-kubernetes-preserve-unknown-fields: true
`
	crdFile, err := os.CreateTemp("", "podmonitor-crd-*.yaml")
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = os.Remove(crdFile.Name()) }()
	_, err = crdFile.WriteString(podMonitorCRD)
	Expect(err).NotTo(HaveOccurred())
	Expect(crdFile.Close()).NotTo(HaveOccurred())

	output, err := runCommand("kubectl", "apply", "-f", crdFile.Name())
	logf.Log.Info("kubectl apply PodMonitor CRD output", "output", output)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply PodMonitor CRD: %s", output)

	output, err = runCommand("kubectl", "wait", "--for=condition=Established",
		"crd/podmonitors.monitoring.coreos.com", "--timeout=60s")
	logf.Log.Info("kubectl wait PodMonitor CRD output", "output", output)
	Expect(err).NotTo(HaveOccurred(), "PodMonitor CRD not Established: %s", output)
}

func overrideParamsEnvImage(kustomizeDir, image string) {
	paramsEnvPath := filepath.Join(kustomizeDir, "params.env")
	data, err := os.ReadFile(paramsEnvPath)
	Expect(err).NotTo(HaveOccurred())
	origParamsEnv = data

	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "SPARK_OPERATOR_CONTROLLER_IMAGE=") {
			lines[i] = "SPARK_OPERATOR_CONTROLLER_IMAGE=" + image
		}
		if strings.HasPrefix(line, "SPARK_OPERATOR_WEBHOOK_IMAGE=") {
			lines[i] = "SPARK_OPERATOR_WEBHOOK_IMAGE=" + image
		}
	}
	Expect(os.WriteFile(paramsEnvPath, []byte(strings.Join(lines, "\n")), 0644)).NotTo(HaveOccurred())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func runCommand(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func runningOnOpenShift() bool {
	groups, err := clientset.Discovery().ServerGroups()
	if err != nil {
		return false
	}
	for _, g := range groups.Groups {
		if g.Name == "route.openshift.io" {
			return true
		}
	}
	return false
}

// ensureKindCompatibleOperatorIngress adds an allow-all ingress NP for operator
// pods on non-OpenShift clusters so the metrics NP doesn't become the sole
// Ingress isolator (which would block apiserver ProxyGet on KIND).
func ensureKindCompatibleOperatorIngress(ctx context.Context) {
	if runningOnOpenShift() {
		return
	}

	metricsNP := &networkingv1.NetworkPolicy{}
	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      "spark-operator-allow-metrics",
		Namespace: ReleaseNamespace,
	}, metricsNP)
	if apierrors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	By("Applying e2e companion NetworkPolicy so KIND is not isolated by spark-operator-allow-metrics alone")
	companion := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-operator-e2e-allow-operator-ingress",
			Namespace: ReleaseNamespace,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name": "spark-operator",
				},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress:     []networkingv1.NetworkPolicyIngressRule{{}},
		},
	}
	existing := &networkingv1.NetworkPolicy{}
	err = k8sClient.Get(ctx, types.NamespacedName{
		Name: companion.Name, Namespace: companion.Namespace,
	}, existing)
	if apierrors.IsNotFound(err) {
		Expect(k8sClient.Create(ctx, companion)).NotTo(HaveOccurred())
		return
	}
	Expect(err).NotTo(HaveOccurred())
	companion.ResourceVersion = existing.ResourceVersion
	Expect(k8sClient.Update(ctx, companion)).NotTo(HaveOccurred())
}

func waitForMutatingWebhookReady(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	mutatingWebhook := admissionregistrationv1.MutatingWebhookConfiguration{}
	err := wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, &mutatingWebhook); err != nil {
			return false, err
		}

		for _, wh := range mutatingWebhook.Webhooks {
			if wh.ClientConfig.CABundle == nil {
				return false, nil
			}

			svcRef := wh.ClientConfig.Service
			if svcRef == nil {
				return false, fmt.Errorf("webhook service is nil")
			}
			endpointSliceList := discoveryv1.EndpointSliceList{}
			if err := k8sClient.List(
				ctx, &endpointSliceList, client.InNamespace(svcRef.Namespace), client.MatchingLabels{discoveryv1.LabelServiceName: svcRef.Name},
			); err != nil {
				return false, err
			}
			if len(endpointSliceList.Items) == 0 {
				return false, nil
			}
		}

		return true, nil
	})
	return err
}

func waitForValidatingWebhookReady(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	validatingWebhook := admissionregistrationv1.ValidatingWebhookConfiguration{}
	err := wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, &validatingWebhook); err != nil {
			return false, err
		}

		for _, wh := range validatingWebhook.Webhooks {
			if wh.ClientConfig.CABundle == nil {
				return false, nil
			}

			svcRef := wh.ClientConfig.Service
			if svcRef == nil {
				return false, fmt.Errorf("webhook service is nil")
			}
			endpointSliceList := discoveryv1.EndpointSliceList{}
			if err := k8sClient.List(
				ctx, &endpointSliceList, client.InNamespace(svcRef.Namespace), client.MatchingLabels{discoveryv1.LabelServiceName: svcRef.Name},
			); err != nil {
				return false, err
			}
			if len(endpointSliceList.Items) == 0 {
				return false, nil
			}
		}

		return true, nil
	})
	return err
}

func patchWebhookNamespaceSelectors(mutatingKey, validatingKey types.NamespacedName) {
	mw := &admissionregistrationv1.MutatingWebhookConfiguration{}
	Expect(k8sClient.Get(context.TODO(), mutatingKey, mw)).NotTo(HaveOccurred())
	for i := range mw.Webhooks {
		appendToNamespaceSelector(mw.Webhooks[i].NamespaceSelector, TestNamespace)
	}
	Expect(k8sClient.Update(context.TODO(), mw)).NotTo(HaveOccurred())

	vw := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	Expect(k8sClient.Get(context.TODO(), validatingKey, vw)).NotTo(HaveOccurred())
	for i := range vw.Webhooks {
		appendToNamespaceSelector(vw.Webhooks[i].NamespaceSelector, TestNamespace)
	}
	Expect(k8sClient.Update(context.TODO(), vw)).NotTo(HaveOccurred())
}

func appendToNamespaceSelector(selector *metav1.LabelSelector, namespace string) {
	if selector == nil {
		return
	}
	for i, expr := range selector.MatchExpressions {
		if expr.Key == "kubernetes.io/metadata.name" && expr.Operator == metav1.LabelSelectorOpIn {
			for _, v := range expr.Values {
				if v == namespace {
					return
				}
			}
			selector.MatchExpressions[i].Values = append(selector.MatchExpressions[i].Values, namespace)
		}
	}
}

func waitForSparkApplicationCompleted(ctx context.Context, key types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	app := &v1beta2.SparkApplication{}
	err := wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, app); err != nil {
			return false, err
		}
		switch app.Status.AppState.State {
		case v1beta2.ApplicationStateFailedSubmission, v1beta2.ApplicationStateFailed:
			return false, errors.New(app.Status.AppState.ErrorMessage)
		case v1beta2.ApplicationStateCompleted:
			return true, nil
		}
		return false, nil
	})

	// Upstream error string from internal/controller/sparkapplication/submission.go — update if wording changes.
	if err != nil && strings.Contains(err.Error(), "driver pod already exist") {
		logf.Log.Info("SparkApplication reported 'driver pod already exist' — falling back to watching driver pod directly", "app", key.Name)
		return waitForDriverPodCompleted(ctx, key)
	}
	return err
}

func waitForDriverPodCompleted(ctx context.Context, appKey types.NamespacedName) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, WaitTimeout)
	defer cancelFunc()

	driverPodName := fmt.Sprintf("%s-driver", appKey.Name)
	podKey := types.NamespacedName{Namespace: appKey.Namespace, Name: driverPodName}

	return wait.PollUntilContextCancel(cancelCtx, PollInterval, true, func(ctx context.Context) (bool, error) {
		pod := &corev1.Pod{}
		if err := k8sClient.Get(ctx, podKey, pod); err != nil {
			return false, nil
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return true, nil
		case corev1.PodFailed:
			return false, fmt.Errorf("driver pod %s failed", driverPodName)
		}
		return false, nil
	})
}

func overrideSparkAppImage(app *v1beta2.SparkApplication) {
	if sparkAppImage != "" {
		app.Spec.Image = &sparkAppImage
		ifNotPresent := string(corev1.PullIfNotPresent)
		app.Spec.ImagePullPolicy = &ifNotPresent
	}
}
