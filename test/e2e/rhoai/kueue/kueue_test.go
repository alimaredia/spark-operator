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
	"fmt"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

const (
	// Workload namespace, matching the LocalQueue namespace in
	// examples/openshift/kueue/*.yaml. Deliberately not the operator's own
	// namespace: Spark workloads do not belong there.
	KueueTestNamespace  = "spark-test"
	KueueLocalQueue     = "spark-lq"
	KueueClusterQueue   = "spark-cq"
	KueueResourceFlavor = "spark-rf"

	SparkImage     = "ghcr.io/apache/spark-docker/spark:3.5.4"
	SparkVersion   = "3.5.4"
	SparkJar       = "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.4.jar"
	SparkMainClass = "org.apache.spark.examples.SparkPi"
	SparkSA        = "spark-operator-spark"

	KueueWaitTimeout  = 8 * time.Minute
	KueuePollInterval = 2 * time.Second
)

// ============================================================================
// KUEUE + SPARKAPPLICATION INTEGRATION TESTS
// ============================================================================
//
// These tests validate the Kueue + SparkApplication integration on OpenShift.
//
// Prerequisites:
//   - RHBoK v1.4.0+ installed with SparkApplicationIntegration feature gate
//   - Spark Operator installed
//   - Kueue CR created with SparkApplication in frameworks list
//   - ResourceFlavor, ClusterQueue, LocalQueue created (see kueue-resources.yaml)
//
// Run with:
//   KUBECONFIG=$HOME/.kube/config \
//   go test -v -tags openshift ./examples/openshift/kueue/ \
//     -ginkgo.v -ginkgo.focus="Kueue SparkApplication Integration" -timeout 35m
//
// ============================================================================

var _ = Describe("Kueue SparkApplication Integration", func() {

	ctx := context.Background()

	BeforeEach(func() {
		By("Ensuring namespace has kueue.openshift.io/managed=true label")
		ns := &corev1.Namespace{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: KueueTestNamespace}, ns)).To(Succeed())
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		if ns.Labels["kueue.openshift.io/managed"] != "true" {
			ns.Labels["kueue.openshift.io/managed"] = "true"
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())
		}

		By("Verifying Kueue resources exist")
		lq := &unstructured.Unstructured{}
		lq.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "kueue.x-k8s.io",
			Version: "v1beta2",
			Kind:    "LocalQueue",
		})
		err := k8sClient.Get(ctx, types.NamespacedName{
			Name:      KueueLocalQueue,
			Namespace: KueueTestNamespace,
		}, lq)
		Expect(err).NotTo(HaveOccurred(), "LocalQueue %s must exist. Apply kueue-resources.yaml first.", KueueLocalQueue)
	})

	// ========================================================================
	// AC1: Basic Admission Lifecycle
	// ========================================================================
	Context("Basic Admission Lifecycle", func() {
		var app *v1beta2.SparkApplication

		AfterEach(func() {
			cleanupSparkApplication(ctx, app)
		})

		It("Should admit a SparkApplication via Kueue and run to completion", func(specCtx SpecContext) {
			app = newSparkPiKueue("kueue-ac1", "100")

			By("Creating SparkApplication with kueue queue label")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
			GinkgoWriter.Printf("Created SparkApplication %s\n", app.Name)

			By("Waiting for SparkApplication to complete")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			err := waitForSparkAppState(specCtx, key, v1beta2.ApplicationStateCompleted)
			logSparkAppStatus(specCtx, key)
			Expect(err).NotTo(HaveOccurred(), "SparkApplication should complete successfully via Kueue admission")

			By("Verifying quota was reclaimed")
			Eventually(func(g Gomega) {
				lq := getLocalQueue(ctx, KueueLocalQueue, KueueTestNamespace)
				admitted, found, err := unstructured.NestedInt64(lq.Object, "status", "admittedWorkloads")
				g.Expect(err).NotTo(HaveOccurred(), "Failed to read admittedWorkloads")
				g.Expect(found).To(BeTrue(), "admittedWorkloads field not found in LocalQueue status")
				g.Expect(admitted).To(Equal(int64(0)), "Admitted workloads should be 0 after completion")
			}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Succeed())

			GinkgoWriter.Printf("AC1 PASSED: SparkApplication admitted, ran, completed, quota reclaimed\n")
		}, SpecTimeout(KueueWaitTimeout))
	})

	// ========================================================================
	// AC2: Quota Enforcement
	// ========================================================================
	Context("Quota Enforcement", func() {
		var apps []*v1beta2.SparkApplication

		AfterEach(func() {
			for _, app := range apps {
				cleanupSparkApplication(ctx, app)
			}
			apps = nil
		})

		It("Should keep excess jobs suspended when quota is exhausted", func(specCtx SpecContext) {
			By("Submitting a long-running SparkApplication that consumes quota (2 CPU: 1 driver + 1 executor)")
			app1 := newSparkPiKueue("kueue-ac2-a", "500000")
			apps = append(apps, app1)

			Expect(k8sClient.Create(ctx, app1)).To(Succeed())
			GinkgoWriter.Printf("Created SparkApplication %s\n", app1.Name)

			By("Waiting for first app to start running")
			key1 := types.NamespacedName{Namespace: app1.Namespace, Name: app1.Name}
			err := waitForSparkAppState(specCtx, key1, v1beta2.ApplicationStateRunning)
			Expect(err).NotTo(HaveOccurred(), "First app should reach Running state")

			By("Submitting a second SparkApplication that should exceed quota (2 more CPU vs 3 total)")
			app2 := newSparkPiKueue("kueue-ac2-b", "100")
			apps = append(apps, app2)
			Expect(k8sClient.Create(ctx, app2)).To(Succeed())
			GinkgoWriter.Printf("Created SparkApplication %s (should be queued)\n", app2.Name)

			By("Verifying the second app remains suspended/pending (not running) while first app holds quota")
			key2 := types.NamespacedName{Namespace: app2.Namespace, Name: app2.Name}
			quotaEnforced := false
			Consistently(func(g Gomega) {
				app1Current := &v1beta2.SparkApplication{}
				g.Expect(k8sClient.Get(ctx, key1, app1Current)).To(Succeed())
				if app1Current.Status.AppState.State != v1beta2.ApplicationStateRunning {
					GinkgoWriter.Printf("  app1 no longer running (%s), ending check early\n",
						app1Current.Status.AppState.State)
					return
				}

				currentApp := &v1beta2.SparkApplication{}
				g.Expect(k8sClient.Get(ctx, key2, currentApp)).To(Succeed())
				state := currentApp.Status.AppState.State
				GinkgoWriter.Printf("  app1: %s, app2: %s, suspend: %v\n",
					app1Current.Status.AppState.State, state, currentApp.Spec.Suspend)
				g.Expect(state).NotTo(Equal(v1beta2.ApplicationStateRunning),
					"Second app should NOT be running while quota is exhausted")
				g.Expect(state).NotTo(Equal(v1beta2.ApplicationStateCompleted),
					"Second app should NOT have completed while quota is exhausted")
				quotaEnforced = true
			}).WithTimeout(15 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

			Expect(quotaEnforced).To(BeTrue(), "Should have observed app2 blocked while app1 was running")

			By("Verifying ClusterQueue shows pending workloads")
			cq := getClusterQueue(ctx, KueueClusterQueue)
			pending, found, err := unstructured.NestedInt64(cq.Object, "status", "pendingWorkloads")
			Expect(err).NotTo(HaveOccurred(), "Failed to read pendingWorkloads")
			Expect(found).To(BeTrue(), "pendingWorkloads field not found in ClusterQueue status")
			GinkgoWriter.Printf("ClusterQueue pending workloads: %d\n", pending)
			Expect(pending).To(BeNumerically(">=", 1), "Should have at least 1 pending workload")

			GinkgoWriter.Printf("AC2 PASSED: Excess job remains suspended when quota is exhausted\n")
		}, SpecTimeout(KueueWaitTimeout))
	})

	// ========================================================================
	// AC3: Quota Reclamation
	// ========================================================================
	Context("Quota Reclamation", func() {
		var apps []*v1beta2.SparkApplication

		AfterEach(func() {
			for _, app := range apps {
				cleanupSparkApplication(ctx, app)
			}
			apps = nil
		})

		It("Should admit the next queued job after a completing job frees quota", func(specCtx SpecContext) {
			By("Submitting a SparkApplication that consumes most of the quota")
			app1 := newSparkPiKueue("kueue-ac3-a", "5000")
			apps = append(apps, app1)
			Expect(k8sClient.Create(ctx, app1)).To(Succeed())

			key1 := types.NamespacedName{Namespace: app1.Namespace, Name: app1.Name}
			By("Waiting for first app to start running")
			Expect(waitForSparkAppState(specCtx, key1, v1beta2.ApplicationStateRunning)).To(Succeed())

			By("Submitting a second SparkApplication that should be queued")
			app2 := newSparkPiKueue("kueue-ac3-b", "100")
			apps = append(apps, app2)
			Expect(k8sClient.Create(ctx, app2)).To(Succeed())
			GinkgoWriter.Printf("Created queued SparkApplication %s\n", app2.Name)

			By("Waiting for first app to complete and free quota")
			Expect(waitForSparkAppState(specCtx, key1, v1beta2.ApplicationStateCompleted)).To(Succeed())
			GinkgoWriter.Printf("First app completed, quota should be reclaimed\n")

			By("Verifying second app is admitted and completes")
			key2 := types.NamespacedName{Namespace: app2.Namespace, Name: app2.Name}
			err := waitForSparkAppState(specCtx, key2, v1beta2.ApplicationStateCompleted)
			logSparkAppStatus(specCtx, key2)
			Expect(err).NotTo(HaveOccurred(), "Second app should be admitted and complete after quota is freed")

			By("Verifying no workloads are pending in the queue")
			Eventually(func(g Gomega) {
				cq := getClusterQueue(ctx, KueueClusterQueue)
				pending, found, err := unstructured.NestedInt64(cq.Object, "status", "pendingWorkloads")
				g.Expect(err).NotTo(HaveOccurred(), "Failed to read pendingWorkloads")
				g.Expect(found).To(BeTrue(), "pendingWorkloads field not found in ClusterQueue status")
				g.Expect(pending).To(Equal(int64(0)), "No workloads should be pending after reclamation")
			}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Succeed())

			GinkgoWriter.Printf("AC3 PASSED: Queued job admitted and completed after quota reclamation\n")
		}, SpecTimeout(KueueWaitTimeout))
	})

	// ========================================================================
	// AC4: Resume After Suspension
	// ========================================================================
	Context("Resume After Suspension", func() {
		var apps []*v1beta2.SparkApplication

		AfterEach(func() {
			for _, app := range apps {
				cleanupSparkApplication(ctx, app)
			}
			apps = nil
		})

		It("Should pick up a suspended job and run to completion when quota becomes available", func(specCtx SpecContext) {
			By("Submitting a long-running SparkApplication to consume quota")
			blocker := newSparkPiKueue("kueue-ac4-blocker", "50000")
			apps = append(apps, blocker)
			Expect(k8sClient.Create(ctx, blocker)).To(Succeed())

			blockerKey := types.NamespacedName{Namespace: blocker.Namespace, Name: blocker.Name}
			Expect(waitForSparkAppState(specCtx, blockerKey, v1beta2.ApplicationStateRunning)).To(Succeed())
			GinkgoWriter.Printf("Blocker app is running\n")

			By("Submitting a second app that should be suspended due to quota")
			waiter := newSparkPiKueue("kueue-ac4-waiter", "100")
			apps = append(apps, waiter)
			Expect(k8sClient.Create(ctx, waiter)).To(Succeed())

			waiterKey := types.NamespacedName{Namespace: waiter.Namespace, Name: waiter.Name}

			By("Verifying the waiter app remains suspended/pending while blocker holds quota")
			Consistently(func(g Gomega) {
				currentApp := &v1beta2.SparkApplication{}
				g.Expect(k8sClient.Get(ctx, waiterKey, currentApp)).To(Succeed())
				state := currentApp.Status.AppState.State
				GinkgoWriter.Printf("  Waiter app state: %s, suspend: %v\n", state, currentApp.Spec.Suspend)
				g.Expect(state).NotTo(Equal(v1beta2.ApplicationStateRunning),
					"Waiter should NOT be running while blocker holds quota")
				g.Expect(state).NotTo(Equal(v1beta2.ApplicationStateCompleted),
					"Waiter should NOT have completed while blocker holds quota")
			}).WithTimeout(15 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

			By("Deleting the blocker app to free quota")
			Expect(k8sClient.Delete(ctx, blocker)).To(Succeed())
			GinkgoWriter.Printf("Deleted blocker app to free quota\n")

			By("Waiting for the waiter app to be admitted and complete")
			err := waitForSparkAppState(specCtx, waiterKey, v1beta2.ApplicationStateCompleted)
			logSparkAppStatus(specCtx, waiterKey)
			Expect(err).NotTo(HaveOccurred(), "Waiter app should be admitted and complete after blocker is removed")

			GinkgoWriter.Printf("AC4 PASSED: Suspended job picked up and completed after quota freed\n")
		}, SpecTimeout(KueueWaitTimeout))
	})
})

// ============================================================================
// Kueue Test Helper Functions
// ============================================================================

func newSparkPiKueue(name string, piIterations string) *v1beta2.SparkApplication {
	uniqueName := fmt.Sprintf("%s-%s-%04d", name, time.Now().Format("0405"), rand.Intn(10000))
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      uniqueName,
			Namespace: KueueTestNamespace,
			Labels: map[string]string{
				"kueue.x-k8s.io/queue-name": KueueLocalQueue,
			},
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                v1beta2.SparkApplicationTypeScala,
			Mode:                v1beta2.DeployModeCluster,
			Image:               ptr.To(SparkImage),
			ImagePullPolicy:     ptr.To("IfNotPresent"),
			MainClass:           ptr.To(SparkMainClass),
			MainApplicationFile: ptr.To(SparkJar),
			Arguments:           []string{piIterations},
			SparkVersion:        SparkVersion,
			RestartPolicy: v1beta2.RestartPolicy{
				Type: v1beta2.RestartPolicyNever,
			},
			Volumes: []corev1.Volume{
				{
					Name: "spark-work-dir",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:           ptr.To(int32(1)),
					Memory:          ptr.To("512m"),
					ServiceAccount:  ptr.To(SparkSA),
					SecurityContext: &corev1.SecurityContext{},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "spark-work-dir", MountPath: "/opt/spark/work-dir"},
					},
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:           ptr.To(int32(1)),
					Memory:          ptr.To("512m"),
					SecurityContext: &corev1.SecurityContext{},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "spark-work-dir", MountPath: "/opt/spark/work-dir"},
					},
				},
				Instances: ptr.To(int32(1)),
			},
		},
	}
	return app
}

func waitForSparkAppState(ctx context.Context, key types.NamespacedName, targetState v1beta2.ApplicationStateType) error {
	cancelCtx, cancelFunc := context.WithTimeout(ctx, KueueWaitTimeout)
	defer cancelFunc()

	app := &v1beta2.SparkApplication{}
	return wait.PollUntilContextCancel(cancelCtx, KueuePollInterval, true, func(ctx context.Context) (bool, error) {
		if err := k8sClient.Get(ctx, key, app); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		state := app.Status.AppState.State
		switch {
		case state == targetState:
			return true, nil
		case state == v1beta2.ApplicationStateFailed && targetState != v1beta2.ApplicationStateFailed:
			return false, fmt.Errorf("SparkApplication %s failed: %s", key.Name, app.Status.AppState.ErrorMessage)
		case state == v1beta2.ApplicationStateFailedSubmission && targetState != v1beta2.ApplicationStateFailedSubmission:
			return false, fmt.Errorf("SparkApplication %s submission failed: %s", key.Name, app.Status.AppState.ErrorMessage)
		}
		return false, nil
	})
}

func logSparkAppStatus(ctx context.Context, key types.NamespacedName) {
	app := &v1beta2.SparkApplication{}
	if err := k8sClient.Get(ctx, key, app); err == nil {
		GinkgoWriter.Printf("SparkApplication %s status:\n", key.Name)
		GinkgoWriter.Printf("  State: %s\n", app.Status.AppState.State)
		GinkgoWriter.Printf("  Suspend: %v\n", app.Spec.Suspend)
		GinkgoWriter.Printf("  SubmissionAttempts: %d\n", app.Status.SubmissionAttempts)
		if app.Status.AppState.ErrorMessage != "" {
			GinkgoWriter.Printf("  Error: %s\n", app.Status.AppState.ErrorMessage)
		}
	}
}

func cleanupSparkApplication(ctx context.Context, app *v1beta2.SparkApplication) {
	if app == nil {
		return
	}
	currentApp := &v1beta2.SparkApplication{}
	key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
	if err := k8sClient.Get(ctx, key, currentApp); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		GinkgoWriter.Printf("Warning: Failed to get SparkApplication %s: %v\n", app.Name, err)
		return
	}
	if err := k8sClient.Delete(ctx, currentApp); err != nil {
		GinkgoWriter.Printf("Warning: Failed to delete SparkApplication %s: %v\n", app.Name, err)
	} else {
		GinkgoWriter.Printf("Cleaned up SparkApplication %s\n", app.Name)
	}
	Eventually(func(g Gomega) {
		err := k8sClient.Get(ctx, key, &v1beta2.SparkApplication{})
		g.Expect(err).To(HaveOccurred())
	}).WithContext(ctx).WithTimeout(60 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
}

func getLocalQueue(ctx context.Context, name, namespace string) *unstructured.Unstructured {
	lq := &unstructured.Unstructured{}
	lq.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kueue.x-k8s.io",
		Version: "v1beta2",
		Kind:    "LocalQueue",
	})
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, lq)).To(Succeed())
	return lq
}

func getClusterQueue(ctx context.Context, name string) *unstructured.Unstructured {
	cq := &unstructured.Unstructured{}
	cq.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kueue.x-k8s.io",
		Version: "v1beta2",
		Kind:    "ClusterQueue",
	})
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, cq)).To(Succeed())
	return cq
}
