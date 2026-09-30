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

package e2e_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// versionGateContainerName mirrors the init container name the native submitter
// injects (internal/controller/sparkapplication.versionGateContainerName). It is
// duplicated here to keep this e2e test a black-box consumer that does not import
// the operator's internal packages.
const versionGateContainerName = "spark-version-gate"

// mismatchVersion returns a well-formed Spark version guaranteed to differ from
// the app's current (image-matching) spec.sparkVersion, so the in-pod gate always
// sees a mismatch regardless of the suite's configured SPARK_VERSION.
func mismatchVersion(app *v1beta2.SparkApplication) string {
	if app.Spec.SparkVersion == "3.5.0" {
		return "3.4.0"
	}
	return "3.5.0"
}

var _ = Describe("Native submitter Spark-version gate", func() {
	ctx := context.Background()

	Context("when spec.sparkVersion does not match the image's Spark version", func() {
		var app *v1beta2.SparkApplication

		BeforeEach(func() {
			// The image runs the suite's configured Spark version (SPARK_VERSION, or
			// the example default). Declare a DIFFERENT, well-formed version so the
			// image still passes admission but the in-pod gate detects the mismatch
			// (image version != declared version) and fails the driver pod before the
			// Spark driver process ever starts.
			app = loadSparkPi("e2e-version-gate-mismatch")
			app.Spec.SparkVersion = mismatchVersion(app)

			By("Creating a SparkApplication whose declared Spark version mismatches its image")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			// The spec deletes the app itself; this only cleans up if it failed early.
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			fresh := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, fresh); err == nil {
				_ = k8sClient.Delete(ctx, fresh)
			}
		})

		It("fails the application with the gate's reason and garbage-collects all owned resources", func() {
			appKey := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}

			By("Waiting for the SparkApplication to reach the Failed state")
			fetched := &v1beta2.SparkApplication{}
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, appKey, fetched)).To(Succeed())
				g.Expect(fetched.Status.AppState.State).To(Equal(v1beta2.ApplicationStateFailed))
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

			By("Confirming the failure reason came from the version gate, not a generic driver failure")
			Expect(fetched.Status.AppState.ErrorMessage).To(
				ContainSubstring(versionGateContainerName),
				"expected the driver init container's reason to be surfaced, got: %q",
				fetched.Status.AppState.ErrorMessage)
			Expect(fetched.Status.AppState.ErrorMessage).To(
				ContainSubstring("does not match spec.sparkVersion"))

			By("Verifying the version-gate init container terminated non-zero and the driver never started")
			driverPodKey := types.NamespacedName{Namespace: app.Namespace, Name: util.GetDriverPodName(app)}
			driverPod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, driverPodKey, driverPod)).To(Succeed())

			gateState := initContainerTerminated(driverPod, versionGateContainerName)
			Expect(gateState).NotTo(BeNil(), "the %q init container should have terminated", versionGateContainerName)
			Expect(gateState.ExitCode).NotTo(BeZero(), "the gate must exit non-zero on a version mismatch")
			Expect(mainDriverContainerStarted(driverPod)).To(BeFalse(),
				"the Spark driver container must never start when the gate fails")

			By("Recording the ConfigMaps and Services owned by the app or its driver pod")
			ownerUIDs := map[types.UID]bool{fetched.UID: true, driverPod.UID: true}
			ownedConfigMaps := ownedConfigMapNames(ctx, app.Namespace, ownerUIDs)
			ownedServices := ownedServiceNames(ctx, app.Namespace, ownerUIDs)

			By("Deleting the SparkApplication")
			Expect(k8sClient.Delete(ctx, fetched)).To(Succeed())

			By("Verifying the app, its driver pod, and every owned resource are garbage-collected")
			Eventually(func(g Gomega) {
				g.Expect(apierrors.IsNotFound(k8sClient.Get(ctx, appKey, &v1beta2.SparkApplication{}))).
					To(BeTrue(), "SparkApplication should be deleted")
				g.Expect(apierrors.IsNotFound(k8sClient.Get(ctx, driverPodKey, &corev1.Pod{}))).
					To(BeTrue(), "driver pod should be garbage-collected")
				for _, name := range ownedConfigMaps {
					key := types.NamespacedName{Namespace: app.Namespace, Name: name}
					g.Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key, &corev1.ConfigMap{}))).
						To(BeTrue(), "owned ConfigMap %q should be garbage-collected", name)
				}
				for _, name := range ownedServices {
					key := types.NamespacedName{Namespace: app.Namespace, Name: name}
					g.Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key, &corev1.Service{}))).
						To(BeTrue(), "owned Service %q should be garbage-collected", name)
				}
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		})
	})
})

// initContainerTerminated returns the terminated state of the named init container,
// or nil if it is absent or not terminated.
func initContainerTerminated(pod *corev1.Pod, name string) *corev1.ContainerStateTerminated {
	for _, s := range pod.Status.InitContainerStatuses {
		if s.Name == name {
			return s.State.Terminated
		}
	}
	return nil
}

// mainDriverContainerStarted reports whether the Spark driver container ever began
// executing (running or terminated). A failed init container with restartPolicy:
// Never must keep it in Waiting, so this returns false.
func mainDriverContainerStarted(pod *corev1.Pod) bool {
	for _, s := range pod.Status.ContainerStatuses {
		if s.State.Running != nil || s.State.Terminated != nil {
			return true
		}
	}
	return false
}

// ownedBy reports whether any of the owner references points at one of the UIDs.
func ownedBy(refs []metav1.OwnerReference, uids map[types.UID]bool) bool {
	for _, r := range refs {
		if uids[r.UID] {
			return true
		}
	}
	return false
}

func ownedConfigMapNames(ctx context.Context, namespace string, uids map[types.UID]bool) []string {
	list := &corev1.ConfigMapList{}
	Expect(k8sClient.List(ctx, list, client.InNamespace(namespace))).To(Succeed())
	var names []string
	for i := range list.Items {
		if ownedBy(list.Items[i].OwnerReferences, uids) {
			names = append(names, list.Items[i].Name)
		}
	}
	return names
}

func ownedServiceNames(ctx context.Context, namespace string, uids map[types.UID]bool) []string {
	list := &corev1.ServiceList{}
	Expect(k8sClient.List(ctx, list, client.InNamespace(namespace))).To(Succeed())
	var names []string
	for i := range list.Items {
		if ownedBy(list.Items[i].OwnerReferences, uids) {
			names = append(names, list.Items[i].Name)
		}
	}
	return names
}
