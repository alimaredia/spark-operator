package e2e_test

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("Example ScheduledSparkApplication", func() {
	Context("spark-pi-scheduled", func() {
		ctx := context.Background()
		path := filepath.Join("examples", "spark-pi-scheduled.yaml")
		var scheduledApp *v1beta2.ScheduledSparkApplication

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			scheduledApp = &v1beta2.ScheduledSparkApplication{}
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			// Override schedule to fire quickly for testing.
			scheduledApp.Spec.Schedule = "@every 1m"

			By("Creating ScheduledSparkApplication")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}

			By("Cleaning up child SparkApplications")
			appList := &v1beta2.SparkApplicationList{}
			Expect(k8sClient.List(ctx, appList,
				client.InNamespace(scheduledApp.Namespace),
				client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
			)).To(Succeed())
			for i := range appList.Items {
				_ = k8sClient.Delete(ctx, &appList.Items[i])
			}
		})

		It("Should reach Scheduled state and populate NextRun", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for ScheduledSparkApplication to reach Scheduled state")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateScheduled))
				g.Expect(app.Status.NextRun.IsZero()).To(BeFalse())
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		})

		It("Should spawn a child SparkApplication that completes successfully", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for a child SparkApplication to be created")
			var childName string
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(BeEmpty())
				childName = app.Status.LastRunName
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())

			By("Verifying the child SparkApplication resource exists")
			childKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: childName}
			childApp := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, childKey, childApp)).To(Succeed())

			By("Verifying the child has the scheduled app label")
			Expect(childApp.Labels).To(HaveKeyWithValue(common.LabelScheduledSparkAppName, scheduledApp.Name))

			By("Verifying the child has an owner reference to the scheduled app")
			Expect(childApp.OwnerReferences).NotTo(BeEmpty())
			found := false
			for _, ref := range childApp.OwnerReferences {
				if ref.Name == scheduledApp.Name && ref.Kind == "ScheduledSparkApplication" {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue())

			By("Waiting for the child SparkApplication to complete")
			Expect(waitForSparkApplicationCompleted(ctx, childKey)).NotTo(HaveOccurred())

			By("Verifying LastRun is set")
			app := &v1beta2.ScheduledSparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			Expect(app.Status.LastRun.IsZero()).To(BeFalse())

			By("Checking driver logs of the child SparkApplication")
			driverPodName := util.GetDriverPodName(childApp)
			logStream, err := clientset.CoreV1().Pods(scheduledApp.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{
				LimitBytes: ptr.To(int64(1 << 20)),
			}).Stream(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(logStream.Close()).To(Succeed()) }()
			scanner := bufio.NewScanner(logStream)
			foundPi := false
			for scanner.Scan() {
				if strings.Contains(scanner.Text(), "Pi is roughly 3") {
					foundPi = true
					break
				}
			}
			Expect(scanner.Err()).NotTo(HaveOccurred())
			Expect(foundPi).To(BeTrue(), "expected driver logs to contain 'Pi is roughly 3'")
		})
	})

	Context("suspend and resume", func() {
		ctx := context.Background()
		path := filepath.Join("examples", "spark-pi-scheduled.yaml")
		scheduledApp := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			scheduledApp.Name = "spark-pi-scheduled-suspend"
			scheduledApp.Spec.Schedule = "@every 1m"
			scheduledApp.Spec.Suspend = ptr.To(true)

			By("Creating suspended ScheduledSparkApplication")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}

			By("Cleaning up child SparkApplications")
			appList := &v1beta2.SparkApplicationList{}
			Expect(k8sClient.List(ctx, appList,
				client.InNamespace(scheduledApp.Namespace),
				client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
			)).To(Succeed())
			for i := range appList.Items {
				_ = k8sClient.Delete(ctx, &appList.Items[i])
			}
		})

		It("Should not spawn children while suspended, then resume scheduling", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Verifying no child SparkApplications are created while suspended")
			Consistently(func(g Gomega) {
				appList := &v1beta2.SparkApplicationList{}
				g.Expect(k8sClient.List(ctx, appList,
					client.InNamespace(scheduledApp.Namespace),
					client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
				)).To(Succeed())
				g.Expect(appList.Items).To(BeEmpty())
			}).WithTimeout(90 * time.Second).WithPolling(2 * time.Second).Should(Succeed())

			By("Resuming the ScheduledSparkApplication")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				app.Spec.Suspend = ptr.To(false)
				g.Expect(k8sClient.Update(ctx, app)).To(Succeed())
			}).WithTimeout(10 * time.Second).WithPolling(PollInterval).Should(Succeed())

			By("Waiting for a child SparkApplication to be created after resume")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(BeEmpty())
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())
		})
	})

	Context("concurrency policy Forbid", func() {
		ctx := context.Background()
		path := filepath.Join("examples", "spark-pi-scheduled.yaml")
		scheduledApp := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			scheduledApp.Name = "spark-pi-scheduled-forbid"
			scheduledApp.Spec.Schedule = "@every 1m"
			scheduledApp.Spec.ConcurrencyPolicy = v1beta2.ConcurrencyForbid
			// Use a high iteration count so the child outlives at least one schedule tick.
			scheduledApp.Spec.Template.Arguments = []string{"1000000"}

			By("Creating ScheduledSparkApplication")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}

			By("Cleaning up child SparkApplications")
			appList := &v1beta2.SparkApplicationList{}
			Expect(k8sClient.List(ctx, appList,
				client.InNamespace(scheduledApp.Namespace),
				client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
			)).To(Succeed())
			for i := range appList.Items {
				_ = k8sClient.Delete(ctx, &appList.Items[i])
			}
		})

		It("Should not create a second child while the first is still running", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for the first child SparkApplication to be created")
			var firstChildName string
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(BeEmpty())
				firstChildName = app.Status.LastRunName
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())

			By("Verifying the first child is still running and no second child is created")
			// Wait through another schedule period; assert that the first child
			// remains active the entire time and no additional child appears.
			Consistently(func(g Gomega) {
				firstChildKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: firstChildName}
				firstChild := &v1beta2.SparkApplication{}
				g.Expect(k8sClient.Get(ctx, firstChildKey, firstChild)).To(Succeed())
				g.Expect(firstChild.Status.AppState.State).NotTo(Equal(v1beta2.ApplicationStateCompleted),
					"first child completed before the next tick — test cannot validate Forbid")
				g.Expect(firstChild.Status.AppState.State).NotTo(Equal(v1beta2.ApplicationStateFailed),
					"first child failed before the next tick — test cannot validate Forbid")

				appList := &v1beta2.SparkApplicationList{}
				g.Expect(k8sClient.List(ctx, appList,
					client.InNamespace(scheduledApp.Namespace),
					client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
				)).To(Succeed())
				g.Expect(appList.Items).To(HaveLen(1))
			}).WithTimeout(90 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
		})
	})

	Context("concurrency policy Replace", func() {
		ctx := context.Background()
		path := filepath.Join("examples", "spark-pi-scheduled.yaml")
		scheduledApp := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			scheduledApp.Name = "spark-pi-scheduled-replace"
			scheduledApp.Spec.Schedule = "@every 1m"
			scheduledApp.Spec.ConcurrencyPolicy = v1beta2.ConcurrencyReplace
			// Use a high iteration count so the child outlives at least one schedule tick.
			scheduledApp.Spec.Template.Arguments = []string{"1000000"}

			By("Creating ScheduledSparkApplication")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}

			By("Cleaning up child SparkApplications")
			appList := &v1beta2.SparkApplicationList{}
			Expect(k8sClient.List(ctx, appList,
				client.InNamespace(scheduledApp.Namespace),
				client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
			)).To(Succeed())
			for i := range appList.Items {
				_ = k8sClient.Delete(ctx, &appList.Items[i])
			}
		})

		It("Should replace the running child when the next run is due", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for the first child SparkApplication to be created and start running")
			var firstChildName string
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(BeEmpty())
				firstChildName = app.Status.LastRunName

				firstChildKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: firstChildName}
				firstChild := &v1beta2.SparkApplication{}
				g.Expect(k8sClient.Get(ctx, firstChildKey, firstChild)).To(Succeed())
				g.Expect(firstChild.Status.AppState.State).To(Equal(v1beta2.ApplicationStateRunning))
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())

			By("Waiting for LastRunName to change while the first child is still running")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(Equal(firstChildName))
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())

			By("Verifying the first child was deleted (terminated before natural completion)")
			firstChildKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: firstChildName}
			Eventually(func(g Gomega) {
				firstChild := &v1beta2.SparkApplication{}
				err := k8sClient.Get(ctx, firstChildKey, firstChild)
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
					"first child should have been deleted by Replace policy")
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		})
	})

	Context("history limits", func() {
		ctx := context.Background()
		path := filepath.Join("examples", "spark-pi-scheduled.yaml")
		scheduledApp := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			scheduledApp.Name = "spark-pi-scheduled-history"
			scheduledApp.Spec.Schedule = "@every 1m"
			scheduledApp.Spec.SuccessfulRunHistoryLimit = ptr.To(int32(1))
			scheduledApp.Spec.FailedRunHistoryLimit = ptr.To(int32(1))

			By("Creating ScheduledSparkApplication")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}

			By("Cleaning up child SparkApplications")
			appList := &v1beta2.SparkApplicationList{}
			Expect(k8sClient.List(ctx, appList,
				client.InNamespace(scheduledApp.Namespace),
				client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
			)).To(Succeed())
			for i := range appList.Items {
				_ = k8sClient.Delete(ctx, &appList.Items[i])
			}
		})

		It("Should prune past successful run names beyond the history limit", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for the first child SparkApplication to complete")
			var firstChildName string
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(BeEmpty())
				firstChildName = app.Status.LastRunName
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())
			firstChildKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: firstChildName}
			Expect(waitForSparkApplicationCompleted(ctx, firstChildKey)).NotTo(HaveOccurred())

			By("Waiting for a second, distinct child SparkApplication to complete")
			var secondChildName string
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(Equal(firstChildName))
				secondChildName = app.Status.LastRunName
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())
			secondChildKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: secondChildName}
			Expect(waitForSparkApplicationCompleted(ctx, secondChildKey)).NotTo(HaveOccurred())

			By("Verifying PastSuccessfulRunNames retains only the most recent run")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.PastSuccessfulRunNames).To(HaveLen(1))
				g.Expect(app.Status.PastSuccessfulRunNames).To(ContainElement(secondChildName))
				g.Expect(app.Status.PastSuccessfulRunNames).NotTo(ContainElement(firstChildName))
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		})
	})

	Context("failed run history limit", func() {
		ctx := context.Background()
		path := filepath.Join("bad_examples", "fail-scheduled-application.yaml")
		scheduledApp := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			By("Creating ScheduledSparkApplication")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}

			By("Cleaning up child SparkApplications")
			appList := &v1beta2.SparkApplicationList{}
			Expect(k8sClient.List(ctx, appList,
				client.InNamespace(scheduledApp.Namespace),
				client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
			)).To(Succeed())
			for i := range appList.Items {
				_ = k8sClient.Delete(ctx, &appList.Items[i])
			}
		})

		It("Should prune past failed run names beyond the history limit", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for the first child SparkApplication to fail")
			var firstChildName string
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(BeEmpty())
				firstChildName = app.Status.LastRunName
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())
			firstChildKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: firstChildName}
			Eventually(func(g Gomega) {
				childApp := &v1beta2.SparkApplication{}
				g.Expect(k8sClient.Get(ctx, firstChildKey, childApp)).To(Succeed())
				g.Expect(childApp.Status.AppState.State).To(SatisfyAny(
					Equal(v1beta2.ApplicationStateFailed),
					Equal(v1beta2.ApplicationStateFailedSubmission),
				))
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

			By("Waiting for a second, distinct child SparkApplication to fail")
			var secondChildName string
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(Equal(firstChildName))
				secondChildName = app.Status.LastRunName
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())
			secondChildKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: secondChildName}
			Eventually(func(g Gomega) {
				childApp := &v1beta2.SparkApplication{}
				g.Expect(k8sClient.Get(ctx, secondChildKey, childApp)).To(Succeed())
				g.Expect(childApp.Status.AppState.State).To(SatisfyAny(
					Equal(v1beta2.ApplicationStateFailed),
					Equal(v1beta2.ApplicationStateFailedSubmission),
				))
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

			By("Verifying PastFailedRunNames retains only the most recent run")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.PastFailedRunNames).To(HaveLen(1))
				g.Expect(app.Status.PastFailedRunNames).To(ContainElement(secondChildName))
				g.Expect(app.Status.PastFailedRunNames).NotTo(ContainElement(firstChildName))
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		})
	})

	Context("valid timezone", func() {
		ctx := context.Background()
		path := filepath.Join("examples", "spark-pi-scheduled.yaml")
		scheduledApp := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			scheduledApp.Name = "spark-pi-scheduled-tz"
			scheduledApp.Spec.Schedule = "@every 1m"
			scheduledApp.Spec.TimeZone = "UTC"

			By("Creating ScheduledSparkApplication")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}

			By("Cleaning up child SparkApplications")
			appList := &v1beta2.SparkApplicationList{}
			Expect(k8sClient.List(ctx, appList,
				client.InNamespace(scheduledApp.Namespace),
				client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
			)).To(Succeed())
			for i := range appList.Items {
				_ = k8sClient.Delete(ctx, &appList.Items[i])
			}
		})

		It("Should reach Scheduled state and spawn a child", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for ScheduledSparkApplication to reach Scheduled state")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateScheduled))
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

			By("Waiting for a child SparkApplication to be created")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(BeEmpty())
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())
		})
	})

	Context("invalid timezone", func() {
		ctx := context.Background()
		path := filepath.Join("bad_examples", "fail-scheduled-invalid-timezone.yaml")
		scheduledApp := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			By("Creating ScheduledSparkApplication with invalid timezone")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}
		})

		It("Should reach FailedValidation state with a reason mentioning timezone", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for ScheduledSparkApplication to reach FailedValidation state")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateFailedValidation))
				g.Expect(app.Status.Reason).To(ContainSubstring("timezone"))
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

			By("Verifying no child SparkApplications are created despite the schedule")
			Consistently(func(g Gomega) {
				appList := &v1beta2.SparkApplicationList{}
				g.Expect(k8sClient.List(ctx, appList,
					client.InNamespace(scheduledApp.Namespace),
					client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
				)).To(Succeed())
				g.Expect(appList.Items).To(BeEmpty())
			}).WithTimeout(90 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
		})
	})

	Context("invalid schedule", func() {
		ctx := context.Background()
		path := filepath.Join("bad_examples", "fail-scheduled-invalid-schedule.yaml")
		scheduledApp := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			By("Creating ScheduledSparkApplication with invalid schedule")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}
		})

		It("Should reach FailedValidation state with a reason", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for ScheduledSparkApplication to reach FailedValidation state")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateFailedValidation))
				g.Expect(app.Status.Reason).NotTo(BeEmpty())
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

			By("Verifying no child SparkApplications are created despite the schedule")
			Consistently(func(g Gomega) {
				appList := &v1beta2.SparkApplicationList{}
				g.Expect(k8sClient.List(ctx, appList,
					client.InNamespace(scheduledApp.Namespace),
					client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
				)).To(Succeed())
				g.Expect(appList.Items).To(BeEmpty())
			}).WithTimeout(90 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
		})
	})
})
