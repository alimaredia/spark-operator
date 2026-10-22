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

package rhoai_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkconnect"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// This test runs against the RHOAI/ODH overlay, which enables the
// OperandNetworkPolicy feature gate. The operator therefore creates the operand
// NetworkPolicies (intra-Spark ingress + allow-all egress) for the Connect
// server and executors, and the suite provisions the consumer NetworkPolicy in
// the test namespace (admitting 15002 from same-namespace pods) so the client
// pod below can reach the Spark Connect gRPC endpoint.
var _ = Describe("SparkConnect Query", func() {
	Context("Execute a query via Spark Connect", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "..", "examples", "sparkconnect", "spark-connect.yaml")

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			By("Parsing SparkConnect from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())
			defer func() { Expect(file.Close()).To(Succeed()) }()

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())

			conn = &v1alpha1.SparkConnect{}
			Expect(decoder.Decode(conn)).NotTo(HaveOccurred())
			conn.Namespace = TestNamespace
			conn.Name = "spark-connect-query"

			By("Creating SparkConnect")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: conn.Namespace, Name: conn.Name}
			if err := k8sClient.Get(ctx, key, conn); err == nil {
				By("Deleting SparkConnect")
				Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
			}
		})

		It("Should execute a Scala query through the Spark Connect endpoint", func() {
			serverPodName := sparkconnect.GetServerPodName(conn)
			serviceName := sparkconnect.GetServerServiceName(conn)

			By("Waiting for server pod to be ready")
			Eventually(func() bool {
				server := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: conn.Namespace, Name: serverPodName}, server); err != nil {
					return false
				}
				return util.IsPodReady(server)
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Waiting for executor pods to be ready")
			Eventually(func() bool {
				executors := &corev1.PodList{}
				Expect(k8sClient.List(ctx, executors,
					client.InNamespace(conn.Namespace),
					client.MatchingLabels(sparkconnect.GetExecutorSelectorLabels(conn)),
				)).NotTo(HaveOccurred())

				instances := int(ptr.Deref(conn.Spec.Executor.Instances, 0))
				if len(executors.Items) != instances {
					return false
				}
				for _, executor := range executors.Items {
					if !util.IsPodReady(&executor) {
						return false
					}
				}
				return true
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Waiting for the server service to be created")
			Eventually(func() bool {
				svc := &corev1.Service{}
				return k8sClient.Get(ctx, types.NamespacedName{Namespace: conn.Namespace, Name: serviceName}, svc) == nil
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Reading the operator controller image")
			operatorDeploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ReleaseNamespace,
				Name:      "spark-operator-controller",
			}, operatorDeploy)).To(Succeed())
			operatorImage := operatorDeploy.Spec.Template.Spec.Containers[0].Image

			By("Creating Spark Connect client pod")
			clientPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "spark-connect-client-",
					Namespace:    conn.Namespace,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "spark-connect-client",
							Image:           operatorImage,
							ImagePullPolicy: corev1.PullNever,
							Command:         []string{"sleep", "infinity"},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			}
			Expect(controllerutil.SetOwnerReference(conn, clientPod, k8sClient.Scheme())).To(Succeed())
			Expect(k8sClient.Create(ctx, clientPod)).To(Succeed())

			By("Waiting for Spark Connect client pod to be ready")
			Eventually(func() bool {
				pod := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: conn.Namespace, Name: clientPod.Name}, pod); err != nil {
					return false
				}
				return util.IsPodReady(pod)
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Executing a Scala query via Spark Connect from the client pod")
			connectURL := fmt.Sprintf("sc://%s.%s.svc:15002", serviceName, conn.Namespace)
			scalaScript := fmt.Sprintf(
				`import org.apache.spark.sql.SparkSession
val remoteSpark = SparkSession.builder().remote("%s").getOrCreate()
val df = remoteSpark.range(100).selectExpr("id", "id * 2 as doubled")
println("ROW_COUNT=" + df.count())
val row = df.filter("id = 7").collect()(0)
println("VALIDATE=" + row.getAs[Long]("doubled"))
System.exit(0)
`,
				connectURL,
			)
			output, err := runCommandWithInput(
				scalaScript,
				"kubectl", "exec", "-i", "-n", conn.Namespace, clientPod.Name, "--",
				"bash", "-c",
				"export HOME=/tmp && ${SPARK_HOME}/bin/spark-shell --master 'local[1]' --conf spark.ui.enabled=false",
			)
			Expect(err).NotTo(HaveOccurred(), "Scala Spark Connect query failed: %s", output)
			Expect(output).To(ContainSubstring("ROW_COUNT=100"), "expected 100 rows, got: %s", output)
			Expect(output).To(ContainSubstring("VALIDATE=14"), "expected doubled value 14 for id=7, got: %s", output)
		})
	})
})

func runCommandWithInput(input string, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), WaitTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), fmt.Errorf("command %s timed out after %s", name, WaitTimeout)
	}
	return string(out), err
}
