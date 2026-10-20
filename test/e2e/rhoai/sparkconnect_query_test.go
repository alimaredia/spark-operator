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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
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
// connectServerPort is the Spark Connect gRPC port the server and its Service
// expose (sparkConnectServerPort in the reconciler, unexported).
const connectServerPort int32 = 15002

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

		It("Should execute a query through the Spark Connect endpoint", func() {
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

			By("Waiting for the server service to be created with the Connect port")
			Eventually(func() bool {
				svc := &corev1.Service{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: conn.Namespace, Name: serviceName}, svc); err != nil {
					return false
				}
				for _, p := range svc.Spec.Ports {
					if p.Port == connectServerPort {
						return true
					}
				}
				return false
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Waiting for the server service endpoints to be ready")
			Eventually(func() bool {
				sliceList := &discoveryv1.EndpointSliceList{}
				if err := k8sClient.List(ctx, sliceList,
					client.InNamespace(conn.Namespace),
					client.MatchingLabels{discoveryv1.LabelServiceName: serviceName},
				); err != nil {
					return false
				}
				for _, slice := range sliceList.Items {
					for _, ep := range slice.Endpoints {
						if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
							return true
						}
					}
				}
				return false
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

			// PySpark is the only Spark Connect client this image can drive
			// non-interactively. The bundled Scala Connect REPL (spark-shell
			// --remote) is Ammonite-based and requires an interactive TTY, while the
			// classic spark-shell silently ignores .remote() ("spark.connect.remote
			// configuration is not supported in Classic mode") and runs the query
			// locally. python3 has no pre-created session, so builder.remote(...)
			// genuinely connects. The SESSION_CLASS guard fails loudly if it ever
			// falls back to a local session (kubeflow/spark-operator#3218).
			//
			// The image bundles PySpark under /opt/spark/python but not the Spark
			// Connect client dependencies. Install them as a separate step (network
			// bound) so the query exec gets its own, shorter budget and a hang is
			// cleanly attributed.
			By("Installing the Spark Connect Python client dependencies")
			pipOutput, err := runCommandWithInputTimeout(WaitTimeout, "",
				"kubectl", "exec", "-n", conn.Namespace, clientPod.Name, "--",
				"bash", "-c",
				"export HOME=/tmp && python3 -m pip install --quiet --no-cache-dir "+
					"grpcio grpcio-status protobuf pandas pyarrow",
			)
			Expect(err).NotTo(HaveOccurred(), "pip install of Spark Connect deps failed: %s", pipOutput)

			By("Executing a PySpark query via Spark Connect from the client pod")
			connectURL := fmt.Sprintf("sc://%s.%s.svc.cluster.local:%d", serviceName, conn.Namespace, connectServerPort)
			// Mirror the documented RHOAI workbench flow: a small createDataFrame +
			// filter, evaluated over Spark Connect. This is a LocalRelation that the
			// server's driver folds and returns directly (ConvertToLocalRelation), so
			// it exercises the full client->server gRPC query path without depending
			// on the distributed driver<->executor data plane. A distributed job such
			// as range(100).count() instead hangs in KIND because the separate
			// executor pods can't complete tasks back to the driver.
			//
			// Run unbuffered (python3 -u) with progress markers so partial output is
			// visible if anything stalls rather than being lost on a timeout kill.
			pythonScript := fmt.Sprintf(`import traceback
from pyspark.sql import SparkSession
print("CONNECTING")
try:
    spark = SparkSession.builder.remote("%s").getOrCreate()
    session_class = type(spark).__module__ + "." + type(spark).__name__
    print("SESSION_CLASS=" + session_class)
    if "connect" not in session_class.lower():
        print("REMOTE_SESSION_CHECK=FAILED: " + session_class)
        raise SystemExit(1)
    data = [("Alice", 34), ("Bob", 45), ("Claire", 66)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    print("QUERYING")
    rows = df.filter(df.Age > 40).collect()
    print("ROW_COUNT=" + str(len(rows)))
    print("NAMES=" + ",".join(sorted(r["Name"] for r in rows)))
except SystemExit:
    raise
except Exception:
    traceback.print_exc()
    raise SystemExit(1)
`, connectURL)
			output, err := runCommandWithInputTimeout(4*time.Minute, pythonScript,
				"kubectl", "exec", "-i", "-n", conn.Namespace, clientPod.Name, "--",
				"bash", "-c",
				"export HOME=/tmp && "+
					"export PYTHONPATH=/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip) && "+
					"python3 -u -",
			)
			Expect(err).NotTo(HaveOccurred(), "PySpark Spark Connect query failed: %s", output)
			Expect(output).To(MatchRegexp(`SESSION_CLASS=\S*connect`),
				"Spark Connect session fell back to a local session (kubeflow/spark-operator#3218): %s", output)
			Expect(output).NotTo(ContainSubstring("REMOTE_SESSION_CHECK=FAILED"),
				"Spark Connect remote-session check failed: %s", output)
			Expect(output).To(ContainSubstring("ROW_COUNT=2"), "expected 2 rows with Age > 40, got: %s", output)
			Expect(output).To(ContainSubstring("NAMES=Bob,Claire"), "expected Bob and Claire, got: %s", output)
		})
	})
})

func runCommandWithInputTimeout(timeout time.Duration, input string, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	if input != "" {
		cmd.Stdin = strings.NewReader(input)
	}
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), fmt.Errorf("command %s timed out after %s", name, timeout)
	}
	return string(out), err
}
