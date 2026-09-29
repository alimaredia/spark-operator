/*
Copyright 2025 The Kubeflow authors.

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

package sparkdrivercreator

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Spark's bare k8s labels (Constants.scala). NOTE these are Spark's own label
// keys — deliberately NOT the operator's namespaced keys in pkg/common — because
// the driver pod must match what stock spark-submit produces.
const (
	labelSparkVersion     = "spark-version"
	labelSparkAppSelector = "spark-app-selector"
	labelSparkAppName     = "spark-app-name"
	labelSparkRole        = "spark-role"
	sparkRoleDriver       = "driver"
)

// Env var names Spark sets on the driver container (Constants.scala).
const (
	envSparkUser          = "SPARK_USER"
	envApplicationID      = "SPARK_APPLICATION_ID"
	envDriverBindAddress  = "SPARK_DRIVER_BIND_ADDRESS"
	driverBindAddressPath = "status.podIP"
)

// Container port names (Constants.scala).
const (
	portNameDriverRPC     = "driver-rpc-port"
	portNameBlockManager  = "blockmanager"
	portNameUI            = "spark-ui"
	portNameConnectServer = "spark-connect"
)

// sparkPod mirrors Spark's SparkPod: the pod plus the "main" container that
// feature steps thread through and mutate. Kept separate (rather than indexing
// into pod.Spec.Containers) so steps compose exactly like Spark's
// KubernetesDriverBuilder.buildFromFeatures, and the container is folded back
// into the pod only once, after the last step.
type sparkPod struct {
	pod       *corev1.Pod
	container *corev1.Container
}

// initialSparkPod returns the empty starting point steps build up from, matching
// Spark's SparkPod.initialPod().
func initialSparkPod() sparkPod {
	return sparkPod{pod: &corev1.Pod{}, container: &corev1.Container{}}
}

// featureStep configures a sparkPod, mirroring Spark's
// KubernetesFeatureConfigStep.configurePod. Steps are folded left-to-right.
type featureStep interface {
	configurePod(sparkPod) sparkPod
}

// basicDriverFeatureStep is the pure-Go port of Spark's BasicDriverFeatureStep.
// It sets the driver pod's identity (name, labels, annotations), the main
// container (name, image, ports, core env, resources), and pod-level scheduling
// (restartPolicy, nodeSelector, schedulerName). Pinned by test/oracle.
type basicDriverFeatureStep struct {
	conf *driverConf
}

func newBasicDriverFeatureStep(conf *driverConf) *basicDriverFeatureStep {
	return &basicDriverFeatureStep{conf: conf}
}

func (s *basicDriverFeatureStep) configurePod(in sparkPod) sparkPod {
	c := s.conf
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	// --- container ---
	if container.Name == "" {
		container.Name = defaultDriverContainerNm
	}
	container.Image = c.image
	container.ImagePullPolicy = c.imagePullPolicy
	container.Ports = append(container.Ports, s.containerPorts()...)
	container.Env = append(container.Env, s.containerEnv()...)
	container.Resources = s.resources()

	// --- pod metadata ---
	pod.Name = c.driverPodName
	pod.Labels = mergeInto(pod.Labels, c.labels)
	pod.Annotations = mergeInto(pod.Annotations, c.annotations)

	// --- pod spec ---
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	pod.Spec.NodeSelector = mergeInto(pod.Spec.NodeSelector, c.nodeSelector)
	if c.schedulerName != "" {
		pod.Spec.SchedulerName = c.schedulerName
	}

	return sparkPod{pod: pod, container: container}
}

// containerEnv builds the driver container's core env in Spark's order:
// SPARK_USER, SPARK_APPLICATION_ID, the user's custom driver env, then
// SPARK_DRIVER_BIND_ADDRESS (a fieldRef to the pod IP).
func (s *basicDriverFeatureStep) containerEnv() []corev1.EnvVar {
	c := s.conf
	sparkUser := c.proxyUser
	if sparkUser == "" {
		sparkUser = currentUserName()
	}

	env := []corev1.EnvVar{
		{Name: envSparkUser, Value: sparkUser},
		{Name: envApplicationID, Value: c.appID},
	}
	env = append(env, c.environment...)
	env = append(env, corev1.EnvVar{
		Name: envDriverBindAddress,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "v1",
				FieldPath:  driverBindAddressPath,
			},
		},
	})
	return env
}

// containerPorts emits the driver container ports from the shared resolved port
// set, as TCP container ports.
func (s *basicDriverFeatureStep) containerPorts() []corev1.ContainerPort {
	var ports []corev1.ContainerPort
	for _, p := range s.conf.ports() {
		ports = append(ports, corev1.ContainerPort{
			Name:          p.name,
			ContainerPort: p.port,
			Protocol:      corev1.ProtocolTCP,
		})
	}
	return ports
}

// resources builds the driver container's resource requirements: a cpu request
// (and optional cpu limit), plus memory-with-overhead as both request and limit.
func (s *basicDriverFeatureStep) resources() corev1.ResourceRequirements {
	c := s.conf
	mem := resource.MustParse(quantityMiB(c.memoryWithOverheadMiB))
	req := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(c.coresRequest),
		corev1.ResourceMemory: mem,
	}
	lim := corev1.ResourceList{
		corev1.ResourceMemory: mem,
	}
	if c.limitCores != "" {
		lim[corev1.ResourceCPU] = resource.MustParse(c.limitCores)
	}
	return corev1.ResourceRequirements{Requests: req, Limits: lim}
}

// quantityMiB renders a MiB count as a k8s quantity string, e.g. 1408 -> "1408Mi".
func quantityMiB(mib int64) string {
	return strconv.FormatInt(mib, 10) + "Mi"
}

// mergeInto merges src into dst (allocating dst if nil), src winning on key
// collisions. Returns nil if the result is empty so the field stays omitted.
func mergeInto(dst, src map[string]string) map[string]string {
	if len(dst) == 0 && len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = map[string]string{}
	}
	for k, v := range src {
		dst[k] = v
	}
	if len(dst) == 0 {
		return nil
	}
	return dst
}
