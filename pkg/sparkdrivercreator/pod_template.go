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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// Pod-template constants, mirroring Spark's PodTemplateConfigMapStep and
// Constants.scala. The classic operator submission path always synthesizes an
// executor pod-template file and passes it via
// spark.kubernetes.executor.podTemplateFile; PodTemplateConfigMapStep then
// embeds that file verbatim in an immutable ConfigMap, mounts it on the driver,
// and rewrites the conf to the in-pod mount path so the driver can hand it to
// the executors it launches.
const (
	// podSpecConfigMapSuffix is appended to the resource-name prefix to form the
	// executor pod-template ConfigMap name (KubernetesClientUtils: the driver's
	// "<prefix>-driver-podspec-conf-map").
	podSpecConfigMapSuffix = "-driver-podspec-conf-map"
	// podTemplateKey is the single data key under which the executor template text
	// is stored (Constants.POD_TEMPLATE_CONFIGMAP_KEY).
	podTemplateKey = "podspec-configmap-key"
	// podTemplateVolumeName / podTemplateMountPath / podTemplateFileName describe
	// how the ConfigMap is projected onto the driver pod
	// (Constants.POD_TEMPLATE_VOLUME, POD_TEMPLATE_DIR_NAME, EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME).
	podTemplateVolumeName = "pod-template-volume"
	podTemplateMountPath  = "/opt/spark/pod-template"
	podTemplateFileName   = "pod-spec-template.yml"

	// execPodTemplateMountFile is the mount-path value
	// spark.kubernetes.executor.podTemplateFile is rewritten to. Unlike the
	// submit-time driver.podTemplateFile (a host path that is dead at runtime and
	// omitted here), this points inside the driver pod, so it is reproducible and
	// kept.
	execPodTemplateMountFile = podTemplateMountPath + "/" + podTemplateFileName

	confExecPodTemplateFile          = "spark.kubernetes.executor.podTemplateFile"
	confExecPodTemplateContainerName = "spark.kubernetes.executor.podTemplateContainerName"

	// defaultExecutorContainerNm is the executor container name the operator always
	// passes (common.Spark3DefaultExecutorContainerName); Spark serializes it as
	// spark.kubernetes.executor.podTemplateContainerName.
	defaultExecutorContainerNm = "spark-kubernetes-executor"
)

// podTemplateConfigMapFeatureStep is the pure-Go port of Spark's
// PodTemplateConfigMapStep. When the submission carries an executor pod template
// it mounts the (separately built) executor pod-template ConfigMap on the driver
// so the driver can read it when launching executors. When no template is present
// it is a no-op, so stock cases (minimal/overrides) are unaffected.
type podTemplateConfigMapFeatureStep struct {
	conf *driverConf
}

func newPodTemplateConfigMapFeatureStep(conf *driverConf) *podTemplateConfigMapFeatureStep {
	return &podTemplateConfigMapFeatureStep{conf: conf}
}

func (s *podTemplateConfigMapFeatureStep) configurePod(in sparkPod) sparkPod {
	if !s.conf.hasExecPodTemplate {
		return in
	}
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      podTemplateVolumeName,
		MountPath: podTemplateMountPath,
	})
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: podTemplateVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: s.conf.podSpecConfigMapName},
				// No file mode here: Spark's pod-template item omits it (unlike the
				// conf volume, which stamps 0644).
				Items: []corev1.KeyToPath{
					{Key: podTemplateKey, Path: podTemplateFileName},
				},
			},
		},
	})

	return sparkPod{pod: pod, container: container}
}

// buildPodSpecConfigMap ports PodTemplateConfigMapStep's ConfigMap: the executor
// pod-template text embedded verbatim under a single data key, wrapped in an
// immutable ConfigMap owned by the driver pod. Unlike the driver conf ConfigMap
// it carries no namespace in its body (Spark omits it here) and no extra data
// keys. Only built when the submission carries an executor template.
func buildPodSpecConfigMap(conf *driverConf) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: configMapAPIVersion, Kind: configMapKind},
		ObjectMeta: metav1.ObjectMeta{
			Name:            conf.podSpecConfigMapName,
			OwnerReferences: driverPodOwnerReference(conf),
		},
		Immutable: ptr.To(true),
		Data: map[string]string{
			podTemplateKey: conf.execPodTemplateYAML,
		},
	}
}
