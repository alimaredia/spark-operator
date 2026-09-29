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
	"k8s.io/utils/ptr"
)

// Conf-volume constants, mirroring KubernetesClientApplication.run and
// KubernetesClientUtils. The driver's spark.properties is delivered via a
// ConfigMap mounted at SPARK_CONF_DIR; the entry lands as a single key/path.
const (
	envSparkConfDir    = "SPARK_CONF_DIR"
	sparkConfDirPath   = "/opt/spark/conf"
	confVolumeName     = "spark-conf-volume-driver"
	sparkPropertiesKey = "spark.properties"
	// confFileMode is 0644 (decimal 420), the mode Spark's buildKeyToPathObjects
	// stamps on each projected conf file.
	confFileMode int32 = 420
)

// attachConfVolume ports the conf-volume wiring Spark performs in
// KubernetesClientApplication.run *after* buildFromFeatures: it exports
// SPARK_CONF_DIR, mounts the driver ConfigMap at that path, and adds the backing
// volume. This is not a feature step — Spark does it inline once the steps have
// run — so we apply it last, then the caller folds the container into the pod.
func attachConfVolume(in sparkPod, conf *driverConf) sparkPod {
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	container.Env = append(container.Env, corev1.EnvVar{
		Name:  envSparkConfDir,
		Value: sparkConfDirPath,
	})
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      confVolumeName,
		MountPath: sparkConfDirPath,
	})
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: confVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: conf.configMapName},
				Items: []corev1.KeyToPath{
					{
						Key:  sparkPropertiesKey,
						Path: sparkPropertiesKey,
						Mode: ptr.To(confFileMode),
					},
				},
			},
		},
	})

	return sparkPod{pod: pod, container: container}
}
