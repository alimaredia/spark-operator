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
)

// Hadoop-conf constants, mirroring Spark's Config.scala / Constants.scala.
const (
	// confHadoopConfigMapName names a pre-existing ConfigMap holding the Hadoop
	// config files (KUBERNETES_HADOOP_CONF_CONFIG_MAP). This is the input conf.
	confHadoopConfigMapName = "spark.kubernetes.hadoop.configMapName"
	// confExecHadoopConfigMapName is the system property HadoopConfDriverFeatureStep
	// records so executors mount the same Hadoop ConfigMap (HADOOP_CONFIG_MAP_NAME).
	confExecHadoopConfigMapName = "spark.kubernetes.executor.hadoopConfigMapName"

	// hadoopConfVolumeName / hadoopConfDirPath / envHadoopConfDir are the pod volume
	// name, its in-pod mount path, and the env var Spark points at that path.
	hadoopConfVolumeName = "hadoop-properties"
	hadoopConfDirPath    = "/opt/hadoop/conf"
	envHadoopConfDir     = "HADOOP_CONF_DIR"
)

// hadoopConfFeatureStep is the pure-Go port of Spark's HadoopConfDriverFeatureStep,
// limited to its pre-existing-ConfigMap mode: when spark.kubernetes.hadoop.configMapName
// names an existing ConfigMap, it mounts that ConfigMap on the driver as the Hadoop
// config dir and sets HADOOP_CONF_DIR. It creates no API object (only the
// HADOOP_CONF_DIR local-directory mode does, which is a submit-host artifact the
// native path does not reproduce and cannot be expressed via conf). It runs after
// MountVolumes and before PodTemplateConfigMap/LocalDirs in Spark's feature order, so
// the HADOOP_CONF_DIR env precedes SPARK_LOCAL_DIRS. No-op when the conf is unset.
type hadoopConfFeatureStep struct {
	conf *driverConf
}

func newHadoopConfFeatureStep(conf *driverConf) *hadoopConfFeatureStep {
	return &hadoopConfFeatureStep{conf: conf}
}

func (s *hadoopConfFeatureStep) configurePod(in sparkPod) sparkPod {
	if s.conf.hadoopConfigMapName == "" {
		return in
	}
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: hadoopConfVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: s.conf.hadoopConfigMapName},
			},
		},
	})
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      hadoopConfVolumeName,
		MountPath: hadoopConfDirPath,
	})
	container.Env = append(container.Env, corev1.EnvVar{
		Name:  envHadoopConfDir,
		Value: hadoopConfDirPath,
	})

	return sparkPod{pod: pod, container: container}
}
