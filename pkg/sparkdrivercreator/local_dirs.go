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

const (
	envSparkLocalDirs    = "SPARK_LOCAL_DIRS"
	localDirVolumePrefix = "spark-local-dir-"
)

// localDirsFeatureStep ports Spark's LocalDirsFeatureStep for the reproducible
// case: no user-configured local dirs, so a single emptyDir volume is mounted at
// a generated /var/data/spark-<uuid> path and exported via SPARK_LOCAL_DIRS.
//
// The upstream step also honors spark.local.dir / SPARK_LOCAL_DIRS and the
// tmpfs medium option; those are out of scope for the deny-listed subset and
// would be handled explicitly if/when added.
type localDirsFeatureStep struct {
	conf *driverConf
}

func newLocalDirsFeatureStep(conf *driverConf) *localDirsFeatureStep {
	return &localDirsFeatureStep{conf: conf}
}

func (s *localDirsFeatureStep) configurePod(in sparkPod) sparkPod {
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	dir := s.conf.localDir
	volName := localDirVolumePrefix + "1"

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name:         volName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      volName,
		MountPath: dir,
	})
	container.Env = append(container.Env, corev1.EnvVar{
		Name:  envSparkLocalDirs,
		Value: dir,
	})

	return sparkPod{pod: pod, container: container}
}
