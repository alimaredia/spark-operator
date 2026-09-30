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
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	envSparkLocalDirs    = "SPARK_LOCAL_DIRS"
	localDirVolumePrefix = "spark-local-dir-"

	// confLocalDirsTmpfs is spark.kubernetes.local.dirs.tmpfs: when true, the
	// synthesized default local-dir emptyDir is backed by tmpfs (Memory medium)
	// instead of node disk.
	confLocalDirsTmpfs = "spark.kubernetes.local.dirs.tmpfs"
)

// localDirsFeatureStep ports Spark's LocalDirsFeatureStep. It has two branches,
// matching upstream:
//
//   - Reuse: when an earlier step (MountVolumesFeatureStep) already mounted
//     volume(s) named "spark-local-dir-*" — i.e. the user declared their own
//     spark-local-dir-N volume in the SparkApplication, carrying its own emptyDir
//     medium/sizeLimit (or a hostPath/PVC source) — Spark does not add a second
//     default volume. It only points SPARK_LOCAL_DIRS at those existing mount
//     paths, preserving the user's volume verbatim.
//
//   - Default: with no user-configured local dirs, a single emptyDir volume is
//     mounted at a generated /var/data/spark-<uuid> path and exported via
//     SPARK_LOCAL_DIRS. The emptyDir is backed by tmpfs (Memory) when
//     spark.kubernetes.local.dirs.tmpfs is set, else node disk.
//
// spark.local.dir / SPARK_LOCAL_DIRS overrides of the default path remain out of
// scope for the deny-listed subset and would be handled explicitly if/when added.
type localDirsFeatureStep struct {
	conf *driverConf
}

func newLocalDirsFeatureStep(conf *driverConf) *localDirsFeatureStep {
	return &localDirsFeatureStep{conf: conf}
}

func (s *localDirsFeatureStep) configurePod(in sparkPod) sparkPod {
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	// Reuse branch: honor local-dir volumes a prior step already mounted.
	var existingDirs []string
	for _, m := range container.VolumeMounts {
		if strings.HasPrefix(m.Name, localDirVolumePrefix) {
			existingDirs = append(existingDirs, m.MountPath)
		}
	}
	if len(existingDirs) > 0 {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  envSparkLocalDirs,
			Value: strings.Join(existingDirs, ","),
		})
		return sparkPod{pod: pod, container: container}
	}

	// Default branch: synthesize a single emptyDir at the generated path.
	dir := s.conf.localDir
	volName := localDirVolumePrefix + "1"

	emptyDir := &corev1.EmptyDirVolumeSource{}
	if s.conf.sparkConf[confLocalDirsTmpfs] == "true" {
		emptyDir.Medium = corev1.StorageMediumMemory
	}

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name:         volName,
		VolumeSource: corev1.VolumeSource{EmptyDir: emptyDir},
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
