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
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Driver-volume constants, mirroring Spark's KubernetesVolumeUtils /
// MountVolumesFeatureStep. Driver volumes are supplied entirely as conf
// (spark.kubernetes.driver.volumes.<type>.<name>.…), so the native builder reads
// them from the resolved SparkConf exactly as Spark does.
const (
	driverVolumesConfPrefix = "spark.kubernetes.driver.volumes."

	volumeTypePVC = "persistentVolumeClaim"

	// mount.* / options.* leaf keys.
	volMountPathKey     = "mount.path"
	volMountReadOnlyKey = "mount.readOnly"
	volMountSubPathKey  = "mount.subPath"
	volMountSubPathExpr = "mount.subPathExpr"
	volOptClaimNameKey  = "options.claimName"
	volOptStorageClass  = "options.storageClass"
	volOptSizeLimitKey  = "options.sizeLimit"

	// pvcOnDemand is the sentinel claimName that asks Spark to create the PVC.
	pvcOnDemand = "OnDemand"
	// pvcNameInfix/Suffix build the generated driver PVC name
	// "<resourceNamePrefix>-driver-pvc-<i>" (MountVolumesFeatureStep).
	pvcNameInfix = "-driver-pvc-"

	pvcKind = "PersistentVolumeClaim"

	// Access modes (MountVolumesFeatureStep): ReadWriteOncePod by default,
	// ReadWriteOnce under spark.kubernetes.legacy.useReadWriteOnceAccessMode.
	pvcAccessModeDefault = "ReadWriteOncePod"
	pvcAccessModeLegacy  = "ReadWriteOnce"
	confLegacyPVCAccess  = "spark.kubernetes.legacy.useReadWriteOnceAccessMode"
)

// driverVolume is a resolved driver volume spec. Only persistentVolumeClaim is
// ported so far (the reproducible on-demand PVC path); other volume types return
// an error from parsing rather than being silently dropped.
type driverVolume struct {
	name             string
	mountPath        string
	mountReadOnly    bool
	mountSubPath     string
	mountSubPathExpr string

	// claimName is the resolved PVC name (OnDemand already substituted).
	claimName string
	// createPVC is true when Spark would create the PVC object (claimName was
	// OnDemand and both storageClass and sizeLimit are set).
	createPVC    bool
	storageClass string
	sizeLimit    string
}

// parseDriverVolumes resolves the driver.volumes.* conf into ordered volume
// specs, mirroring KubernetesVolumeUtils.parseVolumesWithPrefix +
// MountVolumesFeatureStep's OnDemand substitution. Specs are sorted by volume
// name for a deterministic order (Spark iterates an unordered Set, so multi-volume
// ordering is only well-defined here for a single volume — enough for the oracle).
func parseDriverVolumes(sparkConf map[string]string, resourceNamePrefix string) ([]driverVolume, error) {
	// Group leaf keys by "<type>.<name>".
	type key struct{ typ, name string }
	groups := map[key]map[string]string{}
	order := []key{}
	for k, v := range sparkConf {
		if !strings.HasPrefix(k, driverVolumesConfPrefix) {
			continue
		}
		rest := strings.TrimPrefix(k, driverVolumesConfPrefix)
		parts := strings.SplitN(rest, ".", 3)
		if len(parts) < 3 {
			continue
		}
		gk := key{typ: parts[0], name: parts[1]}
		if groups[gk] == nil {
			groups[gk] = map[string]string{}
			order = append(order, gk)
		}
		groups[gk][parts[2]] = v
	}
	if len(groups) == 0 {
		return nil, nil
	}

	sort.Slice(order, func(i, j int) bool { return order[i].name < order[j].name })

	out := make([]driverVolume, 0, len(order))
	for i, gk := range order {
		props := groups[gk]
		if gk.typ != volumeTypePVC {
			return nil, fmt.Errorf("driver volume %q has type %q; only %q is supported by the native builder",
				gk.name, gk.typ, volumeTypePVC)
		}
		mountPath, ok := props[volMountPathKey]
		if !ok || mountPath == "" {
			return nil, fmt.Errorf("driver volume %q: missing %s", gk.name, volMountPathKey)
		}
		claimTemplate, ok := props[volOptClaimNameKey]
		if !ok || claimTemplate == "" {
			return nil, fmt.Errorf("driver volume %q: missing %s", gk.name, volOptClaimNameKey)
		}
		storageClass := props[volOptStorageClass]
		sizeLimit := props[volOptSizeLimitKey]

		// OnDemand -> "<prefix>-driver-pvc-<i>" (i is the index across all driver
		// volumes, matching MountVolumesFeatureStep).
		claimName := strings.ReplaceAll(claimTemplate, pvcOnDemand,
			fmt.Sprintf("%s%s%d", resourceNamePrefix, pvcNameInfix, i))

		out = append(out, driverVolume{
			name:             gk.name,
			mountPath:        mountPath,
			mountReadOnly:    props[volMountReadOnlyKey] == "true",
			mountSubPath:     props[volMountSubPathKey],
			mountSubPathExpr: props[volMountSubPathExpr],
			claimName:        claimName,
			// A PVC object is created only when it is OnDemand AND both storageClass
			// and size are set (MountVolumesFeatureStep gate).
			createPVC:    claimTemplate == pvcOnDemand && storageClass != "" && sizeLimit != "",
			storageClass: storageClass,
			sizeLimit:    sizeLimit,
		})
	}
	return out, nil
}

// mountVolumesFeatureStep is the pure-Go port of Spark's MountVolumesFeatureStep:
// it adds each driver volume (and its mount) to the driver pod. It runs before the
// pod-template and local-dirs steps, matching Spark's feature order, so the volume
// list order is: driver volumes, pod-template, local-dir, conf. It is a no-op when
// there are no driver volumes, so stock cases are unaffected.
type mountVolumesFeatureStep struct {
	conf *driverConf
}

func newMountVolumesFeatureStep(conf *driverConf) *mountVolumesFeatureStep {
	return &mountVolumesFeatureStep{conf: conf}
}

func (s *mountVolumesFeatureStep) configurePod(in sparkPod) sparkPod {
	if len(s.conf.driverVolumes) == 0 {
		return in
	}
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	for _, v := range s.conf.driverVolumes {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:        v.name,
			MountPath:   v.mountPath,
			ReadOnly:    v.mountReadOnly,
			SubPath:     v.mountSubPath,
			SubPathExpr: v.mountSubPathExpr,
		})
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: v.name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: v.claimName,
					ReadOnly:  v.mountReadOnly,
				},
			},
		})
	}

	return sparkPod{pod: pod, container: container}
}

// buildDriverPVCs returns the on-demand PersistentVolumeClaim objects Spark's
// MountVolumesFeatureStep would create as additional (post-pod, owner-referenced)
// resources — one per driver volume whose claimName was OnDemand with a
// storageClass and size. The PVC carries no namespace in its body (Spark omits it,
// like the Service) and the spark-app-selector label.
func buildDriverPVCs(conf *driverConf) []*corev1.PersistentVolumeClaim {
	var pvcs []*corev1.PersistentVolumeClaim
	for _, v := range conf.driverVolumes {
		if !v.createPVC {
			continue
		}
		pvcs = append(pvcs, &corev1.PersistentVolumeClaim{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: pvcKind},
			ObjectMeta: metav1.ObjectMeta{
				Name:            v.claimName,
				Labels:          map[string]string{labelSparkAppSelector: conf.appID},
				OwnerReferences: driverPodOwnerReference(conf),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{conf.pvcAccessMode},
				StorageClassName: &v.storageClass,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(v.sizeLimit),
					},
				},
			},
		})
	}
	return pvcs
}
