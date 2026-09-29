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

	// Supported volume types (the leading "<type>" segment of the conf key).
	volumeTypePVC      = "persistentVolumeClaim"
	volumeTypeEmptyDir = "emptyDir"
	volumeTypeHostPath = "hostPath"
	volumeTypeNFS      = "nfs"

	// mount.* leaf keys, shared by every volume type.
	volMountPathKey     = "mount.path"
	volMountReadOnlyKey = "mount.readOnly"
	volMountSubPathKey  = "mount.subPath"
	volMountSubPathExpr = "mount.subPathExpr"

	// options.* leaf keys, per type.
	volOptClaimNameKey = "options.claimName"
	volOptStorageClass = "options.storageClass"
	volOptSizeLimitKey = "options.sizeLimit"
	volOptMediumKey    = "options.medium"
	volOptPathKey      = "options.path"
	volOptTypeKey      = "options.type"
	volOptServerKey    = "options.server"

	// pvcOnDemand is the sentinel claimName that asks Spark to create the PVC.
	pvcOnDemand = "OnDemand"
	// pvcNameInfix builds the generated driver PVC name
	// "<resourceNamePrefix>-driver-pvc-<i>" (MountVolumesFeatureStep).
	pvcNameInfix = "-driver-pvc-"

	pvcKind = "PersistentVolumeClaim"

	// Access modes (MountVolumesFeatureStep): ReadWriteOncePod by default,
	// ReadWriteOnce under spark.kubernetes.legacy.useReadWriteOnceAccessMode.
	pvcAccessModeDefault = "ReadWriteOncePod"
	pvcAccessModeLegacy  = "ReadWriteOnce"
	confLegacyPVCAccess  = "spark.kubernetes.legacy.useReadWriteOnceAccessMode"
)

// driverVolume is a resolved driver volume spec: the shared mount fields plus the
// per-type pod volume source. persistentVolumeClaim, emptyDir, hostPath and nfs are
// supported (the reproducible subset); any other type errors out of parsing rather
// than being silently dropped.
type driverVolume struct {
	name             string
	mountPath        string
	mountReadOnly    bool
	mountSubPath     string
	mountSubPathExpr string

	// source is the pod volume source (built per type in parseDriverVolumes).
	source corev1.VolumeSource

	// PVC-object creation (persistentVolumeClaim + OnDemand only). createPVC is true
	// when Spark would create the PVC object (claimName was OnDemand and both
	// storageClass and sizeLimit are set); the remaining fields populate it.
	createPVC    bool
	claimName    string
	storageClass string
	sizeLimit    string
}

// parseDriverVolumes resolves the driver.volumes.* conf into ordered volume specs,
// mirroring KubernetesVolumeUtils.parseVolumesWithPrefix + MountVolumesFeatureStep's
// per-type volume construction. Specs are sorted by volume name for a deterministic
// order (Spark iterates an unordered Set, so multi-volume ordering is only
// well-defined here once names are sorted — the oracle relies on this).
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
		mountPath, ok := props[volMountPathKey]
		if !ok || mountPath == "" {
			return nil, fmt.Errorf("driver volume %q: missing %s", gk.name, volMountPathKey)
		}
		v := driverVolume{
			name:             gk.name,
			mountPath:        mountPath,
			mountReadOnly:    props[volMountReadOnlyKey] == "true",
			mountSubPath:     props[volMountSubPathKey],
			mountSubPathExpr: props[volMountSubPathExpr],
		}

		switch gk.typ {
		case volumeTypePVC:
			if err := resolvePVCVolume(&v, props, resourceNamePrefix, i); err != nil {
				return nil, err
			}
		case volumeTypeEmptyDir:
			if err := resolveEmptyDirVolume(&v, props); err != nil {
				return nil, err
			}
		case volumeTypeHostPath:
			if err := resolveHostPathVolume(&v, props); err != nil {
				return nil, err
			}
		case volumeTypeNFS:
			if err := resolveNFSVolume(&v, props); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("driver volume %q has unsupported type %q", gk.name, gk.typ)
		}

		out = append(out, v)
	}
	return out, nil
}

// resolvePVCVolume fills in the persistentVolumeClaim source and, for OnDemand
// claims with a storageClass and size, the PVC-object fields. i is the volume's
// index across all driver volumes, matching MountVolumesFeatureStep's substitution.
func resolvePVCVolume(v *driverVolume, props map[string]string, resourceNamePrefix string, i int) error {
	claimTemplate, ok := props[volOptClaimNameKey]
	if !ok || claimTemplate == "" {
		return fmt.Errorf("driver volume %q: missing %s", v.name, volOptClaimNameKey)
	}
	storageClass := props[volOptStorageClass]
	sizeLimit := props[volOptSizeLimitKey]

	// OnDemand -> "<prefix>-driver-pvc-<i>".
	claimName := strings.ReplaceAll(claimTemplate, pvcOnDemand,
		fmt.Sprintf("%s%s%d", resourceNamePrefix, pvcNameInfix, i))

	v.source = corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
			ClaimName: claimName,
			ReadOnly:  v.mountReadOnly,
		},
	}
	v.claimName = claimName
	v.storageClass = storageClass
	v.sizeLimit = sizeLimit
	// A PVC object is created only when it is OnDemand AND both storageClass and
	// size are set (MountVolumesFeatureStep gate).
	v.createPVC = claimTemplate == pvcOnDemand && storageClass != "" && sizeLimit != ""
	return nil
}

// resolveEmptyDirVolume fills in the emptyDir source. medium and sizeLimit are both
// optional; an unset medium yields the default ("") which serializes as an empty
// emptyDir object, matching Spark's medium.getOrElse("").
func resolveEmptyDirVolume(v *driverVolume, props map[string]string) error {
	src := &corev1.EmptyDirVolumeSource{}
	if medium := props[volOptMediumKey]; medium != "" {
		src.Medium = corev1.StorageMedium(medium)
	}
	if sizeLimit := props[volOptSizeLimitKey]; sizeLimit != "" {
		q, err := resource.ParseQuantity(sizeLimit)
		if err != nil {
			return fmt.Errorf("driver volume %q: invalid %s %q: %w", v.name, volOptSizeLimitKey, sizeLimit, err)
		}
		src.SizeLimit = &q
	}
	v.source = corev1.VolumeSource{EmptyDir: src}
	return nil
}

// resolveHostPathVolume fills in the hostPath source. path is required; type is
// optional (unset means no pre-mount checks, matching Spark's default of "").
func resolveHostPathVolume(v *driverVolume, props map[string]string) error {
	path := props[volOptPathKey]
	if path == "" {
		return fmt.Errorf("driver volume %q: missing %s", v.name, volOptPathKey)
	}
	src := &corev1.HostPathVolumeSource{Path: path}
	if t := props[volOptTypeKey]; t != "" {
		ht := corev1.HostPathType(t)
		src.Type = &ht
	}
	v.source = corev1.VolumeSource{HostPath: src}
	return nil
}

// resolveNFSVolume fills in the nfs source. Both path and server are required.
func resolveNFSVolume(v *driverVolume, props map[string]string) error {
	path := props[volOptPathKey]
	if path == "" {
		return fmt.Errorf("driver volume %q: missing %s", v.name, volOptPathKey)
	}
	server := props[volOptServerKey]
	if server == "" {
		return fmt.Errorf("driver volume %q: missing %s", v.name, volOptServerKey)
	}
	v.source = corev1.VolumeSource{
		NFS: &corev1.NFSVolumeSource{Path: path, Server: server},
	}
	return nil
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
			Name:         v.name,
			VolumeSource: v.source,
		})
	}

	return sparkPod{pod: pod, container: container}
}

// buildDriverPVCs returns the on-demand PersistentVolumeClaim objects Spark's
// MountVolumesFeatureStep would create as additional (post-pod, owner-referenced)
// resources — one per driver volume whose claimName was OnDemand with a
// storageClass and size. The PVC carries no namespace in its body (Spark omits it,
// like the Service) and the spark-app-selector label. Non-PVC volume types create
// no object, so they never appear here.
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
