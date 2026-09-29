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

// Package sparkdrivercreator builds the Spark driver pod (and the resources
// spark-submit would otherwise own — the driver ConfigMap and Service) from a
// SparkApplication, in pure Go.
//
// It is the reproducible subset of Spark's
// KubernetesDriverBuilder.buildFromFeatures (see spark-submit-flow-summary.md).
// Its correctness is pinned by the real-Spark oracle in test/oracle: for each
// supported Spark version, stock spark-submit's driver pod / ConfigMap /
// Service are captured as golden files, and Build's output must match them.
//
// Non-reproducible submit-time behaviors (Kerberos, --packages, file uploads,
// custom feature steps, proxy-user, …) are out of scope here; they are rejected
// up front by the admission webhook (the "deny-list" in approaches-comparison.md),
// so Build only ever sees reproducible inputs. Pod templates ARE reproducible: the
// operator hands the executor template to Build via BuildOptions and it is
// materialized the same way spark-submit's PodTemplateConfigMapStep does.
package sparkdrivercreator

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// podAPIVersion / podKind stamp the driver pod's TypeMeta so the marshaled
// object carries apiVersion/kind like the captured golden does.
const (
	podAPIVersion = "v1"
	podKind       = "Pod"
)

// driverPodOwnerReference returns the owner reference the driver pod's owned
// resources (ConfigMap, Service) carry. It points at the driver pod and marks it
// the controller, but leaves uid unset: the pod's uid is server-assigned at
// creation time, so the submitter adapter fills it in after creating the pod
// (mirroring Spark's Client.run -> addOwnerReference). The oracle normalizes the
// uid on both sides.
func driverPodOwnerReference(conf *driverConf) []metav1.OwnerReference {
	return []metav1.OwnerReference{
		{
			APIVersion: podAPIVersion,
			Kind:       podKind,
			Name:       conf.driverPodName,
			Controller: ptr.To(true),
		},
	}
}

// DriverResources is everything the driver pod needs to exist: the pod itself
// plus the resources spark-submit would otherwise create and own.
type DriverResources struct {
	Pod       *corev1.Pod
	ConfigMap *corev1.ConfigMap
	Service   *corev1.Service
	// PodSpecConfigMap is the immutable executor pod-template ConfigMap Spark's
	// PodTemplateConfigMapStep creates. It is nil unless the submission carries an
	// executor pod template (BuildOptions.ExecutorPodTemplate).
	PodSpecConfigMap *corev1.ConfigMap
}

// BuildOptions carries submit-time inputs the operator supplies out-of-band —
// values that are not part of the SparkApplication spec proper but that stock
// spark-submit would otherwise materialize itself. Today that is the executor
// pod template the classic operator path always synthesizes and passes as a
// file; here it is handed in as an object. A nil template means no pod-template
// ConfigMap (and no driver volume/mount), so stock cases stay unaffected.
//
// The driver pod template is intentionally absent: for the minimal templates the
// operator emits, merging it onto the driver pod is a no-op on the pod body, and
// its only conf trace (driver.podTemplateFile) is a host-specific submit-time
// artifact the native path drops. Non-minimal driver-template merge is a
// documented follow-up.
type BuildOptions struct {
	ExecutorPodTemplate *corev1.PodTemplateSpec
}

// SparkDriverCreator builds driver resources from a SparkApplication. It makes
// no Kubernetes API calls and holds no reconcile state — pure input → output,
// so it is trivially unit-testable and comparable against the oracle. The thin
// SparkApplicationSubmitter adapter in the controller wraps it to apply the
// resources.
type SparkDriverCreator struct{}

// New returns a SparkDriverCreator.
func New() *SparkDriverCreator { return &SparkDriverCreator{} }

// Build runs the reproducible feature steps and returns the driver resources
// for app. It assumes the deny-list has already rejected non-reproducible
// inputs.
func (c *SparkDriverCreator) Build(app *v1beta2.SparkApplication, opts BuildOptions) (*DriverResources, error) {
	conf, err := newDriverConf(app, opts)
	if err != nil {
		return nil, fmt.Errorf("sparkdrivercreator: resolving driver conf: %w", err)
	}

	res := &DriverResources{
		Pod:       buildDriverPod(conf),
		Service:   buildDriverService(conf),
		ConfigMap: buildDriverConfigMap(conf),
	}
	if conf.hasExecPodTemplate {
		res.PodSpecConfigMap = buildPodSpecConfigMap(conf)
	}
	return res, nil
}

// buildDriverPod runs the reproducible feature steps in Spark's order and folds
// the resulting container into the pod, mirroring
// KubernetesDriverBuilder.buildFromFeatures followed by the inline conf-volume
// wiring in KubernetesClientApplication.run.
func buildDriverPod(conf *driverConf) *corev1.Pod {
	steps := []featureStep{
		newBasicDriverFeatureStep(conf),
		newDriverKubernetesCredentialsFeatureStep(conf),
		// PodTemplateConfigMapStep runs before LocalDirsFeatureStep in Spark's
		// feature order, so the pod-template volume/mount precede the local-dir
		// ones. It is a no-op when there is no executor template.
		newPodTemplateConfigMapFeatureStep(conf),
		newLocalDirsFeatureStep(conf),
		newDriverCommandFeatureStep(conf),
	}

	sp := initialSparkPod()
	for _, step := range steps {
		sp = step.configurePod(sp)
	}
	// Conf volume is attached after the feature steps, exactly as Client.run does.
	sp = attachConfVolume(sp, conf)

	pod := sp.pod
	pod.TypeMeta = metav1.TypeMeta{APIVersion: podAPIVersion, Kind: podKind}
	pod.Spec.Containers = append(pod.Spec.Containers, *sp.container)
	return pod
}
