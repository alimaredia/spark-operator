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
// pod templates, custom feature steps, proxy-user, …) are out of scope here;
// they are rejected up front by the admission webhook (the "deny-list" in
// approaches-comparison.md), so Build only ever sees reproducible inputs.
package sparkdrivercreator

import (
	"errors"

	corev1 "k8s.io/api/core/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// ErrNotImplemented is returned by Build until the pure-Go port lands. The
// oracle test skips (rather than fails) while this is the case, so the golden
// files can be committed and wired ahead of the implementation.
var ErrNotImplemented = errors.New("sparkdrivercreator: Build not yet implemented")

// DriverResources is everything the driver pod needs to exist: the pod itself
// plus the resources spark-submit would otherwise create and own.
type DriverResources struct {
	Pod       *corev1.Pod
	ConfigMap *corev1.ConfigMap
	Service   *corev1.Service
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
func (c *SparkDriverCreator) Build(app *v1beta2.SparkApplication) (*DriverResources, error) {
	// TODO(sparkdrivercreator): port BasicDriverFeatureStep, DriverServiceFeatureStep,
	// LocalDirsFeatureStep, MountSecrets/EnvSecrets/MountVolumes, and
	// DriverCommandFeatureStep, plus the driver ConfigMap assembly. Pin each
	// against test/oracle/golden/<version>/.
	return nil, ErrNotImplemented
}
