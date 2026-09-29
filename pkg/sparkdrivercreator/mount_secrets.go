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

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// Secret conf prefixes, mirroring Spark's Config.scala. Both are supplied to the
// driver entirely as conf (the operator translates spec.driver.secrets /
// spec.driver.envSecretKeyRefs into these), so the native builder reads them from
// the resolved SparkConf exactly as KubernetesConf does.
const (
	// spark.kubernetes.driver.secrets.<secretName>=<mountPath> (MountSecretsFeatureStep).
	driverSecretsConfPrefix = "spark.kubernetes.driver.secrets."
	// spark.kubernetes.driver.secretKeyRef.<envName>=<secretName>:<key> (EnvSecretsFeatureStep).
	driverSecretKeyRefConfPrefix = "spark.kubernetes.driver.secretKeyRef."

	// secretVolumeNameSuffix is appended to the secret name to form the pod volume
	// name (MountSecretsFeatureStep.secretVolumeName: "<secretName>-volume").
	secretVolumeNameSuffix = "-volume"
)

// addDriverSecretConf folds the typed driver secret fields (spec.driver.secrets,
// spec.driver.envSecretKeyRefs) into the resolved SparkConf, exactly mirroring the
// operator's driverSecretOption / EnvSecretKeyRefs translation to --conf (which
// stock spark-submit then merges and serializes). Doing it here keeps a single
// source of truth: both the feature steps and the spark.properties passthrough read
// these from the conf, just as they do for a submission that set them via sparkConf
// directly. It is a no-op when the app references no secrets.
//
// NOTE: the GCPServiceAccount / HadoopDelegationToken secret types cause the
// operator to also emit a spark.kubernetes.driverEnv.* conf pointing at the
// credential file; only the base secrets conf is folded in here. Those types are a
// documented follow-up (the oracle would flag them via a red test), so a case using
// them is out of scope until then.
func addDriverSecretConf(sparkConf map[string]string, driver v1beta2.DriverSpec) {
	for _, secret := range driver.Secrets {
		sparkConf[driverSecretsConfPrefix+secret.Name] = secret.Path
	}
	for envName, ref := range driver.EnvSecretKeyRefs {
		sparkConf[driverSecretKeyRefConfPrefix+envName] = ref.Name + ":" + ref.Key
	}
}

// secretMount is a resolved mounted-secret spec (MountSecretsFeatureStep): a
// pre-existing Secret referenced by name and mounted at path. No object is created
// — the Secret must already exist in the namespace.
type secretMount struct {
	name string
	path string
}

// envSecret is a resolved secretKeyRef env var (EnvSecretsFeatureStep): an env var
// whose value comes from key <key> of the pre-existing Secret <secretName>.
type envSecret struct {
	envName    string
	secretName string
	key        string
}

// parseDriverSecrets resolves spark.kubernetes.driver.secrets.* into ordered
// mounted-secret specs, mirroring KubernetesConf.secretNamesToMountPaths. Specs
// are sorted by secret name for a deterministic volume order (Spark iterates an
// unordered Map, so multi-secret ordering is only well-defined here for a single
// secret — enough for the oracle).
func parseDriverSecrets(sparkConf map[string]string) []secretMount {
	var out []secretMount
	for k, v := range sparkConf {
		if !strings.HasPrefix(k, driverSecretsConfPrefix) {
			continue
		}
		name := strings.TrimPrefix(k, driverSecretsConfPrefix)
		if name == "" {
			continue
		}
		out = append(out, secretMount{name: name, path: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// parseDriverEnvSecrets resolves spark.kubernetes.driver.secretKeyRef.* into
// ordered secretKeyRef env vars, mirroring KubernetesConf.secretEnvNamesToKeyRefs.
// The value is "<secretName>:<key>". Sorted by env var name for determinism.
func parseDriverEnvSecrets(sparkConf map[string]string) ([]envSecret, error) {
	var out []envSecret
	for k, v := range sparkConf {
		if !strings.HasPrefix(k, driverSecretKeyRefConfPrefix) {
			continue
		}
		envName := strings.TrimPrefix(k, driverSecretKeyRefConfPrefix)
		if envName == "" {
			continue
		}
		// EnvSecretsFeatureStep requires the value to be exactly "name:key".
		parts := strings.Split(v, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("driver secretKeyRef %q: value %q must be in the form name:key", envName, v)
		}
		out = append(out, envSecret{envName: envName, secretName: parts[0], key: parts[1]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].envName < out[j].envName })
	return out, nil
}

// mountSecretsFeatureStep is the pure-Go port of Spark's MountSecretsFeatureStep:
// for each mounted secret it adds a volume (source: the pre-existing Secret) and a
// driver-container mount. It creates no API object. It runs before EnvSecrets and
// MountVolumes in Spark's feature order, so secret volumes/mounts precede the
// driver-volume and pod-template ones. No-op without mounted secrets.
type mountSecretsFeatureStep struct {
	conf *driverConf
}

func newMountSecretsFeatureStep(conf *driverConf) *mountSecretsFeatureStep {
	return &mountSecretsFeatureStep{conf: conf}
}

func (s *mountSecretsFeatureStep) configurePod(in sparkPod) sparkPod {
	if len(s.conf.driverSecrets) == 0 {
		return in
	}
	pod := in.pod.DeepCopy()
	container := in.container.DeepCopy()

	for _, sec := range s.conf.driverSecrets {
		volumeName := sec.name + secretVolumeNameSuffix
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: sec.name},
			},
		})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: sec.path,
		})
	}

	return sparkPod{pod: pod, container: container}
}

// envSecretsFeatureStep is the pure-Go port of Spark's EnvSecretsFeatureStep: for
// each secretKeyRef it appends a driver-container env var sourced from a
// pre-existing Secret's key. It creates no API object and touches only the
// container's env, which it appends after the basic env vars (matching Spark's
// feature order: EnvSecrets runs after BasicDriver). No-op without secretKeyRefs.
type envSecretsFeatureStep struct {
	conf *driverConf
}

func newEnvSecretsFeatureStep(conf *driverConf) *envSecretsFeatureStep {
	return &envSecretsFeatureStep{conf: conf}
}

func (s *envSecretsFeatureStep) configurePod(in sparkPod) sparkPod {
	if len(s.conf.driverEnvSecrets) == 0 {
		return in
	}
	container := in.container.DeepCopy()

	for _, es := range s.conf.driverEnvSecrets {
		container.Env = append(container.Env, corev1.EnvVar{
			Name: es.envName,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: es.secretName},
					Key:                  es.key,
				},
			},
		})
	}

	return sparkPod{pod: in.pod, container: container}
}
