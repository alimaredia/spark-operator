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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestBuildDriverConfigMap_Minimal(t *testing.T) {
	conf, err := newDriverConf(minimalApp())
	require.NoError(t, err)

	cm := buildDriverConfigMap(conf)

	// Identity + shape.
	assert.Equal(t, "v1", cm.APIVersion)
	assert.Equal(t, "ConfigMap", cm.Kind)
	assert.Equal(t, conf.configMapName, cm.Name)
	assert.Equal(t, conf.namespace, cm.Namespace)
	assert.Empty(t, cm.Labels, "ConfigMap carries no labels")
	require.NotNil(t, cm.Immutable)
	assert.True(t, *cm.Immutable)

	// Owner reference points at the driver pod, controller=true, NO uid.
	require.Len(t, cm.OwnerReferences, 1)
	ref := cm.OwnerReferences[0]
	assert.Equal(t, "Pod", ref.Kind)
	assert.Equal(t, conf.resourceNamePrefix+"-driver", ref.Name)
	assert.Equal(t, ptr.To(true), ref.Controller)
	assert.Empty(t, ref.UID)

	// Data: namespace key + the serialized properties.
	assert.Equal(t, conf.namespace, cm.Data[confNamespace])
	props := cm.Data[sparkPropertiesKey]
	assert.Contains(t, props, "\nspark.app.name=sparkpi\n")
	assert.Contains(t, props, "\nspark.submit.deployMode=cluster\n")
	assert.Contains(t, props, "\nspark.kubernetes.submitInDriver=true\n")
	// Colons in values are Java-escaped.
	assert.Contains(t, props, "spark.kubernetes.container.image=spark\\:")
}

func TestBuildDriverConfigMap_OverridesEmitStructuredKeys(t *testing.T) {
	app := minimalApp()
	app.Spec.Driver = v1beta2.DriverSpec{
		CoreRequest: ptr.To("1"),
		SparkPodSpec: v1beta2.SparkPodSpec{
			Memory:         ptr.To("1g"),
			ServiceAccount: ptr.To("spark"),
			Labels:         map[string]string{"env": "prod"},
			Env:            []corev1.EnvVar{{Name: "MY_ENV", Value: "my-value"}},
			NodeSelector:   map[string]string{"disktype": "ssd"},
		},
	}
	conf, err := newDriverConf(app)
	require.NoError(t, err)

	props := buildDriverConfigMap(conf).Data[sparkPropertiesKey]

	assert.Contains(t, props, "\nspark.driver.memory=1g\n")
	assert.Contains(t, props, "\nspark.kubernetes.driver.request.cores=1\n")
	assert.Contains(t, props, "\nspark.kubernetes.authenticate.driver.serviceAccountName=spark\n")
	assert.Contains(t, props, "\nspark.kubernetes.driver.label.env=prod\n")
	assert.Contains(t, props, "\nspark.kubernetes.driverEnv.MY_ENV=my-value\n")
	assert.Contains(t, props, "\nspark.kubernetes.node.selector.disktype=ssd\n")
	// coreRequest (not cores) was supplied, so spark.driver.cores stays absent.
	assert.NotContains(t, props, "\nspark.driver.cores=")
}

func TestBuildSystemProperties_OmitsUnsetConditionals(t *testing.T) {
	conf, err := newDriverConf(minimalApp())
	require.NoError(t, err)

	props := buildSystemProperties(conf)

	// Nothing user-supplied beyond the base app -> no conditional keys.
	for _, k := range []string{
		confDriverMemory, confDriverCores, confDriverRequestCores,
		confDriverLimitCores, confServiceAccountName,
	} {
		_, ok := props[k]
		assert.Falsef(t, ok, "expected %s to be omitted when unset", k)
	}
	// Base keys are always present.
	assert.Equal(t, "cluster", props[confDeployMode])
	assert.Equal(t, "", props[confPyFiles])
	assert.Equal(t, "java", props[confResourceType])
	assert.Equal(t, conf.serviceName+".default.svc", props[confDriverHost])
}

func TestSerializeProperties_HeaderDateAndSorting(t *testing.T) {
	out := serializeProperties("spark-drv-abcd-conf-map", map[string]string{
		"b.key": "2",
		"a.key": "1",
	})
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	require.Len(t, lines, 4)
	assert.Equal(t, "#Java properties built from Kubernetes config map with name: spark-drv-abcd-conf-map", lines[0])
	assert.True(t, strings.HasPrefix(lines[1], "#"), "second line is the date comment")
	assert.Equal(t, "a.key=1", lines[2])
	assert.Equal(t, "b.key=2", lines[3])
}

func TestSaveConvert_Escaping(t *testing.T) {
	assert.Equal(t, "local\\:///a.jar", saveConvert("local:///a.jar", false))
	assert.Equal(t, "a\\=b", saveConvert("a=b", false))
	assert.Equal(t, "a\\#b", saveConvert("a#b", false))
	assert.Equal(t, "a\\\\b", saveConvert("a\\b", false))
	// Values escape only a leading space; keys escape every space.
	assert.Equal(t, "a b", saveConvert("a b", false))
	assert.Equal(t, "a\\ b", saveConvert("a b", true))
	assert.Equal(t, "\\ x", saveConvert(" x", false))
}
