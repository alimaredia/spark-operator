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
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestBuildDriverService_Minimal(t *testing.T) {
	conf, err := newDriverConf(minimalApp(), BuildOptions{})
	require.NoError(t, err)

	svc := buildDriverService(conf)

	// Identity: <prefix>-driver-svc, only the app-selector label.
	assert.Equal(t, conf.resourceNamePrefix+"-driver-svc", svc.Name)
	assert.Equal(t, map[string]string{labelSparkAppSelector: conf.appID}, svc.Labels)
	assert.Empty(t, svc.Namespace, "service body carries no namespace")

	// Owner reference points at the driver pod, controller=true, NO uid (filled
	// post-create by the submitter).
	require.Len(t, svc.OwnerReferences, 1)
	ref := svc.OwnerReferences[0]
	assert.Equal(t, "v1", ref.APIVersion)
	assert.Equal(t, "Pod", ref.Kind)
	assert.Equal(t, conf.resourceNamePrefix+"-driver", ref.Name)
	assert.Equal(t, ptr.To(true), ref.Controller)
	assert.Empty(t, ref.UID, "pod uid is unknown at build time")

	// Headless, single-stack IPv4.
	assert.Equal(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)
	assert.Equal(t, []corev1.IPFamily{corev1.IPv4Protocol}, svc.Spec.IPFamilies)
	require.NotNil(t, svc.Spec.IPFamilyPolicy)
	assert.Equal(t, corev1.IPFamilyPolicySingleStack, *svc.Spec.IPFamilyPolicy)

	// Selector = the four preset labels; publishNotReadyAddresses left off.
	assert.Equal(t, conf.labels, svc.Spec.Selector)
	assert.False(t, svc.Spec.PublishNotReadyAddresses)

	// Ports: the three always-on ports, targetPort == port, no protocol/appProtocol.
	assert.Equal(t, []corev1.ServicePort{
		{Name: portNameDriverRPC, Port: 7078, TargetPort: intstr.FromInt32(7078)},
		{Name: portNameBlockManager, Port: 7079, TargetPort: intstr.FromInt32(7079)},
		{Name: portNameUI, Port: 4040, TargetPort: intstr.FromInt32(4040)},
	}, svc.Spec.Ports)
}

func TestBuildDriverService_SelectorIncludesCustomLabels(t *testing.T) {
	app := minimalApp()
	app.Spec.Driver = v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{
		Labels: map[string]string{"env": "prod"},
	}}
	conf, err := newDriverConf(app, BuildOptions{})
	require.NoError(t, err)

	svc := buildDriverService(conf)

	// The selector matches the pod labels, custom labels included.
	assert.Equal(t, "prod", svc.Spec.Selector["env"])
	assert.Equal(t, "sparkpi", svc.Spec.Selector[labelSparkAppName])

	// The metadata labels stay minimal (only the app selector) regardless.
	assert.Equal(t, map[string]string{labelSparkAppSelector: conf.appID}, svc.Labels)
}

func TestGenerateDriverServiceName_FallbackWhenTooLong(t *testing.T) {
	short := generateDriverServiceName("sparkpi-0123456789abcdef")
	assert.Equal(t, "sparkpi-0123456789abcdef-driver-svc", short)

	longPrefix := strings.Repeat("a", 60)
	long := generateDriverServiceName(longPrefix)
	assert.LessOrEqual(t, len(long), kubernetesDNSLabelMaxSize)
	assert.True(t, strings.HasPrefix(long, "spark-"), "fallback keeps the spark- prefix")
	assert.True(t, strings.HasSuffix(long, "-driver-svc"))
}
