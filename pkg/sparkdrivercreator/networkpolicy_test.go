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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// app420 is minimalApp pinned to Spark 4.2.0, the version that introduces the
// driver NetworkPolicy and the default spark-connect service port.
func app420() *v1beta2.SparkApplication {
	app := minimalApp()
	app.Spec.SparkVersion = "4.2.0"
	app.Spec.Image = ptr.To("spark:4.2.0")
	app.Spec.MainApplicationFile = ptr.To("local:///opt/spark/examples/jars/spark-examples_2.13-4.2.0.jar")
	return app
}

func TestBuildDriverNetworkPolicy_NilBeforeGate(t *testing.T) {
	// 4.0.4 does not create a NetworkPolicy, so nothing is emitted and 4.0.4
	// output stays unchanged.
	conf, err := newDriverConf(minimalApp(), BuildOptions{})
	require.NoError(t, err)
	assert.Nil(t, buildDriverNetworkPolicy(conf), "no NetworkPolicy before the gate version")
}

func TestBuildDriverNetworkPolicy_At420(t *testing.T) {
	conf, err := newDriverConf(app420(), BuildOptions{})
	require.NoError(t, err)

	np := buildDriverNetworkPolicy(conf)
	require.NotNil(t, np, "4.2.0 must create the driver NetworkPolicy")

	// TypeMeta / identity: "<prefix>-policy", namespace stamped, only the
	// app-selector label.
	assert.Equal(t, "networking.k8s.io/v1", np.APIVersion)
	assert.Equal(t, "NetworkPolicy", np.Kind)
	assert.Equal(t, conf.resourceNamePrefix+"-policy", np.Name)
	assert.Equal(t, conf.namespace, np.Namespace)
	assert.Equal(t, map[string]string{labelSparkAppSelector: conf.appID}, np.Labels)

	// Owner reference points at the driver pod, controller=true, NO uid (filled in
	// post-create by the submitter adapter), matching the Service/ConfigMap.
	require.Len(t, np.OwnerReferences, 1)
	ref := np.OwnerReferences[0]
	assert.Equal(t, "v1", ref.APIVersion)
	assert.Equal(t, "Pod", ref.Kind)
	assert.Equal(t, conf.resourceNamePrefix+"-driver", ref.Name)
	assert.Equal(t, ptr.To(true), ref.Controller)
	assert.Empty(t, ref.UID, "pod uid is unknown at build time")

	// Targets this app's executor pods.
	assert.Equal(t, map[string]string{
		labelSparkAppSelector: conf.appID,
		labelSparkRole:        sparkRoleExecutor,
	}, np.Spec.PodSelector.MatchLabels)

	// Ingress allows only same-application pods; PolicyTypes left unset (defaults
	// to Ingress).
	assert.Nil(t, np.Spec.PolicyTypes)
	require.Len(t, np.Spec.Ingress, 1)
	require.Len(t, np.Spec.Ingress[0].From, 1)
	from := np.Spec.Ingress[0].From[0]
	require.NotNil(t, from.PodSelector)
	assert.Equal(t, map[string]string{labelSparkAppSelector: conf.appID}, from.PodSelector.MatchLabels)
}

func TestBuild_NetworkPolicyVersionGated(t *testing.T) {
	// 4.0.4: no NetworkPolicy on the assembled resources.
	res404, err := New().Build(minimalApp(), BuildOptions{})
	require.NoError(t, err)
	assert.Nil(t, res404.NetworkPolicy, "4.0.4 Build must not emit a NetworkPolicy")

	// 4.2.0: NetworkPolicy present.
	res420, err := New().Build(app420(), BuildOptions{})
	require.NoError(t, err)
	assert.NotNil(t, res420.NetworkPolicy, "4.2.0 Build must emit a NetworkPolicy")
}

func TestConnectPort_VersionGated(t *testing.T) {
	// 4.0.4: the spark-connect port appears on neither the container nor the service.
	conf404, err := newDriverConf(minimalApp(), BuildOptions{})
	require.NoError(t, err)
	assert.NotContains(t, portNames(conf404.ports()), portNameConnectServer,
		"4.0.4 must not expose the spark-connect port")

	svc404 := buildDriverService(conf404)
	assert.NotContains(t, servicePortNames(svc404.Spec.Ports), portNameConnectServer)

	// 4.2.0: the spark-connect port is published at 15002 on both, with the grpc
	// appProtocol on the *service* port only.
	conf420, err := newDriverConf(app420(), BuildOptions{})
	require.NoError(t, err)
	assert.Contains(t, portNames(conf420.ports()), portNameConnectServer,
		"4.2.0 must expose the spark-connect port")

	svc420 := buildDriverService(conf420)
	var connect *corev1.ServicePort
	for i := range svc420.Spec.Ports {
		if svc420.Spec.Ports[i].Name == portNameConnectServer {
			connect = &svc420.Spec.Ports[i]
		}
	}
	require.NotNil(t, connect, "4.2.0 service must carry the spark-connect port")
	assert.Equal(t, int32(15002), connect.Port)
	assert.Equal(t, intstr.FromInt32(15002), connect.TargetPort)
	require.NotNil(t, connect.AppProtocol)
	assert.Equal(t, "grpc", *connect.AppProtocol)
}

func TestConnectPort_UserOverrideRespected(t *testing.T) {
	// An explicit connect port is honored on 4.2.0 (mirrors Spark reading the conf).
	app := app420()
	app.Spec.SparkConf[confConnectPort] = "16000"
	conf, err := newDriverConf(app, BuildOptions{})
	require.NoError(t, err)

	svc := buildDriverService(conf)
	var connect *corev1.ServicePort
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Name == portNameConnectServer {
			connect = &svc.Spec.Ports[i]
		}
	}
	require.NotNil(t, connect)
	assert.Equal(t, int32(16000), connect.Port)
}

func portNames(ports []portSpec) []string {
	names := make([]string, 0, len(ports))
	for _, p := range ports {
		names = append(names, p.name)
	}
	return names
}

func servicePortNames(ports []corev1.ServicePort) []string {
	names := make([]string, 0, len(ports))
	for _, p := range ports {
		names = append(names, p.Name)
	}
	return names
}
