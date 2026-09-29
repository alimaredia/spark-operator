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
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// TestResolveDriverPodName pins Spark's driver-pod naming precedence: the CRD's
// driver.podName wins, then spark.kubernetes.driver.pod.name, else the default
// "<resourceNamePrefix>-driver". Only the pod name is affected — the service and
// ConfigMap keep the random prefix — so operator submissions (which set the conf
// to a deterministic name) still let the oracle recover the prefix from the
// service name.
func TestResolveDriverPodName(t *testing.T) {
	const prefix = "sparkpi-596e31a0eeb865ea"

	tests := []struct {
		name     string
		driver   v1beta2.DriverSpec
		sparkCon map[string]string
		want     string
	}{
		{
			name: "default is <prefix>-driver",
			want: prefix + driverPodNameSuffix,
		},
		{
			name:     "spark conf override wins over default",
			sparkCon: map[string]string{confDriverPodName: "sparkpi-driver"},
			want:     "sparkpi-driver",
		},
		{
			name:     "CRD podName wins over conf and default",
			driver:   v1beta2.DriverSpec{PodName: ptr.To("explicit-driver")},
			sparkCon: map[string]string{confDriverPodName: "conf-driver"},
			want:     "explicit-driver",
		},
		{
			name:     "empty CRD podName falls through to conf",
			driver:   v1beta2.DriverSpec{PodName: ptr.To("")},
			sparkCon: map[string]string{confDriverPodName: "conf-driver"},
			want:     "conf-driver",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveDriverPodName(tt.driver, tt.sparkCon, prefix)
			assert.Equal(t, tt.want, got)
		})
	}
}
