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

package sparkapplication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

const testDriverPodUID = types.UID("test-driver-pod-uid-0001")

func nativeTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1beta2.AddToScheme(scheme))
	return scheme
}

// nativeTestApp returns a minimal reproducible SparkApplication the creator accepts.
func nativeTestApp() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sparkpi",
			Namespace: "default",
			UID:       types.UID("test-app-uid-0001"),
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                v1beta2.SparkApplicationTypeScala,
			Mode:                v1beta2.DeployModeCluster,
			Image:               ptr.To("spark:4.0.0"),
			MainClass:           ptr.To("org.apache.spark.examples.SparkPi"),
			MainApplicationFile: ptr.To("local:///opt/spark/examples/jars/spark-examples.jar"),
			// The operator always sets a deterministic driver pod name so the
			// controller can track the pod; mirror that here.
			SparkConf: map[string]string{"spark.kubernetes.driver.pod.name": "sparkpi-driver"},
		},
		Status: v1beta2.SparkApplicationStatus{SubmissionID: "sub-123"},
	}
}

// uidAssigningClient wraps a fake client so the driver pod gets a deterministic UID
// on Create, mirroring the API server assigning one — which is what the owned
// resources' owner reference must be backfilled with.
func uidAssigningClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(nativeTestScheme(t)).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if pod, ok := obj.(*corev1.Pod); ok && pod.UID == "" {
					pod.UID = testDriverPodUID
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
}

func TestNativeSparkSubmitter_Submit_CreatesResources(t *testing.T) {
	c := uidAssigningClient(t)
	app := nativeTestApp()

	err := NewNativeSparkSubmitter(c).Submit(context.Background(), app)
	require.NoError(t, err)

	ctx := context.Background()

	// The driver pod exists, is namespaced, and is owned by the SparkApplication.
	pod := &corev1.Pod{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "sparkpi-driver"}, pod))
	assert.Equal(t, testDriverPodUID, pod.UID)
	if assert.Len(t, pod.OwnerReferences, 1) {
		assert.Equal(t, "SparkApplication", pod.OwnerReferences[0].Kind)
		assert.Equal(t, app.Name, pod.OwnerReferences[0].Name)
		assert.Equal(t, app.UID, pod.OwnerReferences[0].UID)
	}

	// Every owned resource exists and carries the driver pod as owner, with the
	// server-assigned pod UID backfilled in.
	var owned []client.Object
	cmList := &corev1.ConfigMapList{}
	require.NoError(t, c.List(ctx, cmList, client.InNamespace("default")))
	require.NotEmpty(t, cmList.Items, "expected the driver ConfigMap")
	for i := range cmList.Items {
		owned = append(owned, &cmList.Items[i])
	}
	svcList := &corev1.ServiceList{}
	require.NoError(t, c.List(ctx, svcList, client.InNamespace("default")))
	require.NotEmpty(t, svcList.Items, "expected the driver Service")
	for i := range svcList.Items {
		owned = append(owned, &svcList.Items[i])
	}

	for _, obj := range owned {
		refs := obj.GetOwnerReferences()
		if assert.Lenf(t, refs, 1, "%T should have one owner reference", obj) {
			assert.Equalf(t, "Pod", refs[0].Kind, "%T owner should be the driver pod", obj)
			assert.Equalf(t, "sparkpi-driver", refs[0].Name, "%T owner name", obj)
			assert.Equalf(t, testDriverPodUID, refs[0].UID, "%T owner UID must be backfilled", obj)
		}
	}
}

func TestNativeSparkSubmitter_Submit_DuplicateIsIdempotent(t *testing.T) {
	// Seed an already-existing driver pod for the same submission.
	existing := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "sparkpi-driver"},
	}
	c := uidAssigningClient(t, existing)

	err := NewNativeSparkSubmitter(c).Submit(context.Background(), nativeTestApp())
	assert.NoError(t, err, "a pre-existing driver pod should be treated as a duplicate submission")
}
