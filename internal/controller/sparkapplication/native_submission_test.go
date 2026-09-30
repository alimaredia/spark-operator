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
	networkingv1 "k8s.io/api/networking/v1"
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
	require.NoError(t, networkingv1.AddToScheme(scheme))
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
			SparkVersion:        "4.0.0",
			// No driver pod-name conf is set on purpose: the submitter derives the
			// deterministic "<app>-driver" name itself (mirroring the operator).
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

func TestNativeSparkSubmitter_Submit_NetworkPolicyVersionGated(t *testing.T) {
	ctx := context.Background()

	// A Spark version that does not create a NetworkPolicy: none is applied.
	t.Run("absent before the gate version", func(t *testing.T) {
		c := uidAssigningClient(t)
		require.NoError(t, NewNativeSparkSubmitter(c).Submit(ctx, nativeTestApp()))

		npList := &networkingv1.NetworkPolicyList{}
		require.NoError(t, c.List(ctx, npList, client.InNamespace("default")))
		assert.Empty(t, npList.Items, "no NetworkPolicy should be created before the gate version")
	})

	// Spark 4.2.0: the NetworkPolicy is applied, namespaced, and owned by the driver
	// pod with the server-assigned UID backfilled in.
	t.Run("created and owned at 4.2.0", func(t *testing.T) {
		c := uidAssigningClient(t)
		app := nativeTestApp()
		app.Spec.SparkVersion = "4.2.0"
		app.Spec.Image = ptr.To("spark:4.2.0")
		require.NoError(t, NewNativeSparkSubmitter(c).Submit(ctx, app))

		npList := &networkingv1.NetworkPolicyList{}
		require.NoError(t, c.List(ctx, npList, client.InNamespace("default")))
		require.Len(t, npList.Items, 1, "expected the driver NetworkPolicy at 4.2.0")

		np := npList.Items[0]
		assert.Equal(t, "default", np.Namespace)
		require.Len(t, np.OwnerReferences, 1)
		assert.Equal(t, "Pod", np.OwnerReferences[0].Kind)
		assert.Equal(t, "sparkpi-driver", np.OwnerReferences[0].Name)
		assert.Equal(t, testDriverPodUID, np.OwnerReferences[0].UID, "owner UID must be backfilled")
	})
}

func TestNativeSparkSubmitter_Submit_InjectsOperatorTrackingIdentity(t *testing.T) {
	c := uidAssigningClient(t)
	app := nativeTestApp()

	require.NoError(t, NewNativeSparkSubmitter(c).Submit(context.Background(), app))

	// The submitter must not mutate the caller's app when folding in identity.
	assert.NotContains(t, app.Spec.SparkConf, "spark.kubernetes.driver.pod.name",
		"Submit must operate on a copy, not mutate the caller's SparkConf")
	assert.Empty(t, app.Spec.Driver.Labels, "Submit must not mutate the caller's Driver.Labels")

	ctx := context.Background()

	// The driver pod is created under the deterministic name the reconciler tracks
	// by (GetDriverPodName -> "<app>-driver") and carries the operator tracking
	// labels, matching the classic path (and the mutating webhook's objectSelector).
	pod := &corev1.Pod{}
	require.NoError(t, c.Get(ctx,
		types.NamespacedName{Namespace: "default", Name: "sparkpi-driver"}, pod))
	assert.Equal(t, "sparkpi", pod.Labels["sparkoperator.k8s.io/app-name"])
	assert.Equal(t, "true", pod.Labels["sparkoperator.k8s.io/launched-by-spark-operator"])
	assert.Equal(t, "sub-123", pod.Labels["sparkoperator.k8s.io/submission-id"])
	// No driver template on the test app => flagged for webhook mutation.
	assert.Equal(t, "true", pod.Labels["sparkoperator.k8s.io/mutated-by-spark-operator"])

	// The executor tracking labels must reach spark.properties so the driver stamps
	// them on executor pods and getExecutorPods (by app-name + submission-id) can
	// list them.
	cmList := &corev1.ConfigMapList{}
	require.NoError(t, c.List(ctx, cmList, client.InNamespace("default")))
	require.NotEmpty(t, cmList.Items, "expected the driver ConfigMap")
	props := cmList.Items[0].Data["spark.properties"]
	assert.Contains(t, props, "spark.kubernetes.executor.label.sparkoperator.k8s.io/app-name=sparkpi")
	assert.Contains(t, props, "spark.kubernetes.executor.label.sparkoperator.k8s.io/launched-by-spark-operator=true")
	assert.Contains(t, props, "spark.kubernetes.executor.label.sparkoperator.k8s.io/submission-id=sub-123")
	assert.Contains(t, props, "spark.kubernetes.executor.label.sparkoperator.k8s.io/mutated-by-spark-operator=true")
}

func TestNativeSparkSubmitter_Submit_FoldsExecutorConf(t *testing.T) {
	c := uidAssigningClient(t)
	app := nativeTestApp()
	// Typed executor fields that Build (pkg/sparkdrivercreator) never reads — the
	// native submitter must fold them into spark.properties so executors get them.
	app.Spec.Executor = v1beta2.ExecutorSpec{
		Instances: ptr.To(int32(3)),
		SparkPodSpec: v1beta2.SparkPodSpec{
			Memory:         ptr.To("2g"),
			ServiceAccount: ptr.To("exec-sa"),
			Labels:         map[string]string{"team": "data"},
			Annotations:    map[string]string{"note": "hello"},
		},
	}

	require.NoError(t, NewNativeSparkSubmitter(c).Submit(context.Background(), app))

	cmList := &corev1.ConfigMapList{}
	require.NoError(t, c.List(context.Background(), cmList, client.InNamespace("default")))
	require.NotEmpty(t, cmList.Items)
	props := cmList.Items[0].Data["spark.properties"]

	assert.Contains(t, props, "spark.executor.instances=3")
	assert.Contains(t, props, "spark.executor.memory=2g")
	assert.Contains(t, props, "spark.kubernetes.authenticate.executor.serviceAccountName=exec-sa")
	assert.Contains(t, props, "spark.kubernetes.executor.label.team=data")
	assert.Contains(t, props, "spark.kubernetes.executor.annotation.note=hello")
	// The executor image falls back to the top-level image when unset on the
	// executor. The value's colon is Java-properties-escaped in spark.properties.
	assert.Contains(t, props, `spark.kubernetes.executor.container.image=spark\:4.0.0`)
}

func TestNativeSparkSubmitter_Submit_PythonApp(t *testing.T) {
	c := uidAssigningClient(t)
	app := nativeTestApp()
	app.Spec.Type = v1beta2.SparkApplicationTypePython
	app.Spec.MainClass = nil
	app.Spec.MainApplicationFile = ptr.To("local:///opt/spark/examples/src/main/python/pi.py")
	app.Spec.PythonVersion = ptr.To("3")
	app.Spec.Deps = v1beta2.Dependencies{PyFiles: []string{"local:///opt/lib/a.py", "local:///opt/lib/b.py"}}

	require.NoError(t, NewNativeSparkSubmitter(c).Submit(context.Background(), app))

	ctx := context.Background()

	// The driver launches via PythonRunner against the .py primary resource.
	pod := &corev1.Pod{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "sparkpi-driver"}, pod))
	require.Len(t, pod.Spec.Containers, 1)
	assert.Contains(t, pod.Spec.Containers[0].Args, "org.apache.spark.deploy.PythonRunner")
	assert.Contains(t, pod.Spec.Containers[0].Args, "local:///opt/spark/examples/src/main/python/pi.py")

	cmList := &corev1.ConfigMapList{}
	require.NoError(t, c.List(ctx, cmList, client.InNamespace("default")))
	require.NotEmpty(t, cmList.Items)
	props := cmList.Items[0].Data["spark.properties"]

	assert.Contains(t, props, "spark.kubernetes.resource.type=python")
	assert.Contains(t, props, "spark.kubernetes.memoryOverheadFactor=0.4")
	// Operator-injected typed Python fields folded by the adapter.
	assert.Contains(t, props, "spark.kubernetes.pyspark.pythonVersion=3")
	assert.Contains(t, props, `spark.submit.pyFiles=local\:///opt/lib/a.py,local\:///opt/lib/b.py`)
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
