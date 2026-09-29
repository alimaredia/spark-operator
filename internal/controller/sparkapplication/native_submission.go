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

// Native Submitter
//
// Submits a SparkApplication by building the driver resources in pure Go
// (pkg/sparkdrivercreator) and applying them directly with the Kubernetes API.
// There is no spark-submit, no JVM, and no external submitter service: this is the
// submission strategy that lets the operator image ship without Spark/PySpark.
//
// # Contract
//
//  1. Submission-only, stateless — like the other submitters, it creates the driver
//     pod and the resources spark-submit would otherwise own, then returns. All
//     lifecycle/status ownership stays with the operator via owner references.
//  2. It reproduces only the deterministic subset of spark-submit's client-side
//     behavior. Non-reproducible inputs (Kerberos, file-based credentials, local
//     HADOOP_CONF_DIR, custom feature steps, ...) are rejected up front by the
//     admission webhook (the deny-list), so Build only ever sees reproducible input.
//  3. Ordering mirrors Spark's Client.run: the driver pod is created first so its
//     server-assigned UID can own the ConfigMap / Service / PVCs / executor
//     pod-template ConfigMap, which are applied afterwards.
//  4. Idempotent on retry: because the operator uses a deterministic driver pod
//     name, a pre-existing driver pod for the same submission is treated as success.

package sparkapplication

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/sparkdrivercreator"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var nativeLogger = ctrl.Log.WithName("native-submitter")

// NativeSparkSubmitter submits a SparkApplication by building its driver resources
// in pure Go and applying them with the Kubernetes API.
type NativeSparkSubmitter struct {
	client  client.Client
	creator *sparkdrivercreator.SparkDriverCreator
}

// NativeSparkSubmitter implements SparkApplicationSubmitter.
// This interface is highly experimental and may change or be removed in the future.
var _ SparkApplicationSubmitter = &NativeSparkSubmitter{}

// NewNativeSparkSubmitter returns a NativeSparkSubmitter that applies driver
// resources with the given client.
func NewNativeSparkSubmitter(c client.Client) *NativeSparkSubmitter {
	return &NativeSparkSubmitter{client: c, creator: sparkdrivercreator.New()}
}

// Submit implements SparkApplicationSubmitter interface.
func (s *NativeSparkSubmitter) Submit(ctx context.Context, app *v1beta2.SparkApplication) error {
	res, err := s.creator.Build(app, sparkdrivercreator.BuildOptions{
		ExecutorPodTemplate: buildExecutorPodTemplate(app),
	})
	if err != nil {
		return fmt.Errorf("failed to build driver resources for %s/%s: %w", app.Namespace, app.Name, err)
	}

	// The driver pod is owned by the SparkApplication, so deleting the app garbage
	// collects the pod, which in turn garbage collects everything the pod owns. The
	// classic path gets this owner reference via the operator-synthesized driver pod
	// template; the native path stamps it on here. Build does not set the namespace
	// on the pod/service (spark-submit takes it from the request path), so set it.
	pod := res.Pod
	pod.Namespace = app.Namespace
	pod.OwnerReferences = append(pod.OwnerReferences, util.GetOwnerReference(app))

	nativeLogger.Info("Submitting spark application natively",
		"name", app.Name,
		"namespace", app.Namespace,
		"driverPod", pod.Name,
		"submissionId", app.Status.SubmissionID,
	)

	// Create the driver pod first so its server-assigned UID can own the rest,
	// mirroring Spark's Client.run -> addOwnerReference. A pre-existing pod for the
	// same submission is an idempotent success (deterministic pod name + retries).
	if err := s.client.Create(ctx, pod); err != nil {
		if apierrors.IsAlreadyExists(err) {
			nativeLogger.Info("Driver pod already exists, treating submission as duplicate",
				"name", app.Name, "namespace", app.Namespace, "driverPod", pod.Name)
			return nil
		}
		return fmt.Errorf("failed to create driver pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	// Backfill the driver pod's now-known UID into the owner reference Build already
	// placed on every owned resource, then apply them. Build leaves the owner UID
	// unset for exactly this step (see driverPodOwnerReference in the creator).
	for _, obj := range ownedResources(res) {
		obj.SetNamespace(app.Namespace)
		setOwnerReferenceUID(obj, pod.UID)
		if err := s.client.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create %T %s/%s for %s/%s: %w",
				obj, obj.GetNamespace(), obj.GetName(), app.Namespace, app.Name, err)
		}
	}

	nativeLogger.Info("Submitted successfully",
		"name", app.Name,
		"namespace", app.Namespace,
		"driverPod", pod.Name,
		"driverPodUID", pod.UID,
	)
	return nil
}

// ownedResources returns the resources the driver pod owns, in the order they
// should be applied (all after the pod itself). Optional resources are included
// only when Build produced them.
func ownedResources(res *sparkdrivercreator.DriverResources) []client.Object {
	objs := []client.Object{res.ConfigMap, res.Service}
	if res.PodSpecConfigMap != nil {
		objs = append(objs, res.PodSpecConfigMap)
	}
	for _, pvc := range res.PersistentVolumeClaims {
		objs = append(objs, pvc)
	}
	return objs
}

// setOwnerReferenceUID fills the given UID into every owner reference on obj,
// preserving the exact reference shape Build produced (only the UID is server
// assigned and thus unknown until the driver pod is created).
func setOwnerReferenceUID(obj client.Object, uid types.UID) {
	refs := obj.GetOwnerReferences()
	for i := range refs {
		refs[i].UID = uid
	}
	obj.SetOwnerReferences(refs)
}
