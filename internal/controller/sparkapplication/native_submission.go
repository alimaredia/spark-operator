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
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
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
	// Fold in the submission-time configuration the operator normally injects via
	// buildSparkSubmitArgs but that Build does not reproduce: the deterministic
	// driver pod name, the driver tracking labels, and the full executor conf
	// surface (Build never reads app.Spec.Executor). Operates on a copy so the
	// caller's object is not mutated.
	preparedApp, err := withOperatorSubmissionConf(app)
	if err != nil {
		return fmt.Errorf("failed to prepare native submission for %s/%s: %w", app.Namespace, app.Name, err)
	}
	app = preparedApp

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

	// Guard against a wrong/lying declared spec.sparkVersion: prepend an init
	// container that checks the image's real Spark version before the driver runs.
	// Build already used the declared version to construct this pod, so this is the
	// backstop that refuses to run it on a mismatching image (fails the pod, which
	// is owned by the app, so all created resources are garbage-collected).
	if err := withVersionGate(pod, app); err != nil {
		return fmt.Errorf("failed to add spark version gate for %s/%s: %w", app.Namespace, app.Name, err)
	}

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

// executorConfOptionFuncs are the operator's submission-time executor argument
// builders. Build (pkg/sparkdrivercreator) never reads app.Spec.Executor, so the
// native path folds these confs in itself to reach parity with the classic
// spark-submit path. Reusing the operator's own builders (same package) keeps the
// native path from drifting from classic submission.
var executorConfOptionFuncs = []func(*v1beta2.SparkApplication) ([]string, error){
	executorConfOption,         // labels, instances, image, cores, memory, service account, annotations, secretKeyRefs, java opts, ...
	executorEnvOption,          // spark.executorEnv.*
	executorSecretOption,       // spark.kubernetes.executor.secrets.* (+ credential env)
	executorVolumeMountsOption, // spark.kubernetes.executor.volumes.* (local-dir volumes only)
}

// withOperatorSubmissionConf returns a copy of app with the submission-time
// configuration the operator injects via buildSparkSubmitArgs folded in, so the
// pure-Go Build reproduces what the classic spark-submit path produces. It covers
// exactly the parts Build would otherwise miss (driver typed fields are already
// translated inside Build; the mutating webhook still applies CRD pod-customization):
//
//   - Deterministic driver pod name. The classic path sets
//     spark.kubernetes.driver.pod.name (driverPodNameOption) and the reconciler
//     tracks the driver pod by exactly that name (getDriverPod -> GetDriverPodName).
//     Without it, Build defaults to a random "<prefix>-driver" name that the
//     reconciler can never find. Injected as a conf so it becomes a Build input
//     (owned resources' owner references point at conf.driverPodName, so the name
//     must be fixed before Build, not renamed after).
//
//   - Driver tracking labels. The classic path stamps these on the driver pod
//     (driverConfOption): app-name, launched-by-spark-operator, submission-id, and
//     conditionally mutated-by-spark-operator. They keep the native driver pod
//     label-consistent with the classic path — including matching the mutating
//     webhook's objectSelector (launched-by-spark-operator=true), so the pod is
//     admitted to the same webhook. Set on Spec.Driver.Labels because that is the
//     typed map Build stamps onto the pod (and echoes into spark.properties as
//     spark.kubernetes.driver.label.*), matching the classic conf output.
//
//   - The full executor conf surface. Build never reads app.Spec.Executor, so none
//     of the typed executor fields (image, cores, memory, instances, service
//     account, labels, annotations, secrets, env, local-dir volumes, ...) would
//     otherwise reach the executors — they would launch with Spark defaults. These
//     confs are injected into SparkConf (not the typed Executor spec, which Build
//     ignores) so they flow through Build's conf passthrough into spark.properties;
//     the driver's scheduler then applies them to every executor pod it creates.
//     This also carries the executor tracking labels the reconciler lists executors
//     by (getExecutorPods -> GetResourceLabels: app-name + submission-id).
func withOperatorSubmissionConf(app *v1beta2.SparkApplication) (*v1beta2.SparkApplication, error) {
	out := app.DeepCopy()

	if out.Spec.SparkConf == nil {
		out.Spec.SparkConf = make(map[string]string)
	}
	out.Spec.SparkConf[common.SparkKubernetesDriverPodName] = util.GetDriverPodName(app)

	if out.Spec.Driver.Labels == nil {
		out.Spec.Driver.Labels = make(map[string]string)
	}
	out.Spec.Driver.Labels[common.LabelSparkAppName] = app.Name
	out.Spec.Driver.Labels[common.LabelLaunchedBySparkOperator] = "true"
	out.Spec.Driver.Labels[common.LabelSubmissionID] = app.Status.SubmissionID
	// Mirror driverConfOption: pods without a driver pod template need the webhook
	// to apply CRD-driven fields, so they are flagged for mutation.
	if util.CompareSemanticVersion(app.Spec.SparkVersion, "3.0.0") < 0 || app.Spec.Driver.Template == nil {
		out.Spec.Driver.Labels[common.LabelMutatedBySparkOperator] = "true"
	}

	// Fold the operator's executor confs into SparkConf. These are keyed off the
	// original app's typed executor spec and, like spark-submit's later --conf args,
	// take precedence over any duplicate the user set directly in SparkConf.
	for _, optionFunc := range executorConfOptionFuncs {
		args, err := optionFunc(app)
		if err != nil {
			return nil, err
		}
		if err := foldConfArgs(out.Spec.SparkConf, args); err != nil {
			return nil, err
		}
	}

	// Fold the driver's local-dir volume conf. Build reads driver volumes only from
	// SparkConf (spark.kubernetes.driver.volumes.*), and the mutating webhook
	// deliberately skips spark-local-dir-* volumes (Spark's LocalDirsFeatureStep owns
	// them). Without this, the driver's local-dir volumes — and their emptyDir
	// medium/sizeLimit — never reach the pod, and LocalDirsFeatureStep falls back to a
	// bare default emptyDir. (Non-local-dir driver volumes still come from the webhook,
	// and executor volumes are handled by executorVolumeMountsOption above.)
	driverVolumeArgs, err := driverVolumeMountsOption(app)
	if err != nil {
		return nil, err
	}
	if err := foldConfArgs(out.Spec.SparkConf, driverVolumeArgs); err != nil {
		return nil, err
	}

	// Python apps: fold the operator's typed Python fields that Build cannot read.
	// pythonVersionOption mirrors the classic path's --conf translation; PyFiles is
	// a spark-submit flag (--py-files) that spark-submit itself turns into the
	// spark.submit.pyFiles conf, so translate it here for the native path.
	if app.Spec.Type == v1beta2.SparkApplicationTypePython {
		args, err := pythonVersionOption(app)
		if err != nil {
			return nil, err
		}
		if err := foldConfArgs(out.Spec.SparkConf, args); err != nil {
			return nil, err
		}
		if len(app.Spec.Deps.PyFiles) > 0 {
			out.Spec.SparkConf[confSparkSubmitPyFiles] = strings.Join(app.Spec.Deps.PyFiles, ",")
		}
	}

	return out, nil
}

// confSparkSubmitPyFiles is the Spark conf spark-submit derives from --py-files;
// the native path sets it directly from the CRD's deps.pyFiles.
const confSparkSubmitPyFiles = "spark.submit.pyFiles"

// foldConfArgs parses a spark-submit-style ["--conf", "key=value", ...] slice
// (as produced by the operator's *Option builders) into dst, overwriting on
// collision to mirror spark-submit's last-conf-wins merge.
func foldConfArgs(dst map[string]string, args []string) error {
	for i := 0; i < len(args); i++ {
		if args[i] != "--conf" {
			continue
		}
		i++
		if i >= len(args) {
			return fmt.Errorf("malformed conf args: %q not followed by a value", "--conf")
		}
		key, value, ok := strings.Cut(args[i], "=")
		if !ok {
			return fmt.Errorf("malformed conf %q: expected key=value", args[i])
		}
		dst[key] = value
	}
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
