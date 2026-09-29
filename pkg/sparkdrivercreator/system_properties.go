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
	"strconv"
	"time"
)

// Spark conf keys serialized into the driver ConfigMap's spark.properties. These
// are Spark's own conf keys (not the operator's), because the ConfigMap must
// match what stock spark-submit writes; see the test/oracle differential test.
const (
	confAppID                  = "spark.app.id"
	confAppName                = "spark.app.name"
	confAppSubmitTime          = "spark.app.submitTime"
	confDriverHost             = "spark.driver.host"
	confDriverMemory           = "spark.driver.memory"
	confDriverCores            = "spark.driver.cores"
	confJars                   = "spark.jars"
	confContainerImage         = "spark.kubernetes.container.image"
	confDriverRequestCores     = "spark.kubernetes.driver.request.cores"
	confDriverLimitCores       = "spark.kubernetes.driver.limit.cores"
	confDriverPodName          = "spark.kubernetes.driver.pod.name"
	confServiceAccountName     = "spark.kubernetes.authenticate.driver.serviceAccountName"
	confMemoryOverheadFactor   = "spark.kubernetes.memoryOverheadFactor"
	confNamespace              = "spark.kubernetes.namespace"
	confResourceType           = "spark.kubernetes.resource.type"
	confSubmitInDriver         = "spark.kubernetes.submitInDriver"
	confMaster                 = "spark.master"
	confDeployMode             = "spark.submit.deployMode"
	confPyFiles                = "spark.submit.pyFiles"
	confDriverLabelPrefix      = "spark.kubernetes.driver.label."
	confDriverEnvPrefix        = "spark.kubernetes.driverEnv."
	confNodeSelectorPrefix     = "spark.kubernetes.node.selector."
	memoryOverheadFactorString = "0.1"
	deployModeCluster          = "cluster"
	resourceTypeJava           = "java"
	driverHostSuffix           = ".svc"
)

// buildSystemProperties assembles the driver's resolved system properties — the
// key/value set stock spark-submit serializes into the ConfigMap's
// spark.properties. It reproduces spark-submit's own merge for a reproducible
// cluster-mode submit: SparkSubmit-injected keys, the base structured-field
// translation, and the BasicDriverFeatureStep / DriverServiceFeatureStep
// additions. Conditional keys are emitted only when the user supplied the value,
// matching spark-submit (which serializes only explicitly-set confs).
func buildSystemProperties(c *driverConf) map[string]string {
	props := map[string]string{}

	// User conf passes through first; derived keys below win on collision.
	for k, v := range c.sparkConf {
		props[k] = v
	}

	// SparkSubmit-injected and always-present keys.
	props[confAppName] = c.appName
	props[confAppID] = c.appID
	props[confAppSubmitTime] = strconv.FormatInt(time.Now().UnixMilli(), 10)
	props[confMaster] = c.master
	props[confDeployMode] = deployModeCluster
	props[confPyFiles] = ""
	props[confResourceType] = resourceTypeJava
	props[confContainerImage] = c.image
	props[confNamespace] = c.namespace
	props[confMemoryOverheadFactor] = memoryOverheadFactorString
	props[confSubmitInDriver] = "true"
	props[confDriverPodName] = c.driverPodName
	if c.mainAppResource != "" {
		props[confJars] = c.mainAppResource
	}

	// DriverServiceFeatureStep additions: the driver's advertised host and ports.
	props[confDriverHost] = c.serviceName + "." + c.namespace + driverHostSuffix
	props[confDriverPort] = strconv.Itoa(c.intConf(confDriverPort, defaultDriverPort))
	props[confDriverBMPort] = strconv.Itoa(c.blockManagerPort())

	// Conditional structured-field translations (only when the user set them).
	if c.memory != "" {
		props[confDriverMemory] = c.memory
	}
	if c.coresSet {
		props[confDriverCores] = strconv.Itoa(int(c.cores))
	}
	if c.coresRequestSet {
		props[confDriverRequestCores] = c.coresRequest
	}
	if c.limitCores != "" {
		props[confDriverLimitCores] = c.limitCores
	}
	if c.serviceAccount != "" {
		props[confServiceAccountName] = c.serviceAccount
	}
	for k, v := range c.driverLabelsCustom {
		props[confDriverLabelPrefix+k] = v
	}
	for _, e := range c.environment {
		if e.ValueFrom == nil {
			props[confDriverEnvPrefix+e.Name] = e.Value
		}
	}
	for k, v := range c.nodeSelector {
		props[confNodeSelectorPrefix+k] = v
	}

	// Executor pod template (classic operator path). PodTemplateConfigMapStep
	// rewrites spark.kubernetes.executor.podTemplateFile from the submit-time host
	// path to the in-pod mount path; that (and the container name) are runtime
	// relevant and reproducible, so the native path emits them. The submit-time
	// driver.podTemplateFile/driver.podTemplateContainerName confs are host-specific
	// artifacts and are intentionally omitted (the oracle normalization strips them
	// from the golden to match).
	if c.hasExecPodTemplate {
		props[confExecPodTemplateFile] = execPodTemplateMountFile
		props[confExecPodTemplateContainerName] = c.execPodTemplateContainerName
	}

	// Hadoop config ConfigMap (HadoopConfDriverFeatureStep.getAdditionalPodSystemProperties):
	// record the ConfigMap name so executors mount the same Hadoop config. The input
	// spark.kubernetes.hadoop.configMapName conf is carried through by the sparkConf
	// passthrough; this adds the derived executor-facing property.
	if c.hadoopConfigMapName != "" {
		props[confExecHadoopConfigMapName] = c.hadoopConfigMapName
	}

	return props
}
