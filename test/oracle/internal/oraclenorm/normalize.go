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

// Package oraclenorm canonicalizes Kubernetes objects captured from Spark's
// submission client so that the golden oracle files and the pure-Go
// SparkDriverCreator output can be compared byte-for-byte.
//
// It removes two sources of noise:
//   - server/volatile metadata (uid, resourceVersion, creationTimestamp, …)
//   - per-submission identifiers that Spark randomizes or timestamps, replaced
//     with stable placeholders (see the const block below).
//
// The dynamic values were catalogued from real Spark 4.0.4 captures:
//
//	app id            spark-<32hex>                 (spark-app-selector label, spark.app.id)
//	resource prefix   <appName>-<16hex>             (pod/service names, driver.host, pod.name)
//	configmap id      spark-drv-<16hex>-conf-map    (cm name, pod volume ref)
//	local dir         spark-<uuid>                  (SPARK_LOCAL_DIRS, volume mountPath)
//	submitTime        <millis>                      (spark.app.submitTime)
//	properties date   #<Date>                       (spark.properties header line)
//	master            k8s://<host:port>             (spark.master; env-specific)
//
// Both cmd/oraclegen and the oracle test apply the SAME normalization,
// extracting the per-submission Vars (app id, prefix) from their own driver
// service, so placeholders line up on both sides.
package oraclenorm

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Placeholders substituted for per-submission dynamic values.
const (
	PlaceholderAppID      = "__APP_ID__"
	PlaceholderPrefix     = "__PREFIX__"
	PlaceholderCMID       = "__CM_ID__"
	PlaceholderLocalDir   = "__LOCAL_DIR__"
	PlaceholderSubmitTime = "__SUBMIT_TIME__"
	PlaceholderDate       = "__DATE__"
	PlaceholderMaster     = "__MASTER__"
	PlaceholderSparkUser  = "__SPARK_USER__"
	PlaceholderOwnerUID   = "__OWNER_UID__"
)

// Vars are the per-submission identifiers extracted once per case (from the
// driver pod) and applied uniformly to every object in that case. The
// remaining dynamic values are matched structurally by regex below.
type Vars struct {
	// AppID is Spark's k8s app id, e.g. "spark-73e10c...". From the
	// spark-app-selector label.
	AppID string
	// Prefix is the resource-name prefix "<appName>-<16hex>". The service is
	// "<Prefix>-driver-svc" and the driver pod defaults to "<Prefix>-driver"
	// (unless overridden by spark.kubernetes.driver.pod.name).
	Prefix string
}

var (
	// spark-<32 hex> app id.
	appIDRe = regexp.MustCompile(`spark-[0-9a-f]{32}`)
	// spark-drv-<hex>-conf-map driver ConfigMap name.
	cmIDRe = regexp.MustCompile(`spark-drv-[0-9a-f]+-conf-map`)
	// spark-<uuid> local dir (UUID has the 8-4-4-4-12 dash shape).
	localDirRe = regexp.MustCompile(`spark-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	// A Java Properties date-comment line, e.g. "#Tue Sep 29 11:09:38 EDT 2026".
	propDateRe = regexp.MustCompile(`^#[A-Z][a-z]{2} [A-Z][a-z]{2} .*\d{4}$`)
)

// volatileMetadataKeys are server-populated / non-deterministic metadata fields
// that must never appear in a normalized object.
var volatileMetadataKeys = []string{
	"uid", "resourceVersion", "creationTimestamp", "generation",
	"managedFields", "selfLink",
}

// Extract derives the per-submission Vars from a parsed driver Service object.
// The Service is used (rather than the pod) because its name is always
// "<resourceNamePrefix>-driver-svc", carrying the random prefix even when the pod
// name is overridden via spark.kubernetes.driver.pod.name — in which case the pod
// name no longer contains the prefix.
func Extract(service map[string]any) Vars {
	var v Vars
	meta, _ := service["metadata"].(map[string]any)
	if meta != nil {
		if name, _ := meta["name"].(string); strings.HasSuffix(name, "-driver-svc") {
			v.Prefix = strings.TrimSuffix(name, "-driver-svc")
		}
		if labels, _ := meta["labels"].(map[string]any); labels != nil {
			if sel, _ := labels["spark-app-selector"].(string); sel != "" {
				v.AppID = sel
			}
		}
	}
	return v
}

// Normalize parses a captured request body, strips volatile metadata, replaces
// per-submission identifiers with stable placeholders, and returns canonical
// (sorted-key, indented) JSON.
func Normalize(raw []byte, vars Vars) ([]byte, error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("parse object: %w", err)
	}

	stripVolatile(obj)
	// Handle values that live only inside the ConfigMap's spark.properties
	// string, where line context makes them unambiguous (date comment,
	// submitTime, master URL).
	normalizeSparkProperties(obj)
	// SPARK_USER is the submit host's OS user (Utils.getCurrentUserName) — host
	// dependent, so pin it rather than the machine that ran the capture.
	normalizeSparkUserEnv(obj)
	// Owner-reference uids point at the driver pod's server-assigned uid, which is
	// only known after the pod is created (the recorder fabricates "oracle-uid-pod";
	// the pure-Go builder leaves it unset for the submitter to fill). Pin it so
	// both sides agree.
	normalizeOwnerReferenceUIDs(obj)
	// MountVolumesFeatureStep builds its driver-volume mounts/sources with fabric8,
	// which serializes zero-valued readOnly/subPath/subPathExpr explicitly; Go's
	// typed structs omit them (omitempty). Drop those zero values so the two agree.
	normalizeVolumeZeroValues(obj)

	// Re-serialize (canonical, sorted keys) then do string-level substitution so
	// identifiers are replaced everywhere — including inside spark.properties.
	canon, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal object: %w", err)
	}
	s := string(canon)

	// Explicit vars first (longest / most specific), then structural fallbacks.
	if vars.Prefix != "" {
		s = strings.ReplaceAll(s, vars.Prefix, PlaceholderPrefix)
	}
	if vars.AppID != "" {
		s = strings.ReplaceAll(s, vars.AppID, PlaceholderAppID)
	}
	s = cmIDRe.ReplaceAllString(s, "spark-drv-"+PlaceholderCMID+"-conf-map")
	s = localDirRe.ReplaceAllString(s, PlaceholderLocalDir)
	s = appIDRe.ReplaceAllString(s, PlaceholderAppID)

	return append([]byte(s), '\n'), nil
}

// normalizeSparkProperties rewrites the line-oriented values inside a
// ConfigMap's data["spark.properties"] that are only unambiguous with line
// context.
func normalizeSparkProperties(obj map[string]any) {
	data, _ := obj["data"].(map[string]any)
	if data == nil {
		return
	}
	props, ok := data["spark.properties"].(string)
	if !ok {
		return
	}
	lines := strings.Split(props, "\n")
	out := lines[:0]
	for _, line := range lines {
		switch {
		case propDateRe.MatchString(line):
			line = "#" + PlaceholderDate
		case strings.HasPrefix(line, "spark.app.submitTime="):
			line = "spark.app.submitTime=" + PlaceholderSubmitTime
		case strings.HasPrefix(line, "spark.master="):
			line = "spark.master=" + PlaceholderMaster
		case strings.HasPrefix(line, "spark.kubernetes.driver.podTemplateFile="),
			strings.HasPrefix(line, "spark.kubernetes.driver.podTemplateContainerName="):
			// Submit-time pod-template file artifacts: a host-specific path and the
			// container name that only matters while spark-submit reads the file. The
			// native path applies the driver template in-process and emits neither, so
			// drop these lines to keep the golden comparable. (The executor
			// podTemplateFile is kept: Spark rewrites it to an in-pod mount path that
			// is runtime-relevant and reproducible.)
			continue
		}
		out = append(out, line)
	}
	data["spark.properties"] = strings.Join(out, "\n")
}

// normalizeSparkUserEnv pins the SPARK_USER env var on a driver pod's
// containers. Spark sets it to Utils.getCurrentUserName() — the submit host's
// OS user (e.g. "amaredia") — which would otherwise bake the capture machine's
// account into the golden and break on any other host/CI.
func normalizeSparkUserEnv(obj map[string]any) {
	spec, _ := obj["spec"].(map[string]any)
	if spec == nil {
		return
	}
	containers, _ := spec["containers"].([]any)
	for _, c := range containers {
		container, _ := c.(map[string]any)
		if container == nil {
			continue
		}
		env, _ := container["env"].([]any)
		for _, e := range env {
			entry, _ := e.(map[string]any)
			if entry == nil {
				continue
			}
			if name, _ := entry["name"].(string); name == "SPARK_USER" {
				if _, ok := entry["value"]; ok {
					entry["value"] = PlaceholderSparkUser
				}
			}
		}
	}
}

// normalizeOwnerReferenceUIDs pins the uid of every metadata.ownerReferences
// entry to a stable placeholder, adding the field when absent. The owner pod's
// uid is server-assigned (post-create), so it is not reproducible and the pure-Go
// builder does not set it; this makes the captured value and the builder's
// (absent) value line up.
func normalizeOwnerReferenceUIDs(obj map[string]any) {
	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		return
	}
	refs, _ := meta["ownerReferences"].([]any)
	for _, r := range refs {
		ref, _ := r.(map[string]any)
		if ref == nil {
			continue
		}
		ref["uid"] = PlaceholderOwnerUID
	}
}

// normalizeVolumeZeroValues drops fabric8's explicit zero values from a driver
// pod's volume mounts and PVC volume sources so they match Go's omitempty output:
//   - container volumeMounts: readOnly:false, subPath:"", subPathExpr:""
//   - volumes[].persistentVolumeClaim: readOnly:false
//
// Only zero values are removed (a real readOnly:true or non-empty subPath is
// preserved), and only these keys — so mounts that never carried them (local-dir,
// conf) are unaffected. Non-pod objects have no spec.containers/volumes, so this is
// a no-op for them.
func normalizeVolumeZeroValues(obj map[string]any) {
	spec, _ := obj["spec"].(map[string]any)
	if spec == nil {
		return
	}
	containers, _ := spec["containers"].([]any)
	for _, c := range containers {
		container, _ := c.(map[string]any)
		if container == nil {
			continue
		}
		mounts, _ := container["volumeMounts"].([]any)
		for _, m := range mounts {
			mount, _ := m.(map[string]any)
			if mount == nil {
				continue
			}
			if ro, ok := mount["readOnly"].(bool); ok && !ro {
				delete(mount, "readOnly")
			}
			if sp, ok := mount["subPath"].(string); ok && sp == "" {
				delete(mount, "subPath")
			}
			if spe, ok := mount["subPathExpr"].(string); ok && spe == "" {
				delete(mount, "subPathExpr")
			}
		}
	}
	volumes, _ := spec["volumes"].([]any)
	for _, v := range volumes {
		volume, _ := v.(map[string]any)
		if volume == nil {
			continue
		}
		pvc, _ := volume["persistentVolumeClaim"].(map[string]any)
		if pvc == nil {
			continue
		}
		if ro, ok := pvc["readOnly"].(bool); ok && !ro {
			delete(pvc, "readOnly")
		}
	}
}

func stripVolatile(obj map[string]any) {
	if meta, _ := obj["metadata"].(map[string]any); meta != nil {
		for _, k := range volatileMetadataKeys {
			delete(meta, k)
		}
	}
	// A created object never carries status; drop any the server echoed.
	delete(obj, "status")
}
