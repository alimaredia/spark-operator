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

// Package oracle holds the differential ("oracle") test: for each supported
// Spark version, the driver pod / ConfigMap / Service that stock spark-submit
// would create (captured under golden/<version>/) are the source of truth that
// the pure-Go sparkdrivercreator.SparkDriverCreator must reproduce.
//
// Regenerate the golden files with test/oracle/regen.sh (needs a JVM; installs
// Spark via pyspark). This test itself is pure Go and needs no JVM — it only
// reads the committed golden files.
package oracle

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/sparkdrivercreator"
	"github.com/kubeflow/spark-operator/v2/test/oracle/internal/oraclenorm"
)

// sparkVersions is the oracle matrix. Add a version here once
// test/oracle/setup-spark.sh <ver> && test/oracle/regen.sh has produced its
// golden files under golden/<ver>/.
var sparkVersions = []string{"4.0.4"}

// goldenObjectFiles are the per-case golden files, keyed by the Kind each holds.
var goldenObjectFiles = map[string]string{
	"Pod":       "driver-pod.json",
	"ConfigMap": "configmap.json",
	"Service":   "service.json",
}

func imageFor(v string) string { return "spark:" + v }

func examplesJarFor(v string) string {
	return fmt.Sprintf("local:///opt/spark/examples/jars/spark-examples_2.13-%s.jar", v)
}

// leftoverDynamicRes are patterns that MUST NOT survive normalization — if any
// matches a golden file, a per-submission value leaked through.
var leftoverDynamicRes = map[string]*regexp.Regexp{
	"app id (spark-<32hex>)":       regexp.MustCompile(`spark-[0-9a-f]{32}`),
	"configmap id (spark-drv-...)": regexp.MustCompile(`spark-drv-[0-9a-f]+-conf-map`),
	"local dir (spark-<uuid>)":     regexp.MustCompile(`spark-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`),
	"submitTime (millis)":          regexp.MustCompile(`\b\d{13}\b`),
}

// TestGoldenWellFormed guards the committed golden files: they must exist, be
// valid objects of the expected Kind, be fully normalized (no leftover dynamic
// values), and actually carry the placeholders normalization introduces. This
// runs without a JVM and turns silent golden corruption into a red test.
func TestGoldenWellFormed(t *testing.T) {
	for _, ver := range sparkVersions {
		cases := discoverCases(t, ver)
		require.NotEmpty(t, cases, "no golden cases found for Spark %s", ver)
		for _, name := range cases {
			t.Run(ver+"/"+name, func(t *testing.T) {
				dir := filepath.Join("golden", ver, name)
				for kind, file := range goldenObjectFiles {
					raw, err := os.ReadFile(filepath.Join(dir, file))
					require.NoError(t, err, "golden %s missing", file)

					var obj map[string]any
					require.NoError(t, json.Unmarshal(raw, &obj), "golden %s not valid JSON", file)
					assert.Equal(t, kind, obj["kind"], "golden %s has wrong kind", file)

					for desc, re := range leftoverDynamicRes {
						assert.Falsef(t, re.Match(raw),
							"golden %s still contains an un-normalized %s", file, desc)
					}
				}
				// Normalization must have run: the driver pod name is placeholdered.
				pod := readGolden(t, dir, "driver-pod.json")
				name, _ := pod["metadata"].(map[string]any)["name"].(string)
				assert.Contains(t, name, oraclenorm.PlaceholderPrefix,
					"driver pod name should be normalized to a placeholder")
			})
		}
	}
}

// TestDriverSpecMatchesOracle is the differential test proper: build the driver
// resources in pure Go and require them to match stock spark-submit's captured
// output. Each captured object is its own subtest; an object Build does not yet
// produce (a nil field) skips rather than fails, so the objects can land one at a
// time behind their committed goldens.
func TestDriverSpecMatchesOracle(t *testing.T) {
	for _, ver := range sparkVersions {
		for _, name := range discoverCases(t, ver) {
			t.Run(ver+"/"+name, func(t *testing.T) {
				app := loadApplication(t, name, ver)

				res, err := sparkdrivercreator.New().Build(app)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.NotNil(t, res.Pod, "driver pod must always be built")

				// Vars are extracted from the pod (it carries the app id / prefix /
				// configmap id / local dir the other objects reference).
				vars := oraclenorm.Extract(toUnstructured(t, res.Pod, "Pod", "v1"))
				dir := filepath.Join("golden", ver, name)

				objects := []struct {
					kind string
					file string
					obj  any
				}{
					{"Pod", "driver-pod.json", res.Pod},
					{"ConfigMap", "configmap.json", res.ConfigMap},
					{"Service", "service.json", res.Service},
				}
				for _, o := range objects {
					t.Run(o.kind, func(t *testing.T) {
						if isNilObject(o.obj) {
							t.Skipf("SparkDriverCreator.Build does not produce the %s yet; golden for %s/%s is committed and ready", o.kind, ver, name)
						}
						compare(t, dir, o.file, o.obj, o.kind, "v1", vars)
					})
				}
			})
		}
	}
}

// --- helpers ---

func discoverCases(t *testing.T, ver string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join("golden", ver))
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out
}

func loadApplication(t *testing.T, name, ver string) *v1beta2.SparkApplication {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("cases", name, "application.yaml"))
	require.NoError(t, err)
	s := string(raw)
	s = strings.ReplaceAll(s, "__IMAGE__", imageFor(ver))
	s = strings.ReplaceAll(s, "__EXAMPLES_JAR__", examplesJarFor(ver))

	var app v1beta2.SparkApplication
	require.NoError(t, yaml.Unmarshal([]byte(s), &app), "decode %s/application.yaml", name)
	// The case YAML is version-agnostic; the matrix supplies the version, which
	// drives the spark-version label the builder must emit.
	app.Spec.SparkVersion = ver
	return &app
}

// isNilObject reports whether obj is nil or a typed nil pointer. A nil field in
// DriverResources arrives here boxed in an interface, so a plain obj == nil check
// misses it — reflect is needed to see the nil pointer inside.
func isNilObject(obj any) bool {
	if obj == nil {
		return true
	}
	v := reflect.ValueOf(obj)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

func readGolden(t *testing.T, dir, file string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, file))
	require.NoError(t, err)
	var obj map[string]any
	require.NoError(t, json.Unmarshal(raw, &obj))
	return obj
}

// toUnstructured serializes a typed object to a generic map with kind/apiVersion
// stamped, matching the shape of the captured golden objects.
func toUnstructured(t *testing.T, obj any, kind, apiVersion string) map[string]any {
	t.Helper()
	b, err := json.Marshal(obj)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	m["kind"] = kind
	m["apiVersion"] = apiVersion
	return m
}

// compare normalizes a built object the same way the golden was normalized and
// asserts equality.
func compare(t *testing.T, dir, file string, obj any, kind, apiVersion string, vars oraclenorm.Vars) {
	t.Helper()
	m := toUnstructured(t, obj, kind, apiVersion)
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	got, err := oraclenorm.Normalize(raw, vars)
	require.NoError(t, err)

	want, err := os.ReadFile(filepath.Join(dir, file))
	require.NoError(t, err)

	assert.Equal(t, string(want), string(got), "built %s does not match oracle golden", file)
}
