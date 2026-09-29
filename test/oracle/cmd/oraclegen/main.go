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

// Command oraclegen turns the raw request bodies captured by the recorder into
// normalized golden files, one per Kubernetes Kind (driver-pod.json,
// configmap.json, service.json, …).
//
// It reads <raw>/*.json (each a single captured POST/apply body), groups them
// by Kind, extracts the per-submission Vars from the driver Pod, and writes
// oraclenorm-canonicalized JSON to <out>/<kind>.json plus a vars.json recording
// the placeholders that were substituted.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kubeflow/spark-operator/v2/test/oracle/internal/oraclenorm"
)

func main() {
	rawDir := flag.String("raw", "", "directory of raw captured request bodies (required)")
	outDir := flag.String("out", "", "directory to write normalized golden files into (required)")
	flag.Parse()
	if *rawDir == "" || *outDir == "" {
		log.Fatal("oraclegen: -raw and -out are required")
	}
	if err := run(*rawDir, *outDir); err != nil {
		log.Fatalf("oraclegen: %v", err)
	}
}

type captured struct {
	file string
	kind string
	obj  map[string]any
	raw  []byte
}

func run(rawDir, outDir string) error {
	entries, err := os.ReadDir(rawDir)
	if err != nil {
		return fmt.Errorf("read raw dir: %w", err)
	}

	var items []captured
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(rawDir, e.Name()))
		if err != nil {
			return err
		}
		var obj map[string]any
		if err := json.Unmarshal(b, &obj); err != nil {
			log.Printf("skip %s: not a JSON object: %v", e.Name(), err)
			continue
		}
		kind, _ := obj["kind"].(string)
		if kind == "" {
			log.Printf("skip %s: no kind", e.Name())
			continue
		}
		items = append(items, captured{file: e.Name(), kind: kind, obj: obj, raw: b})
	}
	if len(items) == 0 {
		return fmt.Errorf("no usable objects in %s", rawDir)
	}

	// Extract per-submission Vars from the driver Service so every object in this
	// case is normalized against the same placeholders. The Service name carries
	// the random resource-name prefix even when the pod name is overridden.
	var vars oraclenorm.Vars
	for _, it := range items {
		if it.kind == "Service" {
			vars = oraclenorm.Extract(it.obj)
			break
		}
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	// Group by golden filename (not bare Kind): a pod-template submission emits two
	// ConfigMaps that must land in different files. Later captures win per file
	// (e.g. the owner-referenced re-apply of a pre-resource supersedes its first,
	// owner-less apply).
	byGolden := map[string]captured{}
	for _, it := range items {
		byGolden[goldenFileName(it.kind, objectName(it.obj))] = it
	}

	names := make([]string, 0, len(byGolden))
	for name := range byGolden {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		it := byGolden[name]
		norm, err := oraclenorm.Normalize(it.raw, vars)
		if err != nil {
			return fmt.Errorf("normalize %s (%s): %w", name, it.file, err)
		}
		if err := os.WriteFile(filepath.Join(outDir, name), norm, 0o644); err != nil {
			return err
		}
		log.Printf("wrote %s (from %s)", name, it.file)
	}

	varsJSON, _ := json.MarshalIndent(vars, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "vars.json"), append(varsJSON, '\n'), 0o644); err != nil {
		return err
	}
	return nil
}

// podSpecConfigMapSuffix is the executor pod-template ConfigMap's name suffix
// (KubernetesClientUtils: "<prefix>-driver-podspec-conf-map"). It disambiguates
// the two ConfigMaps a pod-template submission produces. Kept in sync with
// pkg/sparkdrivercreator's constant of the same name.
const podSpecConfigMapSuffix = "-driver-podspec-conf-map"

// goldenFileName maps a captured object to its golden filename. The driver pod
// gets a descriptive name; the executor pod-template ConfigMap is split out by
// its name suffix so it does not collide with the driver conf ConfigMap;
// everything else uses the lower-cased kind.
func goldenFileName(kind, name string) string {
	switch {
	case kind == "Pod":
		return "driver-pod.json"
	case kind == "ConfigMap" && strings.HasSuffix(name, podSpecConfigMapSuffix):
		return "podspec-configmap.json"
	default:
		return strings.ToLower(kind) + ".json"
	}
}

// objectName returns metadata.name from a captured object, or "" if absent.
func objectName(obj map[string]any) string {
	md, ok := obj["metadata"].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := md["name"].(string)
	return name
}
