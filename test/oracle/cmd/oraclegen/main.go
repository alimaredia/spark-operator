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

	// Later captures win per kind (e.g. the owner-referenced re-apply of a
	// pre-resource supersedes its first, owner-less apply).
	byKind := map[string]captured{}
	for _, it := range items {
		byKind[it.kind] = it
	}

	kinds := make([]string, 0, len(byKind))
	for k := range byKind {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)

	for _, kind := range kinds {
		it := byKind[kind]
		norm, err := oraclenorm.Normalize(it.raw, vars)
		if err != nil {
			return fmt.Errorf("normalize %s (%s): %w", kind, it.file, err)
		}
		name := goldenFileName(kind)
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

// goldenFileName maps a Kind to its golden filename. The driver pod gets a
// descriptive name; everything else uses lower-cased kind.
func goldenFileName(kind string) string {
	if kind == "Pod" {
		return "driver-pod.json"
	}
	return strings.ToLower(kind) + ".json"
}
