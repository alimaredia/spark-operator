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

// Command recorder is a minimal fake Kubernetes API server used to capture the
// exact driver-pod / ConfigMap / Service objects that stock `spark-submit`
// (--deploy-mode cluster, master k8s://...) intends to create.
//
// Spark's submission client (KubernetesClientApplication -> Client.run) never
// dry-runs: it POSTs the driver pod and server-side-applies the owned
// resources over HTTP. Pointing spark-submit at this server lets us record
// those request bodies verbatim — the "oracle" that the pure-Go
// SparkDriverCreator must reproduce. See test/oracle/README.md.
//
// It answers just enough of the Kubernetes discovery API to keep the fabric8
// client happy, echoes every mutating request back (with a synthetic uid so
// owner references resolve), and writes each mutating request body to
// <out>/raw as an indexed, kind-tagged JSON file.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "listen address; use :0 for an ephemeral port")
	outDir := flag.String("out", "", "directory to write captured request bodies into (required)")
	portFile := flag.String("port-file", "", "if set, write the chosen host:port to this file once listening")
	flag.Parse()

	if *outDir == "" {
		log.Fatal("recorder: -out is required")
	}
	rawDir := filepath.Join(*outDir, "raw")
	if err := os.MkdirAll(rawDir, 0o755); err != nil {
		log.Fatalf("recorder: mkdir %s: %v", rawDir, err)
	}

	rec := &recorder{rawDir: rawDir}
	srv := &http.Server{Handler: rec}

	ln, err := netListen(*addr)
	if err != nil {
		log.Fatalf("recorder: listen %s: %v", *addr, err)
	}
	hostPort := ln.Addr().String()
	log.Printf("recorder: listening on http://%s  (out=%s)", hostPort, *outDir)
	if *portFile != "" {
		if err := os.WriteFile(*portFile, []byte(hostPort), 0o644); err != nil {
			log.Fatalf("recorder: write port-file: %v", err)
		}
	}

	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Fatalf("recorder: serve: %v", err)
	}
}

type recorder struct {
	rawDir string
	seq    atomic.Int64
	mu     sync.Mutex
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	_ = req.Body.Close()

	switch req.Method {
	case http.MethodGet:
		r.handleDiscovery(w, req)
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		r.handleMutation(w, req, body)
	case http.MethodDelete:
		// Rollback path only runs on error; acknowledge so we never wedge.
		writeJSON(w, http.StatusOK, statusOK())
	default:
		writeJSON(w, http.StatusOK, statusOK())
	}
}

// handleMutation records the request body and echoes the object back with a
// synthetic uid/resourceVersion/creationTimestamp so the fabric8 client treats
// the create/apply as successful and can set owner references.
func (r *recorder) handleMutation(w http.ResponseWriter, req *http.Request, body []byte) {
	obj := map[string]any{}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &obj); err != nil {
			// Not JSON we understand; still record it, but don't fail the client.
			r.writeRaw(req, "unknown", body)
			writeJSON(w, http.StatusOK, statusOK())
			return
		}
	}

	kind, _ := obj["kind"].(string)
	if kind == "" {
		kind = kindFromPath(req.URL.Path)
	}
	r.writeRaw(req, kind, body)

	// Inject server-populated identity so owner references resolve downstream.
	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
		obj["metadata"] = meta
	}
	name, _ := meta["name"].(string)
	if name == "" {
		name = nameFromPath(req.URL.Path)
		if name != "" {
			meta["name"] = name
		}
	}
	meta["uid"] = fmt.Sprintf("oracle-uid-%s", strings.ToLower(kind))
	meta["resourceVersion"] = "1"
	meta["creationTimestamp"] = "2024-01-01T00:00:00Z"

	status := http.StatusOK
	if req.Method == http.MethodPost {
		status = http.StatusCreated
	}
	writeJSON(w, status, obj)
}

// writeRaw persists a mutating request body in capture order. Files are named
// NNN-METHOD-kind.json so the regen step can group by kind deterministically.
func (r *recorder) writeRaw(req *http.Request, kind string, body []byte) {
	if len(body) == 0 {
		return
	}
	n := r.seq.Add(1)
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind == "" {
		kind = "unknown"
	}
	name := fmt.Sprintf("%03d-%s-%s.json", n, strings.ToLower(req.Method), kind)

	r.mu.Lock()
	defer r.mu.Unlock()
	pretty := prettyJSON(body)
	if err := os.WriteFile(filepath.Join(r.rawDir, name), pretty, 0o644); err != nil {
		log.Printf("recorder: write raw %s: %v", name, err)
	}
	log.Printf("recorder: %s %s -> %s (%d bytes)", req.Method, req.URL.Path, name, len(body))
}

// handleDiscovery answers the fabric8 client's version/discovery probes with
// just enough surface for core/v1 and networking.k8s.io/v1 resources.
func (r *recorder) handleDiscovery(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/version":
		writeJSON(w, http.StatusOK, map[string]any{
			"major": "1", "minor": "30", "gitVersion": "v1.30.0", "platform": "linux/amd64",
		})
	case req.URL.Path == "/api":
		writeJSON(w, http.StatusOK, map[string]any{
			"kind": "APIVersions", "versions": []string{"v1"},
			"serverAddressByClientCIDRs": []map[string]any{
				{"clientCIDR": "0.0.0.0/0", "serverAddress": req.Host},
			},
		})
	case req.URL.Path == "/apis":
		writeJSON(w, http.StatusOK, map[string]any{
			"kind": "APIGroupList", "apiVersion": "v1",
			"groups": []map[string]any{apiGroup("networking.k8s.io", "v1")},
		})
	case req.URL.Path == "/api/v1":
		writeJSON(w, http.StatusOK, apiResourceList("v1",
			resource("pods", "Pod", true),
			resource("services", "Service", true),
			resource("configmaps", "ConfigMap", true),
			resource("namespaces", "Namespace", false),
			resource("persistentvolumeclaims", "PersistentVolumeClaim", true),
			resource("secrets", "Secret", true),
		))
	case req.URL.Path == "/apis/networking.k8s.io/v1":
		writeJSON(w, http.StatusOK, apiResourceList("networking.k8s.io/v1",
			resource("networkpolicies", "NetworkPolicy", true),
		))
	default:
		// A specific-resource GET (existence check) — report not found so the
		// client proceeds to create rather than assuming an object exists.
		writeJSON(w, http.StatusNotFound, statusNotFound(req.URL.Path))
	}
}

// --- discovery helpers ---

func apiGroup(name, version string) map[string]any {
	gv := name + "/" + version
	return map[string]any{
		"name": name,
		"versions": []map[string]any{
			{"groupVersion": gv, "version": version},
		},
		"preferredVersion": map[string]any{"groupVersion": gv, "version": version},
	}
}

func apiResourceList(groupVersion string, resources ...map[string]any) map[string]any {
	return map[string]any{
		"kind": "APIResourceList", "apiVersion": "v1",
		"groupVersion": groupVersion, "resources": resources,
	}
}

func resource(name, kind string, namespaced bool) map[string]any {
	return map[string]any{
		"name": name, "singularName": strings.ToLower(kind), "kind": kind,
		"namespaced": namespaced,
		"verbs":      []string{"create", "delete", "get", "list", "patch", "update", "watch"},
	}
}

func statusOK() map[string]any {
	return map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Success", "code": 200}
}

func statusNotFound(path string) map[string]any {
	return map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure",
		"reason": "NotFound", "code": 404, "message": "not found: " + path,
	}
}

// --- path helpers ---

// kindFromPath maps a REST resource path segment to its Kind (best effort;
// used only when the request body omits "kind", e.g. some PATCH shapes).
func kindFromPath(path string) string {
	seg := resourceSegment(path)
	switch seg {
	case "pods":
		return "Pod"
	case "services":
		return "Service"
	case "configmaps":
		return "ConfigMap"
	case "networkpolicies":
		return "NetworkPolicy"
	case "secrets":
		return "Secret"
	case "persistentvolumeclaims":
		return "PersistentVolumeClaim"
	}
	return ""
}

// resourceSegment returns the collection segment (e.g. "pods") from a path like
// /api/v1/namespaces/<ns>/pods[/<name>].
func resourceSegment(path string) string {
	parts := splitPath(path)
	for i, p := range parts {
		if p == "namespaces" && i+2 < len(parts) {
			return parts[i+2]
		}
	}
	if n := len(parts); n > 0 {
		return parts[n-1]
	}
	return ""
}

// nameFromPath returns the trailing resource name for apply-by-name PATCHes:
// /api/v1/namespaces/<ns>/<resource>/<name>.
func nameFromPath(path string) string {
	parts := splitPath(path)
	for i, p := range parts {
		if p == "namespaces" && i+3 < len(parts) {
			return parts[i+3]
		}
	}
	return ""
}

func splitPath(path string) []string {
	raw := strings.Split(strings.Trim(path, "/"), "/")
	out := raw[:0]
	for _, p := range raw {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// --- io helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func prettyJSON(body []byte) []byte {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return body
	}
	// json.Marshal emits map[string]any keys in sorted order, so indented
	// output is deterministic across runs.
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return body
	}
	return append(out, '\n')
}

func netListen(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}
