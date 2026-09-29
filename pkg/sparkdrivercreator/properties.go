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
	"fmt"
	"sort"
	"strings"
	"time"
)

// propertiesHeaderPrefix is the leading comment java.util.Properties.store writes
// via KubernetesClientUtils.buildStringFromPropertiesMap. The ConfigMap name is
// appended verbatim (it is randomized and normalized away in the oracle).
const propertiesHeaderPrefix = "Java properties built from Kubernetes config map with name: "

// javaPropertiesDateLayout renders the timestamp comment java.util.Properties
// writes as the second line, e.g. "Tue Sep 29 11:12:50 EDT 2026". The oracle
// normalizes this line to a placeholder; it need only match Java's shape.
const javaPropertiesDateLayout = "Mon Jan 02 15:04:05 MST 2006"

// serializeProperties renders props as a java.util.Properties.store byte stream,
// matching KubernetesClientUtils.buildStringFromPropertiesMap: a header comment,
// a date comment, then the entries. Keys are emitted in sorted order so the
// output is deterministic (Spark's JDK happens to emit sorted keys; sorting also
// keeps the golden stable across regenerations).
func serializeProperties(configMapName string, props map[string]string) string {
	var b strings.Builder
	b.WriteString("#" + propertiesHeaderPrefix + configMapName + "\n")
	b.WriteString("#" + time.Now().Format(javaPropertiesDateLayout) + "\n")

	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		// Keys escape spaces too (escapeSpace=true); values do not.
		b.WriteString(saveConvert(k, true))
		b.WriteByte('=')
		b.WriteString(saveConvert(props[k], false))
		b.WriteByte('\n')
	}
	return b.String()
}

// saveConvert mirrors java.util.Properties.saveConvert: it escapes the
// characters that are special in a .properties line. escapeSpace escapes every
// space (used for keys); for values only a leading space is escaped. Non-ASCII
// and control characters are emitted as \uXXXX, matching store's default
// escapeUnicode=true.
func saveConvert(s string, escapeSpace bool) string {
	var b strings.Builder
	for i, r := range s {
		switch r {
		case ' ':
			if i == 0 || escapeSpace {
				b.WriteByte('\\')
			}
			b.WriteByte(' ')
		case '\\':
			b.WriteString(`\\`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\f':
			b.WriteString(`\f`)
		case '=', ':', '#', '!':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			if r < 0x20 || r > 0x7e {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}
