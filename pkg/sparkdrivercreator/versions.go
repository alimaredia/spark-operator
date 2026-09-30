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

// SupportedSparkVersions is the set of Apache Spark versions the pure-Go native
// creator is differentially verified against — i.e. the versions for which a
// captured stock-spark-submit golden exists under test/oracle and the creator's
// output is pinned byte-for-byte to it.
//
// This is the single source of truth for "which Spark versions the native
// submitter can be trusted to reproduce":
//
//   - test/oracle iterates it as its version matrix, so a version listed here
//     without goldens fails the oracle suite (and vice versa), and
//   - the native submitter's in-pod version gate admits only images whose real
//     Spark version is in this set (see the version gate in the controller's
//     native submission adapter).
//
// Widening native-submitter support is therefore a single, deliberate edit here,
// coupled to adding the matching oracle goldens.
var SupportedSparkVersions = []string{"4.0.4", "4.2.0"}

// IsSupportedSparkVersion reports whether v is a Spark version the native creator
// is oracle-verified for.
func IsSupportedSparkVersion(v string) bool {
	for _, s := range SupportedSparkVersions {
		if s == v {
			return true
		}
	}
	return false
}

// Version-gated submission behaviors. Spark 4.2.0 adds two submission-time
// resources/ports that the earlier supported version (4.0.4) does not emit, and
// the user requires them to appear ONLY when the SparkApplication's declared
// spec.sparkVersion is one that produces them — so 4.0.4 output stays byte-for-
// byte unchanged. The oracle matrix pins each exact version (not a range), so
// these gate on an exact-version match; widen the set alongside the goldens as
// new versions are added.

// versionCreatesDriverNetworkPolicy reports whether a submission for Spark
// version v creates the driver-owned NetworkPolicy by default. Spark's
// NetworkPolicyFeatureStep (SPARK-55653) added this unconditionally in 4.2.0.
func versionCreatesDriverNetworkPolicy(v string) bool {
	return v == "4.2.0"
}

// versionExposesConnectPort reports whether the driver service and container
// expose the default spark-connect port (grpc, 15002) for Spark version v.
// DriverServiceFeatureStep began publishing it by default in 4.2.0.
func versionExposesConnectPort(v string) bool {
	return v == "4.2.0"
}
