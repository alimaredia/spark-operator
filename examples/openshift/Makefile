.SILENT:

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Directory paths (relative to this Makefile in examples/openshift/)
MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
REPO_ROOT := $(abspath $(MAKEFILE_DIR)/../..)
TESTS_DIR := $(MAKEFILE_DIR)/tests
CONTEXT ?= $(REPO_ROOT)

# Include repo root bin/ in PATH so tools installed by the root Makefile
# (e.g. kind via 'make kind') are available to the shell scripts.
LOCALBIN := $(REPO_ROOT)/bin
export PATH := $(LOCALBIN):$(PATH)

# Configuration
KIND_CLUSTER_NAME ?= spark-operator
CLEANUP ?= true
NAMESPACE ?= spark-operator
CONTROLLER_SA ?= spark-operator-controller
SCHED_NAME ?= rbac-scheduled-smoke
TIMEOUT_SECONDS ?= 180

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Cluster Setup

.PHONY: kind-setup
kind-setup: ## Setup a local Kind cluster for testing.
	@echo "Setting up Kind cluster..."
	chmod +x $(TESTS_DIR)/setup-kind-cluster.sh
	$(TESTS_DIR)/setup-kind-cluster.sh

.PHONY: kind-setup-full
kind-setup-full: ## Setup Kind cluster with docling image and test assets.
	@echo "Setting up Kind cluster with full docling setup..."
	chmod +x $(TESTS_DIR)/setup-kind-cluster.sh
	$(TESTS_DIR)/setup-kind-cluster.sh --with-docling --upload-assets

.PHONY: kind-cleanup
kind-cleanup: ## Delete the Kind cluster and cleanup resources.
	@echo "Cleaning up Kind cluster..."
	chmod +x $(TESTS_DIR)/cleanup-kind-cluster.sh
	$(TESTS_DIR)/cleanup-kind-cluster.sh

##@ Tests
## These targets work on both KIND (local) and OpenShift clusters.
## Use CLEANUP=false to skip cleanup (default: true).
## Example: CLEANUP=false make test-spark-pi

.PHONY: operator-install
operator-install: ## Install Spark operator on KIND or OpenShift cluster.
	@if ! kubectl cluster-info &>/dev/null; then \
		echo "No cluster detected, running kind-setup first..."; \
		$(MAKE) kind-setup; \
	fi
	@echo "Installing Spark operator..."
	chmod +x $(TESTS_DIR)/test-operator-install.sh
	CLEANUP=$(CLEANUP) $(TESTS_DIR)/test-operator-install.sh

.PHONY: test-spark-pi
test-spark-pi: ## Run Spark Pi test on KIND or OpenShift.
	@if ! kubectl get deployment -n spark-operator -l app.kubernetes.io/name=spark-operator &>/dev/null; then \
		echo "Spark operator not found, running operator-install first..."; \
		$(MAKE) operator-install CLEANUP=false; \
	fi
	@echo "Running Spark Pi test..."
	chmod +x $(TESTS_DIR)/test-spark-pi.sh
	CLEANUP=$(CLEANUP) $(TESTS_DIR)/test-spark-pi.sh

.PHONY: test-docling-spark
test-docling-spark: ## Run Docling Spark test on KIND or OpenShift.
	@if ! kubectl get deployment -n spark-operator -l app.kubernetes.io/name=spark-operator &>/dev/null; then \
		echo "Spark operator not found, running operator-install first..."; \
		$(MAKE) operator-install CLEANUP=false; \
	fi
	@echo "Running Docling Spark test..."
	chmod +x $(TESTS_DIR)/test-docling-spark.sh
	CLEANUP=$(CLEANUP) $(TESTS_DIR)/test-docling-spark.sh

.PHONY: e2e-kustomize-test
e2e-kustomize-test: ## Run Go e2e tests using Kustomize manifests for operator installation.
	@echo "Running Go e2e tests with Kustomize installation..."
	cd $(REPO_ROOT) && INSTALL_METHOD=kustomize go test ./examples/openshift/tests/e2e/ -v -ginkgo.v -timeout 30m

.PHONY: scheduledspark-rbac-check
scheduledspark-rbac-check: ## RBAC preflight for ScheduledSparkApplication.
	@echo "Running RBAC preflight for ScheduledSparkApplication..."
	chmod +x $(TESTS_DIR)/check-scheduledspark-rbac.sh
	NAMESPACE=$(NAMESPACE) CONTROLLER_SA=$(CONTROLLER_SA) \
		$(TESTS_DIR)/check-scheduledspark-rbac.sh

.PHONY: scheduledspark-smoke
scheduledspark-smoke: ## ScheduledSparkApplication smoke test.
	@echo "Running ScheduledSparkApplication smoke test..."
	chmod +x $(TESTS_DIR)/test-scheduledspark-smoke.sh
	NAMESPACE=$(NAMESPACE) SCHED_NAME=$(SCHED_NAME) TIMEOUT_SECONDS=$(TIMEOUT_SECONDS) \
		$(TESTS_DIR)/test-scheduledspark-smoke.sh

.PHONY: scheduledspark-verify
scheduledspark-verify: ## RBAC + ScheduledSparkApplication combined verification.
	$(MAKE) scheduledspark-rbac-check
	$(MAKE) scheduledspark-smoke

.PHONY: test-all
test-all: ## Run all tests (operator-install, spark-pi, docling, scheduledspark).
	@echo "Running all OpenShift/KIND tests..."
	$(MAKE) operator-install CLEANUP=false
	$(MAKE) test-spark-pi CLEANUP=false
	$(MAKE) test-docling-spark CLEANUP=false
	$(MAKE) scheduledspark-verify CLEANUP=false
	$(MAKE) test-docling-spark

##@ Image Build/Load (CI and local)

.PHONY: docker-build-file
docker-build-file: ## Build docker image with a specific Dockerfile. Usage: make -C examples/openshift docker-build-file DOCKERFILE=path IMAGE=<full_image>
	@if [ -z "$(DOCKERFILE)" ]; then echo "ERROR: DOCKERFILE is required (e.g., DOCKERFILE=examples/openshift/Dockerfile.odh)"; exit 1; fi
	@if [ -z "$(IMAGE)" ]; then echo "ERROR: IMAGE is required (e.g., IMAGE=quay.io/opendatahub/spark-operator:local)"; exit 1; fi
	@DF="$(DOCKERFILE)"; \
	if [[ "$$DF" != /* ]]; then DF="$(REPO_ROOT)/$$DF"; fi; \
	if [ -n "$(PLATFORM)" ]; then \
		echo "Building for platform $(PLATFORM)"; \
		docker build --platform $(PLATFORM) -f "$$DF" -t ${IMAGE} $(CONTEXT) ; \
	else \
		docker build -f "$$DF" -t ${IMAGE} $(CONTEXT) ; \
	fi

.PHONY: kind-load-image-file
kind-load-image-file: ## Load a specifically built image into Kind. Usage: make -C examples/openshift kind-load-image-file IMAGE=<full_image>
	@if [ -z "$(IMAGE)" ]; then echo "ERROR: IMAGE is required (e.g., IMAGE=quay.io/opendatahub/spark-operator:local)"; exit 1; fi
	kind load docker-image --name $(KIND_CLUSTER_NAME) $(IMAGE)
