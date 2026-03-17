#!/bin/bash
# k8s/deploy.sh - Deploys Docling + PySpark
#
# This script deploys the docling-spark job in the SAME namespace as the operator.
# By default, it uses spark-operator (config/default/).

set -e
NAMESPACE="${NAMESPACE:-spark-operator}"
INPUT_PVC="docling-input"
OUTPUT_PVC="docling-output"
# Use full UBI image (not minimal) because oc cp requires tar
HELPER_IMAGE="registry.access.redhat.com/ubi9/ubi"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Determine which CLI to use
detect_cli() {
    if command -v oc &> /dev/null; then
        CLI="oc"
    else
        CLI="kubectl"
    fi
}

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Helper Functions for PVC Operations
create_helper_pod() {
    local pod_name=$1
    local pvc_name=$2
    local mount_path=$3
    
    if $CLI get pod "$pod_name" -n "$NAMESPACE" &> /dev/null; then
        log_info "Helper pod '$pod_name' already exists"
        return 0
    fi
    
    log_info "Creating helper pod '$pod_name'..."
    
    $CLI run "$pod_name" \
        --image="$HELPER_IMAGE" \
        --restart=Never \
        -n "$NAMESPACE" \
        --overrides="{
            \"spec\": {
                \"containers\": [{
                    \"name\": \"$pod_name\",
                    \"image\": \"$HELPER_IMAGE\",
                    \"command\": [\"sleep\", \"3600\"],
                    \"volumeMounts\": [{
                        \"name\": \"data\",
                        \"mountPath\": \"$mount_path\"
                    }]
                }],
                \"volumes\": [{
                    \"name\": \"data\",
                    \"persistentVolumeClaim\": {
                        \"claimName\": \"$pvc_name\"
                    }
                }]
            }
        }"
    
    log_info "Waiting for pod to be ready..."
    $CLI wait --for=condition=Ready "pod/$pod_name" -n "$NAMESPACE" --timeout=120s
    log_success "Helper pod ready"
}

delete_helper_pod() {
    local pod_name=$1
    if $CLI get pod "$pod_name" -n "$NAMESPACE" &> /dev/null; then
        log_info "Deleting helper pod '$pod_name'..."
        $CLI delete pod "$pod_name" -n "$NAMESPACE" --ignore-not-found=true
    fi
}

# Commands
cmd_deploy() {
    echo ""
    echo "=============================================="
    echo "  Deploying Docling + PySpark"
    echo "=============================================="
    echo "  Namespace: $NAMESPACE"
    echo "=============================================="

    # Step 1: Verify namespace exists (should be created by operator installation)
    echo ""
    log_info "1. Verifying namespace exists..."
    if ! $CLI get namespace "$NAMESPACE" &> /dev/null; then
        log_error "Namespace '$NAMESPACE' does not exist!"
        echo "   Please install the operator first:"
        echo "   oc apply -k config/default/ --server-side=true"
        exit 1
    fi
    log_success "Namespace '$NAMESPACE' exists"
    # For oc users, also switch to the project context
    if [ "$CLI" == "oc" ]; then
        oc project $NAMESPACE
    fi

    # Step 2: Create PVCs (only if they don't exist)
    echo ""
    log_info "2. Ensuring PVCs exist..."
    # Update PVC namespace dynamically using sed
    if ! $CLI get pvc "$INPUT_PVC" -n "$NAMESPACE" &> /dev/null; then
        sed "s/namespace: spark-operator/namespace: $NAMESPACE/" "$SCRIPT_DIR/docling-input-pvc.yaml" | $CLI apply -f -
    else
        log_info "PVC '$INPUT_PVC' already exists"
    fi
    if ! $CLI get pvc "$OUTPUT_PVC" -n "$NAMESPACE" &> /dev/null; then
        sed "s/namespace: spark-operator/namespace: $NAMESPACE/" "$SCRIPT_DIR/docling-output-pvc.yaml" | $CLI apply -f -
    else
        log_info "PVC '$OUTPUT_PVC' already exists"
    fi

    # Step 3: Submit Spark Application
    echo ""
    echo "3. Submitting Spark Application..."
    # Update namespace and apply - use sed to replace namespace dynamically
    sed "s/namespace: spark-operator/namespace: $NAMESPACE/" "$SCRIPT_DIR/docling-spark-app.yaml" | \
        $CLI replace --force -f - || \
        sed "s/namespace: spark-operator/namespace: $NAMESPACE/" "$SCRIPT_DIR/docling-spark-app.yaml" | \
        $CLI create -f -

    echo ""
    log_success "Deployment complete!"
    echo ""
    echo "📊 Check status:"
    echo "   $CLI get sparkapplications -n $NAMESPACE"
    echo "   $CLI get pods -n $NAMESPACE -w"
    echo ""
    echo "📝 View logs:"
    echo "   $CLI logs -f docling-spark-job-driver -n $NAMESPACE"
    echo ""
    echo "🌐 Access Spark UI (when driver is running):"
    echo "   $CLI port-forward -n $NAMESPACE svc/docling-spark-job-ui-svc 4040:4040"
    echo "   Open: http://localhost:4040"
    echo ""
}

cmd_upload() {
    local local_dir=$1
    
    if [[ -z "$local_dir" ]]; then
        log_error "Usage: $0 upload <local-directory>"
        echo "Example: $0 upload ./my-pdfs/"
        exit 1
    fi
    
    if [[ ! -d "$local_dir" ]]; then
        log_error "Directory '$local_dir' does not exist"
        exit 1
    fi
    
    # Verify namespace exists
    if ! $CLI get namespace "$NAMESPACE" &> /dev/null; then
        log_error "Namespace '$NAMESPACE' does not exist!"
        echo "   Please install the operator first."
        exit 1
    fi
    
    # Check if PVC exists
    if ! $CLI get pvc "$INPUT_PVC" -n "$NAMESPACE" &> /dev/null; then
        log_info "Creating input PVC..."
        sed "s/namespace: spark-operator/namespace: $NAMESPACE/" "$SCRIPT_DIR/docling-input-pvc.yaml" | $CLI apply -f -
    fi
    
    echo ""
    echo "=============================================="
    echo "  Uploading files to Input PVC"
    echo "=============================================="
    echo ""
    
    create_helper_pod "pvc-uploader" "$INPUT_PVC" "/input"
    
    log_info "Files to upload:"
    ls -la "$local_dir"
    echo ""
    
    log_info "Copying files to PVC..."
    $CLI cp "$local_dir/." "pvc-uploader:/input/" -n "$NAMESPACE"
    
    log_info "Verifying files on PVC:"
    $CLI exec "pvc-uploader" -n "$NAMESPACE" -- ls -la /input/
    
    echo ""
    log_info "Security context (shows SCC-assigned fsGroup):"
    $CLI exec "pvc-uploader" -n "$NAMESPACE" -- id
    
    echo ""
    log_success "Upload complete!"
    log_info "Files are ready on the input PVC."
    log_info "Run the Spark job: ./k8s/deploy.sh"
    
    delete_helper_pod "pvc-uploader"
}

cmd_download() {
    local local_dir=$1
    
    if [[ -z "$local_dir" ]]; then
        log_error "Usage: $0 download <local-directory>"
        echo "Example: $0 download ./results/"
        exit 1
    fi
    
    mkdir -p "$local_dir"
    
    if ! $CLI get pvc "$OUTPUT_PVC" -n "$NAMESPACE" &> /dev/null; then
        log_error "Output PVC '$OUTPUT_PVC' does not exist"
        exit 1
    fi
    
    echo ""
    echo "=============================================="
    echo "  Downloading results from Output PVC"
    echo "=============================================="
    echo ""
    
    create_helper_pod "pvc-downloader" "$OUTPUT_PVC" "/output"
    
    log_info "Files on output PVC:"
    $CLI exec "pvc-downloader" -n "$NAMESPACE" -- ls -la /output/ || log_warning "Output may be empty"
    
    log_info "Copying files to '$local_dir'..."
    $CLI cp "pvc-downloader:/output/." "$local_dir/" -n "$NAMESPACE"
    
    echo ""
    log_info "Downloaded files:"
    ls -la "$local_dir"
    
    log_success "Download complete!"
    
    delete_helper_pod "pvc-downloader"
}

cmd_status() {
    echo ""
    echo "=============================================="
    echo "  Status"
    echo "=============================================="
    echo ""
    
    log_info "PVCs:"
    $CLI get pvc -n "$NAMESPACE" 2>/dev/null || echo "  No PVCs found"
    echo ""
    
    log_info "SparkApplications:"
    $CLI get sparkapplication -n "$NAMESPACE" 2>/dev/null || echo "  No SparkApplications found"
    echo ""
    
    log_info "Pods:"
    $CLI get pods -n "$NAMESPACE" 2>/dev/null || echo "  No pods found"
    echo ""
    
    if [ "$CLI" == "oc" ]; then
        log_info "CSI Driver fsGroup support:"
        $CLI get csidriver ebs.csi.aws.com -o jsonpath='{.spec.fsGroupPolicy}' 2>/dev/null && echo "" || echo "  Could not check"
    fi
}

cmd_cleanup() {
    echo ""
    log_info "Cleaning up helper pods..."
    delete_helper_pod "pvc-uploader"
    delete_helper_pod "pvc-downloader"
    log_success "Cleanup complete!"
}

show_usage() {
    echo ""
    echo "Usage: $0 [command] [arguments]"
    echo ""
    echo "Commands:"
    echo "  (no args)        Deploy the Spark application"
    echo "  upload <dir>     Upload files to input PVC"
    echo "  download <dir>   Download results from output PVC"
    echo "  status           Show PVC and job status"
    echo "  cleanup          Remove helper pods"
    echo ""
    echo "Workflow:"
    echo "  1. Install operator: oc apply -k config/default/ --server-side=true"
    echo "  2. Upload PDFs:      $0 upload ./my-pdfs/"
    echo "  3. Run job:          $0"
    echo "  4. View logs:        oc logs -f docling-spark-job-driver -n $NAMESPACE"
    echo "  5. Delete job:       oc delete sparkapplication docling-spark-job -n $NAMESPACE"
    echo "  6. Download:         $0 download ./results/"
    echo ""
    echo "Examples:"
    echo "  # Default (spark-operator namespace)"
    echo "  ./k8s/deploy.sh"
}

# =============================================================================
# Main
# =============================================================================

detect_cli

case "${1:-}" in
    upload)
        cmd_upload "$2"
        ;;
    download)
        cmd_download "$2"
        ;;
    status)
        cmd_status
        ;;
    cleanup)
        cmd_cleanup
        ;;
    -h|--help|help)
        show_usage
        ;;
    "")
        cmd_deploy
        ;;
    *)
        log_error "Unknown command: $1"
        show_usage
        exit 1
        ;;
esac
