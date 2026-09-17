#!/usr/bin/env bash
# ==============================================
# Tools Script
# Available commands:
#   nf-setup - Install reference nextflow version (25.04.2), patch jar
#   install  - Build and install plugin (runs `make install` in script dir)
#   test     - Run test (cd into ./test and run nextflow...; output -> cmdout.log)
#   clean    - Clean test artifacts (delete everything in ./test except nextflow.config)
# ==============================================

set -euo pipefail

# Resolve the directory this script lives in (works even when sourced via symlink)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$SCRIPT_DIR/test"
KEEP_FILE="nextflow.config"

show_usage() {
    echo "Usage: $(basename "$0") <command>"
    echo
    echo "Available commands:"
    echo "  nf-setup - Install reference nextflow version (25.04.2), patch jar"
    echo "  install  - Build and install plugin (runs 'make install' in the script directory)"
    echo "  test     - Run tests (changes directory into './test' relative to the script and runs Nextflow)"
    echo "  clean    - Remove everything in './test' except the file '$KEEP_FILE'"
}

CMD="${1:-}"

if [ -z "$CMD" ]; then
    show_usage
    exit 1
fi

case "$CMD" in
    nf-setup)
        NEXTFLOW_VERSION="${NXF_VER:-25.04.2}"
        echo "==> Installing Nextflow v$NEXTFLOW_VERSION"

        if ! command -v curl >/dev/null 2>&1; then
            echo "Error: curl is required to run the official installer. Install curl and retry." >&2
            exit 11
        fi

        (
            echo "==> Running official installer (get.nextflow.io) with NXF_VER=$NEXTFLOW_VERSION"
            NXF_VER="$NEXTFLOW_VERSION" curl -fsSL https://get.nextflow.io | bash || {
                rc=$?; echo "Installer failed with exit code $rc" >&2; exit $rc
            }
        )

        PATCH_SCRIPT="$SCRIPT_DIR/patch/patch-nextflow.sh"
        if [ -x "$PATCH_SCRIPT" ]; then
            echo "==> Running patch script: $PATCH_SCRIPT"
            "$PATCH_SCRIPT"
            echo "==> Patch script completed."
        elif [ -f "$PATCH_SCRIPT" ]; then
            echo "==> Found patch script but not executable; running via bash:"
            bash "$PATCH_SCRIPT"
            echo "==> Patch script completed."
        else
            echo "==> No patch script found at: $PATCH_SCRIPT (skipping patch step)"
        fi

        echo
        echo "==> Nextflow v$NEXTFLOW_VERSION installed"
        echo
        echo "Note: the installer uses NXF_VER to pick the version. You can also set NXF_VER in your environment"
        ;;
    install)
        echo "==> Running 'make install' in script directory: $SCRIPT_DIR"
        ( cd "$SCRIPT_DIR" && make install )
        echo "==> make install finished."
        ;;
    test)
        if [ ! -d "$TEST_DIR" ]; then
            echo "Error: test directory does not exist: $TEST_DIR" >&2
            exit 2
        fi

        echo "==> Running Nextflow in: $TEST_DIR"
        (
            cd "$TEST_DIR" || { echo "Failed to cd to $TEST_DIR" >&2; exit 3; }
            nextflow run nextflow-io/rnaseq-nf -with-docker | tee cmdout.log
            nf_status=${PIPESTATUS[0]:-0}
            echo "==> nextflow exit status: $nf_status"
            exit "$nf_status"
        )
        exit_status=$?
        if [ $exit_status -ne 0 ]; then
            echo "==> Test command failed with status $exit_status" >&2
            exit $exit_status
        fi
        echo "==> Test command completed successfully."
        ;;
    clean)
        if [ ! -d "$TEST_DIR" ]; then
            echo "Warning: test directory does not exist: $TEST_DIR (nothing to clean)"
            exit 0
        fi
        case "$TEST_DIR" in
            "$SCRIPT_DIR"/* | "$SCRIPT_DIR")
                # OK
                ;;
            *)
                echo "Refusing to operate: test dir ($TEST_DIR) is not inside script dir ($SCRIPT_DIR)." >&2
                exit 4
                ;;
        esac

        echo "==> Cleaning test directory: $TEST_DIR"
        find "$TEST_DIR" -mindepth 1 ! -path "$TEST_DIR/$KEEP_FILE" -exec rm -rf {} + || true

        echo "==> Clean complete. Preserved (if present): $TEST_DIR/$KEEP_FILE"
        ;;
    *)
        echo "Unknown command: $CMD" >&2
        show_usage
        exit 1
        ;;
esac
