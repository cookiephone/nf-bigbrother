#!/usr/bin/env bash
# ==============================================
# nf-bigbrother helper script
#
#   nf-setup - Install the reference Nextflow version (25.04.2) and patch it
#              so tasks emit machine info + extra resource counters
#   install  - Build the plugin and install it into ~/.nextflow/plugins
#   test     - Run the container-free example pipeline and render its graph
#   clean    - Remove generated run artifacts from the example directory
# ==============================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$SCRIPT_DIR/examples/local-pipeline"

show_usage() {
    echo "Usage: $(basename "$0") <nf-setup|install|test|clean>"
}

CMD="${1:-}"
[ -z "$CMD" ] && { show_usage; exit 1; }

case "$CMD" in
    nf-setup)
        NEXTFLOW_VERSION="${NXF_VER:-25.04.2}"
        echo "==> Installing Nextflow v$NEXTFLOW_VERSION"
        command -v curl >/dev/null 2>&1 || { echo "Error: curl is required" >&2; exit 11; }
        NXF_VER="$NEXTFLOW_VERSION" curl -fsSL https://get.nextflow.io | bash

        PATCH_SCRIPT="$SCRIPT_DIR/patch/patch-nextflow.sh"
        if [ -f "$PATCH_SCRIPT" ]; then
            echo "==> Applying command-wrapper patch"
            bash "$PATCH_SCRIPT"
        else
            echo "==> No patch script at $PATCH_SCRIPT (skipping)"
        fi
        echo "==> Nextflow v$NEXTFLOW_VERSION installed and patched"
        ;;

    install)
        echo "==> make install"
        ( cd "$SCRIPT_DIR" && make install )
        ;;

    test)
        [ -d "$EXAMPLE_DIR" ] || { echo "Error: missing $EXAMPLE_DIR" >&2; exit 2; }
        echo "==> Running example pipeline in $EXAMPLE_DIR"
        (
            cd "$EXAMPLE_DIR"
            nextflow run main.nf -ansi-log false
            complete=$(ls bb_out/complete_*.json 2>/dev/null | head -1 || true)
            if [ -n "$complete" ] && command -v dot >/dev/null 2>&1; then
                echo "==> Rendering physical graph"
                "$SCRIPT_DIR/tools/bb_dag.py" "$complete" -f png --metrics -o physical-graph.png
            fi
        )
        ;;

    clean)
        [ -d "$EXAMPLE_DIR" ] || exit 0
        echo "==> Cleaning run artifacts in $EXAMPLE_DIR"
        ( cd "$EXAMPLE_DIR" && rm -rf bb_out work results .nextflow* *.dot *.svg *.png )
        echo "==> Done (pipeline source preserved)"
        ;;

    *)
        echo "Unknown command: $CMD" >&2
        show_usage
        exit 1
        ;;
esac
