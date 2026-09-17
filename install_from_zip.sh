#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <plugin.zip>"
  exit 1
fi

ZIPFILE="$1"
DEST="$HOME/.nextflow/plugins"

PLUGIN_NAME="$(basename "$ZIPFILE" .zip)"
PLUGIN_DIR="$DEST/$PLUGIN_NAME"

if [[ -d "$PLUGIN_DIR" ]]; then
  echo "Removing existing plugin directory: $PLUGIN_DIR"
  rm -rf "$PLUGIN_DIR"
fi

mkdir -p "$PLUGIN_DIR"
unzip -o "$ZIPFILE" -d "$PLUGIN_DIR"

echo "Installed plugin into $PLUGIN_DIR"