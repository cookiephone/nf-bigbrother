#!/usr/bin/env bash

set -euo pipefail

NXF_JAR="${NXF_JAR:-$HOME/.nextflow/framework/25.04.2/nextflow-25.04.2-one.jar}"
REPLACEMENT_COMMAND_TRACE="$(cd -- "$(dirname -- "$0")" && pwd)/custom-command-trace.txt"

BACKUP_JAR="${NXF_JAR}.bak"

if [[ ! -f "$BACKUP_JAR" ]]; then
  cp "$NXF_JAR" "$BACKUP_JAR"
  echo "Backup created at $BACKUP_JAR"
else
  echo "Backup already exists at $BACKUP_JAR; skipping backup."
fi

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

unzip -q "$NXF_JAR" -d "$TMPDIR"

cp "$REPLACEMENT_COMMAND_TRACE" "$TMPDIR/nextflow/executor/command-trace.txt"

pushd "$TMPDIR" > /dev/null
zip -qr "$NXF_JAR" .
popd > /dev/null

echo "Patched $NXF_JAR with new command-trace.txt"
