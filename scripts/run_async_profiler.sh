#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <pid> <output_dir> <timestamp_tag>"
  exit 1
fi

PID="$1"
OUTPUT_DIR="$2"
STAMP="$3"
ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-}"

if [[ -z "$ASYNC_PROFILER_HOME" ]]; then
  echo "ASYNC_PROFILER_HOME must be set to async-profiler installation path"
  exit 1
fi

PROFILER_SCRIPT="$ASYNC_PROFILER_HOME/profiler.sh"
if [[ ! -x "$PROFILER_SCRIPT" ]]; then
  echo "Expected executable profiler.sh at $PROFILER_SCRIPT (check ASYNC_PROFILER_HOME)"
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

"$PROFILER_SCRIPT" -d 30 -e cpu -f "$OUTPUT_DIR/cpu-${STAMP}.svg" "$PID"
"$PROFILER_SCRIPT" -d 30 -e lock -f "$OUTPUT_DIR/lock-${STAMP}.svg" "$PID"
"$PROFILER_SCRIPT" -d 30 -e alloc -f "$OUTPUT_DIR/alloc-${STAMP}.svg" "$PID"

echo "async-profiler artifacts generated in $OUTPUT_DIR"
