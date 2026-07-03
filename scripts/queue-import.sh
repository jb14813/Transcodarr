#!/bin/bash
# Thin custom script for Sonarr/Radarr.
# Drops a job file into the shared queue for the transcoder container.
# Returns immediately — the transcoder picks it up async.

set -euo pipefail

QUEUE_DIR="${TRANSCODARR_QUEUE_DIR:-/queue}"

if [ "${radarr_eventtype:-}" = "Test" ] || [ "${sonarr_eventtype:-}" = "Test" ]; then
  echo "[queue-import] Connection test OK"
  exit 0
fi

SERVICE=""
FILE=""

if [ "${radarr_eventtype:-}" = "Download" ] || [ "${radarr_eventtype:-}" = "Upgrade" ]; then
  SERVICE="radarr"
  FILE="${radarr_moviefile_path:-}"
elif [ "${sonarr_eventtype:-}" = "Download" ] || [ "${sonarr_eventtype:-}" = "Upgrade" ]; then
  SERVICE="sonarr"
  FILE="${sonarr_episodefile_path:-}"
else
  exit 0
fi

if [ -z "$FILE" ]; then
  exit 0
fi

if ! mkdir -p "$QUEUE_DIR" 2>/dev/null; then
  echo "[queue-import] ERROR: cannot create queue dir $QUEUE_DIR" >&2
  exit 1
fi

# Write to .tmp first, then atomic rename to .job
TMP="$QUEUE_DIR/.$(date +%s%N)-$$.tmp"
if ! printf '%s\n%s\n' "$SERVICE" "$FILE" > "$TMP"; then
  echo "[queue-import] ERROR: failed to write job file" >&2
  rm -f "$TMP"
  exit 1
fi

JOB="$QUEUE_DIR/$(echo -n "$FILE" | md5sum | cut -d' ' -f1)_$(date +%s%N).job"
if ! mv "$TMP" "$JOB"; then
  echo "[queue-import] ERROR: failed to rename job file" >&2
  rm -f "$TMP"
  exit 1
fi

echo "[queue-import] Queued $SERVICE: $FILE"
exit 0
