#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# tc-queue-job.sh — manually enqueue a file as a priority import job
#
# Writes a .job file into /queue/ in the exact format queue-import.sh produces
# from Sonarr/Radarr webhooks, so the job bridge picks it up on its next poll
# and treats it as a priority import (jumps the bulk queue).
#
# Intended for:
#   - Reproducing a specific file's failure after a worker/filter fix
#   - Retriggering encodes for files whose .job was consumed on a prior
#     terminal-path failure (quarantine, already_ok misclassification, etc.)
#   - Canarying a specific shape (e.g. hevc 1920x960 yuv420p10le) without
#     waiting for Sonarr/Radarr to emit a webhook
#
# Usage (inside container):
#   /scripts/tc-queue-job.sh <radarr|sonarr> <absolute-path>
#
# Usage (from host):
#   docker exec transcodarr /scripts/tc-queue-job.sh sonarr \
#     "/tv/TV22/Squid Game/Season 1/Squid Game - S01E01 - Red Light, Green Light WEBRip-1080p.mkv"
#
# Path handling:
#   The absolute path must be valid inside the container. Typical library
#   paths: /movies/... (radarr) or /tv/... (sonarr). The job bridge resolves
#   the physical /disk{N} mount from this path at dispatch time.
#
# Exit:
#   0 on success (job file written, stdout shows the path)
#   1 on usage error or unwritable queue dir
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

QUEUE_DIR="${QUEUE_DIR:-/queue}"

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") <radarr|sonarr> <absolute-path>

  Writes a priority .job file to $QUEUE_DIR/ that the transcodarr job
  bridge will pick up on its next poll (~2s) and treat as a priority
  import. File must exist at the given path inside the container.

Examples:
  $(basename "$0") radarr "/movies/Dune (2021)/Dune 2021.mkv"
  $(basename "$0") sonarr "/tv/TV22/Squid Game/Season 1/Squid Game - S01E01.mkv"
EOF
  exit 1
}

if [ $# -ne 2 ]; then
  usage
fi

SERVICE=$1
FILE=$2

case "$SERVICE" in
  radarr|sonarr) ;;
  *) echo "ERROR: service must be 'radarr' or 'sonarr', got '$SERVICE'" >&2; usage ;;
esac

if [ -z "$FILE" ] || [ "${FILE:0:1}" != "/" ]; then
  echo "ERROR: path must be absolute (start with /), got '$FILE'" >&2
  exit 1
fi

if [ ! -d "$QUEUE_DIR" ]; then
  echo "ERROR: queue dir '$QUEUE_DIR' not found or not a directory" >&2
  exit 1
fi

if [ ! -w "$QUEUE_DIR" ]; then
  echo "ERROR: queue dir '$QUEUE_DIR' is not writable" >&2
  exit 1
fi

if [ ! -e "$FILE" ]; then
  echo "WARN: file '$FILE' does not exist inside the container — the worker will fail at probe time" >&2
fi

# Match queue-import.sh naming: <md5(filepath)>_<ns_timestamp>.job
HASH=$(printf '%s' "$FILE" | md5sum | cut -d' ' -f1)
NS=$(date +%s%N)
JOB_PATH="$QUEUE_DIR/${HASH}_${NS}.job"

# Two-line body matching queue-import.sh's format
printf '%s\n%s\n' "$SERVICE" "$FILE" > "$JOB_PATH"

echo "Created: $JOB_PATH"
echo "  service: $SERVICE"
echo "  path:    $FILE"
echo
echo "Job bridge polls every 2s — the file will appear in the priority"
echo "queue shortly. Container must not be paused for processing to start."
