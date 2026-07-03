#!/bin/bash
# transcodarr-config.sh — Config layer (runs before entrypoint)
#
# 1. If config.json doesn't exist → seed from env vars
# 2. If config.json exists → read it, export as TRANSCODARR_* env vars
# 3. Snapshot boot config for GUI change detection
# 4. Run the entrypoint as a child (PID 1 stays here for clean restart)
#
# This script is the container entrypoint. Everything downstream
# (entrypoint, workers, queue builder) just reads env vars as usual.

set -euo pipefail

STATE_DIR="${TRANSCODARR_STATE_DIR:-/state}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONFIG_FILE="$STATE_DIR/config.json"
export CONFIG_FILE

log() { echo "[config] $(date '+%H:%M:%S') $1"; }

mkdir -p "$STATE_DIR"

# Step 1: Seed config.json from env vars if it doesn't exist
if [[ ! -f "$CONFIG_FILE" ]]; then
  perl "$SCRIPT_DIR/transcodarr-config-init.pl" \
    && log "Created config.json from environment defaults" \
    || { log "ERROR: Failed to create config.json"; exit 1; }
fi

# Step 2: Read config.json and export as env vars (overwrites compose values)
eval "$(perl "$SCRIPT_DIR/transcodarr-config-read.pl")" \
  || { log "ERROR: Failed to read config.json"; exit 1; }

# Step 3: Snapshot the boot config (GUI compares this vs current to detect changes)
cp -f "$CONFIG_FILE" "$STATE_DIR/config.boot.json"
log "Loaded settings from config.json"

# Step 4: Run the entrypoint as a child, forward signals for clean shutdown
bash "$SCRIPT_DIR/transcodarr-entrypoint.sh" &
CHILD=$!
trap "kill $CHILD 2>/dev/null; wait $CHILD 2>/dev/null; exit" SIGTERM SIGINT
wait $CHILD
