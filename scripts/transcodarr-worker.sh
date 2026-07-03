#!/bin/bash
# transcodarr-worker.sh — Core transcode worker for Transcodarr.
# Processes ONE file using ffmpeg CUDA pipeline (GPU video re-encode)
# or ffmpeg copy mode (audio-only jobs where video is already H.264).
# All probing uses ffprobe (Phase 2 — HandBrake scan removed).
#
# Usage: transcodarr-worker.sh <service> <file> [event_type]
#   service    : "radarr" or "sonarr"
#   file       : absolute path inside container (e.g. /movies/Title/file.mkv)
#   event_type : "Bulk", "Import", "Test" (for logging, optional)
#
# Exit 0 on success, 1 on failure.

set -euo pipefail

# Source shared lib (probe_audio_streams, fingerprint, classify_file)
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/transcodarr-lib.sh"
source "$SCRIPT_DIR/transcodarr-codec-tables.sh"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STATE_DIR="${TRANSCODARR_STATE_DIR:-/state}"
TMP_DIR="${TRANSCODARR_TMP_DIR:-}"

# Video
TARGET_CODEC="${TRANSCODARR_TARGET_CODEC:-h264}"
HW_DECODING="${TRANSCODARR_HW_DECODING:-cuda}"

# Quality preset is the top-level user choice. When non-custom it
# overrides the individual quality fields below. PRESETS table is
# defined in transcodarr-codec-tables.sh sourced at line 19.
PRESET="${TRANSCODARR_PRESET:-balanced}"
# USER_PRESET preserves the user-facing preset name for logging/lookup
# after $PRESET is later overwritten with the vendor encoder preset
# (p5, medium, etc.) at the codec-table-resolution step (~L822).
USER_PRESET="$PRESET"

if [ "$PRESET" != "custom" ] && [ -n "${PRESETS[$PRESET:quality_tier]:-}" ]; then
  # Apply preset bundle — overwrite individual env-derived values.
  QUALITY_TIER="${PRESETS[$PRESET:quality_tier]}"
  ENCODER_SPEED="${PRESETS[$PRESET:encoder_speed]}"
  NVENC_EXTRAS_ENABLED="${PRESETS[$PRESET:nvenc_extras]}"
  QSV_EXTRAS_ENABLED="${PRESETS[$PRESET:qsv_extras]}"
  TRANSCODARR_AV1_FILM_GRAIN="${PRESETS[$PRESET:film_grain]}"
else
  # Custom: read individual fields from env (advanced mode).
  QUALITY_TIER="${TRANSCODARR_QUALITY_TIER:-excellent}"
  ENCODER_SPEED="${TRANSCODARR_ENCODER_SPEED:-medium}"
  NVENC_EXTRAS_ENABLED="${TRANSCODARR_NVENC_EXTRAS:-false}"
  QSV_EXTRAS_ENABLED="${TRANSCODARR_QSV_EXTRAS:-false}"
fi

# Audio
TARGET_AUDIO_CODEC="${TRANSCODARR_AUDIO_CODEC:-aac}"
# Map config codec name → ffmpeg encoder name. The defaults are wrong for
# anything where ffmpeg's native encoder differs from the libfoo encoder
# (libfdk_aac for aac quality; libopus is stable, the native opus encoder
# is experimental and demands -strict -2).
case "$TARGET_AUDIO_CODEC" in
  aac)  FFMPEG_AUDIO_ENCODER="libfdk_aac" ;;
  opus) FFMPEG_AUDIO_ENCODER="libopus" ;;
  *)    FFMPEG_AUDIO_ENCODER="$TARGET_AUDIO_CODEC" ;;
esac
AUDIO_LANG=$(normalize_audio_language_tag "${TRANSCODARR_AUDIO_LANG:-eng}")
MAX_CHANNELS="${TRANSCODARR_MAX_CHANNELS:-6}"
SUB_LANG=$(normalize_audio_language_tag "${TRANSCODARR_SUB_LANG:-${TRANSCODARR_AUDIO_LANG:-eng}}")

# Container & subtitles
OUTPUT_CONTAINER="${TRANSCODARR_OUTPUT_CONTAINER:-auto}"
SUBTITLE_MODE="${TRANSCODARR_SUBTITLE_MODE:-copy_matching}"

# Stderr logging
STDERR_LOGGING="${TRANSCODARR_STDERR_LOGGING:-true}"
FLAG_SHORT_RADARR_RUNTIME="${TRANSCODARR_FLAG_SHORT_RADARR_RUNTIME:-false}"

# Resolution
MAX_WIDTH="${TRANSCODARR_MAX_WIDTH:-1920}"
MAX_HEIGHT="${TRANSCODARR_MAX_HEIGHT:-1080}"

ITEM_ROUTE="${TRANSCODARR_ITEM_ROUTE:-bulk}"

PROCESSED_TSV="$STATE_DIR/processed.tsv"
FAILED_TSV="$STATE_DIR/failed-files.tsv"
QUARANTINE_DIR="$STATE_DIR/quarantine"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
JOB_TAG="${TRANSCODARR_JOB_TAG:-}"
log() { echo "[transcodarr] $SERVICE${JOB_TAG:+ [$JOB_TAG]} $*" >&2; }

cleanup() {
  if [[ -n "${TMP_OUT:-}" && -f "$TMP_OUT" ]]; then
    rm -f "$TMP_OUT"
    log "Cleaned up temp file"
  fi
  if [[ -n "${REPLACE_TMP:-}" && -f "$REPLACE_TMP" ]]; then
    rm -f "$REPLACE_TMP"
    log "Cleaned up replace temp file"
  fi
  # Release file lock if held
  [[ -n "${LOCK_FILE:-}" ]] && rm -rf "$LOCK_FILE" 2>/dev/null || true
  [[ -n "${RENAME_DEST_LOCK:-}" ]] && rm -rf "$RENAME_DEST_LOCK" 2>/dev/null || true
  # Clear worker phase key
  [[ -n "${PHASE_KEY:-}" ]] && $QUEUE_CLI DEL "$PHASE_KEY" > /dev/null 2>&1 || true
  # Release SSD reservation lease (only if dispatched through LB pipeline
  # with TMP_DIR enabled — TRANSCODARR_SSD_LEASE_KEY is empty otherwise).
  # The helper is idempotent: DEL on a missing key and SREM on a non-member
  # are both no-ops, so consumer-side releases that race with this one are
  # harmless. SIGKILL'd workers skip this entirely — the space_monitor
  # reconciliation sweep and lease-key TTL are the safety nets there.
  release_ssd_lease "${TRANSCODARR_SSD_LEASE_KEY:-}"
}
trap cleanup EXIT

record_processed() {
  local status="$1"
  # Phase-1 rename: callers may pass the renamed library path + the exact
  # output size. Default to today's behavior (stat $INPUT) when omitted.
  local record_path="${2:-$INPUT}"
  local out_size="${3:-$(stat -c%s "$record_path" 2>/dev/null || echo 0)}"
  local orig_size disk_name
  orig_size="${INPUT_SIZE_CHECK:-0}"
  disk_name=""
  if [[ -n "${TRANSCODARR_DISK_READ_PATH:-}" ]]; then
    disk_name=$(echo "$TRANSCODARR_DISK_READ_PATH" | grep -oP '^/\Kdisk\d+' || true)
  fi
  # Format: path, service, mode, vcodec, channels, orig_size, out_size, timestamp, disk
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$record_path" "$SERVICE" "$status" "${VIDEO_CODEC:-unknown}" "${SELECTED_CHANNELS:-0}" \
    "$orig_size" "$out_size" "$(date -Iseconds)" "${disk_name:-unknown}" \
    >> "$PROCESSED_TSV" 2>/dev/null || true
}

# ── Rename-on-convert (Phase 1) — place output, guard collisions, drop stale ──
# Places the encoded file at its final on-disk path (renaming the container
# extension when RENAME), removes the stale original, and records the on-disk
# path that was written in NEW_READ_PATH (the reliable path to stat for
# fingerprints — FINAL_DISK on direct-disk branches, the FUSE name otherwise).
# Exits non-zero (after record_failed) on a collision or a failed mv, BEFORE the
# success epilogue, so a non-replaced file is never recorded as a success.
#   $1 src    = the staged output to move (REPLACE_TMP or TMP_OUT)
#   $2 orig   = the original on-disk path for this branch (DISK_WRITE_PATH/INPUT)
#   $3 final  = the new-extension on-disk path for this branch (FINAL_DISK/FINAL_FUSE)
_place_output() {
  local src="$1" orig="$2" final="$3"
  local dest="$orig"
  if [ "$RENAME" = true ]; then
    dest="$final"
    local lock_dir lock_hash rename_lock
    lock_dir="${LOCK_DIR:-$STATE_DIR/locks}"
    lock_hash=$(printf '%s' "$FINAL_FUSE" | md5sum | cut -d' ' -f1)
    rename_lock="${lock_dir}/rename-${lock_hash}.lock"
    if ! mkdir -p "$lock_dir" 2>/dev/null; then
      log "ERROR: rename target lock directory unavailable: $lock_dir"
      record_failed "rename_lock_failed" "rename_failure"
      exit 1
    fi
    if ! mkdir "$rename_lock" 2>/dev/null; then
      log "ERROR: rename target already has an active writer, refusing to clobber: $FINAL_FUSE"
      record_failed "rename_collision" "rename_failure"
      exit 1
    fi
    RENAME_DEST_LOCK="$rename_lock"
    # Union-aware pre-existence collision guard: /movies & /tv are an shfs union
    # over /disk1..N, so a colliding <stem>.<OUTPUT_EXT> may live on either
    # namespace. Refuse to overwrite a DIFFERENT real file (-ef, not string cmp).
    if { [ -e "$FINAL_FUSE" ] || { [ -n "$FINAL_DISK" ] && [ -e "$FINAL_DISK" ]; }; } \
       && ! [ "$FINAL_FUSE" -ef "$INPUT" ]; then
      log "ERROR: rename target already exists (collision), refusing to clobber: $FINAL_FUSE"
      record_failed "rename_collision" "rename_failure"
      exit 1
    fi
  fi
  if mv -f -- "$src" "$dest"; then
    NEW_READ_PATH="$dest"
    if [ "$RENAME" = true ] && ! [ "$dest" -ef "$orig" ]; then
      rm -f -- "$orig"
      log "Renamed container .$INPUT_EXT -> .$OUTPUT_EXT: $orig -> $dest"
    fi
  else
    record_failed "rename_mv_failed" "rename_failure"
    exit 1
  fi
}

_copy_back_or_fail() {
  local src="$1" dest="$2"
  if cp -- "$src" "$dest"; then
    return 0
  fi
  log "ERROR: copy-back failed: $src -> $dest"
  record_failed "copy_back_failed" "output_failure"
  exit 1
}

# Robustly remove the OLD library path's stat-fingerprint rail rows — the broad
# (fully_classified) AND narrow (verified:aac_lc) rails — when a rename changes
# the file's identity. The *_remove_path helpers fast-return on a path-index
# SISMEMBER miss and only fall back to a locked-TSV scan when the index is
# :ready, so on a non-ready index a stale TSV row survives. Mirror
# release_admission: ensure the index is ready (rebuild with the CANONICAL token
# — see rebuild_cache_rails_for_rescan, lib.sh:1570-1572), then delegate; else
# remove from the locked TSV directly.
_rename_clear_old_rails() {
  local old_path="$1"
  [ -n "$old_path" ] || return 0
  local ready

  # Broad rail — canonical rebuild token is "verified:fully_classified".
  ready=$($QUEUE_CLI GET "tc:idx:fully_classified:ready" 2>/dev/null || echo "")
  if [ "$ready" != "1" ]; then
    rebuild_rail_index fully_classified "$(fully_classified_hashes_tsv_path)" "verified:fully_classified" || true
    ready=$($QUEUE_CLI GET "tc:idx:fully_classified:ready" 2>/dev/null || echo "")
  fi
  if [ "$ready" = "1" ]; then
    fully_classified_remove_path "$old_path" || true
  else
    _remove_fully_classified_path_locked_inner "$(fully_classified_hashes_tsv_path)" "$old_path" || true
  fi

  # Narrow rail — canonical rebuild token is "verified:aac_lc".
  ready=$($QUEUE_CLI GET "tc:idx:verified:ready" 2>/dev/null || echo "")
  if [ "$ready" != "1" ]; then
    rebuild_rail_index verified "$(verified_hashes_tsv_path)" "verified:aac_lc" || true
    ready=$($QUEUE_CLI GET "tc:idx:verified:ready" 2>/dev/null || echo "")
  fi
  if [ "$ready" = "1" ]; then
    verified_hash_remove_path "$old_path" || true
  else
    _remove_verified_hash_path_locked_inner "$(verified_hashes_tsv_path)" "$old_path" || true
  fi
}

record_failed() {
  local reason="$1"
  local failure_class="${2:-n/a}"
  local orig_size disk_name
  orig_size="${INPUT_SIZE_CHECK:-0}"
  disk_name=""
  if [[ -n "${TRANSCODARR_DISK_READ_PATH:-}" ]]; then
    disk_name=$(echo "$TRANSCODARR_DISK_READ_PATH" | grep -oP '^/\Kdisk\d+' || true)
  fi
  # Delegate the display row (col-4 replace, one-row-per-path) + the
  # content-hash gate row (col-1 replace) to the shared helper so the
  # classifier-time lang_detect path (Plan B) and the worker can't
  # drift. orig_size is bytes (INPUT_SIZE_CHECK = stat -c%s).
  record_failure_row "$INPUT" "$SERVICE" "$reason" "$failure_class" \
    "${VIDEO_CODEC:-unknown}" "${SELECTED_CHANNELS:-0}" "$orig_size" \
    "${disk_name:-unknown}" "${INPUT_READ:-$INPUT}"
  if [ -n "${TRANSCODARR_FAILURE_MARKER:-}" ]; then
    : > "$TRANSCODARR_FAILURE_MARKER" 2>/dev/null || true
  fi
}

quarantine() {
  local reason="$1"
  if [[ "$TEST_MODE" == "true" ]]; then
    log "DRY: would quarantine ($reason): $INPUT"
    record_failed "quarantine_dry: $reason" "quarantine"
    return 0
  fi
  local dest="$QUARANTINE_DIR/$SERVICE"
  mkdir -p "$dest"
  if mv -- "$INPUT" "$dest/" 2>/dev/null; then
    log "Quarantined: $reason"
    record_failed "quarantine: $reason" "quarantine"
  else
    log "ERROR: Failed to move file to quarantine ($reason): $INPUT"
    record_failed "quarantine_mv_failed: $reason" "quarantine_failed"
  fi
  # Remove any .job files for this path — otherwise they become orphans that
  # startup_job_bridge skips forever (file no longer at original path) and
  # accumulate in /queue without ever being cleaned up.
  cleanup_job_files_for_path "$INPUT"
  notify_arr_rescan
}

# ---------------------------------------------------------------------------
# Plex / Arr notifications — gated by ALL test modes
# ---------------------------------------------------------------------------
TEST_MODE="${TRANSCODARR_TEST_MODE:-false}"

_is_any_test_mode() {
  [[ "$TEST_MODE" == "true" ]] && return 0
  [[ "${TRANSCODARR_API_TEST_MODE:-false}" == "true" ]] && return 0
  [[ "${TRANSCODARR_SWEET16_TEST:-false}" == "true" ]] && return 0
  [[ "${TRANSCODARR_DEADHEAD_TEST:-false}" == "true" ]] && return 0
  [[ "${TRANSCODARR_ALMOSTHOME_TEST:-false}" == "true" ]] && return 0
  return 1
}

notify_plex() {
  if _is_any_test_mode; then
    log "DRY: would notify Plex (disabled for testing)"
    return 0
  fi
  # Phase-1 rename: optional target path (default $INPUT) so the epilogue can
  # point the refresh/analyze at the renamed file.
  local item_path="${1:-$INPUT}"
  local section_id=""
  case "$SERVICE" in
    radarr) section_id="${PLEX_MOVIE_SECTION_ID:-}" ;;
    sonarr) section_id="${PLEX_TV_SECTION_ID:-}"    ;;
  esac
  if [[ -n "${PLEX_URL:-}" && -n "${PLEX_TOKEN:-}" && -n "$section_id" ]]; then
    local plex_url="${PLEX_URL%/}"
    if ! integration_url_allowed_shell "$plex_url"; then
      log "WARN: Plex notification skipped; invalid PLEX_URL"
      return 0
    fi
    local plex_target_dir="$(dirname "$item_path")"
    local plex_path=""
    if ! plex_path=$(PLEX_MOVIE_PATH_ROOT="${PLEX_MOVIE_PATH_ROOT:-/movies}" PLEX_TV_PATH_ROOT="${PLEX_TV_PATH_ROOT:-/tv}" perl "$SCRIPT_DIR/transcodarr-plex-path.pl" map --service "$SERVICE" --path "$plex_target_dir" 2>&1); then
      log "WARN: Plex path mapping mismatch ($plex_path); skipping Plex refresh"
      return 0
    fi
    local sections_xml=""
    if ! sections_xml=$(curl -fsS --max-time 10 --get \
      --data-urlencode "X-Plex-Token=${PLEX_TOKEN}" \
      -- "${plex_url}/library/sections" 2>/dev/null); then
      log "WARN: Plex section locations unavailable; cannot prove path mapping"
      return 0
    fi
    if ! printf '%s' "$sections_xml" | perl "$SCRIPT_DIR/transcodarr-plex-path.pl" validate-section --section-id "$section_id" --path "$plex_path"; then
      log "WARN: Plex path mismatch for section $section_id ($plex_path); skipping Plex refresh"
      return 0
    fi
    curl -fsS --max-time 10 -o /dev/null \
      -X POST --get \
      --data-urlencode "path=${plex_path}" \
      --data-urlencode "X-Plex-Token=${PLEX_TOKEN}" \
      -- "${plex_url}/library/sections/${section_id}/refresh" \
      || log "WARN: Plex refresh failed for section $section_id"

    local plex_item_path=""
    if ! plex_item_path=$(PLEX_MOVIE_PATH_ROOT="${PLEX_MOVIE_PATH_ROOT:-/movies}" PLEX_TV_PATH_ROOT="${PLEX_TV_PATH_ROOT:-/tv}" perl "$SCRIPT_DIR/transcodarr-plex-path.pl" map --service "$SERVICE" --path "$item_path" 2>&1); then
      log "WARN: Plex item path mapping mismatch ($plex_item_path); skipping Plex analyze"
      return 0
    fi
    if ! printf '%s' "$sections_xml" | perl "$SCRIPT_DIR/transcodarr-plex-path.pl" validate-section --section-id "$section_id" --path "$plex_item_path"; then
      log "WARN: Plex item path mismatch for section $section_id ($plex_item_path); skipping Plex analyze"
      return 0
    fi
    local plex_item_type=""
    case "$SERVICE" in
      radarr) plex_item_type="1" ;;
      sonarr) plex_item_type="4" ;;
    esac
    local item_xml=""
    if ! item_xml=$(curl -fsS --max-time 10 --get \
      --data-urlencode "type=${plex_item_type}" \
      --data-urlencode "file=${plex_item_path}" \
      --data-urlencode "X-Plex-Container-Size=2" \
      --data-urlencode "X-Plex-Token=${PLEX_TOKEN}" \
      -- "${plex_url}/library/sections/${section_id}/all" 2>/dev/null); then
      log "WARN: Plex metadata lookup failed for $plex_item_path; skipping Plex analyze"
      return 0
    fi
    local plex_rating_key=""
    plex_rating_key=$(printf '%s' "$item_xml" | perl "$SCRIPT_DIR/transcodarr-plex-path.pl" rating-key --path "$plex_item_path" 2>/dev/null || true)
    if [[ -z "$plex_rating_key" ]]; then
      log "WARN: Plex metadata item not found for $plex_item_path; skipping Plex analyze"
      return 0
    fi
    curl -fsS --max-time 10 -o /dev/null \
      -X PUT \
      -H "X-Plex-Token: ${PLEX_TOKEN}" \
      -- "${plex_url}/library/metadata/${plex_rating_key}/analyze" \
      && log "Plex analyze triggered for metadata item $plex_rating_key" \
      || log "WARN: Plex analyze failed for metadata item $plex_rating_key"
  else
    if [[ -n "${PLEX_URL:-}" || -n "${PLEX_TOKEN:-}" ]]; then
      local missing=()
      [[ -z "${PLEX_URL:-}" ]] && missing+=("PLEX_URL")
      [[ -z "${PLEX_TOKEN:-}" ]] && missing+=("PLEX_TOKEN")
      [[ -z "$section_id" ]] && missing+=("section_id")
      log "WARN: Plex notification skipped for $SERVICE; missing ${missing[*]}"
    fi
  fi
}

# NOTE: This triggers a full library rescan rather than a per-file refresh.
# The arr v3 API supports per-movie/series rescan via "movieId"/"seriesId"
notify_arr_rescan() {
  if _is_any_test_mode; then
    log "DRY: would notify $SERVICE (disabled for testing)"
    return 0
  fi

  local url="" key="" cmd="" id_field=""
  case "$SERVICE" in
    radarr) url="${RADARR_URL:-}"; key="${RADARR_API_KEY:-}"; cmd="RescanMovie"; id_field="movieId" ;;
    sonarr) url="${SONARR_URL:-}"; key="${SONARR_API_KEY:-}"; cmd="RescanSeries"; id_field="seriesId" ;;
  esac
  url="${url%/}"
  if [[ -n "$url" && -n "$key" ]]; then
    if ! integration_url_allowed_shell "$url"; then
      log "WARN: ${SERVICE} rescan skipped; invalid integration URL"
      return 0
    fi
    local body="{\"name\":\"${cmd}\"}"
    if [[ -n "$ARR_ID" && "$ARR_ID" != "0" ]]; then
      body="{\"name\":\"${cmd}\",\"${id_field}\":${ARR_ID}}"
    fi
    curl -sf -o /dev/null \
      -X POST \
      -H "Content-Type: application/json" \
      -H "X-Api-Key: ${key}" \
      -d "$body" \
      -- "${url}/api/v3/command" \
      || log "WARN: ${SERVICE} rescan failed"
  fi
}

# Single grep-able decision summary printed before each ffmpeg invocation.
# All key fields in one line so a `grep "PLAN" /var/log/...` reconstructs
# what the worker decided per file. Format is intentionally compact:
#   PLAN[mode] src=codec/WxH/pix/trc container=src→out video=enc/cq=N/preset=P
#       tonemap=... deinterlace=... tune=... audio=...
log_encode_plan() {
  local plan="PLAN[$MODE]"
  plan+=" src=$SOURCE_VCODEC/${VIDEO_WIDTH}x${VIDEO_HEIGHT}"
  [ -n "${SRC_PIX_FMT:-}" ] && plan+="/$SRC_PIX_FMT"
  if [ -n "${SRC_COLOR_TRANSFER:-}" ] && [ "$SRC_COLOR_TRANSFER" != "unknown" ]; then
    plan+="/trc=$SRC_COLOR_TRANSFER"
  fi
  plan+=" container=${INPUT_EXT:-?}→${OUTPUT_EXT:-?}"

  if [ "$MODE" = "gpu" ]; then
    plan+=" user_preset=${USER_PRESET:-?}/tier=${QUALITY_TIER:-?}/speed=${ENCODER_SPEED:-?}"
    plan+=" video=$ENCODER/cq=$CQ/preset=$PRESET/profile=$PROFILE/pix=$PIXFMT"
    # HDR handling — only show when source is HDR (otherwise noise).
    if [ "${IS_HDR:-false}" = "true" ]; then
      plan+=" hdr_mode=${HDR_HANDLING:-auto}"
      if [ "${NEED_TONEMAP:-false}" = "true" ]; then
        # Honor the startup-probed tonemap path. CPU branch logs as
        # mobius (the actual operator) — not the stale "hable" label.
        local _plan_hdr_path="${TRANSCODARR_HDR_TONEMAP_PATH:-libplacebo}"
        case "${FILTER_HW_DECODING:-$HW_DECODING}" in
          cuda|qsv)
            case "$_plan_hdr_path" in
              libplacebo) plan+=" tonemap=libplacebo(spline→bt709)" ;;
              opencl)     plan+=" tonemap=tonemap_opencl(mobius→bt709)" ;;
              *)          plan+=" tonemap=zscale+mobius→bt709(hwdownload)" ;;
            esac
            ;;
          *)
            plan+=" tonemap=zscale+mobius→bt709"
            ;;
        esac
      fi
      [ "${PRESERVE_HDR:-false}" = "true" ] && plan+=" hdr=preserve"
    fi
    [ "${IS_INTERLACED:-false}" = "true" ] && plan+=" deinterlace=yes(${SRC_FIELD_ORDER})"
    [ "${CUDA_SOFTWARE_FILTERS:-false}" = "true" ] && plan+=" sw_filter_fallback=on"
    [ "${IS_ANIMATION:-false}" = "true" ] && plan+=" tune=animation"
    [[ "${NVENC_EXTRAS_ENABLED:-}" = "true" && "${HW_DECODING:-}" = "cuda" ]] && plan+=" nvenc_extras=on"
    [[ "${QSV_EXTRAS_ENABLED:-}" = "true" && "${HW_DECODING:-}" = "qsv" ]] && plan+=" qsv_extras=on"
  elif [ "$MODE" = "audio_only" ]; then
    plan+=" video=copy"
  fi

  if [ "${AUDIO_ENC_FLAGS[1]:-}" = "copy" ]; then
    plan+=" audio=copy[$SRC_AUDIO_CODEC/${SELECTED_CHANNELS}ch]"
  else
    plan+=" audio=$TARGET_AUDIO_CODEC/${EFFECTIVE_CHANNELS}ch@${AUDIO_BR}"
    [ "$SRC_AUDIO_CODEC" != "$TARGET_AUDIO_CODEC" ] && plan+="(from:$SRC_AUDIO_CODEC)"
    (( SELECTED_CHANNELS != EFFECTIVE_CHANNELS )) && plan+="(downmix:${SELECTED_CHANNELS}ch→${EFFECTIVE_CHANNELS}ch)"
  fi

  plan+=" subs=$SUBTITLE_MODE"
  log "$plan"
}

# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------
SERVICE="${1:-}"
INPUT="${2:-}"
EVENT_TYPE="${3:-Import}"
ARR_ID="${4:-}"

# Direct disk read path (bypasses FUSE if set by entrypoint)
INPUT_READ="${TRANSCODARR_DISK_READ_PATH:-$INPUT}"

if [[ -z "$SERVICE" || -z "$INPUT" ]]; then
  echo "Usage: transcodarr-worker.sh <service> <file> [event_type]" >&2
  exit 1
fi

if [[ "$SERVICE" != "radarr" && "$SERVICE" != "sonarr" ]]; then
  echo "Error: service must be 'radarr' or 'sonarr'" >&2
  exit 1
fi

# Skip temp files from in-progress transcodes
if [[ "$INPUT" == *.transcode.tmp.* ]]; then
  log "Skipping temp file: $INPUT"
  exit 0
fi

log "=== Worker start ($EVENT_TYPE) ==="
log "File: $INPUT"

# ---------------------------------------------------------------------------
# Ensure state directories exist
# ---------------------------------------------------------------------------
mkdir -p "$STATE_DIR" "$QUARANTINE_DIR"

# ---------------------------------------------------------------------------
# Step 1 — File lock (prevent two workers on the same file)
# ---------------------------------------------------------------------------
LOCK_DIR="$STATE_DIR/locks"
mkdir -p "$LOCK_DIR"
INPUT_HASH=$(echo -n "$INPUT" | md5sum 2>/dev/null | cut -d' ' -f1 || echo "nohash_$$")
_LOCK_CANDIDATE="$LOCK_DIR/$INPUT_HASH.lock"

if ! mkdir "$_LOCK_CANDIDATE" 2>/dev/null; then
  log "Skipping: another worker is processing $INPUT"
  exit 0
fi
# Only set LOCK_FILE after we own the lock — cleanup trap checks this variable
LOCK_FILE="$_LOCK_CANDIDATE"

if [[ ! -f "$INPUT_READ" ]]; then
  log "ERROR: File not found: $INPUT"
  record_failed "not_found" "input_missing"
  exit 1
fi

INPUT_SIZE_CHECK=$(stat -c%s "$INPUT_READ" 2>/dev/null || echo 0)
if [[ "$INPUT_SIZE_CHECK" -eq 0 ]]; then
  log "ERROR: File is 0 bytes: $INPUT"
  record_failed "zero_byte" "input_invalid"
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 2 — Quick integrity check via ffprobe
# ---------------------------------------------------------------------------
log "Scanning..."
probe_rc=0
probe_result=$(ffprobe -v error -show_entries format=duration \
  -of default=noprint_wrappers=1:nokey=1 "$INPUT_READ" 2>/dev/null) || probe_rc=$?
if (( probe_rc != 0 )) || [ -z "$probe_result" ]; then
  log "ERROR: Scan failed — file is corrupt or unreadable"
  quarantine "scan_failed"
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 3 — Determine work needed (ffprobe-based)
# ---------------------------------------------------------------------------

# Video codec
VIDEO_CODEC=$(ffprobe -v quiet -select_streams v:0 -show_entries stream=codec_name \
  -of default=noprint_wrappers=1:nokey=1 "$INPUT_READ" 2>/dev/null | sed -n '1p' || echo "unknown")
VIDEO_CODEC=$(echo "$VIDEO_CODEC" | tr -d '[:space:]')
if [ -z "$VIDEO_CODEC" ] || [ "$VIDEO_CODEC" = "" ]; then
  log "ERROR: Scan failed — no video stream found"
  quarantine "scan_failed"
  exit 1
fi

# Video dimensions
read -r VIDEO_WIDTH VIDEO_HEIGHT < <(probe_video_dimensions "$INPUT_READ")
VIDEO_WIDTH="${VIDEO_WIDTH:-0}"
VIDEO_HEIGHT="${VIDEO_HEIGHT:-0}"

# Duration — ffprobe can return "N/A" for some containers, guard against it
ORIG_DURATION=$(ffprobe -v quiet -show_entries format=duration \
  -of default=noprint_wrappers=1:nokey=1 "$INPUT_READ" 2>/dev/null || echo 0)
case "$ORIG_DURATION" in *[!0-9.]*|"") ORIG_DURATION=0 ;; esac
ORIG_DURATION=$(printf '%.0f' "$ORIG_DURATION")

# Color metadata + field order — single probe pass. Used to drive HDR
# tone-mapping, deinterlacing, and explicit -color_* tagging on output.
# References:
#  - HDR detection: smpte2084 (PQ) / arib-std-b67 (HLG) per ffmpeg color
#    transfer characteristics; see codec-database-addendum.md context +
#    HandBrake colorspace.c.
#  - Deinterlace detection: ffprobe field_order ∈ {tt,bb,tb,bt} =
#    interlaced; progressive / unknown / "" = treat as progressive
#    (false-positive deinterlace is worse than false-negative per
#    Phase A research).
SRC_PROBE=$(ffprobe -v quiet -select_streams v:0 \
  -show_entries stream=color_transfer,color_primaries,color_space,pix_fmt,field_order \
  -of default=noprint_wrappers=1 "$INPUT_READ" 2>/dev/null || true)
SRC_COLOR_TRANSFER=$(echo "$SRC_PROBE" | grep -m1 -oP '^color_transfer=\K.*' | tr -d '[:space:]')
SRC_COLOR_PRIMARIES=$(echo "$SRC_PROBE" | grep -m1 -oP '^color_primaries=\K.*' | tr -d '[:space:]')
SRC_COLOR_SPACE=$(echo "$SRC_PROBE" | grep -m1 -oP '^color_space=\K.*' | tr -d '[:space:]')
SRC_PIX_FMT=$(echo "$SRC_PROBE" | grep -m1 -oP '^pix_fmt=\K.*' | tr -d '[:space:]')
SRC_FIELD_ORDER=$(echo "$SRC_PROBE" | grep -m1 -oP '^field_order=\K.*' | tr -d '[:space:]')

# HDR detection — primary signal is color_transfer. Mis-tag guard:
# 8-bit + bt709 transfer = SDR even if other fields suggest HDR
# (some files are mis-tagged BT.2020 primaries on 8-bit yuv420p).
IS_HDR=false
case "$SRC_COLOR_TRANSFER" in
  smpte2084|arib-std-b67) IS_HDR=true ;;
esac

# Interlaced detection — only field_order based for v1; idet probe is
# expensive and unreliable on short samples. Unknown/empty = treat as
# progressive.
IS_INTERLACED=false
case "$SRC_FIELD_ORDER" in
  tt|bb|tb|bt) IS_INTERLACED=true ;;
esac

# Worker-side media flag emission was deleted in Phase 7-followup.
# All media-property flags (interlaced, unusual_pix_fmt,
# low_bitrate_suspect, short_radarr_runtime, unverified_lang) are now
# emitted by ffprobe_worker post-classify via compute_flags_for_file +
# set_path_flags against the Valkey tc:flags:by_path hash. The classifier
# already has every value the worker used; emitting from both was
# duplicate state with no single source of truth.

# Animation heuristic — path-based. Only triggers -tune animation for
# software libx264/libx265 encoders. Catches /Anime/, /Animation/,
# /Cartoons/ in directory names.
IS_ANIMATION=false
if echo "$INPUT" | grep -iqE '/(Anime|Animation|Cartoons?)/'; then
  IS_ANIMATION=true
fi

# ---------------------------------------------------------------------------
# Audio stream probing and track selection (via probe_audio_streams from lib)
# ---------------------------------------------------------------------------

# Read probe_audio_streams output into arrays.
# Each line uses ASCII Unit Separator (0x1F) so empty title/handler fields
# do not collapse and shift codec data into the wrong slot.
AUDIO_ORDINALS=()
AUDIO_LANGS=()
AUDIO_CHANNELS_ARR=()
AUDIO_TITLES=()
AUDIO_CODECS=()
AUDIO_HANDLERS=()
AUDIO_DISPS=()

while IFS=$'\037' read -r ordinal lang channels title codec handler disp; do
  [ -z "$ordinal" ] && continue
  AUDIO_ORDINALS+=("$ordinal")
  AUDIO_LANGS+=("$lang")
  AUDIO_CHANNELS_ARR+=("$channels")
  AUDIO_TITLES+=("$title")
  AUDIO_CODECS+=("$codec")
  AUDIO_HANDLERS+=("$handler")
  AUDIO_DISPS+=("$disp")
done < <(probe_audio_streams "$INPUT_READ")

AUDIO_STREAMS=${#AUDIO_ORDINALS[@]}

# Sanity check: file with no audio at all
if (( AUDIO_STREAMS < 1 )); then
  log "WARN: File has no audio streams — flagging for review"
  record_failed "no_audio_streams" "input_invalid"
  exit 1
fi

# Select best audio track (AUDIO_TRACK is 1-based for compatibility with
# the existing -map "0:a:$((AUDIO_TRACK - 1))" pattern in both GPU and
# audio-only encode paths).
#
# Priority:
#   1. First preferred-language non-commentary track
#   2. Single track with missing/unknown language → use, flag unverified_lang
#   3. Single track explicitly wrong language → FAIL (wrong_lang_<code>)
#   4. Multiple tracks, none preferred-language → FAIL
AUDIO_TRACK=1

LANG_MATCH=0      # first track matching preferred language + not commentary

for i in "${!AUDIO_ORDINALS[@]}"; do
  track_num=$((i + 1))
  lang="${AUDIO_LANGS[$i]:-und}"
  lang_norm=$(normalize_audio_language_tag "$lang")
  disp="${AUDIO_DISPS[$i]:-}"
  is_commentary=false

  if is_commentary_track "${AUDIO_TITLES[$i]:-}" "${AUDIO_HANDLERS[$i]:-}" "$disp"; then
    is_commentary=true
  fi

  # Check language match (preferred language, non-commentary)
  lang_match=false
  if [[ "$lang_norm" == "$AUDIO_LANG" ]]; then
    lang_match=true
  fi
  if [[ "$lang_match" == true ]] && [[ "$is_commentary" == false ]] && (( LANG_MATCH == 0 )); then
    LANG_MATCH=$track_num
  fi
done

# Priority: lang+clean > sole track (flagged) > skip
# Commentary-only English is not usable — treat as missing
if (( LANG_MATCH > 0 )); then
  AUDIO_TRACK=$LANG_MATCH
elif (( AUDIO_STREAMS == 1 )); then
  # Check if sole track is commentary — not usable even if language matches
  sole_disp="${AUDIO_DISPS[0]:-}"
  sole_is_commentary=false
  is_commentary_track "${AUDIO_TITLES[0]:-}" "${AUDIO_HANDLERS[0]:-}" "$sole_disp" && sole_is_commentary=true
  if [[ "$sole_is_commentary" == true ]]; then
    log "ERROR: single audio track is commentary — skipping for re-download"
    record_failed "commentary_only" "policy_skip"
    exit 1
  fi

  sole_lang="${AUDIO_LANGS[0]:-}"
  sole_lang_norm=$(normalize_audio_language_tag "$sole_lang")
  if [[ "$sole_lang_norm" == "und" ]] && [[ -n "$sole_lang" ]] && [[ "${sole_lang,,}" != "und" ]] && [[ "${sole_lang,,}" != "unknown" ]]; then
    log "WARN: sole audio track has non-language tag '$sole_lang' — treating as unknown"
  fi
  # Check if sole track matches preferred language.
  sole_lang_ok=false
  case "$sole_lang_norm" in
    ""|und|unknown) sole_lang_ok=true ;;  # untagged — use and flag
    *)
      if [[ "$sole_lang_norm" == "$AUDIO_LANG" ]]; then
        sole_lang_ok=true
      fi
      ;;
  esac
  if [[ "$sole_lang_ok" == false ]]; then
    # Explicitly tagged as wrong language — fail
    log "ERROR: single audio track is '$sole_lang_norm', not $AUDIO_LANG — skipping for re-download"
    record_failed "wrong_lang_${sole_lang_norm}" "policy_skip"
    exit 1
  fi
  AUDIO_TRACK=1
  # Phase 7-followup: unverified_lang flag emission moved to
  # ffprobe_worker (classify-time, current-state index). The worker
  # still WARNs on missing tags so the operator sees it in logs, but
  # does not write to flagged-files.tsv directly.
  case "$sole_lang_norm" in
    ""|und|unknown)
      log "WARN: single audio track with no $AUDIO_LANG tag — using it (may not be $AUDIO_LANG)"
      ;;
  esac
else
  log "ERROR: $AUDIO_STREAMS audio tracks but none are $AUDIO_LANG — skipping for re-download"
  record_failed "no_${AUDIO_LANG}_audio" "policy_skip"
  exit 1
fi

if (( AUDIO_TRACK != 1 )); then
  log "Selected audio track $AUDIO_TRACK (lang=$AUDIO_LANG, skipped non-matching/commentary)"
fi

# Sanity: clamp track number to actual stream count
if (( AUDIO_TRACK > AUDIO_STREAMS )); then
  log "WARN: selected track $AUDIO_TRACK exceeds stream count $AUDIO_STREAMS, falling back to 1"
  AUDIO_TRACK=1
fi

# Get channel count for the SELECTED audio track (1-based index → 0-based array)
SELECTED_CHANNELS="${AUDIO_CHANNELS_ARR[$((AUDIO_TRACK - 1))]:-0}"
if [ -z "$SELECTED_CHANNELS" ] || [ "$SELECTED_CHANNELS" = "0" ]; then
  SELECTED_CHANNELS="${AUDIO_CHANNELS_ARR[0]:-0}"  # fallback to first track
fi

# Get codec for the selected audio track before logging/decision making
SELECTED_CODEC="${AUDIO_CODECS[$((AUDIO_TRACK - 1))]:-unknown}"
SELECTED_CODEC_LOWER=$(echo "$SELECTED_CODEC" | tr '[:upper:]' '[:lower:]')
SELECTED_PROFILE=$(ffprobe -v quiet -select_streams "a:$((AUDIO_TRACK - 1))" \
  -show_entries stream=profile -of default=nokey=1:noprint_wrappers=1 \
  "$INPUT_READ" 2>/dev/null | head -1 || true)

log "Detected: video=$VIDEO_CODEC ${VIDEO_WIDTH}x${VIDEO_HEIGHT} audio=$SELECTED_CODEC ${SELECTED_CHANNELS}ch (track $AUDIO_TRACK of $AUDIO_STREAMS) duration=${ORIG_DURATION}s"

NEEDS_VIDEO=false
NEEDS_AUDIO=false
MODE=""

# Normalize source video codec name
VIDEO_CODEC_LOWER=$(echo "$VIDEO_CODEC" | tr '[:upper:]' '[:lower:]')
case "$VIDEO_CODEC_LOWER" in
  h264|x264|avc|h.264) SOURCE_VCODEC="h264" ;;
  hevc|h265|h.265)     SOURCE_VCODEC="hevc" ;;
  av1)                 SOURCE_VCODEC="av1" ;;
  *)                   SOURCE_VCODEC="other" ;;
esac

# Video needs re-encoding if source doesn't match target codec
if [[ "$SOURCE_VCODEC" != "$TARGET_CODEC" ]]; then
  NEEDS_VIDEO=true
fi

# Resolution check: anything above max dimensions needs scaling down
if (( VIDEO_WIDTH > MAX_WIDTH )) || (( VIDEO_HEIGHT > MAX_HEIGHT )); then
  NEEDS_VIDEO=true
  log "Resolution ${VIDEO_WIDTH}x${VIDEO_HEIGHT} exceeds max ${MAX_WIDTH}x${MAX_HEIGHT} — will scale down"
fi

# Audio compatibility check: selected track must be safe to passthrough.
if ! audio_passthrough_ok "$TARGET_AUDIO_CODEC" "$SELECTED_CODEC_LOWER" "$SELECTED_PROFILE"; then
  NEEDS_AUDIO=true
fi

# Audio channel check: use SELECTED track's channel count for downmix decision
if (( SELECTED_CHANNELS > MAX_CHANNELS )) || (( AUDIO_STREAMS > 1 )); then
  NEEDS_AUDIO=true
fi

if $NEEDS_VIDEO; then
  MODE="gpu"
elif $NEEDS_AUDIO; then
  MODE="audio_only"
else
  MODE="ok"
fi

log "Decision: mode=$MODE (needs_video=$NEEDS_VIDEO needs_audio=$NEEDS_AUDIO target=$TARGET_CODEC source=$SOURCE_VCODEC)"

# ---------------------------------------------------------------------------
# Already at spec — nothing to do
# ---------------------------------------------------------------------------
if [[ "$MODE" == "ok" ]]; then
  log "File already meets spec"
  # Clean up any .job files — otherwise a priority import that turns out to be
  # already-at-spec re-ingests on every restart and hogs priority slots forever.
  cleanup_job_files_for_path "$INPUT"
  failed_display_remove_path "$INPUT" || true
  failed_hash_remove_path "$INPUT" || true
  record_processed "already_ok"
  # Phase 4: the worker's MODE=ok decision is itself a "fully classified
  # under current policy" verdict. Record so Direct Queue submissions
  # (which bypass admission and reach the worker) populate the broad
  # cache for future short-circuit. fully_classified_record dedupes by
  # (path, hash) so a no-op re-recording is harmless.
  fully_classified_record "$INPUT" "$INPUT"
  notify_plex
  exit 0
fi

# ---------------------------------------------------------------------------
# Determine container format and build temp path
# ---------------------------------------------------------------------------
EXT="${INPUT##*.}"
EXT_LOWER=$(echo "$EXT" | tr '[:upper:]' '[:lower:]')

# Container remap: exotic extensions → safe output container
INPUT_EXT="$EXT_LOWER"
OUTPUT_EXT="${CONTAINER_REMAP[$EXT_LOWER]:-$EXT_LOWER}"
if [ "$OUTPUT_CONTAINER" = "mkv" ] || [ "$OUTPUT_CONTAINER" = "mp4" ]; then
  OUTPUT_EXT="$OUTPUT_CONTAINER"
fi
if [ "$OUTPUT_EXT" != "$INPUT_EXT" ]; then
  log "Container remap: .$INPUT_EXT -> .$OUTPUT_EXT"
fi

# ── Rename-on-convert (Phase 1) — derive final destinations + state key ──────
# RENAME only when the container actually changes AND the basename carries an
# extension (a truly extensionless basename is replaced in place, never renamed).
RENAME=false
if [ "$OUTPUT_EXT" != "$INPUT_EXT" ] && [[ "$(basename -- "$INPUT")" == *.* ]]; then
  RENAME=true
fi
# Derive the stem from the BASENAME (not ${INPUT%.*} over the whole path, which
# would strip at a dot in a parent directory and write outside the title folder).
_in_dir=$(dirname -- "$INPUT"); _in_base=$(basename -- "$INPUT")
FINAL_FUSE="${_in_dir}/${_in_base%.*}.${OUTPUT_EXT}"
# Direct-disk env is optional; under set -u derive FINAL_DISK only when present.
FINAL_DISK=""
if [ -n "${TRANSCODARR_DISK_WRITE_PATH:-}" ]; then
  _dw_dir=$(dirname -- "$TRANSCODARR_DISK_WRITE_PATH"); _dw_base=$(basename -- "$TRANSCODARR_DISK_WRITE_PATH")
  FINAL_DISK="${_dw_dir}/${_dw_base%.*}.${OUTPUT_EXT}"
fi
# Stored library path for state + notify; NEW_READ_PATH (the stat path) is set by
# _place_output to the on-disk dest. Default both to the no-rename case.
if [ "$RENAME" = true ]; then NEW_PATH="$FINAL_FUSE"; else NEW_PATH="$INPUT"; fi
NEW_READ_PATH="$NEW_PATH"

# Container-specific muxer flags — applied at end of every ffmpeg invocation
# right before the output path. Always begin with `-f <muxer>` so ffmpeg
# doesn't infer the container from the output filename's extension —
# critical because TMP_OUT below intentionally has no trailing extension
# (Sonarr/Radarr library scanners match on extension, even for dotfiles,
# so a `.foo.tmp.PID.mkv` gets picked up mid-encode and confuses the
# *arr's library state). MP4 set is HandBrake's production default
# (codec-database-addendum.md:122-129). The wildcard branch fails loud
# instead of silently producing an extensionless mystery file — the
# container remap table in scripts/transcodarr-codec-tables.sh sends
# every container we accept to either mkv or mp4, so any other value
# is a programming error worth catching here.
case "$OUTPUT_EXT" in
  mkv) MUXER_FLAGS=( -f matroska ) ;;
  mp4) MUXER_FLAGS=( -f mp4 "${MP4_MUXER_FLAGS[@]}" ) ;;
  *)   log "ERROR: unsupported output container for extensionless tmp: .$OUTPUT_EXT"
       exit 1 ;;
esac

# Timestamp-repair input flags for problem containers:
#   +genpts                       generates missing PTS from DTS
#   -avoid_negative_ts make_zero  fixes non-monotonic / negative DTS
# Research (codec-database-addendum.md:21,173) says these address SEPARATE
# failure modes — +genpts alone won't fix non-monotonic DTS, and vice
# versa. For all GENPTS_CONTAINERS we apply both since neither hurts a
# clean file.
GENPTS_FLAGS=()
if [[ -n "${GENPTS_CONTAINERS[$INPUT_EXT]+x}" ]]; then
  GENPTS_FLAGS=(-fflags +genpts -avoid_negative_ts make_zero)
fi

# Shared map/metadata/disposition flags applied to every ffmpeg invocation
# (GPU attempt, GPU retry, audio_only attempt, audio_only retry).
#
#   -map_metadata 0   — copy global container metadata (title/date/genre)
#   -map_chapters 0   — copy chapters; NOT default when -map filters streams
#   -disposition:a:0 default — flag the selected audio as default so Plex's
#                              "default audio" logic picks the right stream
#                              (otherwise disposition is inherited from the
#                              source's Nth stream, which may not have it)
#   -map 0:t?         — copy MKV font/chapter attachments into MKV output
#                       ONLY (MP4 doesn't support attachments). ASS/SSA
#                       subtitles with custom fonts render with fallback
#                       in Plex without these.
#   -metadata encoder= / comment= / description=  — scrub stale attribution
#                       that -map_metadata 0 would otherwise carry from
#                       HandBrake / MakeMKV source files.
MAP_EXTRA_FLAGS=(-map_chapters 0 -disposition:a:0 default)
if [ "$OUTPUT_EXT" = "mkv" ]; then
  MAP_EXTRA_FLAGS+=(-map '0:t?')
fi
MAP_EXTRA_FLAGS+=(-metadata encoder= -metadata comment= -metadata description=)

if [ -n "$TMP_DIR" ]; then
  mkdir -p "$TMP_DIR"
  TMP_OUT="${TMP_DIR}/$(basename "${INPUT}").transcode.tmp.$$"
elif [ -n "${TRANSCODARR_DISK_WRITE_PATH:-}" ]; then
  # No TMP_DIR — write directly to the same physical disk as the source.
  # Use DISK_WRITE_PATH (direct disk mount) not INPUT (FUSE) so the
  # tmp file and original are on the same filesystem for atomic mv.
  # Dot prefix + NO trailing media extension keeps Sonarr/Radarr library
  # scanners from picking the file up mid-encode (their extension match
  # is dotfile-tolerant, so the trailing `.mkv` was the actual leak).
  # ffmpeg's container is forced via the `-f <muxer>` we pre-loaded into
  # MUXER_FLAGS above, not inferred from the filename.
  TMP_OUT="$(dirname "$TRANSCODARR_DISK_WRITE_PATH")/.$(basename "$TRANSCODARR_DISK_WRITE_PATH").transcode.tmp.$$"
else
  TMP_OUT="$(dirname "$INPUT")/.$(basename "$INPUT").transcode.tmp.$$"
fi

# Compute EFFECTIVE_CHANNELS: the actual output channel count after
# applying BOTH the user's MAX_CHANNELS cap and the codec's hard limit.
# Downmix layout, bitrate key, and Opus mapping_family must all use this —
# otherwise (a) a 7.1 source → 5.1 library tier gets the 7.1 bitrate, or
# (b) an AC3 selection with 7.1 source tries to encode 7.1 via a 5.1-max
# codec and fails noisily. Worker cap is defence-in-depth; GUI also
# restricts max_channels per codec.
CODEC_MAX_CH="${AUDIO_CODEC_MAX_CHANNELS[$TARGET_AUDIO_CODEC]:-8}"
REQUESTED_CH=$(( SELECTED_CHANNELS > MAX_CHANNELS ? MAX_CHANNELS : SELECTED_CHANNELS ))
EFFECTIVE_CHANNELS=$(( REQUESTED_CH > CODEC_MAX_CH ? CODEC_MAX_CH : REQUESTED_CH ))
if (( EFFECTIVE_CHANNELS != REQUESTED_CH )); then
  log "WARN: $TARGET_AUDIO_CODEC supports max $CODEC_MAX_CH channels; capping from $REQUESTED_CH"
fi

# Emit -ac + -ch_layout when the output differs from the source.
# Note: -ch_layout replaced -channel_layout in ffmpeg 5.1+; old name is a
# deprecated alias that still works but emits a warning and is scheduled
# for removal in 8.x.
FFMPEG_AC_FLAG=()
if (( EFFECTIVE_CHANNELS != SELECTED_CHANNELS )); then
  case "$EFFECTIVE_CHANNELS" in
    1) _layout="mono" ;;
    2) _layout="stereo" ;;
    6) _layout="5.1" ;;
    8) _layout="7.1" ;;
    *) _layout="" ;;
  esac
  if [ -n "$_layout" ]; then
    FFMPEG_AC_FLAG=(-ac "$EFFECTIVE_CHANNELS" -ch_layout "$_layout")
  else
    FFMPEG_AC_FLAG=(-ac "$EFFECTIVE_CHANNELS")
  fi
fi

# libopus needs -mapping_family 1 for >2 channels (per RFC 7845); without it
# surround output fails silently or mis-routes to mono.
if [ "$FFMPEG_AUDIO_ENCODER" = "libopus" ] && (( EFFECTIVE_CHANNELS > 2 )); then
  FFMPEG_AC_FLAG+=(-mapping_family 1)
fi

# Per-channel audio bitrate resolution — key on OUTPUT channels so a
# 7.1→5.1 downmix library gets the 5.1 bitrate, not the 7.1 bitrate.
if   (( EFFECTIVE_CHANNELS <= 1 )); then CH_KEY="mono"
elif (( EFFECTIVE_CHANNELS <= 2 )); then CH_KEY="stereo"
elif (( EFFECTIVE_CHANNELS <= 6 )); then CH_KEY="surround_51"
else                                     CH_KEY="surround_71"; fi

# Audio bitrate lookup chain. When a preset is active (non-custom), the
# preset's table value wins — user overrides are ignored to match the GUI's
# preset-locked behavior. In custom mode the user's override (if set) wins,
# falling back to balanced preset's value.
#   custom     : user override → balanced preset value → 192k floor
#   non-custom : preset value  → balanced preset value → 192k floor
OVERRIDE_VAR="TRANSCODARR_AUDIO_BITRATE_${CH_KEY^^}"
if [ "$USER_PRESET" = "custom" ]; then
  AUDIO_BR="${!OVERRIDE_VAR:-${AUDIO_BITRATE_DEFAULTS[balanced:$TARGET_AUDIO_CODEC:$CH_KEY]:-}}"
else
  AUDIO_BR="${AUDIO_BITRATE_DEFAULTS[$USER_PRESET:$TARGET_AUDIO_CODEC:$CH_KEY]:-${AUDIO_BITRATE_DEFAULTS[balanced:$TARGET_AUDIO_CODEC:$CH_KEY]:-}}"
fi
if [ -z "$AUDIO_BR" ]; then AUDIO_BR="192k"; fi

# Audio passthrough decision: -c:a copy if source already matches target
# AND no downmix is needed AND we're within the codec's channel limit.
# Avoids re-encoding lossy → lossy (which can never improve quality and
# wastes bytes — a 96k AAC source should not be puffed up to 128k just
# because the table says 128k for stereo). Codec change still uses the
# table bitrate since AAC needs more bits than AC3 to sound the same.
SRC_AUDIO_CODEC=$(ffprobe -v quiet -select_streams "a:$((AUDIO_TRACK - 1))" \
  -show_entries stream=codec_name -of default=nokey=1:noprint_wrappers=1 \
  "$INPUT_READ" 2>/dev/null | head -1 || true)
SRC_AUDIO_PROFILE=$(ffprobe -v quiet -select_streams "a:$((AUDIO_TRACK - 1))" \
  -show_entries stream=profile -of default=nokey=1:noprint_wrappers=1 \
  "$INPUT_READ" 2>/dev/null | head -1 || true)
if audio_passthrough_ok "$TARGET_AUDIO_CODEC" "$SRC_AUDIO_CODEC" "$SRC_AUDIO_PROFILE" \
   && (( SELECTED_CHANNELS <= MAX_CHANNELS )) \
   && (( SELECTED_CHANNELS <= CODEC_MAX_CH )); then
  AUDIO_ENC_FLAGS=(-c:a copy)
  log "Audio passthrough: source $SRC_AUDIO_CODEC${SRC_AUDIO_PROFILE:+/$SRC_AUDIO_PROFILE} matches target, no downmix needed"
else
  AUDIO_ENC_FLAGS=(-c:a "$FFMPEG_AUDIO_ENCODER" -b:a "$AUDIO_BR" "${FFMPEG_AC_FLAG[@]}")
  # libfdk_aac: -afterburner 1 enables the higher-quality (slower) inner
  # quantization loop. The encoder defaults to 0 for speed, but the quality
  # win is uncontested (FDK docs + community testing) and the CPU cost is
  # negligible for audio-only re-encode. Always-on for AAC paths.
  if [ "$FFMPEG_AUDIO_ENCODER" = "libfdk_aac" ]; then
    AUDIO_ENC_FLAGS+=(-profile:a aac_low -afterburner 1)
  fi
fi

# ---------------------------------------------------------------------------
# Pre-flight: disk space checks
# ---------------------------------------------------------------------------
INPUT_SIZE=$(stat -c%s "$INPUT_READ" 2>/dev/null || echo 0)

# Check A: configured tmp dir (where encode output is written when TRANSCODARR_TMP_DIR is set)
# This is a second-line defense — LB already reserved space atomically.
if [ -n "$TMP_DIR" ]; then
  TMP_AVAIL_KB=$(df -k "$TMP_DIR" 2>/dev/null | awk 'NR==2{print $4}' || true)
  TMP_NEEDED_KB=$(( INPUT_SIZE * 2 / 1024 ))
  if [ "${TMP_AVAIL_KB:-0}" -gt 0 ] && [ "$TMP_NEEDED_KB" -gt 0 ] && (( TMP_AVAIL_KB < TMP_NEEDED_KB )); then
    log "WARN: SSD tmp low space — need ${TMP_NEEDED_KB}KB, have ${TMP_AVAIL_KB}KB"
    [ -n "${TRANSCODARR_SPACE_FAIL_KIND_FILE:-}" ] && printf "ssd\n" > "$TRANSCODARR_SPACE_FAIL_KIND_FILE" 2>/dev/null || true
    exit 75
  fi
fi

# Check B: destination disk (where file will be copied back)
SPACE_CHECK_DIR=$(dirname "${TRANSCODARR_DISK_WRITE_PATH:-$INPUT}")
DEST_AVAIL_KB=$(df -k "$SPACE_CHECK_DIR" 2>/dev/null | awk 'NR==2{print $4}' || true)
DEST_NEEDED_KB=$(( INPUT_SIZE / 1024 ))
if ! [[ "${DEST_AVAIL_KB:-}" =~ ^[0-9]+$ ]] || [ "${DEST_AVAIL_KB:-0}" -le 0 ]; then
  log "WARN: unable to determine destination disk free space for $SPACE_CHECK_DIR"
  [ -n "${TRANSCODARR_SPACE_FAIL_KIND_FILE:-}" ] && printf "dest:%s\n" "$DEST_NEEDED_KB" > "$TRANSCODARR_SPACE_FAIL_KIND_FILE" 2>/dev/null || true
  exit 75
fi
if [ "${DEST_AVAIL_KB:-0}" -gt 0 ] && [ "$DEST_NEEDED_KB" -gt 0 ] && (( DEST_AVAIL_KB < DEST_NEEDED_KB )); then
  log "WARN: Destination disk low space — need ${DEST_NEEDED_KB}KB, have ${DEST_AVAIL_KB}KB"
  [ -n "${TRANSCODARR_SPACE_FAIL_KIND_FILE:-}" ] && printf "dest:%s\n" "$DEST_NEEDED_KB" > "$TRANSCODARR_SPACE_FAIL_KIND_FILE" 2>/dev/null || true
  exit 75
fi

# ---------------------------------------------------------------------------
# Step 4 — Video transcode (NVIDIA CUDA / Intel QSV / CPU libx264)
# ---------------------------------------------------------------------------
PHASE_KEY="tc:worker:phase:${INPUT_HASH}"
if [[ "$MODE" == "gpu" ]]; then
  $QUEUE_CLI SET "$PHASE_KEY" "processing" EX 14400 > /dev/null 2>&1

  # ── NVDEC capability/filter gate (software-frame fallback) ──
  # Some source codecs/pixfmts don't decode or filter safely as CUDA frames
  # even when `-hwaccel cuda` is set. ffmpeg does NOT gracefully fall back —
  # it can fail at init or graph reinit with "Impossible to convert between
  # formats" (exit 218). Keep the CUDA encoder backend, but feed it software
  # frames so h264_nvenc/hevc_nvenc/av1_nvenc can upload internally.
  #
  # Checks target HW_DECODING=cuda; Intel QSV has a similar-but-different
  # capability matrix (no 4:2:0 12-bit, no MPEG-1, etc.) — not gated
  # here, but same fix pattern if it becomes an issue.
  if [ "$HW_DECODING" = "cuda" ]; then
    _nvdec_fallback=$(cuda_decode_filter_fallback_reason "$VIDEO_CODEC_LOWER" "${SRC_PIX_FMT:-}" "$IS_INTERLACED")
    if [ -n "$_nvdec_fallback" ]; then
      log "NVDEC/filter fallback → software decode+filter, NVENC encode: $_nvdec_fallback"
      CUDA_SOFTWARE_FILTERS=true
    fi
  fi

  # ── Encoder selection from tables ──
  # All lookups use :- defaults so a missing key yields "" instead of
  # aborting under set -u; validate non-empty below so record_failed runs.
  ENCODER="${ENCODER_NAME[$TARGET_CODEC:$HW_DECODING]:-}"
  Q_FLAG="${QUALITY_FLAG[$ENCODER]:-}"
  PRESET="${PRESET_MAP[$TARGET_CODEC:$HW_DECODING:$ENCODER_SPEED]:-}"
  PROFILE="${ENCODER_PROFILE[$ENCODER]:-}"
  PIXFMT="${PIX_FMT[$ENCODER]:-}"

  # Quality tier → CRF resolution
  if (( VIDEO_HEIGHT <= 576 )); then RES_BUCKET="sd"
  elif (( VIDEO_HEIGHT <= 720 )); then RES_BUCKET="hd"
  elif (( VIDEO_HEIGHT <= 1080 )); then RES_BUCKET="fhd"
  else RES_BUCKET="uhd"; fi
  # Quality table is keyed on ENCODER (not codec) — research says CRF/CQ values
  # are NOT comparable across encoders (codec-database-addendum.md:20). Same
  # numeric "transparent" CRF means different actual quality on libx265 vs
  # hevc_nvenc — we maintain per-encoder rows in QUALITY_TIERS.
  CQ="${QUALITY_TIERS[$ENCODER:$QUALITY_TIER:$RES_BUCKET]:-}"

  # Scale filter
  SFILTER="${SCALE_FILTER_NAME[$HW_DECODING]:-}"

  # Validate all lookups resolved — a miss here would otherwise yield a
  # broken ffmpeg invocation and an opaque worker_exit_1 with no class.
  if [ -z "$ENCODER" ] || [ -z "$Q_FLAG" ] || [ -z "$PRESET" ] \
     || [ -z "$PROFILE" ] || [ -z "$PIXFMT" ] || [ -z "$CQ" ] || [ -z "$SFILTER" ]; then
    log "ERROR: codec table lookup failed — target=$TARGET_CODEC hw=$HW_DECODING speed=$ENCODER_SPEED tier=$QUALITY_TIER res=$RES_BUCKET (encoder='$ENCODER' q_flag='$Q_FLAG' preset='$PRESET' profile='$PROFILE' pixfmt='$PIXFMT' cq='$CQ' sfilter='$SFILTER')"
    record_failed "codec_table_miss" "config_error"
    exit 1
  fi

  # Encoder availability check — STRICTLY runtime-aware.
  #
  # Lookup chain:
  #   tc:capabilities[$ENCODER] == "1"  → trust the boot probe, proceed
  #   tc:capabilities[$ENCODER] == "0"  → hard-fail with encoder_not_available
  #   tc:capabilities[$ENCODER] == ""   → inline probe RIGHT NOW with the
  #                                        exact (profile, pix_fmt) this
  #                                        job will use, cache the result
  #                                        in Valkey, then act on it.
  #
  # The inline probe replaces the old `ffmpeg -encoders | grep` fallback.
  # The list-check only proved build-time encoder REGISTRATION; it was
  # blind to runtime incapabilities (Ampere av1_nvenc compiled in but
  # hardware-absent, Pascal HEVC main10 demanded but 8-bit-only hardware,
  # missing NVIDIA container runtime). An inline probe test-encodes a
  # 1-frame clip in ~0.5-1s and produces a definitive runtime answer.
  #
  # First job after a fresh boot / Valkey flush will pay the inline
  # probe cost; every subsequent job hits the Valkey cache for free.
  # Test harness can skip all of this via TRANSCODARR_SKIP_ENCODER_CHECK=1.
  if [ "${TRANSCODARR_SKIP_ENCODER_CHECK:-0}" != "1" ]; then
    _cap_result=""
    if command -v valkey-cli >/dev/null 2>&1; then
      _cap_result=$(valkey-cli HGET tc:capabilities "$ENCODER" 2>/dev/null || true)
    fi

    # Cache miss → call the SHARED retry-probe helper from transcodarr-
    # lib.sh (probe_encoder_runtime) with the exact (profile, pixfmt)
    # combo this job needs. Same 3-attempt exponential-backoff policy
    # the boot sweep uses — a transient device-busy / Plex-contention
    # / driver-startup failure at cache-miss time will NOT cache a
    # permanent false-negative.
    if [ -z "$_cap_result" ]; then
      log "Capability probe cache miss for $ENCODER — running inline probe ($PROFILE/$PIXFMT)..."
      if probe_encoder_runtime "$ENCODER" "$PROFILE" "$PIXFMT"; then
        _cap_result=1
      else
        _cap_result=0
      fi
      if command -v valkey-cli >/dev/null 2>&1; then
        valkey-cli HSET tc:capabilities "$ENCODER" "$_cap_result" >/dev/null 2>&1 || true
      fi
    fi

    if [ "$_cap_result" = "0" ]; then
      log "ERROR: encoder '$ENCODER' probed as UNAVAILABLE. " \
          "target_codec=$TARGET_CODEC, hw_decoding=$HW_DECODING, " \
          "profile=$PROFILE, pix_fmt=$PIXFMT. " \
          "The running hardware/driver/build cannot produce this " \
          "encoder's expected output shape. Common causes: av1_nvenc " \
          "on RTX 30-series or earlier (Ampere lacks AV1 encode " \
          "hardware); Pascal/Maxwell GPUs demanding HEVC main10; " \
          "missing NVIDIA container runtime; ffmpeg build without " \
          "libfdk_aac / libsvtav1. Change target_codec in Settings, " \
          "or pick a different hw_decoding backend."
      record_failed "encoder_not_available" "config_error"
      exit 1
    fi
  fi

  # Hard-refuse known-broken codec × container combinations. Output
  # extension is normally mkv/mp4 (both support every codec we target),
  # but defensive check keeps the door shut if the remap logic ever
  # picks something exotic. AV1 in MPEG-TS is a muxer-level "no";
  # HEVC in AVI same. Both fail at mux init with an opaque error
  # otherwise — this gives a clean classification + message.
  if [ "$OUTPUT_EXT" = "ts" ] && [ "$TARGET_CODEC" = "av1" ]; then
    log "ERROR: AV1 is not supported by the MPEG-TS muxer. Change output container to mkv/mp4."
    record_failed "codec_container_unsupported" "config_error"
    exit 1
  fi
  if [ "$OUTPUT_EXT" = "avi" ] && [ "$TARGET_CODEC" = "hevc" ]; then
    log "ERROR: HEVC is not supported by the AVI muxer. Change output container to mkv/mp4."
    record_failed "codec_container_unsupported" "config_error"
    exit 1
  fi

  log "Starting transcode ($ENCODER via $HW_DECODING)..."

  # ── Output color/HDR decision ────────────────────────────────────────
  # PIXFMT was selected by encoder (nv12 = 8-bit, p010le = 10-bit, etc).
  # 10-bit target paths preserve HDR; 8-bit target paths must tone-map
  # to BT.709 SDR if source is HDR.
  TARGET_BIT_DEPTH=8
  case "$PIXFMT" in
    p010le|yuv420p10le|yuv420p12le) TARGET_BIT_DEPTH=10 ;;
  esac

  # HDR handling mode — user-configurable, defaults to auto.
  # Only applies when source is HDR; SDR sources are unaffected.
  HDR_HANDLING="${TRANSCODARR_HDR_HANDLING:-auto}"
  case "$HDR_HANDLING" in
    auto|preserve|tonemap) ;;
    *)
      log "WARN: unknown hdr_handling='$HDR_HANDLING' — falling back to auto"
      HDR_HANDLING="auto"
      ;;
  esac

  NEED_TONEMAP=false
  PRESERVE_HDR=false
  if [ "$IS_HDR" = "true" ]; then
    case "$HDR_HANDLING" in
      auto)
        # Preserve if target is 10-bit, else tonemap.
        if (( TARGET_BIT_DEPTH >= 10 )); then
          PRESERVE_HDR=true
        else
          NEED_TONEMAP=true
        fi
        ;;
      preserve)
        # User explicitly requested preserve — fail loudly if target
        # codec can't carry HDR. No silent data loss.
        if (( TARGET_BIT_DEPTH >= 10 )); then
          PRESERVE_HDR=true
        else
          log "ERROR: hdr_handling=preserve but target codec ($TARGET_CODEC / $PIXFMT) is 8-bit. HDR metadata cannot be preserved to h264. Switch target to hevc/av1 with a 10-bit profile, or change hdr_handling to auto/tonemap."
          record_failed "hdr_preserve_unsupported_target" "config_error"
          exit 1
        fi
        ;;
      tonemap)
        # User forced tonemap regardless of target bit depth.
        NEED_TONEMAP=true
        ;;
    esac
  fi

  # HW acceleration flags — empty string is legitimate (cpu path).
  # NOTE: with libplacebo available we no longer need to drop HW decode
  # for HDR sources; libplacebo handles tonemap on GPU via Vulkan compute.
  read -ra HW_ACCEL_FLAGS <<< "${HW_DECODE_ARGS[$HW_DECODING]:-}"

  # Read the startup-probed HDR tonemap path so build_scale_filter (sourced
  # from transcodarr-lib.sh) knows whether to use libplacebo's Vulkan path,
  # tonemap_opencl, or fall back to CPU zscale. Default to libplacebo if
  # the capability isn't recorded yet (probe hasn't completed) — matches
  # pre-probe behavior, will fail loudly the same way it did before the
  # probe existed.
  TRANSCODARR_HDR_TONEMAP_PATH=$($QUEUE_CLI HGET tc:capabilities hdr_tonemap_path 2>/dev/null)
  TRANSCODARR_HDR_TONEMAP_PATH="${TRANSCODARR_HDR_TONEMAP_PATH:-libplacebo}"
  export TRANSCODARR_HDR_TONEMAP_PATH

  # When tonemap_opencl is the selected path, ffmpeg needs an explicit
  # OpenCL hwdevice initialized + chosen for filters. Without this the
  # hwupload filter has nowhere to upload to and the filter graph fails
  # to init ("Error reinitializing filters"). Inject these flags into
  # HW_ACCEL_FLAGS so they land in the right position (before -i) of the
  # ffmpeg invocation. Only applied when needed — keeps ffmpeg invocations
  # for non-HDR or other-path encodes minimal.
  if [ "$TRANSCODARR_HDR_TONEMAP_PATH" = "opencl" ] && [ "${NEED_TONEMAP:-false}" = "true" ]; then
    HW_ACCEL_FLAGS+=("-init_hw_device" "opencl=ocl" "-filter_hw_device" "ocl")
  fi
  if [ "${CUDA_SOFTWARE_FILTERS:-false}" = "true" ]; then
    HW_ACCEL_FLAGS=()
  fi

  # Output color target for filter-level setparams tagging. See lib
  # function header on why this is filter-level, not encoder-level.
  if [ "$PRESERVE_HDR" = "true" ]; then
    COLOR_TARGET="preserve-hdr"
  elif (( VIDEO_HEIGHT <= 576 )); then
    COLOR_TARGET="bt601"
  else
    COLOR_TARGET="bt709"
  fi

  # HDR static metadata (SMPTE 2086 mastering display + SMPTE 2094-40
  # MaxCLL/MaxFALL). Only probed when we're going to preserve HDR —
  # tonemapped SDR output doesn't carry these. Forwarded to the encoder
  # so Plex clients tone-map against the source's real peak luminance
  # instead of guessing 1000 nits (crushes highlights on OLED targeted
  # at higher peaks, washes out SDR targets at lower peaks).
  HDR_MASTER_DISPLAY=""
  HDR_MAX_CLL=""
  if [ "$PRESERVE_HDR" = "true" ]; then
    _hdr_md=$(probe_hdr_metadata "$INPUT_READ")
    HDR_MASTER_DISPLAY="${_hdr_md%|*}"
    HDR_MAX_CLL="${_hdr_md#*|}"
    [ -n "$HDR_MASTER_DISPLAY" ] && log "HDR master-display extracted: $HDR_MASTER_DISPLAY"
    [ -n "$HDR_MAX_CLL" ]        && log "HDR MaxCLL/MaxFALL extracted: $HDR_MAX_CLL"
    if [ -z "$HDR_MASTER_DISPLAY" ] && [ -z "$HDR_MAX_CLL" ]; then
      log "WARN: PRESERVE_HDR active but no SMPTE 2086/CLL side-data in source — Plex will tone-map with defaults"
    fi
  fi

  # ── Phase 6C dynamic HDR safety gate (+2A HDR10+ continue path) ─
  # Design intent: the correct path for dynamic HDR → SDR is
  # libplacebo with metadata-aware tonemap (st2094-40 for HDR10+,
  # apply_dolbyvision for DV). tonemap_opencl and CPU mobius have
  # no metadata source knob — applying them to dynamic-HDR
  # sources produces flicker / highlight clipping / black crush,
  # which the span-based validation does NOT catch. The source
  # then gets destroyed by move-into-place. (Avatar 2026-05-22.)
  #
  # Phase 6C-followup 2A status (current patch):
  #   - hdr10plus + libplacebo → CONTINUE (build_scale_filter
  #     emits tonemapping=st2094-40 based on
  #     TRANSCODARR_DYN_HDR_KIND).
  #   - dolby_vision + libplacebo → fail (2B pending — needs
  #     profile 5/7/8 fixtures before flipping).
  #   - vivid + libplacebo → fail (no verified conversion path).
  #   - any dynamic kind + opencl/cpu → fail permanently (those
  #     paths can't read metadata).
  #
  # PRESERVE_HDR=true sets NEED_TONEMAP=false above this gate, so
  # it naturally doesn't fire on the preserve path. Static HDR10
  # → SDR is also not blocked: detect_dynamic_hdr matches DYNAMIC
  # side_data names only, never static `Mastering display
  # metadata` / `Content light level metadata`.
  DYN_HDR_KIND=""
  if [ "${NEED_TONEMAP:-false}" = "true" ]; then
    DYN_HDR_KIND=$(detect_dynamic_hdr "${INPUT_READ:-$INPUT}" || true)
  fi

  if [ -n "$DYN_HDR_KIND" ]; then
    case "${TRANSCODARR_HDR_TONEMAP_PATH:-}:${DYN_HDR_KIND}" in
      libplacebo:hdr10plus)
        # Phase 6C-followup 2A: wired. Tell build_scale_filter the
        # kind via env so its libplacebo branch picks st2094-40.
        export TRANSCODARR_DYN_HDR_KIND="hdr10plus"
        log "HDR10+ source — using libplacebo tonemapping=st2094-40 (metadata-aware, Phase 6C-followup 2A)"
        # Fall through; ffmpeg proceeds.
        ;;
      libplacebo:dolby_vision)
        log "ERROR: Dolby Vision source + SDR target. libplacebo is available but the DV apply_dolbyvision config is not yet wired into build_scale_filter (Phase 6C-followup 2B — requires profile 5/7/8 fixtures). Refusing tonemap to avoid destructive replacement of the source."
        record_failed "dynamic_hdr_tonemap_unsupported" "config_error"
        exit 1
        ;;
      libplacebo:vivid)
        log "ERROR: HDR Vivid (CUVA 005.1) source + SDR target. No verified libplacebo conversion path; refusing tonemap to avoid destructive replacement of the source."
        record_failed "dynamic_hdr_tonemap_unsupported" "config_error"
        exit 1
        ;;
      *)
        log "ERROR: $DYN_HDR_KIND source + SDR target. Current tonemap path is '${TRANSCODARR_HDR_TONEMAP_PATH:-unknown}', which has no metadata-aware mode (only libplacebo can correctly handle dynamic HDR). Refusing static tonemap to avoid destructive replacement of the source."
        record_failed "dynamic_hdr_tonemap_unsupported" "config_error"
        exit 1
        ;;
    esac
  fi

  # ── Filter chain construction ────────────────────────────────────────
  # Delegated to build_scale_filter() in transcodarr-lib.sh so the exact
  # same logic is testable via tests/filter-chain-gpu.sh.
  FILTER_HW_DECODING="$HW_DECODING"
  if [ "${CUDA_SOFTWARE_FILTERS:-false}" = "true" ]; then
    FILTER_HW_DECODING=none
  fi

  SCALE_FILTER=$(build_scale_filter \
    "$VIDEO_WIDTH" "$VIDEO_HEIGHT" \
    "$MAX_WIDTH" "$MAX_HEIGHT" \
    "$PIXFMT" "$FILTER_HW_DECODING" \
    "$NEED_TONEMAP" "$IS_INTERLACED" \
    "${SRC_FIELD_ORDER:-}" "${SRC_COLOR_TRANSFER:-}" \
    "$COLOR_TARGET")

  # Video codec flags
  VIDEO_CODEC_FLAGS=(-c:v "$ENCODER" "$Q_FLAG" "$CQ" -preset "$PRESET" -profile:v "$PROFILE")
  [[ -n "${NEEDS_UNCAPPED[$ENCODER]+x}" ]] && VIDEO_CODEC_FLAGS+=(-b:v 0)

  # NVENC quality baseline (always-on, not gated behind extras toggle).
  #   -multipass fullres — NVIDIA-recommended quality refinement
  #                       (NV_ENC_TWO_PASS_FULL_RESOLUTION). Community tools
  #                       default to disabled; NVENC programming guide §3.8.4
  #                       recommends it for HQ encodes.
  # -qmin 0 used to live here. Dropped per 2026-04 audit: in pure CQ mode
  # (b:v 0 + cq N), ffmpeg picks RC_VBR with targetQuality (not VBR_MINQP),
  # so qmin is consulted ONLY if qmax is also set. Left alone it was a
  # no-op; setting it half-configured misled readers into thinking it was
  # doing something. Drop.
  if [[ "$HW_DECODING" = "cuda" ]]; then
    VIDEO_CODEC_FLAGS+=(-multipass fullres)
  fi

  # HEVC UHQ tune (opt-in). Per NVENC programming guide §9 + Scott Laird
  # VMAF benchmark (2025-03): `-tune uhq` measurably improves quality
  # per bit vs the default `hq`, but shifts the CQ-vs-quality curve
  # by ~+7 points. VMAF 95 at hq is CQ 26.2, uhq is CQ 33.4 — same
  # subjective quality, higher CQ number. Without the offset, UHQ
  # produces smaller/darker files with visible detail loss.
  #
  # Apply: replace the CQ value at index 3 in VIDEO_CODEC_FLAGS
  # (0=-c:v, 1=encoder, 2=-cq, 3=<value>, 4=-preset, ...). Cap at 51
  # (NVENC max) so extreme ladder bases don't overflow.
  #
  # h264_nvenc + av1_nvenc don't support -tune uhq — gated off here.
  # QSV / CPU don't support NVENC tunes at all.
  if [ "$ENCODER" = "hevc_nvenc" ] && [ "${TRANSCODARR_HEVC_UHQ_TUNE:-false}" = "true" ]; then
    VIDEO_CODEC_FLAGS+=(-tune uhq)
    _uhq_cq=$((CQ + 7))
    (( _uhq_cq > 51 )) && _uhq_cq=51
    VIDEO_CODEC_FLAGS[3]=$_uhq_cq
    log "UHQ tune enabled — CQ shifted ${CQ} → ${_uhq_cq} (uhq-calibrated for same subjective quality as hq baseline)"
  fi

  # HDR static metadata forwarding (NVENC). Only hevc_nvenc accepts these
  # options — h264 can't carry BT.2020/PQ, av1_nvenc is Ada+ only. Options
  # were added to ffmpeg's NVENC encoder in 5.0 (Codec SDK 11.1); they
  # write SMPTE 2086 + SMPTE 2094-40 SEI into the encoded bitstream.
  if [ "$ENCODER" = "hevc_nvenc" ] && [ "$PRESERVE_HDR" = "true" ]; then
    [ -n "$HDR_MASTER_DISPLAY" ] && VIDEO_CODEC_FLAGS+=(-master_display "$HDR_MASTER_DISPLAY")
    [ -n "$HDR_MAX_CLL" ]        && VIDEO_CODEC_FLAGS+=(-max_cll "$HDR_MAX_CLL")
  fi

  # OPTIONAL extras — per-encoder so AV1 paths drop flags that don't apply
  # (av1_qsv hard-errors on -rdo / -mbbrc; av1_nvenc no-ops -b_ref_mode).
  if [[ "$NVENC_EXTRAS_ENABLED" = "true" && "$HW_DECODING" = "cuda" ]]; then
    read -ra _nvenc_extras <<< "${NVENC_EXTRAS_PER_ENCODER[$ENCODER]:-}"
    VIDEO_CODEC_FLAGS+=("${_nvenc_extras[@]}")
  fi
  if [[ "$QSV_EXTRAS_ENABLED" = "true" && "$HW_DECODING" = "qsv" ]]; then
    read -ra _qsv_extras <<< "${QSV_EXTRAS_PER_ENCODER[$ENCODER]:-}"
    VIDEO_CODEC_FLAGS+=("${_qsv_extras[@]}")
  fi

  # Software-encoder tuning params (always-on, research-cited).
  # libx265: psy-rd / psy-rdoq tuning for perceptual quality over PSNR.
  # libsvtav1: film-grain synthesis (20-40% bitrate win on grainy content)
  #            and tune=0 (VQ / psychovisual mode).
  #
  # tune=0 rationale (per 2026-04 audit): tune=0 (VQ) is the community-
  # recommended default for quality encoding per ffmpeg.party, BlueSwordM
  # guides, and SVT-AV1 v2.x Parameters.md. tune=3 (IQ) only exists in
  # recent mainline (v2.x+) and would be REJECTED on older v1.x / v2.0-2.2
  # builds; tune=0 is backward-compatible with every SVT-AV1 release that
  # has a preset-numeric interface AND produces better subjective quality.
  #
  # libx265 HDR preserve: append `hdr10=1:repeat-headers=1` + mastering
  # display + MaxCLL to LIBX265_PARAMS when PRESERVE_HDR. hdr10=1 makes
  # x265 honor explicit color_primaries/trc/matrix values in SPS VUI;
  # repeat-headers=1 ensures SEI is emitted before every IDR (needed
  # for seekability under HDR-aware clients).
  if [ "$ENCODER" = "libx265" ]; then
    _x265_params="$LIBX265_PARAMS"
    if [ "$PRESERVE_HDR" = "true" ]; then
      _x265_params+=":hdr10=1:repeat-headers=1"
      _x265_params+=":colorprim=bt2020:transfer=${SRC_COLOR_TRANSFER:-smpte2084}:colormatrix=bt2020nc"
      [ -n "$HDR_MASTER_DISPLAY" ] && _x265_params+=":master-display=${HDR_MASTER_DISPLAY}"
      [ -n "$HDR_MAX_CLL" ]        && _x265_params+=":max-cll=${HDR_MAX_CLL}"
    fi
    VIDEO_CODEC_FLAGS+=(-x265-params "$_x265_params")
  elif [ "$ENCODER" = "libsvtav1" ]; then
    # film-grain level is user-tunable (Settings → Video Encoding → AV1 Film Grain).
    # 0 = synthesis disabled (no film-grain key); 1-50 = synthesis level.
    _grain="${TRANSCODARR_AV1_FILM_GRAIN:-8}"
    # Base params: tune + modern psychovisual knobs per 2026-04 audit.
    #   variance-boost-strength=2  — hierarchical-layer consistency,
    #                                 range 0-4 (0=off, 1-4 enable);
    #                                 v2.x+ mainline.
    #   qp-scale-compress-strength=1 — subjective improvement on flat
    #                                 content; hierarchical-layer refine.
    #   frame-luma-bias=40          — improves dark-scene quality, range
    #                                 0-100, 40 is community default.
    #   sharpness=1                 — mild subjective sharpen (range
    #                                 -7..7, 0=neutral).
    #   enable-overlays=1           — overlay frames for scene changes.
    #   scd=1                       — scene change detection.
    # All require SVT-AV1 ≥ 2.0. Older builds will error at encode
    # init with "Unknown parameter" — our classifier catches it and
    # the user sees a clear message to upgrade SVT-AV1.
    _svtav1_params="tune=0:variance-boost-strength=2:qp-scale-compress-strength=1:frame-luma-bias=40:sharpness=1:enable-overlays=1:scd=1"
    if (( _grain > 0 )); then
      _svtav1_params="film-grain=${_grain}:${_svtav1_params}"
    fi
    VIDEO_CODEC_FLAGS+=(-svtav1-params "$_svtav1_params")
  fi

  # Animation -tune for libx264/libx265 when path heuristic matches.
  # x264 docs: animation tune adds bframes, stronger deblock, lower
  # aq-strength — visibly cleaner on flat-colored animated content.
  if [ "$IS_ANIMATION" = "true" ] && { [ "$ENCODER" = "libx264" ] || [ "$ENCODER" = "libx265" ]; }; then
    VIDEO_CODEC_FLAGS+=(-tune animation)
    log "Animation path heuristic matched — applying -tune animation"
  fi

  # Color target resolved before the filter-chain call above.

  # FPS mode — SVT-AV1 crashes on VFR
  if [ "$ENCODER" = "libsvtav1" ]; then
    FPS_MODE="-fps_mode cfr"
  else
    FPS_MODE="-fps_mode vfr"
  fi

  # ── Subtitle handling (container-aware) ──
  SUBTITLE_ARGS=()
  SUB_OUT_IDX=0
  if [ "$SUBTITLE_MODE" != "strip_all" ]; then
    SUB_STREAMS=$(ffprobe -v quiet -select_streams s \
      -show_entries stream=index,codec_name:stream_tags=language \
      -of csv=p=0 "$INPUT_READ" 2>/dev/null || true)

    # Count subtitle streams so copy_matching can preserve a sole
    # untagged track (symmetric with audio selection's behavior for
    # single-track-untagged inputs — e.g. DVD rips with no metadata).
    SUB_COUNT=$(echo "$SUB_STREAMS" | awk 'NF{c++} END{print c+0}')

    while IFS=, read -r idx codec lang; do
      [ -z "$idx" ] && continue
      _lang_norm=$(normalize_audio_language_tag "$lang")
      if [ "$SUBTITLE_MODE" = "copy_matching" ]; then
        # ffprobe emits "" / "und" / "unknown" interchangeably for untagged.
        if [ "$_lang_norm" = "und" ]; then
          if (( SUB_COUNT != 1 )); then
            log "WARN: dropping untagged subtitle stream $idx (codec=$codec) — copy_matching with $SUB_COUNT streams"
            continue
          fi
          log "Keeping sole untagged subtitle stream $idx (codec=$codec)"
        elif [ "$_lang_norm" != "$SUB_LANG" ]; then
          continue
        fi
      fi

      if [ "$OUTPUT_EXT" = "mkv" ]; then
        if [[ -n "${MKV_SUB_SUPPORTED[$codec]+x}" ]]; then
          SUBTITLE_ARGS+=(-map "0:$idx" "-c:s:$SUB_OUT_IDX" copy)
          (( SUB_OUT_IDX++ )) || true
        elif [[ -n "${MKV_SUB_BLOCKED[$codec]+x}" ]]; then
          log "WARN: dropping subtitle stream $idx (codec=$codec) — explicitly incompatible with MKV"
        else
          log "WARN: dropping subtitle stream $idx (codec=$codec) — not supported in MKV output"
        fi
      elif [ "$OUTPUT_EXT" = "mp4" ]; then
        if [[ -n "${MP4_SUB_SUPPORTED[$codec]+x}" ]]; then
          SUBTITLE_ARGS+=(-map "0:$idx" "-c:s:$SUB_OUT_IDX" copy)
          (( SUB_OUT_IDX++ )) || true
        elif [[ -n "${MP4_TEXT_CONVERTIBLE[$codec]+x}" ]]; then
          SUBTITLE_ARGS+=(-map "0:$idx" "-c:s:$SUB_OUT_IDX" mov_text)
          (( SUB_OUT_IDX++ )) || true
        else
          log "WARN: dropping subtitle stream $idx (codec=$codec) — not supported in MP4 output"
        fi
      fi
    done <<< "$SUB_STREAMS"
  fi

  log_encode_plan
  FFMPEG_CMD=(
    ffmpeg -hide_banner -loglevel warning -y
    "${HW_ACCEL_FLAGS[@]}"
    "${GENPTS_FLAGS[@]}"
    -i "$INPUT_READ"
    -map 0:V:0
    -map "0:a:$((AUDIO_TRACK - 1))"
    -vf "$SCALE_FILTER"
    "${VIDEO_CODEC_FLAGS[@]}"
    "${AUDIO_ENC_FLAGS[@]}"
    -map_metadata 0
    "${MAP_EXTRA_FLAGS[@]}"
    "${SUBTITLE_ARGS[@]}"
    $FPS_MODE
    "${MUXER_FLAGS[@]}"
    -max_muxing_queue_size 9999
    "$TMP_OUT"
  )

  # ── Execute ffmpeg with stderr capture ──
  STDERR_TMP=$(mktemp /tmp/tc-stderr.XXXXXX)
  ff_rc=0
  "${FFMPEG_CMD[@]}" 2>"$STDERR_TMP" || ff_rc=$?

  if (( ff_rc != 0 )); then
    log "WARN: ffmpeg failed (code $ff_rc), retrying without subtitles..."
    rm -f "$TMP_OUT"
    FFMPEG_CMD=(
      ffmpeg -hide_banner -loglevel warning -y
      "${HW_ACCEL_FLAGS[@]}"
      "${GENPTS_FLAGS[@]}"
      -i "$INPUT_READ"
      -map 0:V:0
      -map "0:a:$((AUDIO_TRACK - 1))"
      -vf "$SCALE_FILTER"
      "${VIDEO_CODEC_FLAGS[@]}"
      "${AUDIO_ENC_FLAGS[@]}"
      -map_metadata 0
      "${MAP_EXTRA_FLAGS[@]}"
      $FPS_MODE
      "${MUXER_FLAGS[@]}"
      -max_muxing_queue_size 9999
      "$TMP_OUT"
    )
    # Append retry stderr so the first attempt's context is preserved —
    # classification scans the union, so we don't lose the root cause if
    # the first failure was specific (mux_queue_overflow, corrupt_input,
    # subtitle_incompatible) and the retry fails generically.
    ff_rc=0
    "${FFMPEG_CMD[@]}" 2>>"$STDERR_TMP" || ff_rc=$?

    if (( ff_rc != 0 )); then
      # Final failure — classify from stderr
      FAILURE_CLASS="unknown"
      for i in "${!STDERR_PATTERNS[@]}"; do
        if grep -qi "${STDERR_PATTERNS[$i]}" "$STDERR_TMP" 2>/dev/null; then
          FAILURE_CLASS="${STDERR_CLASSES[$i]}"
          break
        fi
      done

      # Persist log if enabled
      if [ "$STDERR_LOGGING" = "true" ]; then
        mkdir -p /state/failures
        _safe_name=$(basename "$INPUT" | sed 's/[^a-zA-Z0-9._-]/_/g')
        mv "$STDERR_TMP" "/state/failures/${_safe_name}.log"
      else
        rm -f "$STDERR_TMP"
      fi

      log "ERROR: ffmpeg GPU failed even without subtitles (code $ff_rc) [$FAILURE_CLASS]"
      record_failed "ffmpeg_gpu_exit_$ff_rc" "$FAILURE_CLASS"
      exit 1
    fi
    # Retry succeeded — subtitles were dropped
    log "WARN: transcode succeeded after dropping subtitles due to first-attempt failure"
    rm -f "$STDERR_TMP"
  else
    # First attempt succeeded
    rm -f "$STDERR_TMP"
  fi

  if [[ ! -f "$TMP_OUT" ]]; then
    log "ERROR: ffmpeg GPU produced no output file"
    record_failed "ffmpeg_gpu_no_output" "output_missing"
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Step 5 — Audio-only transcode (ffmpeg, video copy)
# ---------------------------------------------------------------------------
if [[ "$MODE" == "audio_only" ]]; then
  $QUEUE_CLI SET "$PHASE_KEY" "processing" EX 14400 > /dev/null 2>&1
  log "Starting audio-only transcode (ffmpeg -c:v copy)..."

  log_encode_plan
  FFMPEG_CMD=(
    ffmpeg -hide_banner -loglevel warning -y
    "${GENPTS_FLAGS[@]}"
    -i "$INPUT_READ"
    -map 0:V:0
    -map "0:a:$((AUDIO_TRACK - 1))"
    -c:v copy
    "${AUDIO_ENC_FLAGS[@]}"
    -map_metadata 0
    "${MAP_EXTRA_FLAGS[@]}"
  )

  # ── Subtitle handling (container-aware) — same logic as GPU path ──
  _SUB_ARGS=()
  _SUB_OUT_IDX=0
  if [ "$SUBTITLE_MODE" != "strip_all" ]; then
    _SUB_STREAMS=$(ffprobe -v quiet -select_streams s \
      -show_entries stream=index,codec_name:stream_tags=language \
      -of csv=p=0 "$INPUT_READ" 2>/dev/null || true)

    _SUB_COUNT=$(echo "$_SUB_STREAMS" | awk 'NF{c++} END{print c+0}')

    while IFS=, read -r idx codec lang; do
      [ -z "$idx" ] && continue
      _lang_norm=$(normalize_audio_language_tag "$lang")
      if [ "$SUBTITLE_MODE" = "copy_matching" ]; then
        if [ "$_lang_norm" = "und" ]; then
          if (( _SUB_COUNT != 1 )); then
            log "WARN: dropping untagged subtitle stream $idx (codec=$codec) — copy_matching with $_SUB_COUNT streams"
            continue
          fi
          log "Keeping sole untagged subtitle stream $idx (codec=$codec)"
        elif [ "$_lang_norm" != "$SUB_LANG" ]; then
          continue
        fi
      fi

      if [ "$OUTPUT_EXT" = "mkv" ]; then
        if [[ -n "${MKV_SUB_SUPPORTED[$codec]+x}" ]]; then
          _SUB_ARGS+=(-map "0:$idx" "-c:s:$_SUB_OUT_IDX" copy)
          (( _SUB_OUT_IDX++ )) || true
        elif [[ -n "${MKV_SUB_BLOCKED[$codec]+x}" ]]; then
          log "WARN: dropping subtitle stream $idx (codec=$codec) — explicitly incompatible with MKV"
        else
          log "WARN: dropping subtitle stream $idx (codec=$codec) — not supported in MKV output"
        fi
      elif [ "$OUTPUT_EXT" = "mp4" ]; then
        if [[ -n "${MP4_SUB_SUPPORTED[$codec]+x}" ]]; then
          _SUB_ARGS+=(-map "0:$idx" "-c:s:$_SUB_OUT_IDX" copy)
          (( _SUB_OUT_IDX++ )) || true
        elif [[ -n "${MP4_TEXT_CONVERTIBLE[$codec]+x}" ]]; then
          _SUB_ARGS+=(-map "0:$idx" "-c:s:$_SUB_OUT_IDX" mov_text)
          (( _SUB_OUT_IDX++ )) || true
        else
          log "WARN: dropping subtitle stream $idx (codec=$codec) — not supported in MP4 output"
        fi
      fi
    done <<< "$_SUB_STREAMS"
  fi
  if [ ${#_SUB_ARGS[@]} -gt 0 ]; then
    FFMPEG_CMD+=("${_SUB_ARGS[@]}")
  fi

  FFMPEG_CMD+=("${MUXER_FLAGS[@]}" -max_muxing_queue_size 9999 "$TMP_OUT")

  # ── Execute ffmpeg with stderr capture ──
  STDERR_TMP=$(mktemp /tmp/tc-stderr.XXXXXX)
  ff_rc=0
  "${FFMPEG_CMD[@]}" 2>"$STDERR_TMP" || ff_rc=$?

  if (( ff_rc != 0 )); then
    # Retry without subtitles — unknown codecs cause ffmpeg to fail
    log "WARN: ffmpeg failed (code $ff_rc), retrying without subtitles..."
    rm -f "$TMP_OUT"
    FFMPEG_CMD=(
      ffmpeg -hide_banner -loglevel warning -y
      "${GENPTS_FLAGS[@]}"
      -i "$INPUT_READ"
      -map 0:V:0
      -map "0:a:$((AUDIO_TRACK - 1))"
      -c:v copy
      "${AUDIO_ENC_FLAGS[@]}"
      -map_metadata 0
      "${MAP_EXTRA_FLAGS[@]}"
      "${MUXER_FLAGS[@]}"
      -max_muxing_queue_size 9999
      "$TMP_OUT"
    )
    # Append retry stderr — see GPU branch above for rationale.
    ff_rc=0
    "${FFMPEG_CMD[@]}" 2>>"$STDERR_TMP" || ff_rc=$?

    if (( ff_rc != 0 )); then
      # Final failure — classify from stderr
      FAILURE_CLASS="unknown"
      for i in "${!STDERR_PATTERNS[@]}"; do
        if grep -qi "${STDERR_PATTERNS[$i]}" "$STDERR_TMP" 2>/dev/null; then
          FAILURE_CLASS="${STDERR_CLASSES[$i]}"
          break
        fi
      done

      # Persist log if enabled
      if [ "$STDERR_LOGGING" = "true" ]; then
        mkdir -p /state/failures
        _safe_name=$(basename "$INPUT" | sed 's/[^a-zA-Z0-9._-]/_/g')
        mv "$STDERR_TMP" "/state/failures/${_safe_name}.log"
      else
        rm -f "$STDERR_TMP"
      fi

      log "ERROR: ffmpeg failed even without subtitles (code $ff_rc) [$FAILURE_CLASS]"
      record_failed "ffmpeg_exit_$ff_rc" "$FAILURE_CLASS"
      exit 1
    fi
    # Retry succeeded — subtitles were dropped
    log "WARN: transcode succeeded after dropping subtitles due to first-attempt failure"
    rm -f "$STDERR_TMP"
  else
    # First attempt succeeded
    rm -f "$STDERR_TMP"
  fi

  if [[ ! -f "$TMP_OUT" ]]; then
    log "ERROR: ffmpeg produced no output file"
    record_failed "ffmpeg_no_output" "output_missing"
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Step 6 — Validate output (ffprobe)
# ---------------------------------------------------------------------------
$QUEUE_CLI SET "$PHASE_KEY" "verifying" EX 14400 > /dev/null 2>&1
log "Validating output..."

# Check that output has at least one video and one audio stream
# Guard with || true so ffprobe failure doesn't trigger set -e before we record it
OUT_VSTREAMS=$(ffprobe -v quiet -select_streams v -show_entries stream=index -of csv=p=0 "$TMP_OUT" 2>/dev/null | wc -l || true)
OUT_ASTREAMS=$(ffprobe -v quiet -select_streams a -show_entries stream=index -of csv=p=0 "$TMP_OUT" 2>/dev/null | wc -l || true)
OUT_VSTREAMS="${OUT_VSTREAMS:-0}"
OUT_ASTREAMS="${OUT_ASTREAMS:-0}"

if (( OUT_VSTREAMS < 1 )); then
  log "ERROR: Output has no video stream"
  record_failed "validation_no_video" "validation_failure"
  exit 1
fi
if (( OUT_ASTREAMS < 1 )); then
  log "ERROR: Output has no audio stream"
  record_failed "validation_no_audio" "validation_failure"
  exit 1
fi

# Verify output codec matches what we intended
if [[ "$MODE" == "gpu" ]]; then
  OUT_CODEC=$(ffprobe -v quiet -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 "$TMP_OUT" 2>/dev/null || echo "unknown")
  # Normalize output codec for comparison
  case "$OUT_CODEC" in
    h264|x264|avc) OUT_CODEC_NORM="h264" ;;
    hevc|h265)     OUT_CODEC_NORM="hevc" ;;
    av1)           OUT_CODEC_NORM="av1" ;;
    *)             OUT_CODEC_NORM="$OUT_CODEC" ;;
  esac
  if [[ "$OUT_CODEC_NORM" != "$TARGET_CODEC" ]]; then
    log "ERROR: Output codec is '$OUT_CODEC', expected $TARGET_CODEC"
    record_failed "validation_wrong_codec" "validation_failure"
    exit 1
  fi
fi

# Duration check: compare the video AND audio stream DURATIONS (spans)
# between source and output, not container-level `format=duration` and
# not raw end-pts values.
#
# Container duration is `max(all stream durations)` in MKV, so long-running
# subtitle tracks (PGS/ASS with their own timeline) inflate it past the
# real video end. A correctly-transcoded output that drops or re-times
# those subs can then look 20–40 seconds shorter than the source at the
# container level, tripping the tolerance and producing a false-positive
# validation failure (known-issue #4). Per-stream spans sidestep this
# because we never look at the subtitle track.
#
# Raw end-pts comparison breaks on sources with non-zero `start_time`
# (e.g. files cut with `-copyts -ss N`, or any capture that began at
# stream timestamp != 0). ffmpeg normalizes the output timeline to 0 by
# default, so source last_pts and output last_pts differ by start_time
# even when the real durations match. `get_stream_span` returns
# `last_pts - first_pts` for each probe, so the comparison is apples to
# apples regardless of the source's timeline offset.
#
# Seek hint: we use the SOURCE's `format=duration` as the seek anchor
# for BOTH source and output probes — not each file's own. The output
# MKV's format=duration can be a stale pre-declared value that doesn't
# match actual written content, causing seeks to overshoot the real end
# of stream. The source's format_dur is stable. Even when it's inflated
# by subtitle overhang, `format_dur - 120` still lands safely inside the
# real video and audio streams for both source and output.
#
# When the source's format_dur is unavailable (N/A / empty / 0), we pass
# `DURATION_SEEK_FROM=0` as a sentinel to `get_stream_span`, which then
# skips the seek-based fast path entirely and goes straight to a
# full-stream scan. Without this, passing seek_from=0 to the fast path
# would trigger `-read_intervals "0%+200"` which silently validates only
# the first 200 seconds of the stream (a subtle regression).
#
# Source audio stream spec is `a:$((AUDIO_TRACK-1))` because the worker
# may have selected a non-first audio track (language match, commentary
# skip). Output is always `a:0` — we map exactly one audio stream.

SRC_FORMAT_DUR_RAW=$(ffprobe -v quiet -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$INPUT_READ" 2>/dev/null || echo 0)
SRC_FORMAT_DUR="${SRC_FORMAT_DUR_RAW%.*}"
case "$SRC_FORMAT_DUR" in *[!0-9]*|"") SRC_FORMAT_DUR=0 ;; esac
if (( SRC_FORMAT_DUR > 120 )); then
  DURATION_SEEK_FROM=$(( SRC_FORMAT_DUR - 120 ))
else
  # Sentinel: forces full-stream fallback in get_stream_span. See helper
  # docstring for why passing a real 0 here would silently validate the
  # wrong window.
  DURATION_SEEK_FROM=0
fi

ORIG_AUDIO_SPEC="a:$((AUDIO_TRACK - 1))"
ORIG_V_SPAN=$(get_stream_span "$INPUT_READ" "v:0" "$DURATION_SEEK_FROM")
ORIG_A_SPAN=$(get_stream_span "$INPUT_READ" "$ORIG_AUDIO_SPEC" "$DURATION_SEEK_FROM")
OUT_V_SPAN=$(get_stream_span "$TMP_OUT" "v:0" "$DURATION_SEEK_FROM")
OUT_A_SPAN=$(get_stream_span "$TMP_OUT" "a:0" "$DURATION_SEEK_FROM")

# A span of 0 means the helper couldn't measure the stream — no fast-path
# packet seek found anything AND the full-stream scan fallback also
# returned empty/invalid. With the negative-pts character-class bug fixed
# in lib.sh, that only happens for genuinely broken streams (no packets,
# corrupt container, unreadable codec). Those are real failures and must
# be reported as such. No skip, no "trust ffmpeg and move on" — if the
# validator can't confirm the span, the encode is rejected.
V_DIFF=$(( ORIG_V_SPAN - OUT_V_SPAN ))
(( V_DIFF < 0 )) && V_DIFF=$(( -V_DIFF ))
A_DIFF=$(( ORIG_A_SPAN - OUT_A_SPAN ))
(( A_DIFF < 0 )) && A_DIFF=$(( -A_DIFF ))

if (( V_DIFF > 10 )) || (( A_DIFF > 10 )); then
  log "ERROR: Stream duration mismatch — video span ${ORIG_V_SPAN}s → ${OUT_V_SPAN}s (Δ${V_DIFF}s), audio span ${ORIG_A_SPAN}s → ${OUT_A_SPAN}s (Δ${A_DIFF}s)"
  record_failed "validation_duration_mismatch" "validation_failure"
  exit 1
fi

# Size check: output must be at least 5% of original OR at least 1MB
# (short episodes and small files can legitimately compress heavily)
ORIG_SIZE=$(stat -c%s "$INPUT_READ")
OUT_SIZE=$(stat -c%s "$TMP_OUT")
MIN_SIZE=$(( ORIG_SIZE / 20 ))
if [ "$MIN_SIZE" -lt 1048576 ]; then MIN_SIZE=1048576; fi
if (( OUT_SIZE < MIN_SIZE )); then
  log "ERROR: Output too small — original=${ORIG_SIZE} output=${OUT_SIZE} min=${MIN_SIZE}"
  record_failed "validation_too_small" "validation_failure"
  exit 1
fi

log "Validation passed (v=$OUT_VSTREAMS a=$OUT_ASTREAMS video_span=${OUT_V_SPAN:-0}s audio_span=${OUT_A_SPAN:-0}s size=${OUT_SIZE})"

# ---------------------------------------------------------------------------
# Step 7 — Replace original (skipped in sweet16 test mode)
# ---------------------------------------------------------------------------
# Pre-copy-back: destination disk space check (exact output size known)
DISK_WRITE_PATH="${TRANSCODARR_DISK_WRITE_PATH:-}"
if [ -n "$DISK_WRITE_PATH" ]; then
  DEST_DIR=$(dirname "$DISK_WRITE_PATH")
  DEST_AVAIL_KB=$(df -k "$DEST_DIR" 2>/dev/null | awk 'NR==2{print $4}' || true)
  COPY_NEEDED_KB=$(( OUT_SIZE / 1024 + 65536 ))  # exact output + 64MB headroom
  if ! [[ "${DEST_AVAIL_KB:-}" =~ ^[0-9]+$ ]] || [ "${DEST_AVAIL_KB:-0}" -le 0 ]; then
    log "WARN: unable to determine destination disk free space for $DEST_DIR"
    [ -n "${TRANSCODARR_SPACE_FAIL_KIND_FILE:-}" ] && printf "dest:%s\n" "$COPY_NEEDED_KB" > "$TRANSCODARR_SPACE_FAIL_KIND_FILE" 2>/dev/null || true
    exit 75
  fi
  if [ "${DEST_AVAIL_KB:-0}" -gt 0 ] && (( DEST_AVAIL_KB < COPY_NEEDED_KB )); then
    log "WARN: Destination disk low for copy-back — need ${COPY_NEEDED_KB}KB, have ${DEST_AVAIL_KB}KB"
    [ -n "${TRANSCODARR_SPACE_FAIL_KIND_FILE:-}" ] && printf "dest:%s\n" "$COPY_NEEDED_KB" > "$TRANSCODARR_SPACE_FAIL_KIND_FILE" 2>/dev/null || true
    exit 75
  fi
fi

if [[ "${TRANSCODARR_SWEET16_TEST:-false}" == "true" ]]; then
  log "SWEET16: output validated, keeping in tmp dir — NOT replacing original"
  TMP_OUT=""
elif [[ "${TRANSCODARR_DEADHEAD_TEST:-false}" == "true" ]]; then
  log "DEADHEAD: output validated, keeping in tmp dir — NOT replacing original"
  TMP_OUT=""
elif [[ "${TRANSCODARR_ALMOSTHOME_TEST:-false}" == "true" ]]; then
  # ALMOSTHOME: copy to destination disk but DO NOT replace original.
  # The .replace.tmp file sits alongside the original for review.
  # Tracked in almosthome-copies.tsv for cleanup or finish-replace later.
  if [ -n "$TMP_DIR" ] && [ -n "$DISK_WRITE_PATH" ]; then
    REPLACE_TMP="${DISK_WRITE_PATH}.replace.tmp.$$"
    _copy_back_or_fail "$TMP_OUT" "$REPLACE_TMP"
    rm -f "$TMP_OUT"
    TMP_OUT=""
    printf '%s\t%s\t%s\t%s\t%s\n' \
      "$(date -Iseconds)" "$SERVICE" "$DISK_WRITE_PATH" "$REPLACE_TMP" "pending" \
      >> "$STATE_DIR/almosthome-copies.tsv" 2>/dev/null || true
    log "ALMOSTHOME: copied to disk — $REPLACE_TMP (original untouched)"
    REPLACE_TMP=""  # prevent cleanup trap from deleting the copy
  elif [ -n "$TMP_DIR" ]; then
    REPLACE_TMP="${INPUT}.replace.tmp.$$"
    _copy_back_or_fail "$TMP_OUT" "$REPLACE_TMP"
    rm -f "$TMP_OUT"
    TMP_OUT=""
    printf '%s\t%s\t%s\t%s\t%s\n' \
      "$(date -Iseconds)" "$SERVICE" "$INPUT" "$REPLACE_TMP" "pending" \
      >> "$STATE_DIR/almosthome-copies.tsv" 2>/dev/null || true
    log "ALMOSTHOME: copied via FUSE — $REPLACE_TMP (original untouched)"
    REPLACE_TMP=""  # prevent cleanup trap from deleting the copy
  else
    # No TMP_DIR — encode was written next to original, just keep it
    log "ALMOSTHOME: encode at $TMP_OUT (original untouched)"
    printf '%s\t%s\t%s\t%s\t%s\n' \
      "$(date -Iseconds)" "$SERVICE" "$INPUT" "$TMP_OUT" "pending" \
      >> "$STATE_DIR/almosthome-copies.tsv" 2>/dev/null || true
    TMP_OUT=""
  fi
elif [ -n "$TMP_DIR" ] && [ -n "$DISK_WRITE_PATH" ]; then
  # Branch A — SSD → direct disk
  REPLACE_TMP="${DISK_WRITE_PATH}.replace.tmp.$$"
  _copy_back_or_fail "$TMP_OUT" "$REPLACE_TMP"
  rm -f "$TMP_OUT"; TMP_OUT=""
  _place_output "$REPLACE_TMP" "$DISK_WRITE_PATH" "$FINAL_DISK"
  REPLACE_TMP=""
  log "Replaced original file (SSD → direct disk)"
elif [ -n "$TMP_DIR" ]; then
  # Branch B — TMP_DIR FUSE fallback
  REPLACE_TMP="${INPUT}.replace.tmp.$$"
  _copy_back_or_fail "$TMP_OUT" "$REPLACE_TMP"
  rm -f "$TMP_OUT"; TMP_OUT=""
  _place_output "$REPLACE_TMP" "$INPUT" "$FINAL_FUSE"
  REPLACE_TMP=""
  log "Replaced original file (via SSD tmp)"
elif [ -n "${TRANSCODARR_DISK_WRITE_PATH:-}" ]; then
  # Branch C — no TMP_DIR, direct disk (atomic mv on same filesystem)
  _place_output "$TMP_OUT" "$TRANSCODARR_DISK_WRITE_PATH" "$FINAL_DISK"
  TMP_OUT=""
  log "Replaced original file (direct disk)"
else
  # Branch D — no TMP_DIR, FUSE
  _place_output "$TMP_OUT" "$INPUT" "$FINAL_FUSE"
  TMP_OUT=""
  log "Replaced original file"
fi

# Fix ownership so arr apps (PUID/PGID) can manage the file. After a rename the
# file lives at NEW_READ_PATH (the on-disk dest _place_output wrote), not the
# original — chown that, on whichever namespace it landed.
MEDIA_UID="${TRANSCODARR_MEDIA_UID:-99}"
MEDIA_GID="${TRANSCODARR_MEDIA_GID:-100}"
if [ -f "$NEW_READ_PATH" ]; then
  chown "${MEDIA_UID}:${MEDIA_GID}" "$NEW_READ_PATH" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# Step 8 — Record success
# ---------------------------------------------------------------------------
# Dry Run / ALMOSTHOME: leave the .job file in /queue AND skip the
# processed.tsv entry. The encode+validate happened, but no state was
# mutated (original file untouched). User can disable dry run + restart
# (Valkey flush clears tc:seen) and the .job will re-dispatch for a
# real run. The dry-run output is tracked in almosthome-copies.tsv
# separately. Notify_plex/notify_arr_rescan are gated internally by
# _is_any_test_mode, so they're already no-ops here.
if [[ "${TRANSCODARR_ALMOSTHOME_TEST:-false}" == "true" ]]; then
  log "=== Worker complete ($MODE, dry run — .job kept, processed.tsv untouched) ==="
  exit 0
fi

# Old-path state clears (what the *arr enqueued under the original name).
cleanup_job_files_for_path "$INPUT"
failed_display_remove_path "$INPUT" || true
failed_hash_remove_path "$INPUT" || true
# RENAME state migration runs ONLY on a real (non-test) replace. SWEET16 and
# DEADHEAD fall through to this epilogue (only ALMOSTHOME early-exits above), and
# on those test modes the file was NOT renamed — it still sits at $INPUT. Without
# the test-mode exclusion they would delete still-valid OLD-path flag + rail rows
# and write a phantom NEW_PATH processed.tsv row.
if [ "$RENAME" = true ] && ! _is_any_test_mode; then
  clear_path_flags "$INPUT" || true     # phantom Flagged-tab row (P10)
  _rename_clear_old_rails "$INPUT"       # robust broad + narrow rail clear
  record_processed "$MODE" "$NEW_PATH" "$OUT_SIZE"   # renamed path + exact OUT_SIZE
else
  record_processed "$MODE"               # today's behavior (path=$INPUT, stat $INPUT)
fi

# Phase 4: cache the just-encoded output. After atomic replace, $INPUT
# holds the new bytes; sample-hashing it now gives the hash of the
# fully-classified-by-construction output (video=target_codec, audio=
# libfdk_aac LC, ≤MAX_CHANNELS, ≤MAX_W×MAX_H, single selected track in
# target language — every classifier dimension satisfied by the
# encode itself). Setting-hash invalidation handles policy drift, so
# this row only authorizes a skip under settings equivalent to those
# the encode was made for. fully_classified_record dedupes by
# (path, hash).
if [ "$RENAME" = true ] && ! _is_any_test_mode; then
  fully_classified_record "$NEW_READ_PATH" "$NEW_PATH"   # loop-stopper, renamed path
else
  fully_classified_record "$INPUT" "$INPUT"              # today's behavior
fi

# ---------------------------------------------------------------------------
# Step 9 — Notify Plex and arr app
# ---------------------------------------------------------------------------
notify_plex "$NEW_PATH"
notify_arr_rescan

log "=== Worker complete ($MODE) ==="
exit 0
