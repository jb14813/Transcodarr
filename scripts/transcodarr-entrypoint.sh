#!/bin/bash
set -euo pipefail

# Transcodarr — automated media transcoder
# Container entrypoint: Valkey streaming pipeline
# Stages: job bridge → API intake → ffprobe pool → disk wrangler → load balancer → worker consumers

STATE_DIR="${TRANSCODARR_STATE_DIR:-/state}"
QUEUE_DIR="${TRANSCODARR_QUEUE_DIR:-/queue}"
GPU_WORKERS="${TRANSCODARR_GPU_WORKERS:-8}"
CPU_WORKERS="${TRANSCODARR_CPU_WORKERS:-16}"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORKER="$SCRIPT_DIR/transcodarr-worker.sh"
QUEUE_BUILDER="$SCRIPT_DIR/transcodarr-queue.sh"

# Shared helpers: fingerprint, is_processed, classify_file_probe, Valkey q_* functions
source "$SCRIPT_DIR/transcodarr-lib.sh"
source "$SCRIPT_DIR/transcodarr-startup-jobs.sh"
source "$SCRIPT_DIR/transcodarr-job-bridge.sh"

# Cache the two dispatch Lua scripts once at startup. These used to be inline
# heredocs inside dispatch_eligible — extracted to files so the unit test
# (tests/ssd-lease-unit.sh) can exercise the exact same admission logic
# against a throwaway valkey without duplicating the Lua.
LUA_ADMIT_AND_MOVE=$(cat "$SCRIPT_DIR/lua/admit_and_move.lua")
LUA_MOVE_ONLY=$(cat "$SCRIPT_DIR/lua/move_only.lua")

# Shared job tracking array — used by startup_job_bridge and job_bridge
declare -A JOB_TRACKED

mkdir -p "$STATE_DIR" "$QUEUE_DIR"
chmod 1777 "$QUEUE_DIR" 2>/dev/null || true

# Failed-hash gate (Phase 5B introduced a policy header to this rail):
# header mismatch → truncate-and-rewrite, invalidating pre-5B
# append-only rows. Before 5B the file had no header at all, so the
# first 5B boot wipes it once and starts fresh with the 7-column row
# schema (path, hash, reason, ts, size, mtime, ctime).
failed_hashes_validate_or_reset

# Verified-hash gate (Phase 1, schema v2 in Phase 5B): validate or
# wipe /state/verified-hashes.tsv against the current policy header.
# Mismatch → file truncated + rewritten with the new header,
# invalidating stale verdicts cleanly.
verified_hashes_validate_or_reset

# Fully-classified cache (Phase 3, schema v2 in Phase 5B): same shape
# as above but a separate rail covering EVERY classifier dimension
# (video codec, resolution, audio profile/codec/channels/language,
# multi-audio). Header includes the env-derived classifier inputs
# normalized to classifier semantics; any env change invalidates the
# rail. The narrow rail is independent — wiping one doesn't touch
# the other.
fully_classified_hashes_validate_or_reset

log() { echo "[transcodarr] $(date '+%H:%M:%S') $1"; }

supervise_stage() {
  local label="$1"
  shift
  (
    while true; do
      stage_rc=0
      "$@" || stage_rc=$?
      log "WARN: $label exited unexpectedly (rc=$stage_rc), restarting in 5s"
      sleep 5
    done
  ) &
  SUPERVISED_PID=$!
}

seed_language_models() {
  local model_dir="${TRANSCODARR_LANGUAGE_MODEL_DIR:-/models}"
  local bundled_dir="/opt/transcodarr/models"
  mkdir -p "$model_dir" 2>/dev/null || true
  [ -d "$model_dir" ] || return 0
  [ -d "$bundled_dir" ] || return 0

  local src dst base copied=0
  for src in "$bundled_dir"/ggml-*.bin; do
    [ -f "$src" ] || continue
    base=$(basename "$src")
    dst="$model_dir/$base"
    if [ ! -f "$dst" ]; then
      cp -a "$src" "$dst" && copied=$((copied + 1))
    fi
  done
  if [ "$copied" -gt 0 ]; then
    log "Seeded $copied bundled language model(s) into $model_dir"
  fi
}

seed_language_models

is_uint() {
  case "${1:-}" in
    ""|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

# disk_is_ignored — fast SISMEMBER against the runtime ignored-disks set.
# The set is seeded at startup from TRANSCODARR_IGNORED_DISKS (CSV), then
# kept in sync by the API when Settings saves disks.ignored. This helper is
# intentionally used only by the LB dispatch gate.
disk_is_ignored() {
  local disk="${1:-}"
  [ -z "$disk" ] && return 1
  local rv
  rv=$($QUEUE_CLI SISMEMBER tc:disk:ignored "$disk" 2>/dev/null || echo 0)
  [ "$rv" = "1" ]
}

dest_space_needed_key() {
  local filepath="$1" file_hash
  file_hash=$(printf '%s' "$filepath" | md5sum 2>/dev/null | cut -d' ' -f1 || true)
  [ -n "$file_hash" ] && printf 'tc:destspace:needed:%s' "$file_hash"
}

space_fail_kind_key() {
  local filepath="$1" file_hash
  file_hash=$(printf '%s' "$filepath" | md5sum 2>/dev/null | cut -d' ' -f1 || true)
  [ -n "$file_hash" ] && printf 'tc:spacefail:kind:%s' "$file_hash"
}

space_retry_timeout_for_kind() {
  case "$1" in
    dest) printf '%s\n' "${TRANSCODARR_DEST_SPACE_RETRY_TIMEOUT:-${TRANSCODARR_SPACE_RETRY_TIMEOUT:-0}}" ;;
    ssd)  printf '%s\n' "${TRANSCODARR_SSD_SPACE_RETRY_TIMEOUT:-${TRANSCODARR_SPACE_RETRY_TIMEOUT:-0}}" ;;
    *)    printf '%s\n' "${TRANSCODARR_SPACE_RETRY_TIMEOUT:-0}" ;;
  esac
}

# ── Discover available disk mounts ────────────────────────────────────────
AVAILABLE_DISKS=()
for d in /disk*/; do
  [ -d "$d" ] && AVAILABLE_DISKS+=("$(basename "$d")")
done

if [ ${#AVAILABLE_DISKS[@]} -gt 0 ]; then
  log "Discovered ${#AVAILABLE_DISKS[@]} disk mounts: ${AVAILABLE_DISKS[*]}"
else
  log "No direct disk mounts found — using FUSE fallback for all files"
fi

# ── Start embedded Valkey ──────────────────────────────────────────────────
valkey-server --daemonize yes --port 6379 \
  --save "" --appendonly no \
  --maxmemory 256mb --maxmemory-policy noeviction \
  --loglevel warning --bind 127.0.0.1
until valkey-cli ping 2>/dev/null | grep -q PONG; do sleep 0.1; done
log "Valkey ready"

# Phase 5D — cache hygiene. Best-effort prune of stale/duplicate
# rows from the three TSVs BEFORE the Valkey index rebuild. Pulls a
# current-library path snapshot from Sonarr+Radarr; rows missing
# from BOTH the Arr snapshot AND disk get pruned, rows present in
# either get kept. Duplicate paths compact to last-row-wins.
#
# Conservative on purpose: a transient Arr failure must NOT wipe the
# durable TSV. The helper returns 0 in every "couldn't get a clean
# snapshot" case and logs WARN — boot continues with the unchanged
# TSV and the next rebuild_rail_index reads it as-is.
#
# Runs BEFORE rebuild_rail_index so Valkey never indexes rows that
# were just pruned from the TSV.
prune_cache_rows_against_arr_snapshot || true

# Phase 5A — admission index rebuild. MUST run AFTER Valkey is ping-
# responsive (it issues DEL / EVAL / SET against the running server).
# Earlier placement before valkey-server started caused all three
# rails to silently fail to populate, leaving the admission dispatcher
# permanently on the legacy awk-scan fallback. Each rail rebuild is
# `|| true` so a single rail's failure doesn't abort boot — but
# rebuild_rail_index now logs WARN for every failure mode so the
# silent-failure trap is closed.
#
# Runs before the probe pool spawns so workers see a populated index
# from their first admission attempt (when rebuild succeeds).
rebuild_rail_index failed           "$(failed_hash_tsv_path)"               "failed"                    || true
rebuild_rail_index verified         "$(verified_hashes_tsv_path)"           "verified:aac_lc"           || true
rebuild_rail_index fully_classified "$(fully_classified_hashes_tsv_path)"   "verified:fully_classified" || true

# Phase 7-followup: hydrate the classifier-owned flag index from the
# durable TSV snapshot. After this, tc:flags:ready=1 and the API
# serves through serve_tsv against the on-disk TSV (regenerating from
# the HSET on the next dirty write). Empty TSV -> zero flags state.
rebuild_flag_index_from_tsv || true

# ── NVIDIA EGL Vulkan ICD setup (Phase 6C-followup step 1) ─────────────────
# Headless containers fail VK_ERROR_INCOMPATIBLE_DRIVER on the upstream
# NVIDIA ICD because library_path points at libGLX_nvidia.so.0 (X11
# client lib). Generate a derivative ICD that points at
# libEGL_nvidia.so.0 instead, then VK_ICD_FILENAMES it. Without this,
# libplacebo can't see the GPU and the capability probe below writes
# tc:capabilities.hdr_tonemap_path=opencl, blocking the metadata-aware
# dynamic-HDR tonemap path. Helper is conservative — silently no-ops
# on non-NVIDIA hosts / missing libEGL / malformed upstream ICD.
# MUST run BEFORE the capability probe so its libplacebo test inherits
# the new VK_ICD_FILENAMES env.
setup_nvidia_egl_vulkan_icd

# ── Encoder capability probe ───────────────────────────────────────────────
# Test-encode a 1-frame clip with each HW encoder to determine what the
# running hardware + driver + ffmpeg build combination can actually use.
# Results go into Valkey hash `tc:capabilities`; API exposes it via
# /api/capabilities so the GUI can hide target_codec options that would
# fail at encode time. Background-run to avoid blocking startup if the
# GPU is momentarily busy — GUI polls the API, a few seconds of "all
# caps unknown" on first page load is acceptable.
bash "$SCRIPT_DIR/transcodarr-probe-capabilities.sh" 2>&1 | \
  while IFS= read -r line; do log "$line"; done &

# ── Language backlog backfill (Plan C §8.1) ───────────────────────────────
# Release the existing no_eng_audio backlog so the feature heals it. Runs
# AFTER the failed-rail index rebuild (rebuild_rail_index, ~line 144) AND
# after the (backgrounded) capability probe is launched — the helper waits
# (bounded) for lang_backend before releasing anything, so the probe need
# not have finished yet. release_admission's hash-row removal sees a ready
# index. Capability + enabled gated inside the helper (echoes 0 + no-ops
# when lang_backend is unusable). Admission-only: display rows are left
# intact until detection resolves them.
if [ "${TRANSCODARR_LANGUAGE_ENABLED:-false}" = "true" ]; then
  _lang_backfill_n=$(lang_backfill_no_eng_audio 2>/dev/null || echo 0)
  log "Language backlog backfill released ${_lang_backfill_n} no-eng-audio MKV file(s)"
  unset _lang_backfill_n
fi

# ── Shutdown trap ──────────────────────────────────────────────────────────
shutdown() {
  log "Shutting down..."
  # Phase 7-followup: snapshot the classifier-owned flag index back to
  # the durable TSV BEFORE Valkey is torn down. Best-effort — if the
  # snapshot fails (Valkey already gone, or an in-flight mutation
  # races us), the last-known-good TSV is preserved. The dirty bit
  # is intentionally NOT cleared here; the next entrypoint's
  # rebuild_flag_index_from_tsv resets it from the on-disk TSV.
  snapshot_flag_index_to_tsv 2>/dev/null || true
  kill $(jobs -p) 2>/dev/null
  wait 2>/dev/null
  valkey-cli SHUTDOWN NOSAVE 2>/dev/null
}
trap shutdown SIGTERM SIGINT

# ── Clean up stale artifacts from previous run (crash/SIGKILL recovery) ────
if [ -d "$STATE_DIR/locks" ]; then
  rm -rf "$STATE_DIR/locks"
  log "Cleared stale locks from previous run"
fi
# Build the list of directories to sweep for orphan tmp files. Every entry
# must exist at this point — `find` will error on missing paths, and with
# `set -o pipefail` the pipeline below would fail the whole entrypoint.
# Deployments that mount only one of /movies or /tv must skip the missing
# one instead of crashing.
TMP_CLEAN_DIRS=""
for candidate in /movies /tv; do
  [ -d "$candidate" ] && TMP_CLEAN_DIRS="$TMP_CLEAN_DIRS $candidate"
done
if [ -n "${TRANSCODARR_TMP_DIR:-}" ] && [ -d "${TRANSCODARR_TMP_DIR}" ]; then
  TMP_CLEAN_DIRS="$TMP_CLEAN_DIRS ${TRANSCODARR_TMP_DIR}"
fi
# Direct disk mounts — dot-prefixed tmp files live here when TMP_DIR is unset
for d in "${AVAILABLE_DISKS[@]}"; do
  [ -d "/${d}" ] && TMP_CLEAN_DIRS="$TMP_CLEAN_DIRS /${d}"
done
if [ -n "$TMP_CLEAN_DIRS" ]; then
  ORPHAN_COUNT=$(find $TMP_CLEAN_DIRS \( -name '*.transcode.tmp.*' -o -name '*.replace.tmp.*' -o -name '.*.transcode.tmp.*' -o -name '.*.replace.tmp.*' \) -delete -print 2>/dev/null | wc -l)
  if [ "$ORPHAN_COUNT" -gt 0 ]; then
    log "Cleaned $ORPHAN_COUNT orphaned temp files"
  fi
fi

# ── Dashboard progress ─────────────────────────────────────────────────────
update_progress() {
  cat > "$STATE_DIR/progress.txt" <<EOF
phase:       $1
status:      $2
gpu_workers: $GPU_WORKERS
cpu_workers: $CPU_WORKERS
updated:     $(date '+%Y-%m-%d %H:%M:%S')
EOF
}

# ── Stage 3: ffprobe pool ─────────────────────────────────────────────────
# N persistent workers consuming from tc:candidates:resolved:ready
# (and the import variant). Wrangler has already enriched each candidate
# with disk_name + disk_read_path; ffprobe uses disk_read_path for all
# I/O (stat, fingerprint, gates, classify) and filepath for identity
# (cache keys, processed.tsv rows, downstream LB payload). Phase 7 swap.

ffprobe_pool() {
  local workers="${TRANSCODARR_VALIDATE_WORKERS:-16}"
  log "Starting $workers ffprobe workers"
  for (( i=0; i<workers; i++ )); do
    ffprobe_worker "$i" &
  done
  wait
}

ffprobe_worker() {
  local worker_id="$1"
  $QUEUE_CLI INCR tc:pool:probe:active > /dev/null 2>&1
  trap '$QUEUE_CLI DECR tc:pool:probe:active > /dev/null 2>&1' EXIT
  while true; do
    # Check import queue first (priority), then bulk
    local item="" source_queue=""
    item=$(q_pop_nonblock tc:candidates:resolved:import:ready)
    if [ -n "$item" ]; then
      source_queue="tc:candidates:resolved:import:ready"
    else
      item=$(q_pop tc:candidates:resolved:ready 5)
      [ -z "$item" ] && continue
      item=$(echo "$item" | tail -1)
      source_queue="tc:candidates:resolved:ready"
    fi
    [ -z "$item" ] && continue

    # Parse 12-field resolved payload. Positions 4-6 (vcodec|ach|acount)
    # are empty placeholders the wrangler emitted — classify fills them
    # below. Position 12 is the internal `reasons` field; it never
    # leaves this function (stripped before LB push).
    local service filepath arr_id _vcodec _ach _acount disk_name disk_read_path input_size_kb first_space_fail_ts route reasons
    IFS='|' read -r service filepath arr_id _vcodec _ach _acount disk_name disk_read_path input_size_kb first_space_fail_ts route reasons <<< "$item"

    # Failed-hash gate (spec r9): skip if this exact bytes already
    # failed. Placed BEFORE q_try_mark because the existing tc:seen
    # dedupe would otherwise consume same-session retries before the
    # gate runs. Direct Queue (/api/direct/...) bypasses this entire
    # function by injecting straight to LB lanes; tc-queue-job.sh
    # does go through the bridge → probe pool and IS gated here.
    # IO/identity split: disk_read_path for the stat-based fingerprint,
    # filepath as the canonical library/cache identity.
    if failed_hash_should_skip "$disk_read_path" "$filepath"; then
      log "skipped (failed-hash match): $filepath"
      $QUEUE_CLI INCR tc:skip:failed_hash > /dev/null 2>&1 || true
      cleanup_job_files_for_path "$filepath"
      q_ack "$source_queue" "$item"
      continue
    fi

    # Fully-classified cache (Phase 3): broad rail. Whole-skip the
    # classifier when the file's bytes were previously classified as
    # result=skip under the SAME classifier policy. Reasons NOT consulted
    # — the broad fact is independently sufficient. Failed-hash above
    # still catches genuinely-bad bytes first. Direct Queue (/api/direct/*)
    # bypasses admission by topology and is never gated here; tc-queue-job.sh
    # priority imports DO flow through this gate and can be short-circuited.
    if fully_classified_should_skip "$disk_read_path" "$filepath"; then
      log "skipped (verified:fully_classified match): $filepath"
      $QUEUE_CLI INCR tc:skip:verified_fully_classified > /dev/null 2>&1 || true
      cleanup_job_files_for_path "$filepath"
      q_ack "$source_queue" "$item"
      continue
    fi

    # Verified-hash gate (Phase 1): whole-skip the classifier ONLY when
    # the queueing reason was solely `aac_profile_unknown`. Any other
    # reason (resolution, channels, lang, codec_mismatch, audio_codec_unknown,
    # multi_audio) means classify_file_probe must run regardless of any
    # cached AAC-LC fact — a narrow fact cannot authorize broad action.
    # Empty reasons (imports / .job ingest) never match this literal.
    if [ "$reasons" = "aac_profile_unknown" ] && verified_hash_should_skip "$disk_read_path" "$filepath"; then
      log "skipped (verified:aac_lc match): $filepath"
      $QUEUE_CLI INCR tc:skip:verified_aac_lc > /dev/null 2>&1 || true
      # Auto-promote to the broad rail (Phase 4): the narrow gate firing
      # means (a) cache has verified:aac_lc for this hash AND (b) queue
      # reasons confirm everything else is fine per Radarr/Sonarr
      # metadata. That's the same fact as a fully-classified row,
      # derived a different way. Recording it lets the broad gate fire
      # first on future restarts (no narrow lookup needed), and over
      # cycles the verified-cache count converges to broad-gate hits.
      fully_classified_record "$disk_read_path" "$filepath"
      cleanup_job_files_for_path "$filepath"
      q_ack "$source_queue" "$item"
      continue
    fi

    # Dedupe: one canonical key per file (filepath:size:inode).
    # On startup, imports claim before bulk. During operation, imports get
    # first shot. Late imports that arrive after bulk claimed are deduped —
    # their .job persists and is cleaned by worker success or next restart.
    # Fingerprint reads stat() from disk_read_path (direct disk, fast)
    # but the dedupe key keys by filepath (library identity).
    local fp
    fp=$(fingerprint "$disk_read_path")
    if ! q_try_mark "${filepath}:${fp}"; then
      q_ack "$source_queue" "$item"
      continue
    fi

    # Probe and classify. classify_file_probe takes (identity, ..., io_path):
    # filepath is the identity that comes back in the result line; the
    # 4th arg routes ffprobe I/O to disk_read_path (direct disk).
    local result_line
    result_line=$(classify_file_probe "$filepath" "$service" "$arr_id" "$disk_read_path" 2>/dev/null) || true
    if [ -z "$result_line" ]; then
      # Probe failed (file missing, corrupt, no video stream) — leave .job pending
      q_ack "$source_queue" "$item"
      continue
    fi

    # Phase 7-followup: read all 15 result-line fields. The trailing
    # 7 (pix_fmt onward) feed compute_flags_for_file below; they
    # are derived from the actual file via ffprobe/stat on disk_read_path.
    local _fp _svc _aid result vcodec ach acount verdict \
          pix_fmt field_order duration_sec file_size_bytes alang_raw vwidth vheight
    # Tab is IFS whitespace in bash, so `IFS=$'\t' read ...` collapses
    # empty fields. Preserve empty alang_raw by translating to a
    # non-whitespace delimiter before read.
    local _result_read_line="${result_line//$'\t'/$'\037'}"
    IFS=$'\037' read -r _fp _svc _aid result vcodec ach acount verdict \
      pix_fmt field_order duration_sec file_size_bytes alang_raw vwidth vheight \
      <<< "$_result_read_line"

    # Record AAC-LC fact when the classifier asserts it. Independent of
    # result: even a result=cpu file (e.g. resolution too big) can have
    # AAC-LC audio bytes, and recording now means a future restart where
    # only the AAC reason remains can whole-skip via the verified gate.
    # Transition cleanup: if the verdict is NOT verified:aac_lc, drop
    # any stale row this path had on the verified rail. Without this,
    # a file that was once AAC-LC but is now (e.g. after a retag) DTS
    # would keep its stale verified row forever — harmless for skip
    # correctness (statfp mismatch blocks the skip) but pollutes the
    # rail and burns ffprobe cycles every scan. SISMEMBER fast-path
    # keeps the remove a no-op when the path isn't in the rail.
    if [ "$verdict" = "verified:aac_lc" ]; then
      verified_hash_record "$disk_read_path" "$filepath" "$verdict"
    else
      verified_hash_remove_path "$filepath" || true
    fi

    # Record fully-classified fact when the classifier returned skip —
    # i.e. EVERY classifier dimension (video codec, resolution, audio
    # profile/codec/channels/language, multi-audio) passed under the
    # current settings. This is the broad-rail record paired with the
    # broad-gate skip above. Phase 3. Transition cleanup mirrors the
    # verified rail above: if result is no longer skip, drop the stale
    # broad-rail row so future scans don't re-probe the same path
    # forever just to compare against an obsolete statfp.
    if [ "$result" = "skip" ]; then
      fully_classified_record "$disk_read_path" "$filepath"
    else
      fully_classified_remove_path "$filepath" || true
    fi

    # Phase 7-followup: replace this path's flag set from current file
    # truth. Empty stdout -> set_path_flags HDELs the field. One call,
    # one atomic write, current-state semantics. Source of truth is the
    # file itself (via classify_file_probe), not Arr metadata.
    local _flag_set
    _flag_set=$(compute_flags_for_file \
      --service "$service" --path "$filepath" \
      --vwidth "$vwidth" --vheight "$vheight" \
      --ach "$ach" --acount "$acount" --alang "$alang_raw" \
      --pix_fmt "$pix_fmt" --field_order "$field_order" \
      --duration "$duration_sec" --file_size "$file_size_bytes")
    set_path_flags "$filepath" "$_flag_set"

    # ── Language-detection divert (Plan C) ─────────────────────────────
    # Restart-applied opt-in. When enabled AND the file is a supported
    # language-tag container AND the capability probe reports a usable
    # lang_backend AND classify-time audio analysis says the file has no
    # known-language non-commentary track but >=1 untagged non-commentary
    # candidate, hand it to the lang_detect pool instead of failing it
    # no_eng_audio in the worker.
    # The payload is enriched with the diagnostic fields lang_detect
    # needs to write a complete record_failure_row without re-probing.
    # orig_size is BYTES (file_size_bytes), matching the worker origSize
    # column — NOT input_size_kb.
    if [ "${TRANSCODARR_LANGUAGE_ENABLED:-false}" = "true" ] \
       && lang_container_supported "$filepath" \
       && [ -n "$($QUEUE_CLI HGET tc:capabilities lang_backend 2>/dev/null)" ] \
       && audio_needs_lang_detection "$disk_read_path"; then
      log "lang-divert: $filepath -> tc:lang:ready"
      q_push tc:lang:ready "${service}|${filepath}|${disk_read_path}|${disk_name}|${vcodec}|${ach}|${file_size_bytes}"
      q_ack "$source_queue" "$item"
      continue
    fi

    # Build 11-field LB payload — same shape today's wrangler emitted.
    # Fields 4-6 come from classify; positions 7-11 (disk_name through
    # route) come straight from the resolved payload. Field 12 (reasons)
    # is dropped — internal-only.
    local lb_payload="${service}|${filepath}|${arr_id}|${vcodec}|${ach}|${acount}|${disk_name}|${disk_read_path}|${input_size_kb}|${first_space_fail_ts}|${route}"

    if [ "$route" = "import" ]; then
      case "$result" in
        gpu) q_push tc:lb:gpu:import:ready "$lb_payload" ;;
        cpu) q_push tc:lb:cpu:import:ready "$lb_payload" ;;
        *)
          # File already meets spec — clean up any .job files
          cleanup_job_files_for_path "$filepath"
          # Log to processed.tsv so it shows in the Done list. Mode
          # `already_ok` distinguishes no-work skips from real transcodes
          # (gpu/cpu/audio_only) so the GUI can label them clearly.
          # disk_name comes from the resolved payload — no re-resolution.
          local _ok_size
          _ok_size=$(stat -c%s "$disk_read_path" 2>/dev/null || stat -c%s "$filepath" 2>/dev/null || echo 0)
          printf '%s\t%s\talready_ok\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$filepath" "$service" "$vcodec" "${ach:-0}" "$_ok_size" "$_ok_size" \
            "$(date -Iseconds)" "${disk_name:-unknown}" \
            >> "$STATE_DIR/processed.tsv" 2>/dev/null
          ;;
      esac
    else
      case "$result" in
        gpu) q_push tc:lb:gpu:ready "$lb_payload" ;;
        cpu) q_push tc:lb:cpu:ready "$lb_payload" ;;
      esac
    fi

    q_ack "$source_queue" "$item"
  done
}

# ── Stage 3.5: Language-detection pool (Plan C) ───────────────────────────
# Spawned at startup only when TRANSCODARR_LANGUAGE_ENABLED=true AND the
# capability probe reported a usable tc:capabilities lang_backend. Pops
# tc:lang:ready items diverted by ffprobe_worker, runs whisper.cpp over
# the untagged non-commentary candidate tracks, and resolves each file:
#   English  -> tag eng + release_admission + remove display + hash rows
#               + priority .job requeue
#   Foreign  -> record_failure_row wrong_lang_<code> (col-4 replace)
#   Undetected/error -> record_failure_row lang_undetected (observe)
# q_ack always fires (BRPOPLPUSH visibility via tc:lang:processing).

lang_detect_pool() {
  local workers="${TRANSCODARR_LANGUAGE_WORKERS:-2}"
  log "Starting $workers language-detection workers"
  for (( i=0; i<workers; i++ )); do
    lang_detect_worker "$i" &
  done
  wait
}

lang_apply_audio_tag() {
  local disk_read_path="$1" ordinal="$2" lang_code="$3" worker_id="${4:-0}"
  local ext="${disk_read_path##*.}"
  ext="${ext,,}"

  case "$ext" in
    mkv)
      local track_num=$(( ordinal + 1 ))
      mkvpropedit "$disk_read_path" --edit "track:a${track_num}" \
        --set "language=${lang_code}" --tags all:
      ;;
    mp4|m4v|mov)
      local dir base tmp_path readback readback_norm
      dir=$(dirname -- "$disk_read_path")
      base=$(basename -- "$disk_read_path")
      tmp_path="${dir}/.${base}.tc-lang-${worker_id}-$$-${ordinal}.tmp.${ext}"
      rm -f -- "$tmp_path" 2>/dev/null || true
      if ! ffmpeg -nostdin -hide_banner -loglevel error -y \
           -i "$disk_read_path" \
           -map 0 -dn -c copy -map_metadata 0 -map_chapters 0 \
           -metadata:s:a:${ordinal} "language=${lang_code}" \
           "$tmp_path"; then
        rm -f -- "$tmp_path" 2>/dev/null || true
        return 1
      fi
      readback=$(ffprobe -v error -select_streams "a:${ordinal}" \
        -show_entries stream_tags=language \
        -of default=noprint_wrappers=1:nokey=1 "$tmp_path" 2>/dev/null || true)
      readback_norm=$(normalize_audio_language_tag "$readback")
      if [ "$readback_norm" != "$lang_code" ]; then
        rm -f -- "$tmp_path" 2>/dev/null || true
        return 1
      fi
      chmod --reference="$disk_read_path" "$tmp_path" 2>/dev/null || true
      chown --reference="$disk_read_path" "$tmp_path" 2>/dev/null || true
      if ! mv -f -- "$tmp_path" "$disk_read_path"; then
        rm -f -- "$tmp_path" 2>/dev/null || true
        return 1
      fi
      ;;
    *)
      return 2
      ;;
  esac
}

lang_apply_audio_tag_locked() {
  local disk_read_path="$1"
  local lock_dir lock_hash tag_lock rc
  lock_dir="${TRANSCODARR_STATE_DIR:-/state}/locks"
  lock_hash=$(printf '%s' "$disk_read_path" | md5sum | cut -d' ' -f1)
  tag_lock="${lock_dir}/lang-tag-${lock_hash}.lock"
  if ! mkdir -p "$lock_dir" 2>/dev/null; then
    return 1
  fi
  if ! mkdir "$tag_lock" 2>/dev/null; then
    return 3
  fi
  if lang_apply_audio_tag "$@"; then
    rc=0
  else
    rc=$?
  fi
  rm -rf "$tag_lock" 2>/dev/null || true
  return "$rc"
}

lang_requeue_priority_job() {
  local service="$1" filepath="$2"
  local jsvc="$service" jhash jns qdir jpath jtmp
  case "$filepath" in /movies/*) jsvc="radarr" ;; /tv/*) jsvc="sonarr" ;; esac
  qdir="${TRANSCODARR_QUEUE_DIR:-/queue}"
  jhash=$(printf '%s' "$filepath" | md5sum | cut -d' ' -f1)
  jns=$(date +%s%N)
  jpath="${qdir}/${jhash}_${jns}.job"
  jtmp="${jpath}.tmp.$$"
  if ! printf '%s\n%s\n' "$jsvc" "$filepath" > "$jtmp"; then
    rm -f -- "$jtmp" 2>/dev/null || true
    return 1
  fi
  if ! mv -f -- "$jtmp" "$jpath"; then
    rm -f -- "$jtmp" 2>/dev/null || true
    return 1
  fi
}

lang_detect_worker() {
  local worker_id="$1"
  $QUEUE_CLI INCR tc:pool:lang:active > /dev/null 2>&1
  trap '$QUEUE_CLI DECR tc:pool:lang:active > /dev/null 2>&1' EXIT

  local model="${TRANSCODARR_LANGUAGE_MODEL:-base}"
  local model_dir="${TRANSCODARR_LANGUAGE_MODEL_DIR:-/models}"
  local model_path="${model_dir}/ggml-${model}.bin"
  local coverage_pct="${TRANSCODARR_LANGUAGE_SAMPLE_COVERAGE_PCT:-10}"
  local head_skip="${TRANSCODARR_LANGUAGE_HEAD_SKIP:-60}"
  local tail_skip="${TRANSCODARR_LANGUAGE_TAIL_SKIP:-60}"
  local conf="${TRANSCODARR_LANGUAGE_MIN_CONFIDENCE:-0.85}"
  # CPU flag: add -ng only when the resolved backend is cpu (device=auto
  # follows tc:capabilities; an explicit device=cpu also forces it).
  local backend
  backend=$($QUEUE_CLI HGET tc:capabilities lang_backend 2>/dev/null) || backend="cpu"
  local ng_flag=""
  if [ "${TRANSCODARR_LANGUAGE_DEVICE:-auto}" = "cpu" ] || [ "$backend" = "cpu" ]; then
    ng_flag="-ng"
  fi

  while true; do
    local paused
    paused=$($QUEUE_CLI GET tc:pause 2>/dev/null || echo "")
    if [ "$paused" = "1" ]; then
      sleep 5
      continue
    fi

    local item
    item=$(q_pop tc:lang:ready 5)
    [ -z "$item" ] && continue
    item=$(echo "$item" | tail -1)
    [ -z "$item" ] && continue

    local service filepath disk_read_path disk_name vcodec channels orig_size
    IFS='|' read -r service filepath disk_read_path disk_name vcodec channels orig_size <<< "$item"

    # File moved/deleted between divert and detection -> skip + ack.
    if [ ! -f "$disk_read_path" ]; then
      log "lang-detect: source gone, skipping: $filepath"
      q_ack tc:lang:ready "$item"
      continue
    fi

    # Candidate selection over the actual file (ordinal-correct).
    local candidates
    candidates=$(lang_pick_candidates "$disk_read_path")
    if [ -z "$candidates" ]; then
      log "lang-detect: no candidates (race or all-commentary): $filepath"
      record_failure_row "$filepath" "$service" "lang_undetected" "unknown" \
        "$vcodec" "$channels" "$orig_size" "$disk_name" "$disk_read_path"
      q_ack tc:lang:ready "$item"
      continue
    fi

    # Duration once per file (drives even sample distribution). Falls back
    # to a fixed mid-window when unknown.
    local dur
    dur=$(ffprobe -v quiet -show_entries format=duration \
      -of default=noprint_wrappers=1:nokey=1 "$disk_read_path" 2>/dev/null | cut -d. -f1)
    dur="${dur:-0}"

    local sample_table
    sample_table=$(lang_build_sample_table "$dur" "$coverage_pct" "$head_skip" "$tail_skip")

    # Per-candidate detect: extract coverage-driven clips, whisper each,
    # then aggregate via lang_aggregate_candidate (majority vote + median).
    # Build the verdict-input lines "ordinal\tcode\tmedian_prob".
    local verdict_input=""
    local cand_ordinal cand_ch cand_lang
    while IFS=$'\t' read -r cand_ordinal cand_ch cand_lang; do
      [ -n "$cand_ordinal" ] || continue
      local pairs=""   # one "code prob" line per successful sample
      local section_id start ss band_start band_end
      while IFS=$'\t' read -r section_id start ss band_start band_end; do
        [ -n "$section_id" ] || continue
        local wav
        wav=$(mktemp "/tmp/tc-lang-${worker_id}-XXXXXX.wav") || continue
        if ffmpeg -nostdin -v error -ss "$start" -i "$disk_read_path" \
             -map "0:a:${cand_ordinal}" -t "$ss" -ac 1 -ar 16000 \
             -f wav -y "$wav" >/dev/null 2>&1; then
          local wout parsed
          wout=$(whisper-cli -m "$model_path" -f "$wav" -dl $ng_flag 2>&1) || true
          # Only successful parses are appended; error samples are excluded.
          if parsed=$(lang_parse_whisper_detect "$wout"); then
            pairs+="${parsed}"$'\n'
          fi
        fi
        rm -f "$wav" 2>/dev/null || true
      done <<< "$sample_table"

      # Majority vote + median over the winning code (Task 3 helper).
      local code="" median="0" agg
      if agg=$(printf '%s' "$pairs" | lang_aggregate_candidate); then
        code="${agg%% *}"; median="${agg##* }"
      fi
      verdict_input+="${cand_ordinal}"$'\037'"${code}"$'\037'"${median}"$'\n'
    done <<< "$candidates"

    # Verdict over ALL candidates with one min_confidence.
    local verdict
    verdict=$(printf '%s' "$verdict_input" | lang_verdict_over_candidates "$conf")
    local vkind vfield1 vfield2
    read -r vkind vfield1 vfield2 <<< "$verdict"

    case "$vkind" in
      english)
        # Capture the pre-tag tc:seen member BEFORE tagging — tagging
        # can change size/inode (fingerprint=size:inode), and this exact
        # member was SADD'd before classify. release_admission can only
        # remove it if we pass it in.
        local pre_seen="${filepath}:$(fingerprint "$disk_read_path")"
        local tag_rc=0
        if lang_apply_audio_tag_locked "$disk_read_path" "$vfield1" "eng" "$worker_id" >/dev/null 2>&1; then
          log "lang-detect: tagged eng audio:${vfield1}: $filepath"
          # Resolve admission FIRST (removes the pre-tag member + the
          # post-tag member), then remove the display + hash rows ourselves
          # — the requeue may classify already_ok and bypass worker-success
          # cleanup.
          release_admission "$filepath" "$disk_read_path" "$pre_seen"
          _atomic_remove_failed_display_path_inner "${TRANSCODARR_STATE_DIR:-/state}/failed-files.tsv" "$filepath" || true
          failed_hash_remove_path "$filepath" || true
          clear_path_flags "$filepath" || true
          if ! lang_requeue_priority_job "$service" "$filepath"; then
            log "WARN: lang-detect could not write priority requeue job after tag: $filepath"
            record_failure_row "$filepath" "$service" "lang_requeue_failed" "unknown" \
              "$vcodec" "$channels" "$orig_size" "$disk_name" "$disk_read_path"
          fi
        else
          tag_rc=$?
          if [ "$tag_rc" = "3" ]; then
            log "lang-detect: tag lock busy, skipping duplicate: $filepath"
            q_ack tc:lang:ready "$item"
            continue
          fi
          log "WARN: lang-detect mkvpropedit failed: $filepath"
          record_failure_row "$filepath" "$service" "lang_tag_failed" "unknown" \
            "$vcodec" "$channels" "$orig_size" "$disk_name" "$disk_read_path"
        fi
        ;;
      foreign)
        # Tag the primary (most-channels) candidate with its detected ISO
        # language (data hygiene) before recording the terminal failure.
        local tag_rc=0
        if lang_apply_audio_tag_locked "$disk_read_path" "$vfield2" "$vfield1" "$worker_id" >/dev/null 2>&1; then
          log "lang-detect: tagged foreign ${vfield1} audio:${vfield2} (data hygiene): $filepath"
        else
          tag_rc=$?
          if [ "$tag_rc" = "3" ]; then
            log "lang-detect: tag lock busy, skipping duplicate: $filepath"
            q_ack tc:lang:ready "$item"
            continue
          fi
          log "WARN: lang-detect foreign tag failed audio:${vfield2}: $filepath"
        fi
        record_failure_row "$filepath" "$service" "wrong_lang_${vfield1}" "policy_skip" \
          "$vcodec" "$channels" "$orig_size" "$disk_name" "$disk_read_path"
        ;;
      *)
        log "lang-detect: undetected: $filepath"
        record_failure_row "$filepath" "$service" "lang_undetected" "unknown" \
          "$vcodec" "$channels" "$orig_size" "$disk_name" "$disk_read_path"
        # Flag unverified_lang for manual review. set_path_flags OVERWRITES
        # the path's whole flag value, so merge: append a row only if the
        # path doesn't already carry an unverified_lang flag.
        local _ex_flags _flag_row
        _ex_flags=$($QUEUE_CLI HGET tc:flags:by_path "$filepath" 2>/dev/null || echo "")
        if ! printf '%s' "$_ex_flags" | grep -q $'\tunverified_lang\t'; then
          _flag_row="$(date -Iseconds)	${service}	unverified_lang	${filepath}	tag=lang_undetected"
          set_path_flags "$filepath" "${_ex_flags:+${_ex_flags}$'\n'}${_flag_row}"
        fi
        ;;
    esac

    q_ack tc:lang:ready "$item"
  done
}

# ── Stage 2: Disk wrangler pool (Phase 7 — runs BEFORE classify) ────────
# Consumes raw candidates from tc:candidates:{import:,}ready, resolves
# physical disk and direct-read path, then pushes a 12-field resolved
# payload to tc:candidates:resolved:{import:,}ready for the ffprobe
# pool to classify. The 12-field shape is:
#   service|filepath|arr_id|vcodec|ach|acount|disk_name|disk_read_path|input_size_kb|first_space_fail_ts|route|reasons
# Positions 4-6 (vcodec|ach|acount) are emitted EMPTY here — classify
# fills them. Position 12 (reasons) is internal to the pre-classifier
# section; ffprobe strips it before pushing the 11-field LB payload.

wrangler_pool() {
  local workers="${TRANSCODARR_WRANGLER_WORKERS:-16}"
  log "Starting $workers disk wrangler workers"
  for (( i=0; i<workers; i++ )); do
    wrangler_worker "$i" &
  done
  wait
}

wrangler_worker() {
  local worker_id="$1"
  $QUEUE_CLI INCR tc:pool:disk:active > /dev/null 2>&1
  trap '$QUEUE_CLI DECR tc:pool:disk:active > /dev/null 2>&1' EXIT
  while true; do
    # Check import queue first (priority), then bulk
    local item="" source_queue=""
    item=$(q_pop_nonblock tc:candidates:import:ready)
    if [ -n "$item" ]; then
      source_queue="tc:candidates:import:ready"
    else
      item=$(q_pop tc:candidates:ready 5)
      [ -z "$item" ] && continue
      item=$(echo "$item" | tail -1)
      source_queue="tc:candidates:ready"
    fi
    [ -z "$item" ] && continue

    # Parse 4-field raw candidate payload: service|filepath|arr_id|reasons
    local service filepath arr_id reasons
    IFS='|' read -r service filepath arr_id reasons <<< "$item"

    # Determine route from source queue
    local route="bulk"
    [ "$source_queue" = "tc:candidates:import:ready" ] && route="import"

    # Resolve disk
    local disk_name disk_read_path
    disk_name=$(resolve_disk "$filepath") || true

    if [ -z "$disk_name" ]; then
      disk_name="fuse"
      disk_read_path="$filepath"
    else
      case "$filepath" in
        /movies/*) disk_read_path="/${disk_name}/Movies/${filepath#/movies/}" ;;
        /tv/*)     disk_read_path="/${disk_name}/TV/${filepath#/tv/}" ;;
        *)         disk_read_path="$filepath" ;;
      esac
    fi

    # Get file size for tmp-path reservation estimates — read from
    # direct disk first, fall back to FUSE only if direct stat fails.
    local file_size_bytes
    file_size_bytes=$(stat -c%s "$disk_read_path" 2>/dev/null || stat -c%s "$filepath" 2>/dev/null || echo 0)
    local input_size_kb=$(( file_size_bytes / 1024 ))

    # Build 12-field resolved payload. Positions 4-6 (vcodec|ach|acount)
    # are empty — classify will fill them. Position 12 is reasons.
    local enriched
    enriched="${service}|${filepath}|${arr_id}|||"
    enriched+="|${disk_name}|${disk_read_path}|${input_size_kb}|0|${route}|${reasons}"

    if [ "$route" = "import" ]; then
      q_push tc:candidates:resolved:import:ready "$enriched"
    else
      q_push tc:candidates:resolved:ready "$enriched"
    fi

    q_ack "$source_queue" "$item"
  done
}

# ── Stage 4: Load balancer (gatekeeper) ──────────────────────────────────
# Scans direct, import, and bulk LB lanes, dispatching eligible items to
# tc:dispatch:gpu:ready / cpu:ready. dispatch_eligible owns runtime gates:
# ignored disks, per-disk concurrency, destination free space, and optional
# SSD temp-pool admission. Atomic Lua handoff prevents job loss. Disk slots
# are still reserved by consumers when they start working.

load_balancer() {
  local max_per_disk="${TRANSCODARR_STREAMS_PER_DISK:-2}"
  log "Load balancer started (max $max_per_disk streams per disk)"

  while true; do
    local dispatched=0

    # ── Direct mode (exclusive, bypasses tc:pause) ──────────────────────
    local direct_active
    direct_active=$($QUEUE_CLI GET tc:direct:active 2>/dev/null || echo "")

    if [ "$direct_active" = "1" ]; then
      local gpu_direct_len cpu_direct_len
      gpu_direct_len=$($QUEUE_CLI LLEN tc:lb:gpu:direct:ready 2>/dev/null || echo 0)
      cpu_direct_len=$($QUEUE_CLI LLEN tc:lb:cpu:direct:ready 2>/dev/null || echo 0)

      # Auto-drain: both direct lanes empty means items have left the
      # direct lanes (dispatched or untagged). Does NOT wait for worker
      # acceptance or encode completion — the LB's responsibility ends
      # at handoff. Clearing the flag lets the next cycle fall through
      # to normal bulk/import logic per the user's tc:pause state.
      if [ "$gpu_direct_len" = "0" ] && [ "$cpu_direct_len" = "0" ]; then
        $QUEUE_CLI DEL tc:direct:active >/dev/null
        continue
      fi

      # Preserve dispatch-depth cap — identical rule to bulk/import branch.
      local gpu_dispatch_len cpu_dispatch_len
      gpu_dispatch_len=$($QUEUE_CLI LLEN tc:dispatch:gpu:ready 2>/dev/null || echo 0)
      cpu_dispatch_len=$($QUEUE_CLI LLEN tc:dispatch:cpu:ready 2>/dev/null || echo 0)

      local gpu_dispatched=0 cpu_dispatched=0
      if (( gpu_direct_len > 0 )) && (( gpu_dispatch_len < GPU_WORKERS )); then
        dispatch_eligible tc:lb:gpu:direct:ready tc:dispatch:gpu:ready "$max_per_disk" && gpu_dispatched=1
      fi
      if (( cpu_direct_len > 0 )) && (( cpu_dispatch_len < CPU_WORKERS )); then
        dispatch_eligible tc:lb:cpu:direct:ready tc:dispatch:cpu:ready "$max_per_disk" && cpu_dispatched=1
      fi

      dispatched=$(( gpu_dispatched + cpu_dispatched ))
      (( dispatched == 0 )) && sleep 1
      continue
    fi
    # ── End direct mode branch ──────────────────────────────────────────

    # Pause gate — stop dispatching while paused, let in-flight work complete
    local paused
    paused=$($QUEUE_CLI GET tc:pause 2>/dev/null || echo "")
    if [ "$paused" = "1" ]; then
      sleep 5
      continue
    fi

    # Cap dispatch queue depth — don't pre-fill beyond consumer count.
    local gpu_dispatch_len cpu_dispatch_len
    gpu_dispatch_len=$($QUEUE_CLI LLEN tc:dispatch:gpu:ready 2>/dev/null || echo 0)
    cpu_dispatch_len=$($QUEUE_CLI LLEN tc:dispatch:cpu:ready 2>/dev/null || echo 0)

    # Priority: dispatch import items first, then bulk
    local gpu_import_len cpu_import_len
    gpu_import_len=$($QUEUE_CLI LLEN tc:lb:gpu:import:ready 2>/dev/null || echo 0)
    cpu_import_len=$($QUEUE_CLI LLEN tc:lb:cpu:import:ready 2>/dev/null || echo 0)

    # Try import first (priority lane). If import dispatch_eligible returns
    # false — meaning every queued import is currently blocked on a per-disk
    # cap or some other gate — fall through to bulk so the bulk queue keeps
    # draining instead of stalling on a single stuck import. Without this
    # fall-through, one import on a saturated disk halts ALL bulk dispatch
    # for that mode (gpu or cpu) until the disk cap clears.
    local gpu_dispatched=0 cpu_dispatched=0
    if (( gpu_import_len > 0 )) && (( gpu_dispatch_len < GPU_WORKERS )); then
      dispatch_eligible tc:lb:gpu:import:ready tc:dispatch:gpu:ready "$max_per_disk" && gpu_dispatched=1
    fi
    if (( gpu_dispatched == 0 )) && (( gpu_dispatch_len < GPU_WORKERS )); then
      dispatch_eligible tc:lb:gpu:ready tc:dispatch:gpu:ready "$max_per_disk" && gpu_dispatched=1
    fi

    if (( cpu_import_len > 0 )) && (( cpu_dispatch_len < CPU_WORKERS )); then
      dispatch_eligible tc:lb:cpu:import:ready tc:dispatch:cpu:ready "$max_per_disk" && cpu_dispatched=1
    fi
    if (( cpu_dispatched == 0 )) && (( cpu_dispatch_len < CPU_WORKERS )); then
      dispatch_eligible tc:lb:cpu:ready tc:dispatch:cpu:ready "$max_per_disk" && cpu_dispatched=1
    fi

    dispatched=$(( gpu_dispatched + cpu_dispatched ))

    (( dispatched == 0 )) && sleep 1
  done
}

dispatch_eligible() {
  local source_queue="$1" dest_queue="$2" max="$3"
  local queue_len
  queue_len=$($QUEUE_CLI LLEN "$source_queue" 2>/dev/null || echo 0)
  (( queue_len == 0 )) && return 1

  local has_tmp_dir=false
  [ -n "${TRANSCODARR_TMP_DIR:-}" ] && has_tmp_dir=true

  local found=false
  local i
  for (( i = queue_len - 1; i >= 0; i-- )); do
    local item
    item=$($QUEUE_CLI LINDEX "$source_queue" "$i" 2>/dev/null || true)
    [ -z "$item" ] && continue

    local disk_name
    disk_name=$(echo "$item" | cut -d'|' -f7)
    [ -z "$disk_name" ] && continue

    # Gate I: ignored disks. This is the only enforcement point: items
    # remain in the lane, but the LB does not hand them to workers while
    # their disk is in tc:disk:ignored. Settings saves update the set live.
    if disk_is_ignored "$disk_name"; then
      continue
    fi

    local active
    active=$($QUEUE_CLI GET "tc:disk:${disk_name}:active" 2>/dev/null || echo 0)
    (( active >= max )) && continue

    # Gate 2: per-disk space
    local disk_space_ok
    disk_space_ok=$($QUEUE_CLI GET "tc:disk:${disk_name}:space_ok" 2>/dev/null || echo 1)
    [ "${disk_space_ok:-1}" = "0" ] && continue

    # Gate 2b: item-sized destination space. The coarse space_ok bit only
    # says the disk is above the configured floor; a large file can still be
    # too big for the free space currently available. Destination-space
    # retries publish an exact need sidecar, otherwise use the source size
    # carried in field 9 as the best admission estimate.
    local disk_free_kb
    disk_free_kb=$($QUEUE_CLI GET "tc:disk:${disk_name}:free_kb" 2>/dev/null || echo "")
    if is_uint "$disk_free_kb"; then
      local item_filepath input_size_kb dest_needed_kb dest_needed_key dest_needed_override
      item_filepath=$(echo "$item" | cut -d'|' -f2)
      input_size_kb=$(echo "$item" | cut -d'|' -f9)
      dest_needed_kb="${input_size_kb:-0}"
      is_uint "$dest_needed_kb" || dest_needed_kb=0

      dest_needed_key=""
      [ -n "$item_filepath" ] && dest_needed_key=$(dest_space_needed_key "$item_filepath" || true)
      if [ -n "$dest_needed_key" ]; then
        dest_needed_override=$($QUEUE_CLI GET "$dest_needed_key" 2>/dev/null || echo "")
        if is_uint "$dest_needed_override" && (( dest_needed_override > dest_needed_kb )); then
          dest_needed_kb="$dest_needed_override"
        fi
      fi

      if (( dest_needed_kb > 0 )) && (( disk_free_kb < dest_needed_kb )); then
        continue
      fi
    fi

    # Gate 3 + atomic move. Two Lua scripts, chosen by TMP_DIR presence:
    #
    #   has_tmp_dir=false → plain LREM+LPUSH (move_only). No lease accounting.
    #   has_tmp_dir=true  → admit_and_move. Sums live leases via the
    #                       tc:ssd:leases index set, admits if space permits,
    #                       SETEX's a per-reservation lease key, SADD's it to
    #                       the index, LREM's the source, LPUSHes the enriched
    #                       item (with the lease key appended as the 12th
    #                       pipe field). All one atomic EVAL — no split-state
    #                       window, no bash-side rollback.
    #
    # Lease reuse on SSD retry: if the item already carries a lease key (it
    # bounced from the exit-75 SSD-space retry path with its original lease key
    # still appended at field 12), detect it in bash, pass `reuse=1` to the
    # Lua, and the Lua refreshes the existing lease's TTL rather than minting
    # a second one. Excluding our own reservation from the sum check prevents
    # the retry from being rejected by its own in-flight charge.
    #
    # Fail-closed liveness: admission requires tc:ssd:space_ready to be
    # present. That key is a 2×check_interval heartbeat refreshed by
    # space_monitor. If the monitor stalls, dies, or valkey gets flushed
    # mid-session, the heartbeat expires and admission starts rejecting —
    # far safer than the previous fail-open-on-missing-key behavior.
    #
    # Return codes (both scripts): 1 = moved, 0 = rejected, -1 = item raced
    # away from source between LINDEX and LREM (retry on next iteration).

    local moved
    if [ "$has_tmp_dir" = true ]; then
      local input_size_kb
      input_size_kb=$(echo "$item" | cut -d'|' -f9)
      input_size_kb="${input_size_kb:-0}"
      local job_est_kb=$(( input_size_kb * 2 ))
      (( job_est_kb < 102400 )) && job_est_kb=102400

      # Detect lease reuse: if the item already has a 12th pipe field, it
      # came back from an SSD-space retry carrying its original lease key.
      local existing_lease
      existing_lease=$(echo "$item" | awk -F'|' 'NF >= 12 { print $12 }')

      local lease_key reuse
      if [ -n "$existing_lease" ]; then
        lease_key="$existing_lease"
        reuse=1
      else
        local item_filepath file_hash nonce
        item_filepath=$(echo "$item" | cut -d'|' -f2)
        file_hash=$(printf '%s' "$item_filepath" | md5sum 2>/dev/null | cut -d' ' -f1)
        nonce=$(mint_lease_nonce)
        lease_key="tc:ssd:lease:${file_hash}:${nonce}"
        reuse=0
      fi

      local lease_ttl
      lease_ttl=$(compute_lease_ttl)

      moved=$($QUEUE_CLI EVAL "$LUA_ADMIT_AND_MOVE" 6 \
        "$source_queue" "$dest_queue" \
        "tc:ssd:free_kb" "tc:ssd:warn_kb" "tc:ssd:leases" "tc:ssd:space_ready" \
        "$item" "$job_est_kb" "$lease_key" "$lease_ttl" "$reuse")
    else
      moved=$($QUEUE_CLI EVAL "$LUA_MOVE_ONLY" 2 "$source_queue" "$dest_queue" "$item")
    fi

    case "${moved:-0}" in
      1)  found=true; break ;;
      0)  continue ;;         # rejected (space, liveness, or invalid ttl)
      -1) continue ;;         # raced, try next item
      *)  continue ;;
    esac
  done

  $found
}

# ── Space monitor ────────────────────────────────────────────────────────
# Periodically checks df on the configured tmp dir (if any) and each disk mount.
# Writes results to Valkey for the LB and worker consumers to read.
# TMP_MAX_KB controls the max tmp pool size. The warn threshold is derived
# from the drive's total size at startup: warn_kb = total_kb - TMP_MAX_KB.

space_monitor() {
  local interval="${TRANSCODARR_SPACE_CHECK_INTERVAL:-30}"
  local disk_warn_kb="${TRANSCODARR_DISK_WARN_KB:-10485760}"
  local tmp_max_kb="${TRANSCODARR_TMP_MAX_KB:-262144000}"  # 250 GiB default

  # Compute SSD warn threshold from available space at startup (not total —
  # total includes space used by other apps like appdata, docker, etc.)
  # warn_kb = free_at_startup - TMP_MAX_KB = minimum free that must remain
  local ssd_warn_kb=0
  if [ -n "${TRANSCODARR_TMP_DIR:-}" ] && [ -d "${TRANSCODARR_TMP_DIR}" ]; then
    local ssd_free_at_start
    ssd_free_at_start=$(df -k "${TRANSCODARR_TMP_DIR}" 2>/dev/null | awk 'NR==2{print $4}' || echo 0)
    ssd_warn_kb=$(( ssd_free_at_start - tmp_max_kb ))
    if (( ssd_warn_kb < 0 )); then ssd_warn_kb=0; fi
    # Store in Valkey so LB Lua can read it
    $QUEUE_CLI SET tc:ssd:warn_kb "$ssd_warn_kb" > /dev/null
    local tmp_max_gib=$(( tmp_max_kb / 1048576 ))
    local ssd_free_gib=$(( ssd_free_at_start / 1048576 ))
    log "Space monitor started (interval=${interval}s, tmp_pool=${tmp_max_gib}GiB, free_at_start=${ssd_free_gib}GiB, warn=${ssd_warn_kb}KB)"
  else
    log "Space monitor started (interval=${interval}s, no tmp dir — disk-only monitoring)"
  fi

  declare -A prev_disk_state
  local prev_ssd_state="ok"

  while true; do
    if [ -n "${TRANSCODARR_TMP_DIR:-}" ] && [ -d "${TRANSCODARR_TMP_DIR}" ]; then
      local ssd_free_kb
      ssd_free_kb=$(df -k "${TRANSCODARR_TMP_DIR}" 2>/dev/null | awk 'NR==2{print $4}' || echo 0)
      # Persistent free_kb: admission gate's liveness is the space_ready
      # heartbeat, not the free_kb TTL. Stale free_kb is safer than missing.
      $QUEUE_CLI SET tc:ssd:free_kb "$ssd_free_kb" > /dev/null
      # Heartbeat: refresh space_ready every cycle with TTL = 2x interval.
      # If this loop stalls or dies, the key expires and the admission Lua
      # fails closed. Cheap insurance against silent monitor death.
      $QUEUE_CLI SET tc:ssd:space_ready 1 EX $(( interval * 2 )) > /dev/null

      # Reconcile SSD lease index: sum live lease keys, drop stale set entries,
      # write the sum to tc:ssd:reserved_kb as a DERIVED metric (UI/log compat).
      # Authoritative state is the per-reservation lease keys; this write is
      # observability only.
      local lease_sum=0
      local lease_member
      while IFS= read -r lease_member; do
        [ -z "$lease_member" ] && continue
        local lease_val
        lease_val=$($QUEUE_CLI GET "$lease_member" 2>/dev/null || echo "")
        if [ -n "$lease_val" ]; then
          lease_sum=$(( lease_sum + lease_val ))
        else
          # Lease expired or was DEL'd but the set entry lingered — sweep.
          $QUEUE_CLI SREM tc:ssd:leases "$lease_member" > /dev/null 2>&1 || true
        fi
      done < <($QUEUE_CLI SMEMBERS tc:ssd:leases 2>/dev/null || true)
      $QUEUE_CLI SET tc:ssd:reserved_kb "$lease_sum" > /dev/null

      local effective_kb=$(( ssd_free_kb - lease_sum ))
      if (( effective_kb < ssd_warn_kb )) && [ "$prev_ssd_state" = "ok" ]; then
        log "WARN: tmp pool full — ${effective_kb}KB effective free (${ssd_free_kb}KB raw - ${lease_sum}KB reserved, threshold ${ssd_warn_kb}KB)"
        prev_ssd_state="low"
      elif (( effective_kb >= ssd_warn_kb )) && [ "$prev_ssd_state" = "low" ]; then
        log "tmp pool space recovered — ${effective_kb}KB effective free"
        prev_ssd_state="ok"
      fi
    fi

    for disk in "${AVAILABLE_DISKS[@]}"; do
      local disk_free_kb
      disk_free_kb=$(df -k "/${disk}" 2>/dev/null | awk 'NR==2{print $4}' || echo 0)
      disk_free_kb="${disk_free_kb:-0}"
      $QUEUE_CLI SET "tc:disk:${disk}:free_kb" "${disk_free_kb:-0}" EX 120 > /dev/null
      if (( disk_free_kb < disk_warn_kb )); then
        $QUEUE_CLI SET "tc:disk:${disk}:space_ok" 0 EX 120 > /dev/null
        if [ "${prev_disk_state[$disk]:-ok}" = "ok" ]; then
          log "WARN: ${disk} low space — ${disk_free_kb}KB free (threshold ${disk_warn_kb}KB)"
          prev_disk_state[$disk]="low"
        fi
      else
        $QUEUE_CLI SET "tc:disk:${disk}:space_ok" 1 EX 120 > /dev/null
        if [ "${prev_disk_state[$disk]:-ok}" = "low" ]; then
          log "${disk} space recovered — ${disk_free_kb}KB free"
          prev_disk_state[$disk]="ok"
        fi
      fi
    done

    sleep "$interval"
  done
}

# ── Stage 5: Worker consumers ────────────────────────────────────────────
# N loops per pool. Pop from dispatch queues with disk slot reservation.
# Atomic INCR-if-under-limit on disk counter; DECR on completion/failure.
# EXIT trap handles consumer crashes.

worker_consumer() {
  local queue="$1" label="$2"
  local timeout="${TRANSCODARR_TIMEOUT:-14400}"
  local max_per_disk="${TRANSCODARR_STREAMS_PER_DISK:-2}"

  # Track current disk for EXIT trap cleanup
  current_disk=""
  trap 'if [ -n "$current_disk" ]; then
    $QUEUE_CLI DECR "tc:disk:${current_disk}:active" > /dev/null 2>&1
  fi' EXIT

  while true; do
    local item
    item=$(q_pop "$queue" 5)
    [ -z "$item" ] && continue
    item=$(echo "$item" | tail -1)
    [ -z "$item" ] && continue

    local service filepath arr_id vcodec ach acount disk_name disk_read_path input_size_kb first_space_fail_ts item_route ssd_lease_key
    IFS='|' read -r service filepath arr_id vcodec ach acount disk_name disk_read_path input_size_kb first_space_fail_ts item_route ssd_lease_key <<< "$item"
    input_size_kb="${input_size_kb:-0}"
    first_space_fail_ts="${first_space_fail_ts:-0}"
    item_route="${item_route:-bulk}"
    ssd_lease_key="${ssd_lease_key:-}"

    # Determine correct LB queue for re-queue paths (bounce-back, exit 75).
    # Direct items must requeue back to the direct lane so they remain
    # user-curated through the whole dispatch cycle.
    local lb_requeue="tc:lb:${label,,}:ready"
    case "$item_route" in
      direct) lb_requeue="tc:lb:${label,,}:direct:ready" ;;
      import) lb_requeue="tc:lb:${label,,}:import:ready" ;;
    esac

    # Atomic disk slot reservation — INCR only if under limit
    local reserved
    reserved=$($QUEUE_CLI EVAL '
      local active = tonumber(redis.call("GET", KEYS[1]) or "0")
      if active >= tonumber(ARGV[1]) then
        return 0
      end
      redis.call("INCR", KEYS[1])
      return 1
    ' 1 "tc:disk:${disk_name}:active" "$max_per_disk")

    if [ "$reserved" != "1" ]; then
      # Disk at limit — atomic return to lb queue. The re-queued item is
      # rebuilt WITHOUT the ssd_lease_key trailing field, and the lease
      # itself is DEL'd + SREM'd in the same EVAL. Reasoning: at-limit
      # bounce means nothing ever ran for this item on the SSD, so the
      # reservation is wasted capacity — better to free it and let
      # re-admission mint fresh once a compute slot opens. Contrast with
      # the exit-75 space-retry path, which keeps the lease on the item
      # because the worker was already consuming the space and a retry
      # wants the same reservation.
      local item_no_lease="${service}|${filepath}|${arr_id}|${vcodec}|${ach}|${acount}|${disk_name}|${disk_read_path}|${input_size_kb}|${first_space_fail_ts}|${item_route}"
      $QUEUE_CLI EVAL '
        redis.call("LREM", KEYS[1], 1, ARGV[1])
        redis.call("LPUSH", KEYS[2], ARGV[2])
        if ARGV[3] ~= "" then
          redis.call("DEL", ARGV[3])
          redis.call("SREM", KEYS[3], ARGV[3])
        end
      ' 3 "${queue%:ready}:processing" "$lb_requeue" "tc:ssd:leases" "$item" "$item_no_lease" "$ssd_lease_key" > /dev/null
      if [ "$item_route" = "direct" ]; then
        $QUEUE_CLI SET tc:direct:active 1 > /dev/null 2>&1 || true
      fi
      continue
    fi

    current_disk="$disk_name"
    log "$label worker [$disk_name]: $(basename "$filepath")"

    # API_TEST_MODE: dry-run. Terminal ack — release the lease so the
    # dry-run doesn't leak reservation.
    if [[ "${TRANSCODARR_API_TEST_MODE:-false}" == "true" ]]; then
      log "DRY-RUN: would process $label $(basename "$filepath") ($disk_name $vcodec ${ach}ch)"
      release_ssd_lease "$ssd_lease_key"
      current_disk=""
      $QUEUE_CLI DECR "tc:disk:${disk_name}:active" > /dev/null
      q_ack "$queue" "$item"
      if [[ "${TRANSCODARR_SWEET16_TEST:-false}" == "true" ]]; then break; fi
      continue
    fi

    # Hold function — after encode, keep slot held, sleep until SIGTERM
    # Used by sweet16 (cap at 26 encodes) and almosthome (cap at 26 copy-backs)
    sweet16_hold() {
      local mode="SWEET16"
      [[ "${TRANSCODARR_ALMOSTHOME_TEST:-false}" == "true" ]] && mode="ALMOSTHOME"
      log "$mode: $label [$disk_name] holding slot ($(basename "$filepath")) — waiting for shutdown"
      q_ack "$queue" "$item"
      # Sleep forever — SIGTERM from container stop will kill this
      while true; do sleep 3600; done
    }

    # Run worker with direct disk paths
    local worker_rc=0
    local space_fail_kind_file=""
    space_fail_kind_file=$(mktemp /tmp/transcodarr-space-fail.XXXXXX 2>/dev/null || true)
    [ -n "$space_fail_kind_file" ] && rm -f "$space_fail_kind_file"
    # Failure marker: worker's record_failed touches this to signal that a
    # specific reason was already logged, so the fallback branch below can
    # skip writing a duplicate generic line. Unique per encode; cleaned up
    # on every disposition path.
    local failure_marker
    failure_marker=$(mktemp -u /tmp/transcodarr-fail.XXXXXX 2>/dev/null || echo "")
    # Build short job tag: disk:type:filename (truncated to 40 chars)
    local short_name
    short_name=$(basename "$filepath")
    short_name="${short_name%.*}"
    [[ ${#short_name} -gt 40 ]] && short_name="${short_name:0:40}"
    TRANSCODARR_JOB_TAG="${disk_name}:${label,,}:${short_name}" \
    TRANSCODARR_ITEM_ROUTE="$item_route" \
    TRANSCODARR_DISK_READ_PATH="$disk_read_path" \
    TRANSCODARR_DISK_WRITE_PATH="$disk_read_path" \
    TRANSCODARR_SSD_LEASE_KEY="$ssd_lease_key" \
    TRANSCODARR_SPACE_FAIL_KIND_FILE="$space_fail_kind_file" \
    TRANSCODARR_FAILURE_MARKER="$failure_marker" \
      timeout --foreground --kill-after=60 "$timeout" \
      bash "$WORKER" "$service" "$filepath" "${item_route^}" "$arr_id" 2>&1 || worker_rc=$?

    local space_fail_kind="ssd"
    local space_fail_needed_kb=0
    if [ -n "${space_fail_kind_file:-}" ] && [ -f "$space_fail_kind_file" ]; then
      local space_fail_raw
      space_fail_raw=$(cat "$space_fail_kind_file" 2>/dev/null || echo "ssd")
      space_fail_kind="${space_fail_raw%%:*}"
      if [ "$space_fail_raw" != "$space_fail_kind" ]; then
        space_fail_needed_kb="${space_fail_raw#*:}"
      fi
      case "$space_fail_kind" in
        dest|ssd) ;;
        *) space_fail_kind="ssd" ;;
      esac
      is_uint "$space_fail_needed_kb" || space_fail_needed_kb=0
      rm -f "$space_fail_kind_file" 2>/dev/null || true
    fi

    if (( worker_rc == 75 )); then
      local retry_kind_key prev_space_fail_kind
      retry_kind_key=$(space_fail_kind_key "$filepath" || true)
      if [ -n "$retry_kind_key" ]; then
        prev_space_fail_kind=$($QUEUE_CLI GET "$retry_kind_key" 2>/dev/null || echo "")
        if [ -n "$prev_space_fail_kind" ] && [ "$prev_space_fail_kind" != "$space_fail_kind" ]; then
          first_space_fail_ts=0
        fi
        $QUEUE_CLI SET "$retry_kind_key" "$space_fail_kind" > /dev/null 2>&1 || true
      fi

      if [ "$space_fail_kind" = "dest" ]; then
        local now
        now=$(date +%s)
        local timeout_val
        timeout_val=$(space_retry_timeout_for_kind dest)
        is_uint "$timeout_val" || timeout_val=0

        case "$first_space_fail_ts" in
          0|""|*[!0-9]*) first_space_fail_ts="$now" ;;
        esac

        local elapsed=$(( now - first_space_fail_ts ))
        local dest_needed_kb="${space_fail_needed_kb:-0}"
        if ! is_uint "$dest_needed_kb" || (( dest_needed_kb <= 0 )); then
          dest_needed_kb="${input_size_kb:-0}"
        fi
        is_uint "$dest_needed_kb" || dest_needed_kb=0

        local dest_needed_key
        dest_needed_key=$(dest_space_needed_key "$filepath" || true)
        if [ -n "$dest_needed_key" ] && (( dest_needed_kb > 0 )); then
          $QUEUE_CLI SET "$dest_needed_key" "$dest_needed_kb" > /dev/null 2>&1 || true
        fi

        # The attempt failed because the destination disk could not hold the
        # temporary/copy-back output. The worker has already abandoned the tmp
        # file, so release any SSD lease and re-admit later from scratch.
        release_ssd_lease "$ssd_lease_key"

        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
          "$(date -Iseconds)" "$service" "dest_space_retry" "$filepath" \
          "${vcodec:-unknown}" "${ach:-0}" "$(( ${input_size_kb:-0} * 1024 ))" "${disk_name:-unknown}" \
          "dest_space_retry" \
          >> "$STATE_DIR/failed-files.tsv" 2>/dev/null || true

        local retry_item="${service}|${filepath}|${arr_id}|${vcodec}|${ach}|${acount}|${disk_name}|${disk_read_path}|${input_size_kb}|${first_space_fail_ts}|${item_route}"
        if (( timeout_val > 0 )) && (( elapsed >= timeout_val )); then
          log "WARN: $label destination disk space timeout (${elapsed}s >= ${timeout_val}s), parking: $(basename "$filepath")"
          $QUEUE_CLI LPUSH "tc:parked:space:${label,,}" "$retry_item" > /dev/null
          printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$(date -Iseconds)" "$service" "parked_dest_space_timeout" "$filepath" \
            "${vcodec:-unknown}" "${ach:-0}" "$(( ${input_size_kb:-0} * 1024 ))" "${disk_name:-unknown}" \
            "dest_space_timeout" \
            >> "$STATE_DIR/failed-files.tsv" 2>/dev/null || true
          [ -n "$dest_needed_key" ] && $QUEUE_CLI DEL "$dest_needed_key" > /dev/null 2>&1 || true
          [ -n "$retry_kind_key" ] && $QUEUE_CLI DEL "$retry_kind_key" > /dev/null 2>&1 || true
        else
          $QUEUE_CLI LPUSH "$lb_requeue" "$retry_item" > /dev/null
          if [ "$item_route" = "direct" ]; then
            $QUEUE_CLI SET tc:direct:active 1 > /dev/null 2>&1 || true
          fi
          if (( timeout_val > 0 )); then
            local remaining=$(( timeout_val - elapsed ))
            log "Re-queued (destination disk space, ${elapsed}s/${timeout_val}s, ${remaining}s left, need ${dest_needed_kb}KB): $(basename "$filepath")"
          else
            log "Re-queued (destination disk space, waiting ${elapsed}s so far, need ${dest_needed_kb}KB): $(basename "$filepath")"
          fi
        fi
      else
        local now
        now=$(date +%s)
        local timeout_val
        timeout_val=$(space_retry_timeout_for_kind ssd)
        is_uint "$timeout_val" || timeout_val=0

        case "$first_space_fail_ts" in
          0|""|*[!0-9]*) first_space_fail_ts="$now" ;;
        esac

        local elapsed=$(( now - first_space_fail_ts ))

        if (( timeout_val > 0 )) && (( elapsed >= timeout_val )); then
          log "WARN: $label SSD space timeout (${elapsed}s >= ${timeout_val}s), parking: $(basename "$filepath")"
          $QUEUE_CLI LPUSH "tc:parked:space:${label,,}" "$item" > /dev/null
          printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$(date -Iseconds)" "$service" "parked_ssd_space_timeout" "$filepath" \
            "${vcodec:-unknown}" "${ach:-0}" "$(( ${input_size_kb:-0} * 1024 ))" "${disk_name:-unknown}" \
            "ssd_space_timeout" \
            >> "$STATE_DIR/failed-files.tsv" 2>/dev/null || true
          # Terminal path: release the SSD lease so its reservation doesn't
          # linger. Note the parked item itself still carries the lease_key
          # field in its stored record; only the live lease state is freed.
          release_ssd_lease "$ssd_lease_key"
          local stale_dest_needed_key
          stale_dest_needed_key=$(dest_space_needed_key "$filepath" || true)
          [ -n "$stale_dest_needed_key" ] && $QUEUE_CLI DEL "$stale_dest_needed_key" > /dev/null 2>&1 || true
          [ -n "$retry_kind_key" ] && $QUEUE_CLI DEL "$retry_kind_key" > /dev/null 2>&1 || true
        else
          # Rebuild the re-queued item with the updated first_space_fail_ts.
          # The previous `${item%|*}|${first_space_fail_ts}` pattern was a
          # latent correctness bug: it stripped item_route (the last field)
          # and replaced it with the timestamp, so the re-queued item's
          # field 11 became a numeric string. On the next worker_consumer
          # pass, item_route=<timestamp>, the `= "import"` test failed, and
          # priority imports got silently demoted to the bulk lane on every
          # space retry. Explicit rebuild fixes that AND preserves the
          # ssd_lease_key for the atomic-lease retry path (the lease key
          # travels with the item so re-admission reuses it instead of
          # double-charging the reservation).
          local updated_item="${service}|${filepath}|${arr_id}|${vcodec}|${ach}|${acount}|${disk_name}|${disk_read_path}|${input_size_kb}|${first_space_fail_ts}|${item_route}${ssd_lease_key:+|${ssd_lease_key}}"
          $QUEUE_CLI LPUSH "$lb_requeue" "$updated_item" > /dev/null
          if [ "$item_route" = "direct" ]; then
            $QUEUE_CLI SET tc:direct:active 1 > /dev/null 2>&1 || true
          fi
          if (( timeout_val > 0 )); then
            local remaining=$(( timeout_val - elapsed ))
            log "Re-queued (SSD space, ${elapsed}s/${timeout_val}s, ${remaining}s left): $(basename "$filepath")"
          else
            log "Re-queued (SSD space, waiting ${elapsed}s so far): $(basename "$filepath")"
          fi
        fi
      fi

      # Release disk slot (tmp-path reservation already released by worker cleanup trap).
      # Clear current_disk BEFORE decrementing so the EXIT trap can't double-DECR if
      # this shell is SIGKILL'd between the two statements.
      current_disk=""
      $QUEUE_CLI DECR "tc:disk:${disk_name}:active" > /dev/null
      [ -n "$failure_marker" ] && rm -f "$failure_marker" 2>/dev/null || true
      q_ack "$queue" "$item"
      # Sweet16 + space failure: break (no successful encode to hold)
      if [[ "${TRANSCODARR_SWEET16_TEST:-false}" == "true" ]]; then break; fi
      continue

    elif (( worker_rc != 0 )); then
      log "WARN: $label worker failed (exit $worker_rc) for $(basename "$filepath")"
      rm -f "${filepath}.transcode.tmp."* 2>/dev/null
      rm -f "${filepath}.replace.tmp."* 2>/dev/null
      # Dot-prefixed tmp files (direct disk writes)
      rm -f "$(dirname "$filepath")/.$(basename "$filepath").transcode.tmp."* 2>/dev/null
      rm -f "$(dirname "$filepath")/.$(basename "$filepath").replace.tmp."* 2>/dev/null
      if [ -n "${TRANSCODARR_TMP_DIR:-}" ]; then
        rm -f "${TRANSCODARR_TMP_DIR}/$(basename "${filepath}").transcode.tmp."* 2>/dev/null
      fi
      fhash=$(echo -n "$filepath" | md5sum 2>/dev/null | cut -d' ' -f1)
      rm -rf "$STATE_DIR/locks/${fhash}.lock" 2>/dev/null
      # Only log a fallback line if the worker did NOT already call record_failed
      # (marker absent = crash, timeout SIGKILL, OOM, or other abrupt exit).
      # This eliminates the duplicate entries that plagued failed-files.tsv.
      if [ -n "$failure_marker" ] && [ ! -f "$failure_marker" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
          "$(date -Iseconds)" "$service" "worker_exit_${worker_rc}" "$filepath" \
          "${vcodec:-unknown}" "${ach:-0}" "$(( ${input_size_kb:-0} * 1024 ))" "${disk_name:-unknown}" \
          "worker_crash" \
          >> "$STATE_DIR/failed-files.tsv" 2>/dev/null || true
      fi
      [ -n "$failure_marker" ] && rm -f "$failure_marker" 2>/dev/null || true
      # Belt-and-suspenders SSD lease release for the SIGKILL case: if the
      # worker shell was SIGKILL'd (timeout escalation, OOM-killer on the
      # shell itself, SIGKILL on the process group), its EXIT trap didn't
      # run and the lease is still live. The helper is idempotent, so a
      # normal non-zero exit where the worker's trap already released is
      # a no-op here. The other safety net is the reconciliation sweep +
      # TTL on the lease key itself.
      release_ssd_lease "$ssd_lease_key"
      local dest_needed_key
      dest_needed_key=$(dest_space_needed_key "$filepath" || true)
      [ -n "$dest_needed_key" ] && $QUEUE_CLI DEL "$dest_needed_key" > /dev/null 2>&1 || true
      local retry_kind_key
      retry_kind_key=$(space_fail_kind_key "$filepath" || true)
      [ -n "$retry_kind_key" ] && $QUEUE_CLI DEL "$retry_kind_key" > /dev/null 2>&1 || true
    fi

    # SWEET16: hold slot after successful encode so the cap-at-26 dev
    # test can stop without losing context. ALMOSTHOME used to share
    # this hold behavior when it was a bounded dev test, but it is now
    # user-facing as Dry Run and needs to release the slot normally so
    # subsequent files keep processing. Without the release, after N
    # dry-run encodes (N = worker count per pool) all slots are held
    # until container restart.
    if [[ "${TRANSCODARR_SWEET16_TEST:-false}" == "true" ]]; then
      sweet16_hold
      # sweet16_hold never returns (sleeps until SIGTERM)
    fi

    # Release disk slot — encode + validate + copy-back all complete.
    # Clear current_disk BEFORE decrementing so the EXIT trap can't double-DECR if
    # this shell is SIGKILL'd between the two statements.
    current_disk=""
    $QUEUE_CLI DECR "tc:disk:${disk_name}:active" > /dev/null

    local dest_needed_key
    dest_needed_key=$(dest_space_needed_key "$filepath" || true)
    [ -n "$dest_needed_key" ] && $QUEUE_CLI DEL "$dest_needed_key" > /dev/null 2>&1 || true
    local retry_kind_key
    retry_kind_key=$(space_fail_kind_key "$filepath" || true)
    [ -n "$retry_kind_key" ] && $QUEUE_CLI DEL "$retry_kind_key" > /dev/null 2>&1 || true
    [ -n "$failure_marker" ] && rm -f "$failure_marker" 2>/dev/null || true
    q_ack "$queue" "$item"
  done
}

start_worker_pool() {
  local queue="$1" label="$2" count="$3"

  log "Starting $count $label consumers"
  for (( i=0; i<count; i++ )); do
    worker_consumer "$queue" "$label" &
  done
}

# ── Start pipeline stages ──────────────────────────────────────────────────

if [[ "${TRANSCODARR_TEST_MODE:-false}" == "true" ]]; then
  log "*** TEST MODE ACTIVE — notifications disabled ***"
fi
# Auto-start: default to paused if not configured
AUTO_START="${TRANSCODARR_AUTO_START:-false}"
if [[ "$AUTO_START" != "true" ]]; then
  $QUEUE_CLI SET tc:pause 1 > /dev/null
  log "Transcodarr starting PAUSED (GPU:$GPU_WORKERS CPU:$CPU_WORKERS) — resume via UI or: docker exec transcodarr valkey-cli DEL tc:pause"
else
  log "Transcodarr starting (GPU:$GPU_WORKERS CPU:$CPU_WORKERS)"
fi

# Phase 1: Blocking — restore priority jobs before anything else
update_progress "queue" "restoring priority jobs"
startup_job_bridge

# Phase 2: Background pipeline
update_progress "queue" "building queues"

(
  while true; do
    bridge_rc=0
    job_bridge || bridge_rc=$?
    log "WARN: job bridge exited unexpectedly (rc=$bridge_rc), restarting in 5s"
    sleep 5
  done
) &
log "Job bridge started"

(
  while true; do
    if [ -x "$SCRIPT_DIR/transcodarr-failure-policy.pl" ]; then
      TRANSCODARR_STATE_DIR="$STATE_DIR" \
      "$SCRIPT_DIR/transcodarr-failure-policy.pl" run >/dev/null 2>&1 \
        || log "WARN: failure policy runner pass failed"
    fi
    sleep "${TRANSCODARR_FAILURE_POLICY_INTERVAL:-60}"
  done
) &
log "Failure policy runner started"

bash "$QUEUE_BUILDER" &
BUILDER_PID=$!

# Stage 2: ffprobe pool (starts immediately, consumes as candidates arrive)
supervise_stage "ffprobe pool" ffprobe_pool
PROBE_PID=$SUPERVISED_PID

# Stage 3: Disk wrangler (resolves disk, enriches items)
supervise_stage "Disk wrangler" wrangler_pool
WRANGLER_PID=$SUPERVISED_PID

# Stage 3.5: Language-detection pool — spawned only when the feature is
# enabled AND the capability probe reported a usable lang_backend. The
# probe runs in the background at startup, so poll briefly for the field
# rather than racing it; absence after the grace window means no usable
# backend and the pool stays unspawned (classifier divert is likewise
# gated on lang_backend, so nothing accumulates in tc:lang:ready).
LANG_PID=""
if [ "${TRANSCODARR_LANGUAGE_ENABLED:-false}" = "true" ]; then
  _lb_cap=""
  for _i in 1 2 3 4 5 6 7 8 9 10; do
    _lb_cap=$($QUEUE_CLI HGET tc:capabilities lang_backend 2>/dev/null) || _lb_cap=""
    [ -n "$_lb_cap" ] && break
    sleep 1
  done
  if [ -n "$_lb_cap" ]; then
    supervise_stage "Language-detection pool" lang_detect_pool
    LANG_PID=$SUPERVISED_PID
    log "Language-detection pool started (backend=$_lb_cap)"
  else
    log "WARN: language enabled but no usable language backend after probe grace window; pool not started"
  fi
  unset _lb_cap _i
fi

# Synchronous SSD-state bootstrap: if TMP_DIR is configured, seed
# tc:ssd:free_kb, tc:ssd:warn_kb, and tc:ssd:space_ready before the LB
# starts so the admission Lua never sees missing-keys (which it now
# treats as fail-closed — see the space-ready heartbeat contract).
# No-op when TMP_DIR is unset — the LB's non-TMP_DIR path doesn't
# touch these keys.
if [ -n "${TRANSCODARR_TMP_DIR:-}" ] && [ -d "${TRANSCODARR_TMP_DIR}" ]; then
  _tc_tmp_max_kb="${TRANSCODARR_TMP_MAX_KB:-262144000}"
  _tc_check_interval="${TRANSCODARR_SPACE_CHECK_INTERVAL:-30}"
  _tc_ssd_free_at_start=$(df -k "${TRANSCODARR_TMP_DIR}" 2>/dev/null | awk 'NR==2{print $4}' || echo 0)
  _tc_ssd_warn_kb=$(( _tc_ssd_free_at_start - _tc_tmp_max_kb ))
  if (( _tc_ssd_warn_kb < 0 )); then _tc_ssd_warn_kb=0; fi
  $QUEUE_CLI SET tc:ssd:free_kb "$_tc_ssd_free_at_start" > /dev/null
  $QUEUE_CLI SET tc:ssd:warn_kb "$_tc_ssd_warn_kb" > /dev/null
  # space_ready is the liveness heartbeat: TTL = 2x check_interval, refreshed
  # by space_monitor every cycle. If space_monitor stalls or dies, this key
  # expires and the admission Lua starts rejecting — fail closed.
  $QUEUE_CLI SET tc:ssd:space_ready 1 EX $(( _tc_check_interval * 2 )) > /dev/null
  log "SSD state seeded: free=${_tc_ssd_free_at_start}KB warn=${_tc_ssd_warn_kb}KB space_ready=1"
  unset _tc_tmp_max_kb _tc_check_interval _tc_ssd_free_at_start _tc_ssd_warn_kb
fi

# Synchronous destination-disk bootstrap: the LB now uses both the coarse
# space_ok bit and the exact free_kb value for per-item destination admission.
# Seed those keys before LB starts; space_monitor refreshes them afterward.
if [ ${#AVAILABLE_DISKS[@]} -gt 0 ]; then
  _tc_disk_warn_kb="${TRANSCODARR_DISK_WARN_KB:-10485760}"
  for _tc_disk in "${AVAILABLE_DISKS[@]}"; do
    _tc_disk_free_kb=$(df -k "/${_tc_disk}" 2>/dev/null | awk 'NR==2{print $4}' || echo 0)
    _tc_disk_free_kb="${_tc_disk_free_kb:-0}"
    $QUEUE_CLI SET "tc:disk:${_tc_disk}:free_kb" "$_tc_disk_free_kb" EX 120 > /dev/null
    if (( _tc_disk_free_kb < _tc_disk_warn_kb )); then
      $QUEUE_CLI SET "tc:disk:${_tc_disk}:space_ok" 0 EX 120 > /dev/null
    else
      $QUEUE_CLI SET "tc:disk:${_tc_disk}:space_ok" 1 EX 120 > /dev/null
    fi
  done
  log "Destination disk state seeded (${#AVAILABLE_DISKS[@]} disks, warn=${_tc_disk_warn_kb}KB)"
  unset _tc_disk_warn_kb _tc_disk _tc_disk_free_kb
fi

# ── Ignored-disks seed ────────────────────────────────────────────────
# Mirror TRANSCODARR_IGNORED_DISKS (CSV) into the Valkey set tc:disk:ignored.
# The load balancer reads this set at dispatch time. The API also mirrors
# Settings saves into the same set, so toggles apply without a restart and
# without mutating scan/probe/import/direct queue state.
$QUEUE_CLI DEL tc:disk:ignored > /dev/null 2>&1 || true
if [ -n "${TRANSCODARR_IGNORED_DISKS:-}" ]; then
  IFS=',' read -ra _tc_ignored_arr <<< "$TRANSCODARR_IGNORED_DISKS"
  _tc_ignored_count=0
  for _tc_ign in "${_tc_ignored_arr[@]}"; do
    _tc_ign=$(echo "$_tc_ign" | tr -d '[:space:]')
    if [[ "$_tc_ign" =~ ^disk[0-9]+$ ]]; then
      $QUEUE_CLI SADD tc:disk:ignored "$_tc_ign" > /dev/null
      _tc_ignored_count=$(( _tc_ignored_count + 1 ))
    fi
  done
  log "Ignored disks seeded ($_tc_ignored_count): $TRANSCODARR_IGNORED_DISKS"
  unset _tc_ignored_arr _tc_ign _tc_ignored_count
fi

# Stage 4: Load balancer (gates per-disk concurrency)
supervise_stage "Load balancer" load_balancer
LB_PID=$SUPERVISED_PID

# Space monitor (polls df, writes to Valkey)
supervise_stage "Space monitor" space_monitor
SPACE_PID=$SUPERVISED_PID

# Status API (lightweight HTTP server for Homepage widget)
TRANSCODARR_SCRIPT_DIR="$SCRIPT_DIR" perl "$SCRIPT_DIR/transcodarr-api.pl" &
API_PID=$!

# Stage 5: Worker consumers (pop from dispatch queues)
# Both real and dry-run modes start consumers — API_TEST_MODE gates inside worker_consumer
start_worker_pool tc:dispatch:gpu:ready "GPU" "$GPU_WORKERS"
start_worker_pool tc:dispatch:cpu:ready "CPU" "$CPU_WORKERS"

# Wait for API intake to finish, then log
wait $BUILDER_PID 2>/dev/null || true
log "API intake complete"

# Let ffprobe pool and worker pools drain
# Everything runs until container is stopped
wait
