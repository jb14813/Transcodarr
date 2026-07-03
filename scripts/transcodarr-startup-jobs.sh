#!/bin/bash
# transcodarr-startup-jobs.sh — Blocking startup scan for .job files
# Runs before queue builder. Ingests all existing .job files as priority.
# Seeds the shared JOB_TRACKED array so job_bridge won't re-ingest them.
#
# Requires: QUEUE_DIR, JOB_TRACKED (associative array), q_push, log
# Must be sourced from entrypoint, not executed standalone.

startup_job_bridge() {
  local count=0
  shopt -s nullglob
  for f in "$QUEUE_DIR"/*.job; do
    [ -f "$f" ] || continue
    local svc filepath
    svc=$(head -1 "$f" 2>/dev/null || true)
    filepath=$(sed -n '2p' "$f" 2>/dev/null || true)
    [ -z "$svc" ] || [ -z "$filepath" ] && continue
    case "$filepath" in /movies/*|/tv/*) ;; *) continue ;; esac
    if [ ! -f "$filepath" ]; then continue; fi
    # 4-tuple payload (Phase 1 verified-hash gate). Empty reasons field
    # → admission cannot whole-skip; restored imports always do a full
    # classify, which is the safe default for unknown-provenance jobs.
    q_push tc:candidates:import:ready "$svc|$filepath|0|"
    JOB_TRACKED["$(basename "$f")"]=1
    count=$((count + 1))
    log "Import restored (priority): $svc $(basename "$filepath")"
  done
  shopt -u nullglob
  if [ "$count" -gt 0 ]; then
    log "Restored $count priority import jobs from previous session"
  fi
}
