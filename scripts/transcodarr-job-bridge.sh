#!/bin/bash
# transcodarr-job-bridge.sh — Background poll for new .job files
# Watches queue directory for new imports, pushes to priority queue.
# Tracks ingested .job files by filename to avoid re-ingestion.
#
# Requires: QUEUE_DIR, JOB_TRACKED (associative array), q_push, log
# Must be sourced from entrypoint, not executed standalone.

job_bridge() {
  shopt -s nullglob
  while true; do
    # Ingest new .job files
    for f in "$QUEUE_DIR"/*.job; do
      [ -f "$f" ] || continue
      local fname
      fname=$(basename "$f")
      # Skip if already tracked
      [ -n "${JOB_TRACKED[$fname]+x}" ] && continue

      local svc filepath
      svc=$(head -1 "$f" 2>/dev/null || true)
      filepath=$(sed -n '2p' "$f" 2>/dev/null || true)
      [ -z "$svc" ] || [ -z "$filepath" ] && continue
      case "$filepath" in /movies/*|/tv/*) ;; *) continue ;; esac
      # If the path is missing, skip without sleeping — orphan .job files
      # (e.g. left behind by a mount-path restructure) would otherwise cost
      # 3s each per poll and silently DoS the bridge. Transient races where
      # the .job lands before the media write finishes are still caught on
      # the next 2s poll cycle.
      [ -f "$filepath" ] || continue

      # 4-tuple payload (Phase 1 verified-hash gate). Empty reasons →
      # admission falls through to full classify. Bridge-ingested jobs
      # come from arr-side custom scripts and don't carry API metadata.
      if ! q_push tc:candidates:import:ready "$svc|$filepath|0|"; then
        log "WARN: job bridge q_push failed for $svc $(basename "$filepath"); will retry"
        continue
      fi
      JOB_TRACKED["$fname"]=1
      log "Import queued (priority): $svc $(basename "$filepath")"
    done

    # Clean up tracking for .job files deleted by worker/skip cleanup
    for fname in "${!JOB_TRACKED[@]}"; do
      if [ ! -f "$QUEUE_DIR/$fname" ]; then
        unset 'JOB_TRACKED[$fname]'
      fi
    done

    sleep 2
  done
}
