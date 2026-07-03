#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# transcodarr-lib.sh — Shared functions for Transcodarr scripts
#
# Sourced by entrypoint.sh, queue.sh, and worker.sh. Not executed directly.
# ─────────────────────────────────────────────────────────────────────────────

# ── Valkey queue helpers ───────────────────────────────────────────────────
# Abstract valkey-cli vs redis-cli. All scripts use these, never the CLI directly.

QUEUE_CLI="${QUEUE_CLI:-valkey-cli}"

q_push()     { $QUEUE_CLI LPUSH "$1" "$2" > /dev/null; }
q_pop()      { $QUEUE_CLI BRPOPLPUSH "$1" "${1%:ready}:processing" "$2"; }
q_ack()      { $QUEUE_CLI LREM "${1%:ready}:processing" 1 "$2" > /dev/null; }
q_len()      { $QUEUE_CLI LLEN "$1"; }
q_list()     { $QUEUE_CLI LRANGE "$1" 0 -1; }

integration_url_allowed_shell() {
  local url="${1:-}" lower authority host
  lower="${url,,}"
  case "$lower" in
    http://*|https://*) ;;
    *) return 1 ;;
  esac

  authority="${url#*://}"
  authority="${authority%%[/?#]*}"
  [ -n "$authority" ] || return 1

  if [[ "$authority" == \[*\]* ]]; then
    host="${authority#\[}"
    host="${host%%\]*}"
  else
    host="${authority%%:*}"
  fi
  host="${host,,}"
  [ -n "$host" ] || return 1

  case "$host" in
    localhost|0.0.0.0|::1|127.*|::ffff:127.*|169.254.*|fe80:*) return 1 ;;
  esac
  return 0
}

# Phase 5C — batched push via Lua EVAL. Replaces N forks (one per q_push)
# with one fork per batch. The Lua body LPUSHes every payload onto the
# list key and INCRBYs the counter key by the count, atomically. Each
# payload is one ARGV bulk string — Valkey treats it as opaque bytes, so
# paths with apostrophes, brackets, spaces, etc. are byte-preserved.
#
# Usage:
#   accepted=$(q_push_batch_lua <list_key> <counter_key> p1 p2 ... pN)
# Echoes the server-reported accepted count (== N on success).
# Exit status: 0 on EVAL success, non-zero if valkey-cli itself failed.
# Caller compares echoed count to requested batch size to detect anomalies.
_QUEUE_LUA_PUSH_BATCH='local n = 0
for i = 1, #ARGV do
  redis.call("LPUSH", KEYS[1], ARGV[i])
  n = n + 1
end
if n > 0 then
  redis.call("INCRBY", KEYS[2], n)
end
return n'

q_push_batch_lua() {
  local list_key="$1" counter_key="$2"
  shift 2
  if [ "$#" -eq 0 ]; then
    echo 0
    return 0
  fi
  $QUEUE_CLI EVAL "$_QUEUE_LUA_PUSH_BATCH" 2 "$list_key" "$counter_key" "$@"
}

# Atomic dedupe: SADD returns 1 if new, 0 if already exists.
# Returns 0 (success/true) if NEW, 1 (failure/false) if duplicate.
q_try_mark() {
  local result
  result=$($QUEUE_CLI SADD tc:seen "$1")
  [ "$result" = "1" ]
}

# Clean up .job files for a completed filepath.
# No-op if no .job files exist. Safe to call on every success path.
cleanup_job_files_for_path() {
  local filepath="$1"
  local hash
  hash=$(echo -n "$filepath" | md5sum 2>/dev/null | cut -d' ' -f1) || return 0
  rm -f "${TRANSCODARR_QUEUE_DIR:-/queue}/${hash}_"*.job 2>/dev/null || true
}

# ── Phase 5A+5B — admission index (Valkey-backed cache rails) ────────
#       + Phase 5B narrowing (this session) — stat fingerprint fast path
#
# Each rail (failed, verified, fully_classified) gets three Valkey
# keys:
#   tc:idx:<rail>:ready          (string, "1" when index in sync with TSV)
#   tc:idx:<rail>:paths          (set of col1 path bytes)
#   tc:idx:<rail>:stat_by_path   (hash: path → sha256(path\0size\0mtime\0token))
#
# Phase 5B dropped the content-hash :fingerprints set from the indexed
# gate. The gate now authorizes a skip ONLY when the CURRENT
# stat-fingerprint matches the recorded statfp for the same path;
# a stat-mismatch (or stat-failure) falls through to classification.
#
# Note: an earlier 5B revision used a :stat_fingerprints
# SET keyed by sha256(path\0...). With one-row-per-path TSV semantics,
# the SET grew stale — recording a new row for an existing path didn't
# evict the old SADD'd statfp, so a gate query for the previous file
# bytes still hit and authorized a skip even though the TSV had a
# different row. Switching to HSET per-path (HSET overwrites in place)
# fixes this and matches the one-row-per-path invariant cleanly.
#
# Sample hash stays in TSV col2 for audit/durability and is still used
# by the legacy fallback path (awk-scan), but is never re-consulted
# in the indexed gate.
#
# TSV row schema (all rails, v2 / failed v1):
#   path \t hash \t col3 \t ts \t size \t mtime \t ctime
# col3 by rail: failed=free-form reason, verified=verified:aac_lc,
# fully_classified=verified:fully_classified.
#
# Token is a per-rail CONSTANT — failed rail's col3 (failure reason)
# is NOT a verdict, so the gate must never read it.
#   failed            → "failed"
#   verified          → "verified:aac_lc"
#   fully_classified  → "verified:fully_classified"
#
# Read-side: dispatcher (e.g. fully_classified_should_skip) consults
# :ready, :paths, then stat() + HGET :stat_by_path. Any Valkey command
# failure delegates to the legacy awk-scan path; "returned 0 (not a
# member)" / empty HGET is genuine non-match → process. Command
# failure ≠ non-match.
#
# Write-side: record helpers use _atomic_replace_path_in_tsv to enforce
# one-row-per-path under flock (atomic temp+mv). They maintain :paths
# and :stat_by_path (HSET overwrites the path's prior statfp in place).
# ANY maintenance failure invalidates :ready (DEL) so subsequent reads
# use the legacy fallback. The TSV stays the durable source of truth —
# next boot's rebuild repopulates from it.

# Compute a stat-fingerprint value. SHA-256 of
# "<path>\0<size>\0<mtime>\0<token>". NUL separators are unambiguous
# (POSIX paths can't contain NUL); 64-char hex output is safe as a
# Valkey hash value.
#
# Deliberately ignore ctime. Live Unraid/FUSE evidence showed ctime
# drifting across restarts/metadata touches while size+mtime stayed
# stable; including ctime turned warm cache rows into false misses and
# kept the candidate drain near the old full-classifier rate. Keep ctime
# in TSV col7 for audit, but do not make it load-bearing for the fast
# admission gate.
#   $1: library path
#   $2: file size (bytes, integer)
#   $3: mtime (seconds since epoch, integer — from `stat -c %Y`)
#   $4: ctime (seconds since epoch, integer — ignored, kept for call compatibility)
#   $5: rail token
_stat_fingerprint() {
  printf '%s\0%s\0%s\0%s' "$1" "$2" "$3" "$5" | sha256sum 2>/dev/null | awk '{print $1}'
}

# Stat a file and echo "<size>|<mtime>|<ctime>" using `stat -c '%s|%Y|%Z'`.
# Returns non-zero if stat fails (file missing, permission denied,
# etc.) — callers (record + dispatcher) treat that as "fall through
# to legacy / classify".
#
# Format chosen for portability: %s (size bytes), %Y (mtime seconds
# since epoch), %Z (ctime seconds since epoch). All POSIX-standard,
# all FUSE-safe. Seconds precision is adequate for Sonarr's atomic-
# rename workflow (mtime always bumps by ≥ 1 second). If sub-second
# resolution is ever needed, lift to `%y` / `%z` (raw strings) — but
# bump the schema version, since the fingerprint inputs change.
_stat_tuple() {
  local path="$1"
  stat -c '%s|%Y|%Z' "$path" 2>/dev/null
}

# Best-effort DEL of a rail's :ready marker. Called from record
# helpers when any index-maintenance step fails. Cannot itself fail
# the calling record operation. Logs via `log` if the sourcing script
# defined one; falls back to stderr printf otherwise (unit tests
# source transcodarr-lib.sh directly and have no `log`).
_invalidate_rail_index_ready() {
  local rail="$1"
  $QUEUE_CLI DEL "tc:idx:${rail}:ready" > /dev/null 2>&1 || true
  if declare -F log >/dev/null 2>&1; then
    log "WARN: rail $rail: index maintenance failed after TSV mutation; invalidating :ready"
  else
    printf '[transcodarr] WARN: rail %s: index maintenance failed after TSV mutation; invalidating :ready\n' "$rail" >&2
  fi
}

# Internal: called by record helpers after a successful atomic TSV
# replacement. Updates the rail's :paths set and :stat_by_path hash
# ONLY when :ready is "1". Any failure (GET, stat fingerprint compute,
# SADD, or HSET) invalidates :ready via _invalidate_rail_index_ready
# so subsequent reads use the legacy fallback.
#
# Phase 5B (HSET shape): replaces 5A's content-hash
# :fingerprints SADD-set maintenance AND the earlier :stat_fingerprints
# SADD-set design. The indexed gate now authorizes skips via HGET
# :stat_by_path <path> equality with the current stat fingerprint.
#   $1: rail name
#   $2: library path (col1)
#   $3: size  (col5)
#   $4: mtime (col6 — seconds since epoch)
#   $5: ctime (col7 — seconds since epoch)
#   $6: rail_token (per-rail constant)
_maintain_rail_index_after_record() {
  local rail="$1" lib_path="$2" size="$3" mtime="$4" ctime="$5" rail_token="$6"
  local ready
  if ! ready=$($QUEUE_CLI GET "tc:idx:${rail}:ready" 2>/dev/null); then
    _invalidate_rail_index_ready "$rail"
    return 0
  fi
  [ "$ready" = "1" ] || return 0

  local statfp
  if ! statfp=$(_stat_fingerprint "$lib_path" "$size" "$mtime" "$ctime" "$rail_token"); then
    _invalidate_rail_index_ready "$rail"
    return 0
  fi
  # HSET overwrites the path's previous statfp in place — matches the
  # one-row-per-path TSV invariant. An earlier SADD-set
  # approach left stale members around after a same-path replacement,
  # letting the indexed gate authorize skips for bytes no longer in
  # the TSV. With HSET, the prior statfp is gone the moment we write.
  if ! $QUEUE_CLI SADD "tc:idx:${rail}:paths" "$lib_path" > /dev/null 2>&1 \
     || ! $QUEUE_CLI HSET "tc:idx:${rail}:stat_by_path" "$lib_path" "$statfp" > /dev/null 2>&1; then
    _invalidate_rail_index_ready "$rail"
    return 0
  fi
  return 0
}

# Atomic one-row-per-path TSV replacement. Reads <tsv>, filters out
# any existing row for <lib_path>, appends the new row, writes to a
# temp file, then `mv` overwrites the real TSV. flock on <tsv>.lock
# serializes concurrent record-helper invocations so two workers
# can't race read-modify-write.
#
# This enforces the Phase 5B "one row per path per rail" invariant.
# Earlier 5A appended (or deduped only on exact path+hash+verdict
# match), letting bloat accumulate.
#
# The header line (col1 starts with `#`) is preserved if present —
# validate_or_reset writes one for verified/fully_classified/failed
# rails. Blank lines (TSV that was just truncated) are tolerated.
#
#   $1: tsv path
#   $2: lib_path (col1 of the new row)
#   $3: full TSV row to append (already tab-delimited, no trailing \n)
# Returns 0 on success, non-zero if mktemp / flock / write fails.
# Internal: the actual read-filter-write-mv. Run while holding the
# rail's lock. Returns 0 on success, non-zero on any I/O failure.
_atomic_replace_path_in_tsv_inner() {
  local tsv="$1" lib_path="$2" new_row="$3"
  local tmp_file
  if ! tmp_file=$(mktemp "${tsv}.XXXXXX" 2>/dev/null); then
    return 1
  fi
  if [ -f "$tsv" ]; then
    # Keep header (#-prefixed) and any row whose col1 != lib_path.
    # awk -v p="$lib_path" — exact field comparison, no regex collisions.
    awk -F'\t' -v p="$lib_path" '
      /^#/ {print; next}
      NF == 0 {next}
      $1 != p {print}
    ' "$tsv" > "$tmp_file" 2>/dev/null || { rm -f "$tmp_file"; return 1; }
  fi
  printf '%s\n' "$new_row" >> "$tmp_file" 2>/dev/null || { rm -f "$tmp_file"; return 1; }
  mv -f "$tmp_file" "$tsv" 2>/dev/null || { rm -f "$tmp_file"; return 1; }
  return 0
}

_atomic_replace_path_in_tsv() {
  local tsv="$1" lib_path="$2" new_row="$3"
  local rail="${4:-}" size="${5:-}" mtime="${6:-}" ctime="${7:-}" rail_token="${8:-}"
  local lock_target="${tsv}.lock"
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  # Preferred locker: flock (Linux production). Falls back to a portable
  # mkdir-based lock when flock isn't on PATH (e.g. Windows git-bash unit
  # tests). mkdir is atomic on POSIX; the lockdir creation either wins or
  # blocks the loser into a sleep-retry until the holder rmdir's it.
  if command -v flock >/dev/null 2>&1; then
    touch "$lock_target" 2>/dev/null || return 1
    (
      flock -x 200 || exit 1
      _atomic_replace_path_in_tsv_inner "$tsv" "$lib_path" "$new_row" || exit 1
      if [ -n "$rail" ]; then
        _maintain_rail_index_after_record "$rail" "$lib_path" "$size" "$mtime" "$ctime" "$rail_token"
      fi
      exit $?
    ) 200>"$lock_target"
    return $?
  fi

  # Portable mkdir-based lock fallback. 30-second cap (~600 × 50ms) so
  # a stale lock dir (process killed mid-write) doesn't wedge tests
  # forever; if it ever fires we'd want to investigate.
  local lockdir="${lock_target}dir" waited=0
  while ! mkdir "$lockdir" 2>/dev/null; do
    sleep 0.05
    waited=$((waited + 1))
    if [ "$waited" -gt 600 ]; then
      return 1
    fi
  done
  _atomic_replace_path_in_tsv_inner "$tsv" "$lib_path" "$new_row" || {
    local rc=$?
    rmdir "$lockdir" 2>/dev/null || true
    return $rc
  }
  if [ -n "$rail" ]; then
    _maintain_rail_index_after_record "$rail" "$lib_path" "$size" "$mtime" "$ctime" "$rail_token"
  fi
  local rc=$?
  rmdir "$lockdir" 2>/dev/null || true
  return $rc
}

_atomic_remove_path_from_tsv_inner() {
  local tsv="$1" lib_path="$2"
  [ -f "$tsv" ] || return 0

  local tmp_file
  if ! tmp_file=$(mktemp "${tsv}.XXXXXX" 2>/dev/null); then
    return 1
  fi
  awk -F'\t' -v p="$lib_path" '
    /^#/ {print; next}
    NF == 0 {next}
    $1 != p {print}
  ' "$tsv" > "$tmp_file" 2>/dev/null || { rm -f "$tmp_file"; return 1; }
  mv -f "$tmp_file" "$tsv" 2>/dev/null || { rm -f "$tmp_file"; return 1; }
  return 0
}

_atomic_remove_failed_display_path_inner() {
  local tsv="$1" lib_path="$2"
  [ -f "$tsv" ] || return 0

  local tmp_file
  if ! tmp_file=$(mktemp "${tsv}.XXXXXX" 2>/dev/null); then
    return 1
  fi
  awk -F'\t' -v p="$lib_path" '
    /^#/ {print; next}
    NF == 0 {next}
    $4 != p {print}
  ' "$tsv" > "$tmp_file" 2>/dev/null || { rm -f "$tmp_file"; return 1; }
  mv -f "$tmp_file" "$tsv" 2>/dev/null || { rm -f "$tmp_file"; return 1; }
  return 0
}

_remove_failed_hash_path_locked_inner() {
  local tsv="$1" lib_path="$2"
  _atomic_remove_path_from_tsv_inner "$tsv" "$lib_path" || return 1
  if ! $QUEUE_CLI SREM "tc:idx:failed:paths" "$lib_path" > /dev/null 2>&1 \
     || ! $QUEUE_CLI HDEL "tc:idx:failed:stat_by_path" "$lib_path" > /dev/null 2>&1; then
    _invalidate_rail_index_ready failed
  fi
  return 0
}

_remove_verified_hash_path_locked_inner() {
  local tsv="$1" lib_path="$2"
  _atomic_remove_path_from_tsv_inner "$tsv" "$lib_path" || return 1
  if ! $QUEUE_CLI SREM "tc:idx:verified:paths" "$lib_path" > /dev/null 2>&1 \
     || ! $QUEUE_CLI HDEL "tc:idx:verified:stat_by_path" "$lib_path" > /dev/null 2>&1; then
    _invalidate_rail_index_ready verified
  fi
  return 0
}

_remove_fully_classified_path_locked_inner() {
  local tsv="$1" lib_path="$2"
  _atomic_remove_path_from_tsv_inner "$tsv" "$lib_path" || return 1
  if ! $QUEUE_CLI SREM "tc:idx:fully_classified:paths" "$lib_path" > /dev/null 2>&1 \
     || ! $QUEUE_CLI HDEL "tc:idx:fully_classified:stat_by_path" "$lib_path" > /dev/null 2>&1; then
    _invalidate_rail_index_ready fully_classified
  fi
  return 0
}

# Single-pass stat-fingerprint computation for boot-time rebuild.
# Reads new-schema TSV rows from stdin:
#   path \t hash \t col3 \t ts \t size \t mtime \t ctime
# Emits "path\tstatfp" lines, one per valid row. One perl process
# handles all SHA-256 work — avoiding 22k+ sha256sum forks per rail
# at boot. Comment lines (col1 starts with `#`) and blank rows are
# skipped. RAIL + RAIL_TOKEN env vars carry the per-rail constants.
#
# Rail-aware col3 (verdict) filter — defense-in-depth that matches
# or exceeds the legacy gate's strictness:
#   - failed:           col3 is a free-form failure reason → no check
#   - verified:         require col3 == "verified:aac_lc"
#   - fully_classified: require col3 == "verified:fully_classified"
#
# Rows missing the stat columns (legacy 4-col format) are SKIPPED
# entirely — Phase 5B requires a stat fingerprint for the index, and
# Phase 5B's schema bump should have already truncated any 4-col TSV
# at boot via *_validate_or_reset. A stray 4-col row this late is
# treated as malformed.
#
# Exits non-zero on missing env vars; row-level parse errors are
# silent skips (caller can't usefully recover per-row).
#   $1: rail name (failed | verified | fully_classified)
#   $2: rail token (constant used in the SHA-256 fingerprint)
_emit_stat_fingerprints_for_rebuild() {
  local rail="$1" rail_token="$2"
  RAIL="$rail" RAIL_TOKEN="$rail_token" perl -MDigest::SHA=sha256_hex -F'\t' -lne '
    BEGIN {
      die "_emit_stat_fingerprints_for_rebuild: RAIL env not set\n"       unless defined $ENV{RAIL}       && length $ENV{RAIL};
      die "_emit_stat_fingerprints_for_rebuild: RAIL_TOKEN env not set\n" unless defined $ENV{RAIL_TOKEN} && length $ENV{RAIL_TOKEN};
    }
    next if /^#/;
    # 7-col new schema: path hash col3 ts size mtime ctime
    next unless @F >= 7;
    my ($p, $h, $v, $ts, $size, $mtime, $ctime) = @F;
    next unless defined $p && length($p) > 0;
    next unless defined $size  && length($size)  > 0;
    next unless defined $mtime && length($mtime) > 0;
    next unless defined $ctime && length($ctime) > 0;
    if ($ENV{RAIL} eq "verified") {
      next unless defined $v && $v eq "verified:aac_lc";
    } elsif ($ENV{RAIL} eq "fully_classified") {
      next unless defined $v && $v eq "verified:fully_classified";
    }
    # failed rail: col3 is free-form reason → no filter
    # ctime is intentionally ignored for the fast-path fingerprint; see
    # _stat_fingerprint above. TSV still carries ctime for audit.
    my $statfp = sha256_hex("$p\0$size\0$mtime\0$ENV{RAIL_TOKEN}");
    print "$p\t$statfp";
  '
}

# Boot-time best-effort loader. Populates the rail's :paths and
# :fingerprints sets from <tsv_path>, then SETs :ready 1 on success.
# Returns 0 on success, non-zero on failure (Valkey unreachable,
# fingerprint emit fails, SADD batch fails). Leaves :ready unset on
# failure — per-helper dispatchers route to legacy awk-scan in that
# case. Probe workers can start regardless: the index is purely
# additive, the legacy path is correctness-preserving.
#   $1: rail name (failed | verified | fully_classified)
#   $2: TSV path
#   $3: rail_token
rebuild_rail_index() {
  local rail="$1" tsv="$2" rail_token="$3"
  local paths_key="tc:idx:${rail}:paths"
  local stat_by_path_key="tc:idx:${rail}:stat_by_path"
  local ready_key="tc:idx:${rail}:ready"
  # DEL the two legacy keys if they exist from earlier deploys:
  #   :fingerprints       — 5A content-hash set
  #   :stat_fingerprints  — 5B's first SADD-set design (superseded)
  # Harmless DEL on a non-existent key.
  local legacy_fp_key="tc:idx:${rail}:fingerprints"
  local legacy_sfp_key="tc:idx:${rail}:stat_fingerprints"

  # Internal: log a WARN line that names this rail + the specific cause.
  # All return-1 sites below funnel through this so the silent-failure
  # mode (rebuild silently fails → dispatcher silently stuck on legacy)
  # cannot recur. Uses `log` if the sourcing script defined one,
  # otherwise stderr printf.
  _rebuild_warn() {
    local cause="$1"
    if declare -F log >/dev/null 2>&1; then
      log "WARN: rail $rail: $cause; leaving :ready unset"
    else
      printf '[transcodarr] WARN: rail %s: %s; leaving :ready unset\n' "$rail" "$cause" >&2
    fi
  }

  # Clear stale state — Valkey is wiped on container restart, but be
  # explicit for the policy-invalidation-during-runtime edge case.
  # Also DELs the two legacy keys (5A :fingerprints, the earlier
  # :stat_fingerprints) if present. No-op if absent.
  $QUEUE_CLI DEL "$ready_key" "$paths_key" "$stat_by_path_key" "$legacy_fp_key" "$legacy_sfp_key" > /dev/null 2>&1 || true

  # Empty rail (TSV missing) is a valid state — mark ready and return.
  if [ ! -f "$tsv" ]; then
    if ! $QUEUE_CLI SET "$ready_key" 1 > /dev/null 2>&1; then
      _rebuild_warn "Valkey SET failed marking empty rail ready (server unreachable?)"
      return 1
    fi
    return 0
  fi

  # Lua batch: takes interleaved (path, statfp) pairs as ARGV. For
  # each pair: SADD path into :paths, HSET path→statfp into
  # :stat_by_path. One EVAL per ~1000 rows.
  local lua_batch_index='local n = 0
for i = 1, #ARGV, 2 do
  redis.call("SADD", KEYS[1], ARGV[i])
  redis.call("HSET", KEYS[2], ARGV[i], ARGV[i+1])
  n = n + 1
end
return n'

  # Spool the emit output to a tempfile so we can check the emit
  # exit status BEFORE doing any EVAL or :ready SET. A previous
  # process-substitution form (`done < <(_emit ... < tsv)`) silently
  # swallowed a non-zero emit exit, allowing partial/empty indexes
  # to land with :ready=1 — that lets known-failed files through
  # the gate without falling back to legacy. Tempfile + explicit
  # exit check makes the failure load-bearing.
  local emit_tmp
  if ! emit_tmp=$(mktemp 2>/dev/null); then
    _rebuild_warn "mktemp failed (no /tmp space?)"
    return 1
  fi

  # NOTE: stderr is NOT redirected into $emit_tmp — perl warnings/errors
  # belong in the container log, not in the data stream that the read
  # loop below will tokenize as path<TAB>statfp.
  if ! _emit_stat_fingerprints_for_rebuild "$rail" "$rail_token" < "$tsv" > "$emit_tmp"; then
    _rebuild_warn "stat-fingerprint emit failed (perl/Digest crash?)"
    rm -f "$emit_tmp"
    return 1
  fi

  local BATCH=()
  local BATCH_SIZE=2000   # 2000 ARGV elements = 1000 (path, statfp) pairs
  local count=0 path statfp

  while IFS=$'\t' read -r path statfp; do
    [ -z "$path" ] && continue
    [ -z "$statfp" ] && continue
    BATCH+=("$path" "$statfp")
    count=$((count + 1))
    if (( ${#BATCH[@]} >= BATCH_SIZE )); then
      if ! $QUEUE_CLI EVAL "$lua_batch_index" 2 "$paths_key" "$stat_by_path_key" "${BATCH[@]}" > /dev/null 2>&1; then
        _rebuild_warn "Valkey EVAL batch failed at row $count (server unreachable?)"
        rm -f "$emit_tmp"
        return 1
      fi
      BATCH=()
    fi
  done < "$emit_tmp"

  if (( ${#BATCH[@]} > 0 )); then
    if ! $QUEUE_CLI EVAL "$lua_batch_index" 2 "$paths_key" "$stat_by_path_key" "${BATCH[@]}" > /dev/null 2>&1; then
      _rebuild_warn "Valkey EVAL final-flush failed (server unreachable?)"
      rm -f "$emit_tmp"
      return 1
    fi
  fi

  rm -f "$emit_tmp"
  if ! $QUEUE_CLI SET "$ready_key" 1 > /dev/null 2>&1; then
    _rebuild_warn "Valkey SET :ready failed after $count rows indexed (server unreachable?)"
    return 1
  fi

  if declare -F log >/dev/null 2>&1; then
    log "rail $rail: indexed $count rows from $tsv"
  fi
  return 0
}

# ── Phase 5D — cache hygiene (boot-time prune + compact) ────────────
#
# Cache TSVs can outlive renamed/deleted media paths (Sonarr/Radarr
# library reorgs, manual moves) and can accumulate duplicate rows
# under historical bugs. 5D adds a boot-time best-effort step that:
#
#   1. Builds a snapshot of CURRENT library paths from Sonarr+Radarr.
#   2. For each rail/current-state TSV, keeps a row when:
#        - the path is in the Arr snapshot, OR
#        - the path still exists on disk (conservative fallback)
#      and prunes the row otherwise.
#   3. Compacts duplicate rail paths to one row each, last-row-wins.
#      Flagged current-state rows keep all rows for a valid path because
#      one path can carry multiple independent flag reasons.
#
# Runs AFTER Valkey is ready and BEFORE rebuild_rail_index so the
# pruned TSV is what the index gets populated from. If Arr is
# unreachable or returns garbage, the whole step is a no-op (the
# durable TSV is the source of truth — never destructively pruned
# without a complete snapshot). Failure logs WARN; boot continues.

# prune_cache_tsv_against_paths <rail> <tsv> <valid_paths_file> [path_col] [min_cols] [compact]
#
# Filters one cache TSV in place using the conservative rule above.
# Header/comment lines are preserved; malformed rows are dropped.
# By default this handles the 7-column rail TSV shape with path in
# column 1 and duplicate compaction enabled. The optional args let the
# flagged-files.tsv current-state snapshot reuse the same rule with path
# in column 4 and compaction disabled.
#
# Returns 0 on success, non-zero on I/O failure (mktemp, perl crash,
# mv). The orchestrator (prune_cache_rows_against_arr_snapshot) maps
# any non-zero exit to a WARN line and continues.
prune_cache_tsv_against_paths() {
  local rail="$1" tsv="$2" valid_paths_file="$3" path_col="${4:-1}" min_cols="${5:-7}" compact="${6:-1}"
  [ -n "$rail" ] && [ -n "$tsv" ] && [ -n "$valid_paths_file" ] || return 1
  [ -f "$tsv" ] || return 0
  [ -f "$valid_paths_file" ] || return 1
  [[ "$path_col" =~ ^[1-9][0-9]*$ ]] || return 1
  [[ "$min_cols" =~ ^[1-9][0-9]*$ ]] || return 1

  local tmp
  tmp=$(mktemp "${tsv}.prune.XXXXXX") || return 1

  # Perl pass — load valid-paths into a hash, stream the TSV, decide
  # row-by-row. Duplicate compaction uses last-row-wins by storing
  # row content in a hash keyed by path; we preserve insertion order
  # via a parallel array. Header (#-prefixed) preserved once at top.
  # Stat (`-e $path`) is a syscall per non-Arr row, but only rows
  # that DON'T appear in the snapshot reach it — Arr-known rows
  # short-circuit before the syscall.
  if ! VALID_PATHS_FILE="$valid_paths_file" RAIL="$rail" \
       PATH_COL="$path_col" MIN_COLS="$min_cols" COMPACT_DUPLICATES="$compact" \
       perl -Mstrict -Mwarnings -e '
    my $valid_file = $ENV{VALID_PATHS_FILE} or die "VALID_PATHS_FILE not set\n";
    my $rail       = $ENV{RAIL} // "?";
    my $path_col   = $ENV{PATH_COL} // 1;
    my $min_cols   = $ENV{MIN_COLS} // 7;
    my $compact    = ($ENV{COMPACT_DUPLICATES} // "1") ne "0";
    die "invalid PATH_COL\n" unless $path_col =~ /^[1-9][0-9]*$/;
    die "invalid MIN_COLS\n" unless $min_cols =~ /^[1-9][0-9]*$/;
    my $path_idx = $path_col - 1;

    my %valid;
    open my $vf, "<", $valid_file or die "open valid paths: $!\n";
    while (my $p = <$vf>) {
      chomp $p;
      $valid{$p} = 1 if length $p;
    }
    close $vf;

    my $header;
    my (%row, @order, %seen);
    my @kept_rows;
    my $pruned = 0;
    my $malformed = 0;

    while (my $line = <STDIN>) {
      if ($line =~ /^#/) {
        $header = $line unless defined $header;
        next;
      }
      chomp(my $stripped = $line);
      next unless length $stripped;
      my @F = split /\t/, $line, -1;
      if (@F < $min_cols || $path_idx >= @F) {
        $malformed++;
        next;
      }
      my $path = $F[$path_idx];
      next unless defined $path && length $path;

      if ($valid{$path} || -e $path) {
        if ($compact) {
          $row{$path} = $line;
          if (!exists $seen{$path}) {
            push @order, $path;
            $seen{$path} = 1;
          }
        } else {
          push @kept_rows, $line;
        }
      } else {
        $pruned++;
      }
    }

    print $header if defined $header;
    my $kept = 0;
    if ($compact) {
      for my $path (@order) {
        next unless exists $row{$path};
        print $row{$path};
        $kept++;
      }
    } else {
      for my $line (@kept_rows) {
        print $line;
        $kept++;
      }
    }
    print STDERR "rail=$rail kept=$kept pruned=$pruned malformed=$malformed\n";
  ' < "$tsv" > "$tmp" 2>>"${tsv}.prune.log"; then
    rm -f "$tmp"
    return 1
  fi

  if ! mv -f "$tmp" "$tsv"; then
    rm -f "$tmp"
    return 1
  fi
  return 0
}

# collect_arr_valid_paths <output_file>
#
# Dumps current library paths from configured Sonarr + Radarr APIs
# (each one optional; both can be configured or only one). Output is
# newline-separated unique paths, one per line.
#
# Returns:
#   0  — at least one Arr was configured and all configured Arrs
#        responded successfully + parsed cleanly
#   1  — at least one configured Arr failed (curl error, non-JSON
#        body, non-array root). DO NOT trust a partial snapshot.
#   2  — neither Arr is configured
#
# Uses the same embedded Perl JSON parser shape as
# scripts/transcodarr-queue.sh's fast endpoint, NOT JSON::PP — keeps
# the dependency surface identical to what queue.sh already runs.
collect_arr_valid_paths() {
  local out="$1"
  [ -n "$out" ] || return 1
  : > "$out" || return 1

  local configured=0
  local perl_lib
  perl_lib=$(mktemp /tmp/transcodarr-prune-json.XXXXXX.pl) || return 1
  cat > "$perl_lib" <<'PERL_JSON'
use strict;
use warnings;

our $json;
our $pos;

sub init_json { local $/; $json = <STDIN>; $pos = 0; }
sub peek { return substr($json, $pos, 1) if $pos < length($json); return ""; }
sub skip_ws { $pos++ while $pos < length($json) && substr($json, $pos, 1) =~ /\s/; }
sub parse_string {
  die "expected quote at pos $pos" unless substr($json, $pos, 1) eq '"';
  $pos++;
  my $start = $pos;
  while ($pos < length($json)) {
    my $c = substr($json, $pos, 1);
    if ($c eq '\\') { $pos += 2; next; }
    if ($c eq '"') {
      my $s = substr($json, $start, $pos - $start);
      $pos++;
      $s =~ s/\\n/\n/g; $s =~ s/\\t/\t/g; $s =~ s/\\r/\r/g;
      $s =~ s/\\"/"/g;
      $s =~ s/\\u([0-9a-fA-F]{4})/chr(hex($1))/ge;
      $s =~ s/\\\\/\\/g;
      $s =~ s/\\(.)/$1/g;
      return $s;
    }
    $pos++;
  }
  die "unterminated string";
}
sub parse_number {
  my $start = $pos;
  $pos++ while $pos < length($json) && substr($json, $pos, 1) =~ /[\d.eE+\-]/;
  return substr($json, $start, $pos - $start) + 0;
}
sub parse_value {
  skip_ws();
  my $c = peek();
  if ($c eq '"') { return parse_string(); }
  if ($c eq '{') { return parse_object(); }
  if ($c eq '[') { return parse_array(); }
  if ($c =~ /[\d\-]/) { return parse_number(); }
  if (substr($json, $pos, 4) eq 'true')  { $pos += 4; return 1; }
  if (substr($json, $pos, 5) eq 'false') { $pos += 5; return 0; }
  if (substr($json, $pos, 4) eq 'null')  { $pos += 4; return undef; }
  die "unexpected char at pos $pos: '$c'";
}
sub parse_object {
  $pos++; skip_ws();
  my %obj;
  if (peek() ne '}') {
    while (1) {
      skip_ws();
      my $key = parse_string();
      skip_ws();
      die "expected colon" unless substr($json, $pos, 1) eq ':';
      $pos++;
      $obj{$key} = parse_value();
      skip_ws();
      last if peek() eq '}';
      die "expected , or }" unless substr($json, $pos, 1) eq ',';
      $pos++;
    }
  }
  $pos++; return \%obj;
}
sub parse_array {
  $pos++; skip_ws();
  my @arr;
  if (peek() ne ']') {
    while (1) {
      push @arr, parse_value();
      skip_ws();
      last if peek() eq ']';
      die "expected , or ]" unless substr($json, $pos, 1) eq ',';
      $pos++;
    }
  }
  $pos++; return \@arr;
}
1;
PERL_JSON

  # Cleanup trap for the temp parser. Even if the function returns
  # early the parser file is removed by the caller's trap chain — but
  # if the function returns successfully the trap below clears it.
  local _rm_perl_lib=1

  # Sonarr — use the fast aggregator endpoint (same one queue.sh uses).
  if [ -n "${SONARR_URL:-}" ] && [ -n "${SONARR_API_KEY:-}" ]; then
    configured=1
    local sonarr_url="${SONARR_URL%/}"
    if ! integration_url_allowed_shell "$sonarr_url"; then
      rm -f "$perl_lib"
      return 1
    fi
    if ! curl -sf --max-time 120 -H "X-Api-Key: ${SONARR_API_KEY}" \
         -- "${sonarr_url}/api/v3/episodefile/transcodarr" 2>/dev/null \
       | perl -e '
           require "'"$perl_lib"'";
           init_json();
           skip_ws();
           my $c = peek();
           die "empty body\n" if $c eq "";
           my $rows = parse_value();
           die "sonarr root is not array\n" unless ref $rows eq "ARRAY";
           for my $r (@$rows) {
             next unless ref $r eq "HASH";
             my $p = $r->{path};
             print "$p\n" if defined $p && length $p;
           }
         ' >> "$out" 2>/dev/null; then
      rm -f "$perl_lib"
      return 1
    fi
  fi

  # Radarr — full /movie endpoint, path lives under movieFile.
  if [ -n "${RADARR_URL:-}" ] && [ -n "${RADARR_API_KEY:-}" ]; then
    configured=1
    local radarr_url="${RADARR_URL%/}"
    if ! integration_url_allowed_shell "$radarr_url"; then
      rm -f "$perl_lib"
      return 1
    fi
    if ! curl -sf --max-time 120 -H "X-Api-Key: ${RADARR_API_KEY}" \
         -- "${radarr_url}/api/v3/movie" 2>/dev/null \
       | perl -e '
           require "'"$perl_lib"'";
           init_json();
           skip_ws();
           my $c = peek();
           die "empty body\n" if $c eq "";
           my $movies = parse_value();
           die "radarr root is not array\n" unless ref $movies eq "ARRAY";
           for my $m (@$movies) {
             next unless ref $m eq "HASH";
             my $mf = $m->{movieFile};
             next unless ref $mf eq "HASH";
             my $p = $mf->{path};
             print "$p\n" if defined $p && length $p;
           }
         ' >> "$out" 2>/dev/null; then
      rm -f "$perl_lib"
      return 1
    fi
  fi

  rm -f "$perl_lib"
  [ "$configured" -eq 1 ] || return 2

  # Dedupe (sort -u). Snapshot order doesn't matter — the consumer
  # builds a hash for O(1) lookup.
  local dedup
  dedup=$(mktemp "${out}.dedup.XXXXXX") || return 1
  if ! sort -u "$out" > "$dedup"; then
    rm -f "$dedup"
    return 1
  fi
  mv -f "$dedup" "$out" || { rm -f "$dedup"; return 1; }
  return 0
}

# Boot-time orchestrator. Best-effort: any failure short-circuits to a
# WARN line and returns 0 so boot continues. The TSV is the source of
# truth — never destructively pruned without a complete Arr snapshot.
prune_cache_rows_against_arr_snapshot() {
  local valid_paths
  if ! valid_paths=$(mktemp /tmp/transcodarr-valid-paths.XXXXXX); then
    log "WARN: cache hygiene skipped: mktemp failed"
    return 0
  fi

  local collect_rc=0
  collect_arr_valid_paths "$valid_paths" 2>/dev/null || collect_rc=$?
  if [ "$collect_rc" -ne 0 ]; then
    if [ "$collect_rc" -eq 2 ]; then
      log "WARN: cache hygiene skipped: no Arr APIs configured"
    else
      log "WARN: cache hygiene skipped: Arr snapshot unavailable (collect rc=$collect_rc)"
    fi
    rm -f "$valid_paths"
    return 0
  fi

  local path_count
  path_count=$(wc -l < "$valid_paths" 2>/dev/null | tr -d ' ')
  if [ -z "$path_count" ] || [ "$path_count" -eq 0 ] 2>/dev/null; then
    log "WARN: cache hygiene skipped: Arr snapshot contained 0 paths"
    rm -f "$valid_paths"
    return 0
  fi

  prune_cache_tsv_against_paths failed           "$(failed_hash_tsv_path)"             "$valid_paths" \
    || log "WARN: cache hygiene failed for failed rail"
  prune_cache_tsv_against_paths verified         "$(verified_hashes_tsv_path)"         "$valid_paths" \
    || log "WARN: cache hygiene failed for verified rail"
  prune_cache_tsv_against_paths fully_classified "$(fully_classified_hashes_tsv_path)" "$valid_paths" \
    || log "WARN: cache hygiene failed for fully_classified rail"
  prune_cache_tsv_against_paths flagged "${TRANSCODARR_FLAGGED_FILES_TSV:-${TRANSCODARR_STATE_DIR:-/state}/flagged-files.tsv}" "$valid_paths" 4 4 0 \
    || log "WARN: cache hygiene failed for flagged current state"

  log "cache hygiene complete: snapshot_paths=$path_count"
  rm -f "$valid_paths"
  return 0
}

# ── Failed-hash gate helpers ───────────────────────────────────────────
# Records SHA-256 of input on worker failure (record_failed). Probe-pool
# admission compares the candidate's current SHA-256 against recorded
# rows for the same path — same content → skip; different content →
# process. Same path can accumulate multiple recorded hashes as bytes
# change over time; any match skips.

failed_files_tsv_path() {
  echo "${TRANSCODARR_FAILED_FILES_TSV:-${TRANSCODARR_STATE_DIR:-/state}/failed-files.tsv}"
}

failed_hash_tsv_path() {
  echo "${TRANSCODARR_FAILED_HASHES_TSV:-${TRANSCODARR_STATE_DIR:-/state}/failed-hashes.tsv}"
}

# Remove resolved path rows from the Failed-tab display TSV. This is not
# the failed-hash gate; it only keeps the user-facing failure list current
# after a later same-path success.
failed_display_remove_path() {
  local lib_path="${1:-}"
  [ -n "$lib_path" ] || return 0

  local tsv lock_target
  tsv=$(failed_files_tsv_path)
  [ -f "$tsv" ] || return 0
  lock_target="${tsv}.lock"
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  if command -v flock >/dev/null 2>&1; then
    touch "$lock_target" 2>/dev/null || return 0
    (
      flock -x 200 || exit 0
      _atomic_remove_failed_display_path_inner "$tsv" "$lib_path" || exit 0
    ) 200>"$lock_target"
    return 0
  fi

  local lockdir="${lock_target}dir" waited=0
  while ! mkdir "$lockdir" 2>/dev/null; do
    sleep 0.05
    waited=$((waited + 1))
    [ "$waited" -le 600 ] || return 0
  done
  _atomic_remove_failed_display_path_inner "$tsv" "$lib_path" || true
  rmdir "$lockdir" 2>/dev/null || true
  return 0
}

# record_failure_row — shared terminal-failure writer (spec §8).
# Writes ONE display row to failed-files.tsv using a COLUMN-4 replace
# (the path lives in column 4 of the display TSV), and records the
# content-hash gate row via failed_hash_record (column-1 replace on
# the hash TSV). Used by the worker (record_failed delegates here) and
# by lang_detect (Plan B) so a single path never carries two display
# rows. orig_size is in BYTES (matches the worker's origSize column).
#
# Deliberately does a col-4 remove-then-append rather than the col-1
# _atomic_replace_path_in_tsv: that helper keys column 1 (the hash
# TSV's schema). On the display TSV, whose path is column 4, the col-1
# replace would never match and would append beside the stale row.
#   $1: path (library path; display col 4 + hash col 1)
#   $2: service
#   $3: reason
#   $4: failure_class
#   $5: vcodec
#   $6: channels
#   $7: orig_size (BYTES)
#   $8: disk
#   $9: read_path (I/O path for the hash compute; disk-direct when active)
record_failure_row() {
  local path="${1:-}" service="${2:-}" reason="${3:-}" failure_class="${4:-n/a}"
  local vcodec="${5:-unknown}" channels="${6:-0}" orig_size="${7:-0}"
  local disk="${8:-unknown}" read_path="${9:-}"
  [ -n "$path" ] || return 0
  [ -n "$read_path" ] || read_path="$path"

  local tsv
  tsv=$(failed_files_tsv_path)
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  # Column-4 replace: drop the path's existing display row, then append
  # the new terminal row. Both operations must be locked together to prevent
  # concurrent callers from appending between our remove and append.
  local lock_target="${tsv}.lock"
  local _new_row
  _new_row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
    "$(date -Iseconds)" "$service" "$reason" "$path" \
    "$vcodec" "$channels" "$orig_size" "$disk" "$failure_class")

  if command -v flock >/dev/null 2>&1; then
    touch "$lock_target" 2>/dev/null || true
    (
      flock -x 200 || exit 0
      _atomic_remove_failed_display_path_inner "$tsv" "$path" || true
      printf '%s\n' "$_new_row" >> "$tsv" || true
    ) 200>"$lock_target"
  else
    local _lockdir="${lock_target}dir" _waited=0
    while ! mkdir "$_lockdir" 2>/dev/null; do
      sleep 0.05; _waited=$((_waited + 1)); [ "$_waited" -le 600 ] || break
    done
    _atomic_remove_failed_display_path_inner "$tsv" "$path" || true
    printf '%s\n' "$_new_row" >> "$tsv" || true
    rmdir "$_lockdir" 2>/dev/null || true
  fi

  # Content-hash gate row (col-1 replace — correct for the hash TSV).
  failed_hash_record "$read_path" "$path" "$reason" || true
}

# release_admission — make a failed file re-enterable (spec §8).
# Clears the tc:seen admission marks, removes the failed-HASH gate row
# robustly (even when the failed index isn't ready), removes stale positive
# admission rows, and clears stale .job files. It does NOT touch the
# user-facing display
# failed-files.tsv row — that cleanup is done only at genuine
# resolution (English success), never here.
#   $1: filepath (library path; tc:seen + hash-row col-1 key)
#   $2: disk_read_path (direct-disk path the tc:seen fingerprint stats)
#   $3: pre_seen_member (optional) — the exact "filepath:size:inode" tc:seen member captured by the caller BEFORE mkvpropedit ran. When empty, computed from the current file (correct for the backfill, which never mutates the file).
release_admission() {
  local filepath="${1:-}" disk_read_path="${2:-}" pre_seen_member="${3:-}"
  [ -n "$filepath" ] || return 0
  [ -n "$disk_read_path" ] || disk_read_path="$filepath"

  # tc:seen is a SET -> SREM the specific member, never DEL the key.
  # The pre-tag member MUST be captured by the caller before mkvpropedit
  # ran (size:inode may change); fall back to the current fingerprint when
  # the caller didn't supply it (backfill never mutates the file).
  local pre
  if [ -n "$pre_seen_member" ]; then
    pre="$pre_seen_member"
  else
    pre="${filepath}:$(fingerprint "$disk_read_path")"
  fi
  $QUEUE_CLI SREM tc:seen "$pre" > /dev/null 2>&1 || true

  # Failed-hash removal robust to a stale/non-ready failed index.
  # failed_hash_remove_path early-returns on a :paths miss and the
  # should-skip gate falls back to a TSV scan when :ready != 1, so an
  # index miss could otherwise leave the file gated. Ensure the index
  # is ready (rebuild if not) before delegating; if the rebuild can't
  # confirm readiness, fall back to the locked-TSV removal which
  # deletes the col-1 row independent of the index.
  local ready
  ready=$($QUEUE_CLI GET "tc:idx:failed:ready" 2>/dev/null || echo "")
  if [ "$ready" != "1" ]; then
    local hash_tsv
    hash_tsv=$(failed_hash_tsv_path)
    rebuild_rail_index failed "$hash_tsv" failed || true
    ready=$($QUEUE_CLI GET "tc:idx:failed:ready" 2>/dev/null || echo "")
  fi
  if [ "$ready" = "1" ]; then
    failed_hash_remove_path "$filepath" || true
  else
    local hash_tsv
    hash_tsv=$(failed_hash_tsv_path)
    _remove_failed_hash_path_locked_inner "$hash_tsv" "$filepath" || true
  fi

  # Recompute the CURRENT (post-tag) fingerprint and SREM it too, but only
  # when it differs from the pre-tag member already removed above. When the
  # caller supplied the pre-tag member and tagging changed size/inode, this
  # removes the new member that the next admission would otherwise collide
  # with; when nothing changed, post == pre and this is correctly skipped.
  local post
  post="${filepath}:$(fingerprint "$disk_read_path")"
  if [ "$post" != "$pre" ]; then
    $QUEUE_CLI SREM tc:seen "$post" > /dev/null 2>&1 || true
  fi

  # Positive admission rows can predate stricter language verification.
  # If left in place, a manual language rescan can be whole-skipped as
  # "verified:fully_classified" before audio_needs_lang_detection() runs.
  # Clear both positive rails for this path as part of making the file
  # re-enterable, using the same index-ready/locked-TSV fallback shape as
  # the failed rail above.
  local verified_ready
  verified_ready=$($QUEUE_CLI GET "tc:idx:verified:ready" 2>/dev/null || echo "")
  if [ "$verified_ready" != "1" ]; then
    local verified_tsv
    verified_tsv=$(verified_hashes_tsv_path)
    rebuild_rail_index verified "$verified_tsv" "verified:aac_lc" || true
    verified_ready=$($QUEUE_CLI GET "tc:idx:verified:ready" 2>/dev/null || echo "")
  fi
  if [ "$verified_ready" = "1" ]; then
    verified_hash_remove_path "$filepath" || true
  else
    local verified_tsv
    verified_tsv=$(verified_hashes_tsv_path)
    _remove_verified_hash_path_locked_inner "$verified_tsv" "$filepath" || true
  fi

  local fully_ready
  fully_ready=$($QUEUE_CLI GET "tc:idx:fully_classified:ready" 2>/dev/null || echo "")
  if [ "$fully_ready" != "1" ]; then
    local fully_tsv
    fully_tsv=$(fully_classified_hashes_tsv_path)
    rebuild_rail_index fully_classified "$fully_tsv" "verified:fully_classified" || true
    fully_ready=$($QUEUE_CLI GET "tc:idx:fully_classified:ready" 2>/dev/null || echo "")
  fi
  if [ "$fully_ready" = "1" ]; then
    fully_classified_remove_path "$filepath" || true
  else
    local fully_tsv
    fully_tsv=$(fully_classified_hashes_tsv_path)
    _remove_fully_classified_path_locked_inner "$fully_tsv" "$filepath" || true
  fi

  cleanup_job_files_for_path "$filepath"
}

# Phase 5B: failed rail gets a policy header for the first time, so
# `*_validate_or_reset` can wipe pre-5B append-only rows cleanly when
# the schema bumps. Pre-5B failed-hashes.tsv had no header at all —
# the very presence of a header (or its absence on a fresh deploy)
# is what validate_or_reset checks.
failed_policy_header() {
  echo "# failed_schema=1|row=path_hash_reason_ts_size_mtime_ctime"
}

# Boot-time check: if the failed-hashes.tsv first line doesn't equal
# the current policy header, truncate-and-rewrite. Creates the file
# if missing. Mirrors verified_hashes_validate_or_reset; new in 5B.
failed_hashes_validate_or_reset() {
  local tsv current header
  tsv=$(failed_hash_tsv_path)
  current=$(failed_policy_header)
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  if [ -f "$tsv" ]; then
    header=$(head -1 "$tsv" 2>/dev/null || true)
    if [ "$header" = "$current" ]; then
      return 0
    fi
    printf '[failed-hashes] policy header changed (was=%q now=%q) — invalidating\n' \
      "${header:-<missing>}" "$current" >&2
  fi

  printf '%s\n' "$current" > "$tsv"
}

# Compute a content fingerprint of $1. For media files (>12 MB), uses
# a sampling strategy: 4 MB at head + 4 MB at middle + 4 MB at tail +
# file size, hashed together via SHA-256. Sub-second regardless of
# file size. For two different files to collide they'd need identical
# bytes at all three 4 MB regions AND identical size — practically
# impossible for distinct media files. Small files (<= 12 MB) are
# hashed end-to-end via plain sha256sum since the sample-hash window
# would overlap and provide no speedup.
# Echoes the hex digest on stdout; returns 1 if the file can't be
# read or sized.
failed_hash_compute() {
  local path="${1:-}"
  [ -n "$path" ] && [ -f "$path" ] || return 1

  local size
  size=$(stat -c%s "$path" 2>/dev/null) || return 1
  [ "$size" -gt 0 ] || return 1

  local sample_mb=4
  local window_bytes=$((sample_mb * 1024 * 1024 * 3))   # 12 MB

  # Small files: whole-file hash (no speedup possible).
  if [ "$size" -le "$window_bytes" ]; then
    local digest
    digest=$(sha256sum "$path" 2>/dev/null | awk '{print $1}')
    [ -n "$digest" ] || return 1
    printf '%s\n' "$digest"
    return 0
  fi

  # Large files: head + middle + tail samples + size, hashed together.
  # Using bs=1M is GNU dd; the linuxserver/ffmpeg base is Debian and
  # accepts it. Both skip offsets are computed in MB to pair with bs=1M.
  local middle_skip_mb=$(( (size / 1048576 / 2) - (sample_mb / 2) ))
  local tail_skip_mb=$(( (size / 1048576) - sample_mb ))
  [ "$middle_skip_mb" -ge 0 ] || middle_skip_mb=0
  [ "$tail_skip_mb" -ge 0 ] || tail_skip_mb=0

  local digest
  digest=$(
    {
      dd if="$path" bs=1M count="$sample_mb" 2>/dev/null
      dd if="$path" bs=1M count="$sample_mb" skip="$middle_skip_mb" 2>/dev/null
      dd if="$path" bs=1M count="$sample_mb" skip="$tail_skip_mb" 2>/dev/null
      printf '%s' "$size"
    } | sha256sum 2>/dev/null | awk '{print $1}'
  )
  [ -n "$digest" ] || return 1
  printf '%s\n' "$digest"
}

# Append one row to failed-hashes.tsv on a worker failure. Returns 0
# (success) regardless of whether a row was added — callers under
# `set -euo pipefail` should still wrap with `|| true` as belt-and-
# suspenders.
#
# Production mode (default) detaches the hash compute + append into
# a background subprocess so the worker returns immediately. The
# subprocess survives normal worker exit via disown + full FD
# redirection (no SIGHUP / SIGPIPE on parent close). Container
# restart still kills it, but sample-hash is sub-second so the
# window for that is essentially zero.
#
# Test mode (TRANSCODARR_FAILED_HASH_SYNC=true) runs everything in
# the foreground so tests can assert row presence immediately.
#
#   $1: read path (INPUT_READ — disk-direct when active)
#   $2: library path (INPUT — what gets stored as col1)
#   $3: reason string
failed_hash_record() {
  local read_path="${1:-}" lib_path="${2:-}" reason="${3:-}"
  [ -n "$read_path" ] && [ -n "$lib_path" ] || return 0

  local tsv
  tsv=$(failed_hash_tsv_path)
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  if [ "${TRANSCODARR_FAILED_HASH_SYNC:-false}" = "true" ]; then
    local hash ts stat_tuple size mtime ctime row
    hash=$(failed_hash_compute "$read_path") || return 0
    stat_tuple=$(_stat_tuple "$read_path") || return 0
    IFS='|' read -r size mtime ctime <<<"$stat_tuple"
    [ -n "$size" ] && [ -n "$mtime" ] && [ -n "$ctime" ] || return 0
    ts=$(date -Iseconds)
    # 7-col row (Phase 5B): path \t hash \t reason \t ts \t size \t mtime \t ctime
    row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s' "$lib_path" "$hash" "$reason" "$ts" "$size" "$mtime" "$ctime")
    # 5B maintenance runs under the same TSV lock as the row replacement
    # so active removal on later success cannot leave TSV/index torn.
    _atomic_replace_path_in_tsv "$tsv" "$lib_path" "$row" failed "$size" "$mtime" "$ctime" "failed" || return 0
    return 0
  fi

  # Detached async — worker doesn't wait. Variables are captured in
  # the subshell at fork time so the parent can exit immediately.
  # 5B maintenance runs INSIDE the subshell and under the same TSV
  # lock as the row replacement.
  (
    local hash ts stat_tuple size mtime ctime row
    hash=$(failed_hash_compute "$read_path") || exit 0
    stat_tuple=$(_stat_tuple "$read_path") || exit 0
    IFS='|' read -r size mtime ctime <<<"$stat_tuple"
    [ -n "$size" ] && [ -n "$mtime" ] && [ -n "$ctime" ] || exit 0
    ts=$(date -Iseconds)
    row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s' "$lib_path" "$hash" "$reason" "$ts" "$size" "$mtime" "$ctime")
    _atomic_replace_path_in_tsv "$tsv" "$lib_path" "$row" failed "$size" "$mtime" "$ctime" "failed" || exit 0
  ) </dev/null >/dev/null 2>&1 &
  disown 2>/dev/null || true
  return 0
}

# Remove a path from the failed rail after a later successful worker run.
# Fast path: if the Valkey failed-path index does not contain the path,
# return immediately without touching the TSV. When it does hit, TSV and
# index deletion are serialized through the rail TSV lock.
#   $1: library path (col1)
failed_hash_remove_path() {
  local lib_path="${1:-}"
  [ -n "$lib_path" ] || return 0

  local in_paths
  in_paths=$($QUEUE_CLI SISMEMBER "tc:idx:failed:paths" "$lib_path" 2>/dev/null) || return 0
  [ "$in_paths" = "1" ] || return 0

  local tsv lock_target
  tsv=$(failed_hash_tsv_path)
  lock_target="${tsv}.lock"
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  if command -v flock >/dev/null 2>&1; then
    touch "$lock_target" 2>/dev/null || return 0
    (
      flock -x 200 || exit 0
      _remove_failed_hash_path_locked_inner "$tsv" "$lib_path" || exit 0
    ) 200>"$lock_target"
    return 0
  fi

  local lockdir="${lock_target}dir" waited=0
  while ! mkdir "$lockdir" 2>/dev/null; do
    sleep 0.05
    waited=$((waited + 1))
    [ "$waited" -le 600 ] || return 0
  done
  _remove_failed_hash_path_locked_inner "$tsv" "$lib_path" || true
  rmdir "$lockdir" 2>/dev/null || true
  return 0
}

# Legacy TSV-scan implementation (renamed in 5A). Public callers go
# through the failed_hash_should_skip dispatcher below.
#   $1: read path (INPUT_READ when called from worker, $filepath when
#       called from admission — FUSE path is fine at admission time
#       since disk-direct isn't resolved until the wrangler stage)
#   $2: library path (what we compare against col1 in the TSV)
failed_hash_should_skip_legacy() {
  local read_path="${1:-}" lib_path="${2:-}"
  [ -n "$read_path" ] && [ -n "$lib_path" ] || return 1

  local tsv
  tsv=$(failed_hash_tsv_path)
  [ -f "$tsv" ] || return 1

  # Cheap path scan — return early if the library path isn't in any row.
  # Use awk with explicit field comparison so we don't false-match on
  # substring collisions (e.g. /movies/A.mkv vs /movies/A.mkv.backup).
  awk -F'\t' -v p="$lib_path" '$1==p {found=1; exit} END{exit (found?0:1)}' "$tsv" \
    || return 1

  # Path is in TSV — compute hash and check for a match.
  local hash
  hash=$(failed_hash_compute "$read_path") || return 1

  awk -F'\t' -v p="$lib_path" -v h="$hash" \
    '$1==p && $2==h {found=1; exit} END{exit (found?0:1)}' "$tsv"
}

# Probe-pool admission gate (5B dispatcher). Returns 0 (skip) if the
# candidate's path is in :paths AND HGET :stat_by_path returns a
# statfp equal to the current file's computed stat fingerprint;
# 1 (process) otherwise. NO fallback to content-hash matching on
# stat mismatch — a changed stat is the signal that the file needs
# re-checking.
#
# Any Valkey command failure delegates to the legacy awk-scan path
# (which still uses path+content-hash matching against TSV col1/col2).
failed_hash_should_skip() {
  local read_path="${1:-}" lib_path="${2:-}"
  [ -n "$read_path" ] && [ -n "$lib_path" ] || return 1

  local ready
  ready=$($QUEUE_CLI GET "tc:idx:failed:ready" 2>/dev/null) || {
    failed_hash_should_skip_legacy "$@"
    return $?
  }
  if [ "$ready" != "1" ]; then
    failed_hash_should_skip_legacy "$@"
    return $?
  fi

  local in_paths
  in_paths=$($QUEUE_CLI SISMEMBER "tc:idx:failed:paths" "$lib_path" 2>/dev/null) || {
    failed_hash_should_skip_legacy "$@"
    return $?
  }
  [ "$in_paths" = "1" ] || return 1

  # 5B fast path: stat the current file, compute statfp, compare
  # against the recorded statfp for THIS path (HGET, not SISMEMBER —
  # see the note in the header comment above). If stat fails
  # (file missing/permission denied), the gate falls through to the
  # legacy path — which will also fail on a missing file via its
  # sample-hash compute.
  local stat_tuple size mtime ctime statfp
  stat_tuple=$(_stat_tuple "$read_path") || return 1
  IFS='|' read -r size mtime ctime <<<"$stat_tuple"
  [ -n "$size" ] && [ -n "$mtime" ] && [ -n "$ctime" ] || return 1
  statfp=$(_stat_fingerprint "$lib_path" "$size" "$mtime" "$ctime" "failed") || return 1

  local recorded_statfp
  recorded_statfp=$($QUEUE_CLI HGET "tc:idx:failed:stat_by_path" "$lib_path" 2>/dev/null) || {
    failed_hash_should_skip_legacy "$@"
    return $?
  }
  # Empty (path absent from hash, despite being in :paths set — race
  # window between SADD and HSET) → process. Match → skip. Mismatch
  # (stat changed since record) → process. No sample-hash fallback
  # for skip.
  [ "$recorded_statfp" = "$statfp" ] && return 0
  return 1
}

# ── Verified-hash gate helpers (Phase 1) ──────────────────────────────
# Positive counterpart to the failed-hash gate. Records SHA-256 of
# admission-classifier-verified bytes when classify_file_probe emits a
# verdict (Phase 1 only: verified:aac_lc). Admission compares the
# candidate's current SHA-256 against recorded rows; same content →
# whole-skip ffprobe, BUT ONLY when the queueing reason was solely
# `aac_profile_unknown` (gated by the entrypoint, not here). Header is
# a literal policy string; mismatch at boot truncates the file.

verified_hashes_tsv_path() {
  echo "${TRANSCODARR_VERIFIED_HASHES_TSV:-${TRANSCODARR_STATE_DIR:-/state}/verified-hashes.tsv}"
}

# Returns the literal policy header line. Bumping schema or policy
# invalidates the cache cleanly at next boot. Human-auditable via
# `head -1 verified-hashes.tsv` — no hash to decode.
# v2 (Phase 5B): row schema extended to 7 columns with stat fields
# (size, mtime, ctime). Pre-5B v1 rows had 4 columns; header mismatch
# triggers truncate-and-rewrite via verified_hashes_validate_or_reset.
verified_policy_header() {
  echo "# verified_schema=2|aac_profile_policy=lc|row=path_hash_verdict_ts_size_mtime_ctime"
}

# Boot-time check: if the TSV's first line doesn't equal the current
# policy header, truncate-and-rewrite. Creates the file if missing.
# Idempotent within a run.
verified_hashes_validate_or_reset() {
  local tsv current header
  tsv=$(verified_hashes_tsv_path)
  current=$(verified_policy_header)
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  if [ -f "$tsv" ]; then
    header=$(head -1 "$tsv" 2>/dev/null || true)
    if [ "$header" = "$current" ]; then
      return 0
    fi
    printf '[verified-hashes] policy header changed (was=%q now=%q) — invalidating\n' \
      "${header:-<missing>}" "$current" >&2
  fi

  printf '%s\n' "$current" > "$tsv"
}

# Append one row when classify_file_probe emits a non-`none` verdict.
# Synchronous by design (Phase 1) — the bounded ffprobe pool throttles
# concurrency. TRANSCODARR_VERIFIED_HASH_SYNC=true is a no-op today;
# kept as a future hook if async-record ever becomes needed.
#   $1: read path (bytes to hash)
#   $2: library path (col1 of the TSV)
#   $3: verdict (e.g. verified:aac_lc)
verified_hash_record() {
  local read_path="${1:-}" lib_path="${2:-}" verdict="${3:-}"
  [ -n "$read_path" ] && [ -n "$lib_path" ] && [ -n "$verdict" ] || return 0

  local tsv
  tsv=$(verified_hashes_tsv_path)
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  local hash ts stat_tuple size mtime ctime row
  hash=$(failed_hash_compute "$read_path") || return 0
  stat_tuple=$(_stat_tuple "$read_path") || return 0
  IFS='|' read -r size mtime ctime <<<"$stat_tuple"
  [ -n "$size" ] && [ -n "$mtime" ] && [ -n "$ctime" ] || return 0
  ts=$(date -Iseconds)
  # 7-col row (Phase 5B): path \t hash \t verdict \t ts \t size \t mtime \t ctime
  row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s' "$lib_path" "$hash" "$verdict" "$ts" "$size" "$mtime" "$ctime")
  # 5B maintenance runs under the same TSV lock as the row replacement
  # so transition-aware removal (verified_hash_remove_path) on a later
  # re-classify cannot interleave with this record's index update and
  # leave TSV/Valkey torn. Mirrors failed_hash_record's locking shape.
  # Verified (narrow) rail's only verdict is "verified:aac_lc" per
  # Phase 1 design; rail_token matches.
  _atomic_replace_path_in_tsv "$tsv" "$lib_path" "$row" verified "$size" "$mtime" "$ctime" "verified:aac_lc" || return 0
  return 0
}

# Remove a path from the verified (narrow AAC-LC) rail after a later
# re-classify whose verdict is no longer "verified:aac_lc". Fast path:
# if the Valkey verified-path index doesn't contain the path, return
# immediately without touching the TSV. Mirrors failed_hash_remove_path.
#   $1: library path (col1)
verified_hash_remove_path() {
  local lib_path="${1:-}"
  [ -n "$lib_path" ] || return 0

  local in_paths
  in_paths=$($QUEUE_CLI SISMEMBER "tc:idx:verified:paths" "$lib_path" 2>/dev/null) || return 0
  [ "$in_paths" = "1" ] || return 0

  local tsv lock_target
  tsv=$(verified_hashes_tsv_path)
  lock_target="${tsv}.lock"
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  if command -v flock >/dev/null 2>&1; then
    touch "$lock_target" 2>/dev/null || return 0
    (
      flock -x 200 || exit 0
      _remove_verified_hash_path_locked_inner "$tsv" "$lib_path" || exit 0
    ) 200>"$lock_target"
    return 0
  fi

  local lockdir="${lock_target}dir" waited=0
  while ! mkdir "$lockdir" 2>/dev/null; do
    sleep 0.05
    waited=$((waited + 1))
    [ "$waited" -le 600 ] || return 0
  done
  _remove_verified_hash_path_locked_inner "$tsv" "$lib_path" || true
  rmdir "$lockdir" 2>/dev/null || true
  return 0
}

# Legacy TSV-scan implementation (renamed in 5A).
verified_hash_should_skip_legacy() {
  local read_path="${1:-}" lib_path="${2:-}"
  [ -n "$read_path" ] && [ -n "$lib_path" ] || return 1

  local tsv
  tsv=$(verified_hashes_tsv_path)
  [ -f "$tsv" ] || return 1

  awk -F'\t' -v p="$lib_path" '$1==p {found=1; exit} END{exit (found?0:1)}' "$tsv" \
    || return 1

  local hash
  hash=$(failed_hash_compute "$read_path") || return 1

  awk -F'\t' -v p="$lib_path" -v h="$hash" \
    '$1==p && $2==h {found=1; exit} END{exit (found?0:1)}' "$tsv"
}

# 5B dispatcher — stat-fingerprint fast path. See failed_hash_should_skip
# for the design rationale (stat mismatch → process, no content-hash
# fallback for skip; Valkey command failure → legacy awk-scan).
#   $1: read path (file to stat)
#   $2: library path (what we compare against col1 in the TSV)
verified_hash_should_skip() {
  local read_path="${1:-}" lib_path="${2:-}"
  [ -n "$read_path" ] && [ -n "$lib_path" ] || return 1

  local ready
  ready=$($QUEUE_CLI GET "tc:idx:verified:ready" 2>/dev/null) || {
    verified_hash_should_skip_legacy "$@"
    return $?
  }
  if [ "$ready" != "1" ]; then
    verified_hash_should_skip_legacy "$@"
    return $?
  fi

  local in_paths
  in_paths=$($QUEUE_CLI SISMEMBER "tc:idx:verified:paths" "$lib_path" 2>/dev/null) || {
    verified_hash_should_skip_legacy "$@"
    return $?
  }
  [ "$in_paths" = "1" ] || return 1

  local stat_tuple size mtime ctime statfp
  stat_tuple=$(_stat_tuple "$read_path") || return 1
  IFS='|' read -r size mtime ctime <<<"$stat_tuple"
  [ -n "$size" ] && [ -n "$mtime" ] && [ -n "$ctime" ] || return 1
  statfp=$(_stat_fingerprint "$lib_path" "$size" "$mtime" "$ctime" "verified:aac_lc") || return 1

  local recorded_statfp
  recorded_statfp=$($QUEUE_CLI HGET "tc:idx:verified:stat_by_path" "$lib_path" 2>/dev/null) || {
    verified_hash_should_skip_legacy "$@"
    return $?
  }
  [ "$recorded_statfp" = "$statfp" ] && return 0
  return 1
}

# ── Fully-classified cache helpers (Phase 3) ──────────────────────────
# Second positive rail alongside Phase 1's narrow AAC-LC cache. Records
# `verified:fully_classified` whenever classify_file_probe returns
# result=skip — i.e. EVERY classifier dimension passed under current
# settings. Admission consults this rail before the narrow rail; a
# match here authorizes a whole-skip regardless of candidate reasons
# (the broad fact is independently sufficient under the same classifier
# policy). Failed-hash gate still runs first; Direct Queue (/api/direct/*)
# bypasses admission entirely. tc-queue-job.sh writes priority .job files
# that flow through the bridge → admission gate, so they ARE eligible
# for cache hits (see proposal §"Admission entry points").

fully_classified_hashes_tsv_path() {
  echo "${TRANSCODARR_FULLY_CLASSIFIED_HASHES_TSV:-${TRANSCODARR_STATE_DIR:-/state}/fully-classified-hashes.tsv}"
}

# Policy header — literal hardcoded portions + env-derived values
# normalized the same way classify_file_probe normalizes them. Settings
# that don't affect the classifier verdict (PRESET, QUALITY_TIER,
# ENCODER_SPEED, etc.) are deliberately NOT in this header so toggling
# them doesn't invalidate the cache. Schema number bumped manually when
# classify_file_probe's hardcoded policy changes (e.g. widening the
# h264-family pass set).
fully_classified_policy_header() {
  local acodec alang maxw maxh maxch
  acodec=$(printf '%s' "${TRANSCODARR_AUDIO_CODEC:-aac}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')
  alang=$(normalize_audio_language_tag "${TRANSCODARR_AUDIO_LANG:-eng}")
  maxw="${TRANSCODARR_MAX_WIDTH:-1920}"
  maxh="${TRANSCODARR_MAX_HEIGHT:-1080}"
  maxch="${TRANSCODARR_MAX_CHANNELS:-6}"
  # v2 (Phase 5B): row schema extended to 7 columns with stat fields.
  # Header mismatch (v1 → v2) triggers truncate-and-rewrite via
  # fully_classified_hashes_validate_or_reset.
  echo "# fully_classified_schema=2|video=h264_family|aac_profile=lc|multi_audio=single|unknown_lang=pass|target_acodec=$acodec|target_alang=$alang|max_w=$maxw|max_h=$maxh|max_ch=$maxch|row=path_hash_verdict_ts_size_mtime_ctime"
}

# Boot-time validation. If the TSV's first line doesn't equal the
# current policy header (string equality), truncate-and-rewrite. Any
# env-derived setting change → header mismatch → cache wiped. Creates
# the file if missing.
fully_classified_hashes_validate_or_reset() {
  local tsv current header
  tsv=$(fully_classified_hashes_tsv_path)
  current=$(fully_classified_policy_header)
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  if [ -f "$tsv" ]; then
    header=$(head -1 "$tsv" 2>/dev/null || true)
    if [ "$header" = "$current" ]; then
      return 0
    fi
    printf '[fully-classified] policy header changed (was=%q now=%q) — invalidating\n' \
      "${header:-<missing>}" "$current" >&2
  fi

  printf '%s\n' "$current" > "$tsv"
}

# reset_cache_rail_tsvs_for_rescan truncates the three durable rail TSVs,
# then lets the existing boot validation helpers write the current policy
# headers. This keeps header generation in one place and preserves the
# existing policy-change audit lines. This is a durable admin reset helper,
# not migration code.
reset_cache_rail_tsvs_for_rescan() {
  local failed_tsv verified_tsv fully_tsv
  failed_tsv=$(failed_hash_tsv_path)
  verified_tsv=$(verified_hashes_tsv_path)
  fully_tsv=$(fully_classified_hashes_tsv_path)

  mkdir -p "$(dirname "$failed_tsv")" "$(dirname "$verified_tsv")" "$(dirname "$fully_tsv")" 2>/dev/null || return 1

  : > "$failed_tsv"   && failed_hashes_validate_or_reset           || return 1
  : > "$verified_tsv" && verified_hashes_validate_or_reset         || return 1
  : > "$fully_tsv"    && fully_classified_hashes_validate_or_reset || return 1
}

# rebuild_cache_rails_for_rescan clears stale Valkey rail indexes and
# rebuilds them from the just-reset TSVs. Even empty/header-only TSVs
# must be rebuilt so tc:idx:<rail>:ready lands at 1 and admission does
# not fall back to legacy awk scans for the next full scan.
rebuild_cache_rails_for_rescan() {
  $QUEUE_CLI DEL \
    tc:idx:failed:ready tc:idx:failed:paths tc:idx:failed:stat_by_path \
    tc:idx:failed:fingerprints tc:idx:failed:stat_fingerprints \
    tc:idx:verified:ready tc:idx:verified:paths tc:idx:verified:stat_by_path \
    tc:idx:verified:fingerprints tc:idx:verified:stat_fingerprints \
    tc:idx:fully_classified:ready tc:idx:fully_classified:paths tc:idx:fully_classified:stat_by_path \
    tc:idx:fully_classified:fingerprints tc:idx:fully_classified:stat_fingerprints \
    >/dev/null 2>&1 || true

  rebuild_rail_index failed           "$(failed_hash_tsv_path)"             "failed"                    || return 1
  rebuild_rail_index verified         "$(verified_hashes_tsv_path)"         "verified:aac_lc"           || return 1
  rebuild_rail_index fully_classified "$(fully_classified_hashes_tsv_path)" "verified:fully_classified" || return 1
}

# Append a row recording that these bytes are fully classified under
# the current policy. Called from multiple sites:
#   - admission gate when classify_file_probe returned result=skip
#   - narrow-gate auto-promotion (Phase 4): when verified:aac_lc cache
#     + reasons=="aac_profile_unknown" both fire, the file is by
#     deduction fully classified, so record that fact too
#   - worker post-success: after successful encode + validation, the
#     output bytes by construction match classifier policy
#
# Verdict is fixed — only one shape of row can live in this rail.
# Synchronous. Skip-if-present dedupe prevents file growth from
# repeated recordings of the same (path, hash) across restarts.
# TRANSCODARR_FULLY_CLASSIFIED_HASH_SYNC=true is a no-op today; kept
# as a hook if async ever becomes necessary.
#   $1: read path (bytes to hash)
#   $2: library path (col1 of the TSV)
fully_classified_record() {
  local read_path="${1:-}" lib_path="${2:-}"
  [ -n "$read_path" ] && [ -n "$lib_path" ] || return 0

  local tsv
  tsv=$(fully_classified_hashes_tsv_path)
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  local hash ts stat_tuple size mtime ctime row
  hash=$(failed_hash_compute "$read_path") || return 0
  stat_tuple=$(_stat_tuple "$read_path") || return 0
  IFS='|' read -r size mtime ctime <<<"$stat_tuple"
  [ -n "$size" ] && [ -n "$mtime" ] && [ -n "$ctime" ] || return 0
  ts=$(date -Iseconds)
  # 7-col row (Phase 5B): path \t hash \t verdict \t ts \t size \t mtime \t ctime
  # Phase 5B replaces 5A's "dedupe on exact (path, hash, verdict) match"
  # with "one row per path" via atomic replacement. New stat tuple →
  # row is replaced; same stat tuple → row is replaced byte-identical
  # (mtime/ctime might differ if Sonarr touched the file metadata).
  row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s' "$lib_path" "$hash" "verified:fully_classified" "$ts" "$size" "$mtime" "$ctime")
  # 5B maintenance runs under the same TSV lock as the row replacement
  # so transition-aware removal (fully_classified_remove_path) on a
  # later re-classify cannot interleave with this record's index update
  # and leave TSV/Valkey torn. Mirrors failed_hash_record's locking
  # shape. Broad rail's only verdict is the constant
  # "verified:fully_classified"; rail_token matches.
  _atomic_replace_path_in_tsv "$tsv" "$lib_path" "$row" fully_classified "$size" "$mtime" "$ctime" "verified:fully_classified" || return 0
  return 0
}

# Remove a path from the fully_classified (broad) rail after a later
# re-classify whose result is no longer "skip". Fast path: if the
# Valkey fully_classified-path index doesn't contain the path, return
# immediately without touching the TSV. Mirrors failed_hash_remove_path.
#   $1: library path (col1)
fully_classified_remove_path() {
  local lib_path="${1:-}"
  [ -n "$lib_path" ] || return 0

  local in_paths
  in_paths=$($QUEUE_CLI SISMEMBER "tc:idx:fully_classified:paths" "$lib_path" 2>/dev/null) || return 0
  [ "$in_paths" = "1" ] || return 0

  local tsv lock_target
  tsv=$(fully_classified_hashes_tsv_path)
  lock_target="${tsv}.lock"
  mkdir -p "$(dirname "$tsv")" 2>/dev/null || true

  if command -v flock >/dev/null 2>&1; then
    touch "$lock_target" 2>/dev/null || return 0
    (
      flock -x 200 || exit 0
      _remove_fully_classified_path_locked_inner "$tsv" "$lib_path" || exit 0
    ) 200>"$lock_target"
    return 0
  fi

  local lockdir="${lock_target}dir" waited=0
  while ! mkdir "$lockdir" 2>/dev/null; do
    sleep 0.05
    waited=$((waited + 1))
    [ "$waited" -le 600 ] || return 0
  done
  _remove_fully_classified_path_locked_inner "$tsv" "$lib_path" || true
  rmdir "$lockdir" 2>/dev/null || true
  return 0
}

# Legacy TSV-scan implementation (renamed in 5A). The explicit col3
# check is defense-in-depth: even though the rail's TSV should only
# contain `verified:fully_classified` by construction, a manual edit
# or future record-side bug shouldn't authorize a skip with the wrong
# verdict string.
fully_classified_should_skip_legacy() {
  local read_path="${1:-}" lib_path="${2:-}"
  [ -n "$read_path" ] && [ -n "$lib_path" ] || return 1

  local tsv
  tsv=$(fully_classified_hashes_tsv_path)
  [ -f "$tsv" ] || return 1

  awk -F'\t' -v p="$lib_path" '$1==p {found=1; exit} END{exit (found?0:1)}' "$tsv" \
    || return 1

  local hash
  hash=$(failed_hash_compute "$read_path") || return 1

  awk -F'\t' -v p="$lib_path" -v h="$hash" \
    '$1==p && $2==h && $3=="verified:fully_classified" {found=1; exit} END{exit (found?0:1)}' "$tsv"
}

# 5B dispatcher — stat-fingerprint fast path. The big admission-drain
# win: replaces ~150ms of awk-scan + ~100ms-4sec of FUSE sample-hash
# read with a 5-10ms stat() + SISMEMBER round-trip.
#   $1: read path (file to stat)
#   $2: library path (what we compare against col1 in the TSV)
fully_classified_should_skip() {
  local read_path="${1:-}" lib_path="${2:-}"
  [ -n "$read_path" ] && [ -n "$lib_path" ] || return 1

  local ready
  ready=$($QUEUE_CLI GET "tc:idx:fully_classified:ready" 2>/dev/null) || {
    fully_classified_should_skip_legacy "$@"
    return $?
  }
  if [ "$ready" != "1" ]; then
    fully_classified_should_skip_legacy "$@"
    return $?
  fi

  local in_paths
  in_paths=$($QUEUE_CLI SISMEMBER "tc:idx:fully_classified:paths" "$lib_path" 2>/dev/null) || {
    fully_classified_should_skip_legacy "$@"
    return $?
  }
  [ "$in_paths" = "1" ] || return 1

  local stat_tuple size mtime ctime statfp
  stat_tuple=$(_stat_tuple "$read_path") || return 1
  IFS='|' read -r size mtime ctime <<<"$stat_tuple"
  [ -n "$size" ] && [ -n "$mtime" ] && [ -n "$ctime" ] || return 1
  statfp=$(_stat_fingerprint "$lib_path" "$size" "$mtime" "$ctime" "verified:fully_classified") || return 1

  local recorded_statfp
  recorded_statfp=$($QUEUE_CLI HGET "tc:idx:fully_classified:stat_by_path" "$lib_path" 2>/dev/null) || {
    fully_classified_should_skip_legacy "$@"
    return $?
  }
  [ "$recorded_statfp" = "$statfp" ] && return 0
  return 1
}

# ── Disk resolution ───────────────────────────────────────────────────────
# Resolves a container media path (/movies/... or /tv/...) to the physical
# disk mount (/disk1, /disk2, etc.) by checking which mount has the file.
# Returns the disk name (e.g. "disk5") or empty string if not found.

resolve_disk() {
  local filepath="$1"
  local relative_path
  case "$filepath" in
    /movies/*) relative_path="Movies/${filepath#/movies/}" ;;
    /tv/*)     relative_path="TV/${filepath#/tv/}" ;;
    *)         echo ""; return 1 ;;
  esac
  for disk_mount in /disk*/; do
    if [ -f "${disk_mount}${relative_path}" ]; then
      echo "$(basename "$disk_mount")"
      return 0
    fi
  done
  echo ""
  return 1
}

# Non-blocking pop — returns immediately if queue is empty.
# Uses RPOPLPUSH with no timeout (instant return).
q_pop_nonblock() {
  $QUEUE_CLI RPOPLPUSH "$1" "${1%:ready}:processing" 2>/dev/null || true
}

# ── Stream duration span probe ────────────────────────────────────────────
#
# get_stream_span <file> <stream_spec> <seek_from>
#
# Returns the duration span (in integer seconds) of the named stream,
# computed as `last_packet_pts - first_packet_pts`. Writing it as a span
# rather than raw end-pts normalizes for sources with non-zero
# `start_time` (e.g. files cut with `-copyts -ss N`), so comparing source
# and output spans remains valid even when ffmpeg resets the output
# timeline to 0.
#
# Arguments:
#   file          absolute path to the media file
#   stream_spec   ffprobe -select_streams argument (e.g. "v:0", "a:0",
#                 "a:${track-1}")
#   seek_from     seconds to seek to before reading the last packet. When
#                 `<= 0` (e.g. because the source's `format=duration` is
#                 N/A or the file is too short to bother seeking), the
#                 helper SKIPS the seek-based fast path entirely and goes
#                 straight to the full-stream scan. This is load-bearing:
#                 passing `seek_from=0` does NOT mean "seek to 0 and read
#                 200s", which would silently validate only the first 200s
#                 of a long file and produce false-positive failures on
#                 any clean encode.
#
# Return: integer seconds as stdout. Returns 0 when neither the fast
# path nor the fallback could measure the stream (caller treats 0 as
# "unmeasurable, skip the comparison" and logs a warning).
#
# Fast path (seek_from > 0):
#   - Probe the last packet via `-read_intervals "${seek_from}%+200"` +
#     `tail -1`.
#   - Probe the first packet via `-read_intervals "0%+5"` + `head -1`.
#   - If either returns empty or non-numeric, force the fallback.
#
# Fallback path:
#   - Single full-stream scan of `packet=pts_time`, take head -1 and
#     tail -1 from the same output. Slow on multi-GB files (~10s for a
#     2h audio stream) but reliable — no seek dependencies, no
#     `format=duration` assumptions, no cue-point requirements.
#
# The span arithmetic is done in awk because bash can't do float
# subtraction on the pts_time strings (they have 6 decimal digits).
get_stream_span() {
  local file="$1" stream_spec="$2" seek_from="$3"
  local first_pts="" last_pts=""

  # Codec-gated short-circuit for TrueHD/MLP. These codecs emit packet
  # pts_time on the first packet only; every subsequent packet returns
  # N/A for both pts_time and dts_time, making packet-based span
  # measurement impossible. Probing all packets just to confirm they're
  # unmeasurable burns IO on multi-GB streams and can consume most of
  # the worker timeout. Read the per-stream MKV TAG:DURATION* value
  # (per-stream, NOT container — so subtitle/format overhang doesn't
  # pollute it) and skip the packet path entirely. If no usable tag is
  # present, return 0 quickly so the validator rejects on unmeasurable
  # source without burning the 4h timeout.
  local codec_name=""
  codec_name=$(ffprobe -v quiet -select_streams "$stream_spec" \
    -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 \
    "$file" 2>/dev/null | head -n 1)
  case "$codec_name" in
    truehd|mlp)
      local tag_duration
      tag_duration=$(ffprobe -v quiet -select_streams "$stream_spec" \
        -show_entries stream_tags -of default=noprint_wrappers=1 \
        "$file" 2>/dev/null \
        | awk -F= '/^TAG:DURATION/ && $2 != "" {print $2; exit}')
      if [ -n "$tag_duration" ]; then
        awk -v t="$tag_duration" 'BEGIN {
          n = split(t, a, ":")
          if (n != 3) exit 1
          printf "%.0f\n", a[1]*3600 + a[2]*60 + a[3]
        }' && return 0
      fi

      # Tier 2 — container format=duration fallback. The codec gate's
      # TAG:DURATION* lookup above is empty for some retail rips
      # (Gladiator II 2024, Smile 2 2024) where the muxer set the
      # container duration but not per-stream tags. format=duration is
      # the file's overall runtime; for these single-feature-stream
      # files it matches the audio coverage to within the validator's
      # >10s tolerance. The packet probe is still never reached for
      # truehd/mlp — Tier 3 below is fast-return 0, not a scan.
      local fmt_duration
      fmt_duration=$(ffprobe -v quiet -show_entries format=duration \
        -of default=nokey=1:noprint_wrappers=1 "$file" 2>/dev/null \
        | head -n 1)
      case "$fmt_duration" in ""|*[!0-9.]*) fmt_duration="" ;; esac
      if [ -n "$fmt_duration" ] && \
         awk -v d="$fmt_duration" 'BEGIN{ exit !(d+0 > 0) }'; then
        if declare -F log >/dev/null 2>&1; then
          log "$codec_name: no TAG:DURATION* tag, using container format=duration (${fmt_duration%.*}s)"
        fi
        printf '%.0f\n' "$fmt_duration"
        return 0
      fi

      # Tier 3 — fast-return 0 (existing). Honest failure when neither
      # per-stream TAG:DURATION* nor positive container format=duration
      # is available.
      echo 0
      return 0
      ;;
  esac

  # Try pts_time first, then dts_time as a fallback. Some codecs (notably
  # VC1 in Blu-ray MKV remuxes) carry dts_time (decode timestamps) but not
  # pts_time (presentation timestamps) — every video packet returns N/A for
  # pts_time. The dts_time fallback lets get_stream_span still measure the
  # stream span for those files instead of returning 0 and triggering a
  # false-positive validation_duration_mismatch.
  local ts_field
  for ts_field in pts_time dts_time; do
    first_pts="" last_pts=""

    if (( seek_from > 0 )); then
      last_pts=$(ffprobe -v quiet -select_streams "$stream_spec" \
        -show_entries "packet=$ts_field" -of default=noprint_wrappers=1:nokey=1 \
        -read_intervals "${seek_from}%+200" \
        "$file" 2>/dev/null | tail -1)
      # For the first-packet probe we DO NOT use `-read_intervals "0%+5"`.
      # That syntax tells ffprobe to seek to pts=0 and read forward, which
      # SKIPS packets with a negative pts — and AAC encoder delay makes the
      # first audio packet of every re-encoded output have first_pts like
      # `-0.043000`. Skipping it and taking the next packet gives a
      # positive-but-wrong first_pts, which then produces a wrong span.
      # Read the stream from the start (no -read_intervals) and take the
      # first line. ffprobe writes output as it reads packets, so head -n 1
      # captures the first packet's timestamp and the pipe closes early via
      # SIGPIPE — fast enough in practice because ffprobe only has to read
      # until the first packet header is parsed.
      first_pts=$(ffprobe -v quiet -select_streams "$stream_spec" \
        -show_entries "packet=$ts_field" -of default=noprint_wrappers=1:nokey=1 \
        "$file" 2>/dev/null | head -n 1)
      # Numeric-validation pattern must allow a leading minus (valid
      # negative timestamp). `[!-0-9.]` reads as "not in the set
      # minus / digit / dot" — the `-` immediately after `!` is treated as
      # a literal minus by POSIX bracket semantics, not as a range start.
      case "$last_pts" in ""|*[!-0-9.]*) last_pts="" ;; esac
      case "$first_pts" in ""|*[!-0-9.]*) first_pts="" ;; esac

      # Seek-sanity gate. The fast-path can return a numerically-valid
      # last_pts that doesn't actually correspond to the end of the
      # stream in two distinct ways:
      #
      #   1. MKV with AC3/DTS audio: ffprobe's -read_intervals anchors
      #      the seek to the nearest container cluster boundary (set
      #      by video keyframe placement), which can land significantly
      #      earlier than seek_from. last_pts ends up < seek_from.
      #
      #   2. MPEG-TS source: the stream's first_pts is non-zero (often
      #      600 on Blu-ray-sourced .m2ts), so the caller's
      #      seek_from = format_duration - 120 lands inside the stream
      #      but well before the real end at first_pts + duration. The
      #      fast-path returns a last_pts inside the seek window
      #      (last_pts > seek_from), still many seconds short of the
      #      actual stream end.
      #
      # Both cases are caught by the same condition: a valid last_pts
      # from `${seek_from}%+200` must reach within 120s of the real
      # stream end (first_pts + seek_from + 120). If it doesn't, force
      # the full-stream scan fallback. Use awk for float-safe
      # comparison.
      if [ -n "$last_pts" ] && [ -n "$first_pts" ]; then
        awk -v lp="$last_pts" -v fp="$first_pts" -v sf="$seek_from" \
          'BEGIN{exit !(lp+0 < fp+0 + sf+0)}' \
          && last_pts=""
      fi
    fi

    if [ -z "$last_pts" ] || [ -z "$first_pts" ]; then
      # ── Phase 6A — full-scan fallback under global lock ───────────
      # Slow but reliable. One ffprobe invocation, head/tail from the
      # same captured output to keep first/last timestamps consistent
      # with the same stream scan.
      #
      # Lock rationale (2026-05-22 timing study):
      #   Single 23 GB Synchronic file alone: ~4 min full scan.
      #   Three overlapping big-file fallbacks (Kick-Ass + General's
      #   Daughter + Great Outdoors): 15-24 min EACH — far worse than
      #   serial. FUSE multiplexes through a single user-space process,
      #   so concurrent full-file reads collapse each other's bandwidth
      #   instead of getting fair share. Serializing the fallback
      #   prevents that collapse without affecting the fast-path
      #   (read_intervals seek) which stays unlocked.
      #
      # Lock target: one global /tmp/transcodarr-fullscan.lock. Per-disk
      # locks were considered but rejected — the FUSE multiplexer is
      # the real bottleneck, not the underlying disks. flock when
      # available (process-death-safe). mkdir fallback only for test
      # environments without flock (Windows git-bash etc.) — in
      # production this branch never runs because Linux always has
      # flock from util-linux.
      local _scan_lock="/tmp/transcodarr-fullscan.lock"
      local _scan _scan_t0_ms _scan_t1_ms _scan_elapsed_ms
      _scan_t0_ms=$(($(date +%s%N) / 1000000))
      printf '[validate-fullscan] start: stream=%s field=%s file=%s\n' \
        "$stream_spec" "$ts_field" "$file" >&2

      if command -v flock >/dev/null 2>&1; then
        touch "$_scan_lock" 2>/dev/null || true
        _scan=$(
          flock -x 200
          ffprobe -v quiet -select_streams "$stream_spec" \
            -show_entries "packet=$ts_field" -of default=noprint_wrappers=1:nokey=1 \
            "$file" 2>/dev/null
        ) 200>"$_scan_lock"
      else
        # Portable mkdir-lock fallback (tests / non-flock environments).
        # Same shape as _atomic_replace_path_in_tsv's fallback: 30-sec cap.
        local _scan_lockdir="${_scan_lock}dir" _scan_waited=0
        while ! mkdir "$_scan_lockdir" 2>/dev/null; do
          sleep 0.05
          _scan_waited=$((_scan_waited + 1))
          if [ "$_scan_waited" -gt 600 ]; then
            break
          fi
        done
        _scan=$(ffprobe -v quiet -select_streams "$stream_spec" \
          -show_entries "packet=$ts_field" -of default=noprint_wrappers=1:nokey=1 \
          "$file" 2>/dev/null)
        rmdir "$_scan_lockdir" 2>/dev/null || true
      fi

      first_pts=$(printf '%s\n' "$_scan" | head -n 1)
      last_pts=$(printf '%s\n' "$_scan" | tail -n 1)
      case "$last_pts" in ""|*[!-0-9.]*) last_pts="" ;; esac
      case "$first_pts" in ""|*[!-0-9.]*) first_pts="" ;; esac

      _scan_t1_ms=$(($(date +%s%N) / 1000000))
      _scan_elapsed_ms=$((_scan_t1_ms - _scan_t0_ms))
      # IMPORTANT: log to stderr only. This function's stdout is the
      # integer span return value — polluting it would break every
      # caller doing span=$(get_stream_span ...).
      printf '[validate-fullscan] done: stream=%s field=%s ms=%d first_pts=%s last_pts=%s\n' \
        "$stream_spec" "$ts_field" "$_scan_elapsed_ms" \
        "${first_pts:-<empty>}" "${last_pts:-<empty>}" >&2
    fi

    # If we got valid timestamps from this field, use them.
    if [ -n "$first_pts" ] && [ -n "$last_pts" ]; then
      break
    fi
    # Otherwise loop continues to try the next timestamp field (dts_time).
  done

  if [ -z "$first_pts" ] || [ -z "$last_pts" ]; then
    echo 0
    return 0
  fi

  awk -v f="$first_pts" -v l="$last_pts" 'BEGIN {
    span = l - f
    if (span < 0) span = -span
    printf "%.0f\n", span
  }'
}

# ── Phase 6C — dynamic HDR safety helper ─────────────────────────────
# Detects HDR10+, Dolby Vision (RPU + Metadata), and Vivid dynamic HDR
# metadata in a source file. Used by the worker's safety gate BEFORE
# invoking ffmpeg.
#
# Design intent: this is a safety gate, NOT a permanent block. The
# correct path for dynamic-HDR → SDR is libplacebo with the
# metadata-aware config (`tonemapping=st2094-40`, `tone_map_metadata=
# hdr10plus` or dovi.application_version, etc.). Until that
# libplacebo config is wired into build_scale_filter (Phase 6C-
# followup), the worker fails-closed on a positive detection so it
# can't destroy the source via static tonemap. Once the libplacebo
# config lands, the gate becomes conditional on tonemap_path
# availability rather than a blanket block.
#
# Background (2026-05-22 Avatar incident):
#   When NEED_TONEMAP=true and the source carries dynamic HDR metadata,
#   the current encode path applies a STATIC tonemap (opencl or CPU
#   mobius — neither has a metadata-source knob). Per-frame dynamic
#   metadata can't be represented in 8-bit SDR via static tonemap, so
#   the output exhibits visible flicker / highlight clipping / black
#   crush. The worker's span-based validation passes (duration is
#   correct) but the encode is visually broken and the source is then
#   destroyed on the move-into-place step. Unrecoverable loss.
#
# Static HDR10 → SDR remains UNTOUCHED — those sources have only the
# static `Mastering display metadata` and `Content light level
# metadata` side-data, which this helper does NOT match.
#
# Match strings come from FFmpeg 8.0 libavutil/side_data.c lines 37-44.
# These are the frame-level (not stream-tag) names ffprobe emits in
# JSON when -show_frames includes the side_data array:
#
#   - "HDR Dynamic Metadata SMPTE2094-40 (HDR10+)"
#   - "HDR Dynamic Metadata CUVA 005.1 2021 (Vivid)"
#   - "Dolby Vision RPU Data"
#   - "Dolby Vision Metadata"
#
# Frame side_data is authoritative — title tags can lie. A file titled
# "HDR10+" with stripped metadata won't fire this guard (correctly:
# its bytes are no longer dynamic-HDR and a static tonemap is fine).
# Conversely, a file with dynamic side_data but no title tag still
# fires the guard.
#
# Probe is bounded to ONE frame via `-read_intervals "%+#1"` — cheap,
# typically completes in tens of milliseconds even on FUSE. Dynamic
# HDR metadata is per-frame on the formats we care about (HDR10+ and
# DV both carry it on every frame), so first-frame check is
# sufficient.
#
#   $1: file path
# Returns:
#   0 + echoes one of: "hdr10plus" / "dolby_vision" / "vivid"  — dynamic
#                                                                  HDR detected
#   1 + echoes nothing  — no dynamic HDR detected (or file unreadable)
#
# Phase 6C-followup 2A note: the kind echo lets the worker guard
# route HDR10+ to a different (continue) branch from DV/Vivid (still
# fail-closed). Pre-2A callers that only checked the return code
# still work — they ignore stdout, which has empty default behavior
# in `if detect_dynamic_hdr ...; then` style usage.
detect_dynamic_hdr() {
  local file="$1"
  [ -n "$file" ] && [ -f "$file" ] || return 1

  local probe
  probe=$(ffprobe -v quiet -read_intervals '%+#1' \
    -show_frames -print_format json "$file" 2>/dev/null)
  [ -n "$probe" ] || return 1

  # Literal-substring match against the four canonical dynamic-HDR
  # side_data_type names. ffprobe emits these as JSON string values
  # under "side_data_type", so the surrounding double-quotes anchor
  # the match against accidental matches in other string fields
  # (e.g., a tag value that happens to contain "HDR10+"). Both DV
  # variants (RPU + Metadata) map to the same "dolby_vision" kind.
  case "$probe" in
    *'"HDR Dynamic Metadata SMPTE2094-40 (HDR10+)"'*)    echo "hdr10plus";    return 0 ;;
    *'"HDR Dynamic Metadata CUVA 005.1 2021 (Vivid)"'*)  echo "vivid";        return 0 ;;
    *'"Dolby Vision RPU Data"'*)                          echo "dolby_vision"; return 0 ;;
    *'"Dolby Vision Metadata"'*)                          echo "dolby_vision"; return 0 ;;
  esac

  return 1
}

# ── Phase 6C-followup step 1 — NVIDIA EGL Vulkan ICD setup ────────────
# Make libplacebo find the GPU on headless NVIDIA hosts (e.g. Unraid).
#
# Problem: the NVIDIA Container Toolkit injects /etc/vulkan/icd.d/
# nvidia_icd.json, but its `library_path` points at libGLX_nvidia.so.0.
# Inside a headless container without X11 client libs, that ICD load
# fails with VK_ERROR_INCOMPATIBLE_DRIVER and libplacebo falls back to
# "no suitable device, giving up" — the capability probe then writes
# tc:capabilities.hdr_tonemap_path=opencl, locking out the metadata-
# aware tonemap path we need for dynamic HDR.
#
# Fix (per NVIDIA driver docs for headless environments — see installed-
# components README on download.nvidia.com): use libEGL_nvidia.so.0
# instead of libGLX_nvidia.so.0. This helper generates a derivative
# ICD file with that one substitution, then exports VK_ICD_FILENAMES
# so Vulkan-using callers pick it up instead of the default file.
#
# Conservative on all preconditions — leaves the env UNSET when:
#   - The upstream NVIDIA ICD file doesn't exist (non-NVIDIA host).
#   - libEGL_nvidia.so.0 isn't in the linker cache (driver caps
#     don't include `graphics`, or EGL libs weren't injected).
#   - The substitution didn't actually produce an EGL library_path
#     (malformed source / already-non-GLX content).
# In those cases the existing OpenCL fallback path stays the active
# tonemap target — never silently degrades, never silently breaks
# Intel/CPU deployments.
#
# Paths are env-overridable for tests:
#   TRANSCODARR_VULKAN_ICD_SRC  — upstream NVIDIA ICD file
#   TRANSCODARR_VULKAN_ICD_DST  — derivative EGL ICD output path
setup_nvidia_egl_vulkan_icd() {
  local upstream="${TRANSCODARR_VULKAN_ICD_SRC:-/etc/vulkan/icd.d/nvidia_icd.json}"
  local egl_file="${TRANSCODARR_VULKAN_ICD_DST:-/etc/vulkan/icd.d/nvidia_egl_icd.json}"

  # This helper owns the Transcodarr Vulkan ICD override. Clear any
  # inherited value first so precondition failures cannot leave a stale
  # VK_ICD_FILENAMES active for the capability probe.
  unset VK_ICD_FILENAMES

  # Precondition 1: upstream ICD present (NVIDIA Container Toolkit
  # injects this on NVIDIA-enabled hosts; absent on Intel/CPU/CI).
  if [ ! -f "$upstream" ]; then
    _icd_log "Vulkan ICD: nvidia_icd.json not present (non-NVIDIA host?) — skipping EGL override"
    return 0
  fi

  # Precondition 2: libEGL_nvidia.so.0 resolvable via linker cache.
  # Don't try to dlopen it; ldconfig -p is enough to confirm the EGL
  # library was injected by the container runtime.
  if ! ldconfig -p 2>/dev/null | grep -qw 'libEGL_nvidia.so.0'; then
    _icd_log "Vulkan ICD: libEGL_nvidia.so.0 not in linker cache — skipping EGL override"
    return 0
  fi

  # Generate the derivative via in-place substring substitution of the
  # library_path. Using perl (not sed) avoids JSON-quoting / forward-
  # slash escaping pitfalls. Preserve api_version and any other fields
  # the upstream ICD carries — those track driver versions and must
  # not be hardcoded.
  mkdir -p "$(dirname "$egl_file")" 2>/dev/null || true
  if ! perl -pe 's{libGLX_nvidia\.so\.0}{libEGL_nvidia.so.0}g' "$upstream" > "$egl_file" 2>/dev/null; then
    _icd_log "WARN: Vulkan ICD: failed to write $egl_file — skipping EGL override"
    rm -f "$egl_file" 2>/dev/null || true
    return 0
  fi

  # Verify the output contains a libEGL_nvidia.so.0 library_path. If
  # the upstream file didn't contain libGLX_nvidia.so.0 to substitute,
  # the perl pass would have produced a file without an EGL path —
  # don't export VK_ICD_FILENAMES against that.
  if ! grep -q 'libEGL_nvidia.so.0' "$egl_file"; then
    _icd_log "Vulkan ICD: upstream nvidia_icd.json did not contain libGLX_nvidia.so.0 — skipping EGL override"
    rm -f "$egl_file" 2>/dev/null || true
    return 0
  fi

  # All preconditions met. Export the env so the libplacebo probe
  # (and any other Vulkan caller) picks up the EGL ICD instead of
  # the upstream GLX-targeting one.
  export VK_ICD_FILENAMES="$egl_file"
  _icd_log "Vulkan ICD: EGL override active (VK_ICD_FILENAMES=$egl_file)"
}

# Internal log helper for setup_nvidia_egl_vulkan_icd. Uses the
# sourcing script's `log` when defined, otherwise stderr printf.
_icd_log() {
  if declare -F log >/dev/null 2>&1; then
    log "$1"
  else
    printf '[transcodarr] %s\n' "$1" >&2
  fi
}

# ── SSD reservation lease helpers ─────────────────────────────────────────
# Used when TRANSCODARR_TMP_DIR is set. These are no-ops in the
# non-TMP_DIR path — the LB only consults them when has_tmp_dir=true.

# Mint a random 8-hex-char nonce for a new lease key. 32 bits of entropy
# from /dev/urandom is sufficient for collision avoidance within the narrow
# "same filepath reserved twice in rapid succession" window that matters.
mint_lease_nonce() {
  head -c 4 /dev/urandom 2>/dev/null | od -An -tx1 2>/dev/null | tr -d ' \n' || printf '%08x' $(( RANDOM * 32768 + RANDOM ))
}

# Release an SSD lease key by DELing the lease and removing it from the
# tc:ssd:leases index set. No-op if the key name is empty (non-TMP_DIR
# items or already-released leases). Idempotent — safe to call multiple
# times on the same key, so consumer-side and worker-side releases can
# overlap without error. Both operations tolerate missing state: DEL on
# a non-existent key returns 0, SREM on a non-member returns 0.
release_ssd_lease() {
  local key="$1"
  [ -z "$key" ] && return 0
  $QUEUE_CLI DEL "$key" > /dev/null 2>&1 || true
  $QUEUE_CLI SREM tc:ssd:leases "$key" > /dev/null 2>&1 || true
}

# Compute lease TTL in seconds. Default = TRANSCODARR_TIMEOUT + 60s (the
# kill-after grace on `timeout --foreground --kill-after=60`) + 300s
# (reconciliation buffer). TRANSCODARR_SSD_RESERVE_TTL_SEC overrides for
# isolated tests, which use a low value (e.g. 5) to let the reconciliation
# cycle run in seconds instead of hours.
#
# Defensive: validate both inputs as positive integers. A bad override
# (empty / non-numeric / zero / negative) would cause SETEX in the admission
# Lua to error AFTER the item has already been LREM'd from the source queue
# — Redis doesn't roll back prior writes on runtime error, so the item
# would be lost. Fall back to a safe default on any invalid input.
compute_lease_ttl() {
  local tc_timeout="${TRANSCODARR_TIMEOUT:-14400}"
  [[ "$tc_timeout" =~ ^[1-9][0-9]*$ ]] || tc_timeout=14400
  local default_ttl=$(( tc_timeout + 60 + 300 ))
  local override="${TRANSCODARR_SSD_RESERVE_TTL_SEC:-}"
  if [[ "$override" =~ ^[1-9][0-9]*$ ]]; then
    echo "$override"
  else
    echo "$default_ttl"
  fi
}

# ── ffprobe audio metadata ─────────────────────────────────────────────────

# Normalize language tags from ffprobe / Arr metadata into Transcodarr's
# configured language keys. Files in the wild are not consistent: English can
# be tagged as eng, en, English, or BCP-47 variants such as en-US.
normalize_audio_language_tag() {
  # Phase 5E (fork-stripping): builtin-only normalization. The old form
  # piped through three tr invocations per call; this version uses
  # bash parameter expansion which produces identical output for
  # every input in tests/queue-fork-stripping-parity-unit.sh (123
  # parity assertions cover messy labels, NBSP, multi-word, etc.).
  local lang="${1:-}"
  lang="${lang,,}"               # lowercase  (was: tr '[:upper:]' '[:lower:]')
  lang="${lang//_/-}"             # _ → -      (was: tr '_' '-')
  lang="${lang//[[:space:]]/}"    # strip space (was: tr -d '[:space:]')

  case "$lang" in
    ""|und|unknown|none|null|n/a) echo "und"; return 0 ;;
  esac

  case "$lang" in
    en|eng|en-*|eng-*|english*|cpe) echo "eng"; return 0 ;;
    es|spa|esp|es-*|spa-*|spanish*) echo "spa"; return 0 ;;
    fr|fre|fra|fr-*|fre-*|fra-*|french*) echo "fre"; return 0 ;;
    de|ger|deu|de-*|ger-*|deu-*|german*) echo "ger"; return 0 ;;
    it|ita|it-*|ita-*|italian*) echo "ita"; return 0 ;;
    ja|jpn|ja-*|jpn-*|japanese*) echo "jpn"; return 0 ;;
    ko|kor|ko-*|kor-*|korean*) echo "kor"; return 0 ;;
    pt|por|pt-*|por-*|portuguese*) echo "por"; return 0 ;;
    ru|rus|ru-*|rus-*|russian*) echo "rus"; return 0 ;;
    zh|zho|chi|cmn|zh-*|zho-*|chi-*|cmn-*|chinese*|mandarin*) echo "zho"; return 0 ;;
  esac

  # Preserve custom ISO-639 style tags; otherwise treat arbitrary labels like
  # "stereo" or "default" as unknown metadata rather than a language.
  if [[ "$lang" =~ ^[a-z]{2,3}$ ]]; then
    echo "$lang"
  else
    echo "und"
  fi
}

# is_commentary_track — shared commentary predicate (spec §12).
# Implements the UNION of the worker's inline regex (worker.sh:576-585:
# commentary|director|cast|descriptive|visually impaired|ad track) and
# verify-bot's vocabulary (isolated|music-only|behind|making|
# interview|featurette|trivia). The union is a superset of the worker's
# set, so the classifier never under-classifies commentary. Plan A ships
# no behavior change — only record_failed is refactored, not the worker's
# audio-selection block (a fast-follow can rewire it onto this helper).
# Title + handler_name are matched case-insensitively against a
# commentary/extra-content vocabulary; disposition flags are matched
# against ffprobe's comment/impaired/descriptions names.
#   $1: title
#   $2: handler_name
#   $3: disposition (comma-joined ffprobe disposition names)
# Returns 0 (true) if commentary, 1 (false) otherwise.
is_commentary_track() {
  local title="${1:-}" handler="${2:-}" disp="${3:-}"
  local title_lower handler_lower
  title_lower="${title,,}"
  handler_lower="${handler,,}"
  local title_re='(^|[^[:alnum:]])(commentary|director|cast|descriptive|visually[[:space:]_-]+impaired|ad[[:space:]_-]+track|isolated|music[[:space:]_-]*only|behind|making|interview|featurette|trivia)([^[:alnum:]]|$)'
  local disp_re='comment|visual_impaired|hearing_impaired|descriptions'
  if printf '%s' "$title_lower"   | grep -qE "$title_re"; then return 0; fi
  if printf '%s' "$handler_lower" | grep -qE "$title_re"; then return 0; fi
  if printf '%s' "${disp,,}"      | grep -qE "$disp_re";  then return 0; fi
  return 1
}

# audio_needs_lang_detection — divert predicate (spec §5, §12).
# True when the file is multi-track AND no NON-commentary track
# normalizes to the preferred language AND there is at least one
# untagged non-commentary candidate (normalized tag in
# und/zxx/mis/mul/qaa). Candidate selection tests the NORMALIZED tag
# (worker.sh:568) so junk labels (stereo/default -> und) are treated
# as untagged, matching the worker that would otherwise fail the file
# no_eng_audio. Consumes probe_audio_streams.
#   $1: filepath
# Returns 0 (true) if the file should divert to language detection.
audio_needs_lang_detection() {
  local filepath="$1"
  local pref
  pref=$(normalize_audio_language_tag "${TRANSCODARR_AUDIO_LANG:-eng}")

  local US=$'\037'
  local count=0 has_pref_noncomm=0 has_untagged_candidate=0
  local ordinal lang channels title codec handler disp
  while IFS="$US" read -r ordinal lang channels title codec handler disp; do
    [ -z "$ordinal" ] && continue
    count=$((count + 1))
    local is_comm=0
    if is_commentary_track "$title" "$handler" "$disp"; then is_comm=1; fi
    local lang_norm
    lang_norm=$(normalize_audio_language_tag "$lang")
    if [ "$is_comm" -eq 0 ] && [ "$lang_norm" = "$pref" ]; then
      has_pref_noncomm=1
    fi
    if [ "$is_comm" -eq 0 ]; then
      case "$lang_norm" in
        und|zxx|mis|mul|qaa) has_untagged_candidate=1 ;;
      esac
    fi
  done < <(probe_audio_streams "$filepath")

  [ "$count" -ge 1 ] || return 1
  [ "$has_pref_noncomm" -eq 0 ] || return 1
  [ "$has_untagged_candidate" -eq 1 ] || return 1
  return 0
}

# Returns success when a possibly multi-value language label includes the
# requested language. Handles Arr labels such as "English", ffprobe tags such
# as "en-US", and separator-heavy strings such as "English, Japanese".
audio_language_list_has() {
  # Phase 5E: builtin-only. Old form pumped the labels through two tr
  # invocations + process substitution. New form replaces separators
  # (`,;|/` plus all whitespace) with newlines via parameter
  # expansion, then iterates via a here-string. Each invocation now
  # has at most ~3 subshell forks (one per command substitution),
  # not ~5+ exec forks.
  local labels="${1:-}" wanted="${2:-}"
  local wanted_norm label_norm token

  wanted_norm=$(normalize_audio_language_tag "$wanted")
  case "$wanted_norm" in ""|und) return 1 ;; esac

  # Whole-string normalize first (matches "English" / "eng-US" directly).
  label_norm=$(normalize_audio_language_tag "$labels")
  [ "$label_norm" = "$wanted_norm" ] && return 0

  # Split into tokens. Replace each separator family with a newline
  # via parameter expansion (no fork), then read tokens line by line.
  # Whitespace is treated as a separator too — old tr -s '[:space:]'
  # collapsed runs of whitespace, but here-string + IFS=$'\n' handles
  # empty-token skipping below, so multi-space separator runs work
  # the same way.
  local cleaned="${labels//,/$'\n'}"
  cleaned="${cleaned//;/$'\n'}"
  cleaned="${cleaned//|/$'\n'}"
  cleaned="${cleaned//\//$'\n'}"
  cleaned="${cleaned//[[:space:]]/$'\n'}"

  while IFS= read -r token; do
    [ -z "$token" ] && continue
    label_norm=$(normalize_audio_language_tag "$token")
    [ "$label_norm" = "$wanted_norm" ] && return 0
  done <<<"$cleaned"

  return 1
}

# Returns success when a source audio stream is safe to copy for the target
# codec. AAC is special: ffprobe reports AAC-LC, HE-AAC, HE-AAC v2, LD, and
# ELD as codec_name=aac, so profile must be LC for Android TV-safe passthrough.
audio_passthrough_ok() {
  local target="${1:-}" src_codec="${2:-}" src_profile="${3:-}"

  target=$(printf '%s' "$target" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')
  src_codec=$(printf '%s' "$src_codec" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')
  src_profile=$(printf '%s' "$src_profile" | tr '[:upper:]' '[:lower:]' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

  [ -n "$target" ] && [ -n "$src_codec" ] || return 1
  [ "$src_codec" = "$target" ] || return 1

  if [ "$target" = "aac" ]; then
    [ "$src_profile" = "lc" ]
    return $?
  fi

  return 0
}

# ── Phase 7-followup: classifier-owned flag index ─────────────────────
#
# compute_flags_for_file — pure flag detector. Given the file-derived
# values classify already extracted from the actual file, returns zero
# or more TSV-shaped rows (ts<TAB>service<TAB>reason<TAB>path<TAB>detail)
# on stdout. Empty stdout means "no flags."
#
# Five flags emitted (non_standard_resolution INTENTIONALLY DROPPED —
# too many legitimate non-standard widths in real libraries):
#   - interlaced            (field_order != progressive/empty/unknown)
#   - unusual_pix_fmt       (not yuv420p variants)
#   - low_bitrate_suspect   (kbps/Mpx < 100)
#   - short_radarr_runtime  (radarr-only, opt-in via env)
#   - unverified_lang       (single audio track w/ empty/und/unknown tag)
#
# Named args (so callers self-document):
#   --service --path --vwidth --vheight --ach --acount --alang
#   --pix_fmt --field_order --duration --file_size
#
# `acodec` intentionally not a parameter — no flag consults it today.
compute_flags_for_file() {
  local service="" path="" vwidth=0 vheight=0 ach=0 acount=1 alang=""
  local pix_fmt="" field_order="" duration=0 file_size=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --service)     service="$2"; shift 2 ;;
      --path)        path="$2"; shift 2 ;;
      --vwidth)      vwidth="$2"; shift 2 ;;
      --vheight)     vheight="$2"; shift 2 ;;
      --ach)         ach="$2"; shift 2 ;;
      --acount)      acount="$2"; shift 2 ;;
      --alang)       alang="$2"; shift 2 ;;
      --pix_fmt)     pix_fmt="$2"; shift 2 ;;
      --field_order) field_order="$2"; shift 2 ;;
      --duration)    duration="$2"; shift 2 ;;
      --file_size)   file_size="$2"; shift 2 ;;
      *) shift ;;
    esac
  done
  local ts; ts=$(date -Iseconds)

  # interlaced
  case "$field_order" in
    progressive|""|unknown) ;;
    *) printf '%s\t%s\tinterlaced\t%s\t%s\n' "$ts" "$service" "$path" "$field_order" ;;
  esac

  # unusual_pix_fmt
  case "$pix_fmt" in
    yuv420p|yuv420p10le|yuv420p10be|yuvj420p|""|unknown) ;;
    *) printf '%s\t%s\tunusual_pix_fmt\t%s\t%s\n' "$ts" "$service" "$path" "$pix_fmt" ;;
  esac

  # low_bitrate_suspect
  if [ "${duration:-0}" -gt 0 ] && [ "${vwidth:-0}" -gt 0 ] \
      && [ "${vheight:-0}" -gt 0 ] && [ "${file_size:-0}" -gt 0 ]; then
    local mpx=$(( vwidth * vheight / 1000000 ))
    [ "$mpx" -lt 1 ] && mpx=1
    local kbps=$(( file_size / duration / mpx / 1024 ))
    if [ "$kbps" -lt 100 ]; then
      printf '%s\t%s\tlow_bitrate_suspect\t%s\t%s kbps/Mpx\n' "$ts" "$service" "$path" "$kbps"
    fi
  fi

  # unverified_lang (sole track empty/und/unknown)
  if [ "${acount:-1}" -eq 1 ] 2>/dev/null; then
    local lang_norm=""
    [ -n "$alang" ] && lang_norm=$(normalize_audio_language_tag "$alang")
    case "$lang_norm" in
      ""|und|unknown)
        printf '%s\t%s\tunverified_lang\t%s\ttag=%s\n' "$ts" "$service" "$path" "${alang:-<empty>}"
        ;;
    esac
  fi

  # short_radarr_runtime (optional, off by default).
  # Use the canonical exported env name; the worker historically aliased
  # this to a local var that does NOT exist in lib.sh / entrypoint scope.
  if [ "${TRANSCODARR_FLAG_SHORT_RADARR_RUNTIME:-false}" = "true" ] \
      && [ "${service,,}" = "radarr" ] \
      && [ "${duration:-0}" -gt 0 ] \
      && [ "${duration:-0}" -lt 3600 ]; then
    local mins=$(( (duration + 59) / 60 ))
    printf '%s\t%s\tshort_radarr_runtime\t%s\t%s min\n' "$ts" "$service" "$path" "$mins"
  fi
}

# set_path_flags / clear_path_flags — runtime current-state writers.
# HSET stores a multi-line value containing all of a path's flag rows;
# HDEL removes the path entirely (used when the file is now flag-free).
# Every mutation marks tc:flags:tsv_dirty so the API knows to regen the
# durable TSV before the next read.
set_path_flags() {
  local path="$1" rows="${2:-}"
  if [ -z "$rows" ]; then
    clear_path_flags "$path"
    return
  fi
  $QUEUE_CLI HSET tc:flags:by_path "$path" "$rows" > /dev/null 2>&1 || true
  $QUEUE_CLI SET  tc:flags:tsv_dirty 1            > /dev/null 2>&1 || true
}

clear_path_flags() {
  $QUEUE_CLI HDEL tc:flags:by_path "$1" > /dev/null 2>&1 || true
  $QUEUE_CLI SET  tc:flags:tsv_dirty 1 > /dev/null 2>&1 || true
}

# rebuild_flag_index_from_tsv — boot-time TSV -> HSET hydrator.
# Reads /state/flagged-files.tsv (durable backing), groups rows by path
# (column 4), and emits one HSET per path with all that path's rows as
# the multi-line value. After completion, SETs tc:flags:ready=1 and
# DELs tc:flags:tsv_dirty so the next API read serves directly from
# the now-canonical Valkey index without an unnecessary regen.
#
# Mirrors the Phase 5A rebuild_rail_index timing — this function MUST
# only run after valkey-cli ping has succeeded.
rebuild_flag_index_from_tsv() {
  # Layered fallback so an API shell-out (bash -c "source lib.sh ...")
  # that has TRANSCODARR_STATE_DIR but not STATE_DIR still resolves
  # the same path the entry-point scripts use.
  local state_dir="${STATE_DIR:-${TRANSCODARR_STATE_DIR:-/state}}"
  local tsv="$state_dir/flagged-files.tsv"
  # ALWAYS wipe the existing hash first so the rebuilt index mirrors
  # the on-disk TSV exactly. Without this, rebuilding from a smaller
  # TSV (e.g. after Clear Flagged + partial rescan, or after manual
  # external edits) would leave orphan paths in the hash with no TSV
  # backing — they'd resurrect on the next snapshot.
  $QUEUE_CLI DEL tc:flags:by_path > /dev/null 2>&1 || true
  if [ ! -s "$tsv" ]; then
    # Empty or missing TSV → mark ready so the API knows "zero flags"
    # is the current state rather than "uninitialized."
    $QUEUE_CLI SET tc:flags:ready 1 > /dev/null 2>&1 || true
    $QUEUE_CLI DEL tc:flags:tsv_dirty > /dev/null 2>&1 || true
    return 0
  fi
  local dropped_corrupt_numeric_lang=0
  if awk -F'\t' '$3=="unverified_lang" && $5 ~ /^tag=[0-9]+$/ { found=1 } END { exit found ? 0 : 1 }' "$tsv"; then
    dropped_corrupt_numeric_lang=1
  fi
  # Group by path (col 4) via perl and emit one HSET per path.
  perl -e '
    my %by_path;
    while (<STDIN>) {
      chomp;
      my @f = split /\t/, $_, -1;
      next unless @f >= 4;
      my $reason = $f[2] // "";
      my $detail = $f[4] // "";
      # Phase 7-followup parser bug produced rows like
      # unverified_lang ... tag=1280/tag=1920 when an empty language
      # field shifted the video width into alang_raw. Language tags are
      # never numeric-only; drop those corrupt persisted rows on rebuild.
      next if $reason eq "unverified_lang" && $detail =~ /^tag=\d+$/;
      my $path = $f[3];
      next unless defined $path && length $path;
      $by_path{$path} //= [];
      push @{$by_path{$path}}, $_;
    }
    for my $p (keys %by_path) {
      my $val = join("\n", @{$by_path{$p}});
      # Print as null-delimited "path\0value\0" pairs for the shell loop.
      print $p, "\0", $val, "\0";
    }
  ' < "$tsv" | while IFS= read -r -d '' path && IFS= read -r -d '' value; do
    $QUEUE_CLI HSET tc:flags:by_path "$path" "$value" > /dev/null 2>&1 || true
  done
  $QUEUE_CLI SET tc:flags:ready 1 > /dev/null 2>&1 || true
  if [ "$dropped_corrupt_numeric_lang" = "1" ]; then
    # HSET is clean but the durable TSV still contains dropped corrupt
    # rows. Mark dirty so the first API read snapshots the clean HSET
    # back over flagged-files.tsv.
    $QUEUE_CLI SET tc:flags:tsv_dirty 1 > /dev/null 2>&1 || true
  else
    $QUEUE_CLI DEL tc:flags:tsv_dirty > /dev/null 2>&1 || true
  fi
}

# snapshot_flag_index_to_tsv — HSET -> durable TSV. Success-gated:
# returns non-zero on Valkey error WITHOUT touching the durable TSV,
# so a transient Valkey blip cannot wipe the last-known-good state.
# Callers MUST only clear tc:flags:tsv_dirty on rc == 0.
snapshot_flag_index_to_tsv() {
  # Same layered fallback as rebuild_flag_index_from_tsv — the API
  # shell-out won't have STATE_DIR set in env but will have
  # TRANSCODARR_STATE_DIR via the api.pl request env.
  local state_dir="${STATE_DIR:-${TRANSCODARR_STATE_DIR:-/state}}"
  local tsv="$state_dir/flagged-files.tsv"
  local tmp
  tmp=$(mktemp "${tsv}.snap.XXXXXX") || return 1

  # HVALS returns each field's value on its own line; values themselves
  # already contain embedded newlines for multi-row paths. Concat as-is.
  if ! $QUEUE_CLI HVALS tc:flags:by_path 2>/dev/null > "$tmp"; then
    rm -f "$tmp"
    return 1
  fi

  # HVALS on an empty hash emits a single trailing newline (1 byte),
  # which serve_tsv would parse as a phantom empty-field row. HSET
  # values themselves never contain blank lines (multi-row values are
  # `\n`-joined non-empty rows), so it's safe to drop every blank line
  # before writing. Empty hash → empty file → serve_tsv returns zero.
  sed -i '/^$/d' "$tmp"

  mv -f "$tmp" "$tsv" || { rm -f "$tmp"; return 1; }
  return 0
}

# build_queue_reasons — canonical CSV of queue reasons for the
# verified-hash gate (Phase 1). Single source of truth for both the
# Radarr and Sonarr query loops in transcodarr-queue.sh. Returns
# alphabetically-sorted CSV so equality checks against
# `aac_profile_unknown` (single-reason whole-skip eligibility) are
# deterministic. Empty stdout means "no needs_work" — don't queue.
#
# Args (all strings, may be empty):
#   $1 vcodec      source video codec
#   $2 acodec      source audio codec (mediaInfo.audioCodec)
#   $3 vres        source resolution "WxH" (may be "0" or empty)
#   $4 ach         source primary-audio channel count
#   $5 acount      source audio stream count
#   $6 alang       source audio language(s) (may be CSV from arr API)
#
# Env consulted: TRANSCODARR_AUDIO_CODEC (target), TRANSCODARR_AUDIO_LANG,
#   TRANSCODARR_API_AUDIO_LANG, TRANSCODARR_API_MAX_WIDTH/HEIGHT/CHANNELS,
#   TRANSCODARR_MAX_WIDTH/HEIGHT/CHANNELS (fallbacks).
#
# AAC-aware rule (target-aware per rev3): only emit `aac_profile_unknown`
# when target=aac AND source=aac. If target is anything else (opus, etc.)
# an AAC source emits `audio_codec_mismatch` — must transcode regardless
# of profile. Missing source acodec emits `audio_codec_unknown`
# (broader, also disqualifies whole-skip).
build_queue_reasons() {
  # Phase 5E (fork-stripping): hot-loop function called once per queue
  # candidate (~20-30k rows per scan). Pre-5E, this function shelled
  # out ~5-8 times per call (tr×2, cut×3, sort+paste pipeline), and
  # also called audio_language_list_has which forked further. The
  # rewrite uses bash parameter expansion exclusively; the only
  # remaining forks are command substitutions on the language path
  # (audio_language_list_has → normalize_audio_language_tag → echo).
  # See tests/queue-fork-stripping-parity-unit.sh for byte-for-byte
  # behavioral equivalence guarantees.
  local vcodec="$1" acodec="$2" vres="$3" ach="$4" acount="$5" alang="$6"
  local -a reasons=()

  # Video codec — lowercase via ${var,,} instead of `printf | tr`.
  local vc_lower="${vcodec,,}"
  case "$vc_lower" in
    h264|x264|avc|h.264) ;;
    *) reasons+=(video_codec_mismatch) ;;
  esac

  # Audio codec — target-aware. See header comment in the original
  # for rationale (target=aac + source=aac → aac_profile_unknown).
  local target_acodec="${TRANSCODARR_AUDIO_CODEC:-aac}"
  if [ -n "$acodec" ]; then
    local ac_lower="${acodec,,}"
    if [ "$target_acodec" = "aac" ] && [ "$ac_lower" = "aac" ]; then
      reasons+=(aac_profile_unknown)
    elif [ "$ac_lower" != "$target_acodec" ]; then
      reasons+=(audio_codec_mismatch)
    fi
  else
    reasons+=(audio_codec_unknown)
  fi

  # Resolution — split "WxH" via parameter expansion. Equivalent to
  # `cut -dx -f1` / `cut -dx -f2` byte-for-byte, including the
  # multi-`x` corner case ("AxBxC" → f2 = "B", NOT "C"). ${var##*x}
  # alone would have taken the last segment instead of the second.
  # Correctness fix.
  local api_max_w="${TRANSCODARR_API_MAX_WIDTH-${TRANSCODARR_MAX_WIDTH:-1920}}"
  local api_max_h="${TRANSCODARR_API_MAX_HEIGHT-${TRANSCODARR_MAX_HEIGHT:-1080}}"
  if [ -n "$vres" ] && [ "$vres" != "0" ]; then
    local res_w="${vres%%x*}"
    local res_h
    if [[ "$vres" == *x* ]]; then
      local _rest="${vres#*x}"
      res_h="${_rest%%x*}"
    else
      res_h="$vres"
    fi
    if [ -n "$api_max_w" ] && [ "${res_w:-0}" -gt "$api_max_w" ] 2>/dev/null; then
      reasons+=(resolution_too_big)
    fi
    if [ -n "$api_max_h" ] && [ "${res_h:-0}" -gt "$api_max_h" ] 2>/dev/null; then
      reasons+=(resolution_too_big)
    fi
  fi

  # Channels (e.g. "5.1" → take whole-channel count). ${var%%.*} =
  # everything before the first '.', equivalent to `cut -d. -f1`.
  local api_max_ch="${TRANSCODARR_API_MAX_CHANNELS-${TRANSCODARR_MAX_CHANNELS:-6}}"
  if [ -n "$api_max_ch" ] && [ -n "$ach" ] && [ "$ach" != "0" ]; then
    local ach_int="${ach%%.*}"
    if [ "$ach_int" -gt "$api_max_ch" ] 2>/dev/null; then
      reasons+=(channels_too_many)
    fi
  fi

  # Multi-audio
  if [ "${acount:-1}" -gt 1 ] 2>/dev/null; then
    reasons+=(multi_audio)
  fi

  # Language
  local api_audio_lang="${TRANSCODARR_API_AUDIO_LANG-${TRANSCODARR_AUDIO_LANG:-eng}}"
  if [ -n "$api_audio_lang" ] && [ -n "$alang" ]; then
    audio_language_list_has "$alang" "$api_audio_lang" || reasons+=(audio_language_mismatch)
  fi

  # Dedup + sort + comma-join the reasons array. Pre-5E used
  # `printf | sort -u | paste -sd ','` (3 forks). The reasons
  # array is bounded to ~6 entries (one per dimension), so
  # in-bash insertion sort + associative-array dedup costs
  # microseconds and zero forks.
  if [ "${#reasons[@]}" -gt 0 ]; then
    local -A seen=()
    local -a uniq=()
    local r
    for r in "${reasons[@]}"; do
      if [ -z "${seen[$r]+x}" ]; then
        seen[$r]=1
        uniq+=("$r")
      fi
    done
    # Insertion sort uniq (n <= 6 in practice — O(n²) is fine).
    local n=${#uniq[@]} i j tmp
    for ((i = 1; i < n; i++)); do
      tmp="${uniq[i]}"
      j=$((i - 1))
      while [ $j -ge 0 ] && [[ "${uniq[j]}" > "$tmp" ]]; do
        uniq[j+1]="${uniq[j]}"
        j=$((j - 1))
      done
      uniq[j+1]="$tmp"
    done
    # Comma-join via local IFS scoping (auto-reverts on function return).
    local IFS=','
    echo "${uniq[*]}"
  fi
}

# ffprobe can emit duplicate width/height values for some program-based
# MPEG-TS/M2TS inputs when probing v:0, which previously collapsed into
# values like 19201920x10801080. Normalize repeated identical integers.
normalize_video_dimension_value() {
  local value="${1:-}"
  value=$(printf '%s' "$value" | tr -d '[:space:]')
  case "$value" in ""|*[!0-9]*) echo 0; return 0 ;; esac

  local len=${#value}
  if (( len % 2 == 0 )); then
    local half=$((len / 2))
    local left="${value:0:half}"
    local right="${value:half}"
    if [ "$left" = "$right" ] && [ "$left" -ge 100 ] 2>/dev/null; then
      echo "$left"
      return 0
    fi
  fi

  echo "$value"
}

probe_video_dimensions() {
  local filepath="$1"
  local raw line f1 f2 f3 w h

  raw=$(ffprobe -v quiet -show_entries stream=codec_type,width,height \
    -of csv=p=0:s='|' "$filepath" 2>/dev/null || true)

  while IFS= read -r line; do
    [ -z "$line" ] && continue
    IFS='|' read -r f1 f2 f3 _ <<< "$line"
    if [ "$f1" = "video" ]; then
      w="$f2"; h="$f3"
    elif [ "$f2" = "video" ]; then
      w="$f1"; h="$f3"
    elif [ "$f3" = "video" ]; then
      w="$f1"; h="$f2"
    else
      continue
    fi

    w=$(normalize_video_dimension_value "$w")
    h=$(normalize_video_dimension_value "$h")
    if [ "$w" -gt 0 ] 2>/dev/null && [ "$h" -gt 0 ] 2>/dev/null; then
      printf '%s %s\n' "$w" "$h"
      return 0
    fi
  done <<< "$raw"

  local raw_w raw_h
  raw_w=$(ffprobe -v quiet -select_streams v:0 -show_entries stream=width \
    -of default=noprint_wrappers=1:nokey=1 "$filepath" 2>/dev/null || true)
  raw_h=$(ffprobe -v quiet -select_streams v:0 -show_entries stream=height \
    -of default=noprint_wrappers=1:nokey=1 "$filepath" 2>/dev/null || true)
  w=$(normalize_video_dimension_value "$raw_w")
  h=$(normalize_video_dimension_value "$raw_h")
  printf '%s %s\n' "$w" "$h"
}

#
# probe_audio_streams <filepath>
#
# Prints one line per audio stream, separated by ASCII Unit Separator (0x1F):
#   audio_ordinal<US>language<US>channels<US>title<US>codec_name<US>handler_name<US>disposition
#
# audio_ordinal is 0-based position among audio-only streams (for -map 0:a:N).
# This is NOT the ffprobe global stream index — files with interleaved subtitle
# or data streams between audio streams will have different global index values.
#
# disposition is a comma-separated list of active dispositions from ffprobe
# (e.g. "comment" for commentary tracks, "visual_impaired" for descriptive).
#
# Returns 0 if ffprobe succeeds (even with zero audio streams).
# Returns 1 if ffprobe fails entirely (corrupt / unreadable file).

# Deduplicate ffprobe CSV output where the first field is a stream
# index. MPEG-TS containers cause ffprobe to emit each stream twice
# (programs[] then streams[]) with the same indices in both blocks but
# different field population. This helper keeps the row with the most
# populated (non-empty) fields per index, strips the leading index
# column, and emits the result in numeric index order.
#
# Input rows whose first field isn't a positive integer (e.g., empty
# section-separator lines) are skipped silently.
_dedupe_indexed_csv() {
  awk -F'|' '
    $1 ~ /^[0-9]+$/ {
      filled = 0
      for (i = 1; i <= NF; i++) if ($i != "") filled++
      if (!($1 in best_filled) || filled > best_filled[$1]) {
        best_filled[$1] = filled
        line[$1] = $0
      }
    }
    END {
      for (idx in line) {
        l = line[idx]
        sub(/^[0-9]+\|/, "", l)
        printf "%d\t%s\n", idx, l
      }
    }' | sort -n | cut -f2-
}

probe_audio_streams() {
  local filepath="$1"
  local raw_csv raw
  # Use pipe delimiter (not tab) because bash IFS treats tabs as whitespace
  # and collapses consecutive empty fields. Pipe is non-whitespace so
  # empty fields between pipes are preserved correctly.
  #
  # The probe includes `index` so we can deduplicate the output. MPEG-TS
  # containers cause ffprobe to emit each audio stream TWICE — once
  # under the programs[] section (which has no language tags) and once
  # under the top-level streams[] section (which carries the tags). The
  # CSV output renders these as two blocks separated by an empty line,
  # so a naive parse would count each audio stream twice and shift the
  # selected-track ordinal past the actual stream count, producing
  # invalid `-map 0:a:N` arguments. _dedupe_indexed_csv collapses the
  # two blocks back to one row per stream index, keeping the row with
  # the most populated fields (i.e., the tagged version).
  raw_csv=$(ffprobe -v quiet -select_streams a \
    -show_entries 'stream=index,codec_name,channels:stream_tags=language,title,handler_name' \
    -of csv=p=0:s='|' "$filepath" 2>/dev/null) || return 1
  raw=$(printf '%s\n' "$raw_csv" | _dedupe_indexed_csv)

  # Empty output means no audio streams — valid result, caller handles it
  [ -z "$raw" ] && return 0

  # Get disposition flags per audio stream (comment, visual_impaired, etc.)
  # Output: one line per audio stream with active disposition names. Same
  # programs[]/streams[] doubling can occur here, so apply the same
  # dedupe.
  local disp_csv disp_raw
  disp_csv=$(ffprobe -v quiet -select_streams a \
    -show_entries 'stream=index:stream_disposition=comment,visual_impaired,hearing_impaired,descriptions' \
    -of csv=p=0:s='|' "$filepath" 2>/dev/null || true)
  disp_raw=$(printf '%s\n' "$disp_csv" | _dedupe_indexed_csv)

  # Build disposition array — each line has 4 flags (0 or 1):
  # comment|visual_impaired|hearing_impaired|descriptions
  local -a DISP_LINES=()
  if [ -n "$disp_raw" ]; then
    while IFS= read -r line; do
      DISP_LINES+=("$line")
    done <<< "$disp_raw"
  fi

  # ffprobe csv output per audio stream (pipe-separated, -select_streams a):
  #   codec_name|channels|language|title|handler_name
  # Fields may be empty when tags are missing. We reorder to the documented
  # output format and prepend a 0-based audio ordinal.
  #
  # The final output uses a non-whitespace delimiter (ASCII Unit Separator)
  # so empty title/handler fields are preserved when the worker reads them.
  local out_sep=$'\037'
  local ordinal=0
  while IFS='|' read -r codec channels language title handler_name; do
    # Normalize empty/missing fields
    language="${language:-und}"
    title="${title:-}"
    handler_name="${handler_name:-}"
    channels="${channels:-0}"
    codec="${codec:-unknown}"

    # Build disposition string from flags
    local disp=""
    if [ "$ordinal" -lt "${#DISP_LINES[@]}" ]; then
      local dline="${DISP_LINES[$ordinal]}"
      IFS='|' read -r d_comment d_visual d_hearing d_desc <<< "$dline"
      local parts=()
      [ "${d_comment:-0}" = "1" ] && parts+=("comment")
      [ "${d_visual:-0}" = "1" ] && parts+=("visual_impaired")
      [ "${d_hearing:-0}" = "1" ] && parts+=("hearing_impaired")
      [ "${d_desc:-0}" = "1" ] && parts+=("descriptions")
      disp=$(IFS=','; echo "${parts[*]}")
    fi

    printf '%s%s%s%s%s%s%s%s%s%s%s%s%s\n' \
      "$ordinal" "$out_sep" \
      "$language" "$out_sep" \
      "$channels" "$out_sep" \
      "$title" "$out_sep" \
      "$codec" "$out_sep" \
      "$handler_name" "$out_sep" \
      "$disp"
    ordinal=$((ordinal + 1))
  done <<< "$raw"
}

# ── Language detection helpers (Plan C) ─────────────────────────────────────
lang_container_supported() {
  local path="${1:-}"
  local ext="${path##*.}"
  ext="${ext,,}"
  case "$ext" in
    mkv|mp4|m4v|mov) return 0 ;;
    *) return 1 ;;
  esac
}

#
# Candidate selection over probe_audio_streams. A candidate is an audio
# stream whose NORMALIZED language tag (normalize_audio_language_tag,
# matching worker.sh:568 — junk like "stereo"/"default" -> und) is one
# of the untagged classes {und,zxx,mis,mul,qaa} AND is NOT commentary
# (is_commentary_track, Plan A). Sorted most-channels-first, tiebreak
# lowest ordinal. Emits "ordinal\tchannels\tnorm_lang" per candidate.
#   $1: filepath (probed via probe_audio_streams)
lang_pick_candidates() {
  local filepath="$1"
  local US=$'\037'
  local ordinal language channels title codec handler disp
  # Buffer candidate rows, then sort. sort -k2,2nr -k1,1n =
  # channels DESC, ordinal ASC.
  local buf=""
  while IFS="$US" read -r ordinal language channels title codec handler disp; do
    [ -n "$ordinal" ] || continue
    local norm
    norm=$(normalize_audio_language_tag "$language")
    case "$norm" in
      und|zxx|mis|mul|qaa) ;;
      *) continue ;;
    esac
    if is_commentary_track "$title" "$handler" "$disp"; then
      continue
    fi
    # Carry und as the normalized label for all untagged classes; the
    # detector overwrites with the real code. channels defaults 0.
    buf+="${ordinal}	${channels:-0}	und"$'\n'
  done < <(probe_audio_streams "$filepath")
  [ -n "$buf" ] || return 0
  printf '%s' "$buf" | sort -t$'\t' -k2,2nr -k1,1n
}

_lang_int_or_default() {
  local value="${1:-}" default="${2:-0}"
  [[ "$value" =~ ^[0-9]+$ ]] && { printf '%s' "$value"; return 0; }
  printf '%s' "$default"
}

_lang_ceil_div() {
  local n="$1" d="$2"
  [ "$d" -gt 0 ] || d=1
  echo $(( (n + d - 1) / d ))
}

# Build deterministic language-verifier sample sections.
# Output:
#   section_id<TAB>start_sec<TAB>duration_sec<TAB>band_start<TAB>band_end
lang_build_sample_table() {
  local dur coverage hs ts target_section min_section max_section min_total max_total deep_max
  dur=$(_lang_int_or_default "${1:-0}" 0)
  coverage=$(_lang_int_or_default "${2:-${TRANSCODARR_LANGUAGE_SAMPLE_COVERAGE_PCT:-10}}" 10)
  hs=$(_lang_int_or_default "${3:-${TRANSCODARR_LANGUAGE_HEAD_SKIP:-60}}" 60)
  ts=$(_lang_int_or_default "${4:-${TRANSCODARR_LANGUAGE_TAIL_SKIP:-60}}" 60)
  target_section=$(_lang_int_or_default "${5:-60}" 60)
  min_section=$(_lang_int_or_default "${6:-20}" 20)
  max_section=$(_lang_int_or_default "${7:-90}" 90)
  min_total=$(_lang_int_or_default "${8:-90}" 90)
  max_total=$(_lang_int_or_default "${9:-900}" 900)
  deep_max=$(_lang_int_or_default "${10:-1800}" 1800)

  [ "$coverage" -gt 0 ] || coverage=10
  [ "$target_section" -gt 0 ] || target_section=60
  [ "$min_section" -gt 0 ] || min_section=20
  [ "$max_section" -ge "$min_section" ] || max_section="$min_section"

  if [ "$dur" -le 0 ]; then
    printf '0\t0\t%d\t0\t%d\n' "$target_section" "$target_section"
    return 0
  fi

  local usable=$(( dur - hs - ts ))
  if [ "$usable" -lt "$min_total" ]; then
    hs=$(( dur * 3 / 100 )); [ "$hs" -ge 1 ] || hs=1
    ts=$(( dur * 3 / 100 )); [ "$ts" -ge 1 ] || ts=1
    usable=$(( dur - hs - ts ))
    if [ "$usable" -lt 1 ]; then
      hs=0; ts=0; usable="$dur"
    fi
  fi
  [ "$usable" -gt 0 ] || usable="$dur"

  local cap="$max_total"
  [ "$coverage" -ge 20 ] && cap="$deep_max"

  local target_total
  target_total=$(_lang_ceil_div "$(( usable * coverage ))" 100)
  [ "$target_total" -ge "$min_total" ] || target_total="$min_total"
  [ "$target_total" -le "$cap" ] || target_total="$cap"
  [ "$target_total" -le "$usable" ] || target_total="$usable"
  [ "$target_total" -gt 0 ] || target_total=1

  local section_count section_len
  section_count=$(_lang_ceil_div "$target_total" "$target_section")
  [ "$section_count" -gt 0 ] || section_count=1
  section_len=$(_lang_ceil_div "$target_total" "$section_count")
  [ "$section_len" -ge "$min_section" ] || section_len="$min_section"
  [ "$section_len" -le "$max_section" ] || section_len="$max_section"
  [ "$section_len" -le "$usable" ] || section_len="$usable"
  [ "$section_len" -gt 0 ] || section_len=1

  local end_limit=$(( dur - ts ))
  [ "$end_limit" -gt "$hs" ] || { hs=0; end_limit="$dur"; }

  local i band_start band_end center start
  for (( i=0; i<section_count; i++ )); do
    band_start=$(( hs + (usable * i) / section_count ))
    band_end=$(( hs + (usable * (i + 1)) / section_count ))
    center=$(( (band_start + band_end) / 2 ))
    start=$(( center - section_len / 2 ))
    [ "$start" -ge "$hs" ] || start="$hs"
    if [ $(( start + section_len )) -gt "$end_limit" ]; then
      start=$(( end_limit - section_len ))
    fi
    [ "$start" -ge 0 ] || start=0
    printf '%d\t%d\t%d\t%d\t%d\n' "$i" "$start" "$section_len" "$band_start" "$band_end"
  done
}

# Parse whisper.cpp -dl output. The detected-language line is pinned to
# the built version (Plan B): "auto-detected language: <code> (p = <0.NN>)".
# Echoes "<norm_code> <prob>" (code normalized via normalize_audio_language_tag,
# e.g. es->spa, en->eng) on a match; returns 1 with no output otherwise.
#   $1: whisper-cli stdout/stderr string
lang_parse_whisper_detect() {
  local out="$1"
  local line
  line=$(printf '%s\n' "$out" | grep -oE 'auto-detected language: [a-z]{2,3} \(p = [0-9.]+\)' | head -1) || true
  [ -n "$line" ] || return 1
  local code prob
  code=$(printf '%s' "$line" | sed -E 's/^auto-detected language: ([a-z]{2,3}) .*/\1/')
  # Normalize the raw whisper code to Transcodarr's tag vocabulary (es->spa,
  # en->eng, ja->jpn, ...) so verdict/action match the worker's
  # wrong_lang_<norm> convention (worker.sh:632).
  code=$(normalize_audio_language_tag "$code")
  prob=$(printf '%s' "$line" | sed -E 's/.*\(p = ([0-9.]+)\)$/\1/')
  [ -n "$code" ] && [ -n "$prob" ] || return 1
  printf '%s %s' "$code" "$prob"
}

# Per-candidate aggregation (spec §5 step 4). Reads stdin: one "code prob"
# pair per line (already filtered to successful parses — error samples are
# not fed in). Weak language-ID guesses from music/silence can drag a real
# speech sample below the verifier threshold; when at least one sample is
# plausibly confident, ignore samples below TRANSCODARR_LANGUAGE_SAMPLE_MIN_PROB
# (default 0.50) before majority/median. Among the remaining samples,
# majority-votes on code; among samples that voted for the winning code,
# returns the MEDIAN probability. Echoes "<code> <median>".
# Empty input -> empty output + nonzero return.
lang_aggregate_candidate() {
  local -a codes=() probs=()
  local c p
  while read -r c p; do
    [ -n "$c" ] || continue
    codes+=("$c"); probs+=("$p")
  done
  [ "${#codes[@]}" -gt 0 ] || return 1

  local sample_floor="${TRANSCODARR_LANGUAGE_SAMPLE_MIN_PROB:-0.50}"
  [[ "$sample_floor" =~ ^[0-9]+([.][0-9]+)?$ ]] || sample_floor="0.50"
  local -a confident_codes=() confident_probs=()
  local i
  for i in "${!codes[@]}"; do
    if awk -v p="${probs[$i]:-0}" -v f="$sample_floor" 'BEGIN{exit ((p+0) >= (f+0)) ? 0 : 1}'; then
      confident_codes+=("${codes[$i]}")
      confident_probs+=("${probs[$i]}")
    fi
  done
  if [ "${#confident_codes[@]}" -gt 0 ]; then
    codes=("${confident_codes[@]}")
    probs=("${confident_probs[@]}")
  fi

  local code
  code=$(printf '%s\n' "${codes[@]}" | sort | uniq -c | sort -rn | awk 'NR==1{print $2}')
  local -a winp=()
  local k
  for k in "${!codes[@]}"; do
    [ "${codes[$k]}" = "$code" ] && winp+=("${probs[$k]}")
  done
  local median
  median=$(printf '%s\n' "${winp[@]}" | sort -n | awk '{a[NR]=$1} END{ if(NR==0){print 0} else if(NR%2){print a[(NR+1)/2]} else {print (a[NR/2]+a[NR/2+1])/2} }')
  printf '%s %s' "$code" "$median"
}

# Verdict over ALL candidates with one min_confidence. Reads stdin lines
# "ordinal\037code\037median_prob" (\037 = ASCII Unit Separator; one per
# candidate; code may be empty for all-error candidates). Echoes one of:
#   english <ordinal>          any candidate code==eng, median >= conf
#   foreign <code> <ordinal>   EVERY candidate is non-eng with median >= conf (primary = first line = most channels)
#   undetected                 any other mix
#   $1: min_confidence (float)
lang_verdict_over_candidates() {
  local conf="$1"
  local first_ordinal="" first_code="" any=0
  local all_foreign=1
  local eng_ordinal=""
  local ordinal code prob ge
  while IFS=$'\037' read -r ordinal code prob; do
    [ -n "$ordinal" ] || continue
    any=1
    [ -z "$first_ordinal" ] && { first_ordinal="$ordinal"; first_code="$code"; }
    # ge=1 when prob >= conf (awk handles the float compare).
    ge=$(awk -v p="${prob:-0}" -v c="$conf" 'BEGIN{print (p+0 >= c+0) ? 1 : 0}')
    if [ "$code" = "eng" ] && [ "$ge" = "1" ]; then
      [ -z "$eng_ordinal" ] && eng_ordinal="$ordinal"
    fi
    # A candidate counts toward "all foreign" only if it is non-eng AND
    # confidently detected. Any eng candidate, OR any below-conf/unknown
    # candidate, breaks the all-foreign condition.
    if [ "$code" = "eng" ] || [ "$ge" != "1" ] || [ -z "$code" ]; then
      all_foreign=0
    fi
  done
  [ "$any" = "1" ] || { echo "undetected"; return 0; }
  if [ -n "$eng_ordinal" ]; then
    echo "english $eng_ordinal"
    return 0
  fi
  if [ "$all_foreign" = "1" ]; then
    echo "foreign $first_code $first_ordinal"
    return 0
  fi
  echo "undetected"
}

_lang_language_release_available() {
  if [ "${TRANSCODARR_LANGUAGE_ENABLED:-false}" != "true" ]; then
    return 1
  fi
  # The capability probe runs in the background at boot; wait (bounded) for
  # it to record a usable lang_backend before releasing anything, else a
  # fresh boot would release 0 (Valkey is non-persistent). The bound is
  # 30s by default; tests override via TRANSCODARR_LANGUAGE_BACKEND_WAIT=0
  # to assert the no-backend path without sleeping.
  local _wait_max="${TRANSCODARR_LANGUAGE_BACKEND_WAIT:-30}"
  local _waited=0
  while [ -z "$($QUEUE_CLI HGET tc:capabilities lang_backend 2>/dev/null)" ] && [ "$_waited" -lt "$_wait_max" ]; do
    sleep 1; _waited=$(( _waited + 1 ))
  done
  if [ -z "$($QUEUE_CLI HGET tc:capabilities lang_backend 2>/dev/null)" ]; then
    return 1
  fi
  local backend
  backend=$($QUEUE_CLI HGET tc:capabilities lang_backend 2>/dev/null) || backend=""
  if [ -z "$backend" ]; then
    return 1
  fi
  return 0
}

_lang_release_failed_language_rows() {
  local allowed_reasons="|$1|"
  local container_mode="${2:-mkv}"
  local state_dir="${TRANSCODARR_STATE_DIR:-${STATE_DIR:-/state}}"
  local tsv="${state_dir}/failed-files.tsv"
  # Enabled/backend gate — shared by startup AND /api/lang/rescan so a
  # disabled or unavailable feature can never release .jobs back into the
  # pipeline where they would not divert.
  if ! _lang_language_release_available; then
    echo 0; return 0
  fi
  [ -f "$tsv" ] || { echo 0; return 0; }

  local released=0
  local ts svc reason path rest disk_name disk_read_path row row_us
  # Column 4 is the path; reason is column 3. Bash treats tab as IFS
  # whitespace and collapses empty fields, so translate to a non-whitespace
  # delimiter before read.
  while IFS= read -r row; do
    row_us="${row//$'\t'/$'\037'}"
    IFS=$'\037' read -r ts svc reason path rest <<< "$row_us"
    case "$allowed_reasons" in
      *"|${reason}|"*) ;;
      *) continue ;;
    esac
    case "$container_mode" in
      supported) lang_container_supported "$path" || continue ;;
      *) case "$path" in *.mkv) ;; *) continue ;; esac ;;
    esac
    disk_name=$(resolve_disk "$path") || disk_name=""
    if [ -n "$disk_name" ]; then
      case "$path" in
        /movies/*) disk_read_path="/${disk_name}/Movies/${path#/movies/}" ;;
        /tv/*)     disk_read_path="/${disk_name}/TV/${path#/tv/}" ;;
        *)         disk_read_path="$path" ;;
      esac
    else
      disk_read_path="$path"
    fi
    release_admission "$path" "$disk_read_path"
    # Write a priority .job (same naming as tc-queue-job.sh).
    local job_svc="$svc" hash ns job_path
    case "$job_svc" in radarr|sonarr) ;; *) job_svc="sonarr" ;; esac
    case "$path" in /movies/*) job_svc="radarr" ;; /tv/*) job_svc="sonarr" ;; esac
    hash=$(printf '%s' "$path" | md5sum | cut -d' ' -f1)
    ns=$(date +%s%N)
    job_path="${TRANSCODARR_QUEUE_DIR:-/queue}/${hash}_${ns}.job"
    printf '%s\n%s\n' "$job_svc" "$path" > "$job_path" 2>/dev/null || true
    released=$((released + 1))
  done < "$tsv"
  echo "$released"
}

_lang_release_flagged_unverified_language_rows() {
  local state_dir="${TRANSCODARR_STATE_DIR:-${STATE_DIR:-/state}}"
  local tsv="${state_dir}/flagged-files.tsv"
  if ! _lang_language_release_available; then
    echo 0; return 0
  fi
  [ -f "$tsv" ] || { echo 0; return 0; }

  local released=0
  local ts svc reason path detail disk_name disk_read_path row row_us
  while IFS= read -r row; do
    row_us="${row//$'\t'/$'\037'}"
    IFS=$'\037' read -r ts svc reason path detail <<< "$row_us"
    [ "$reason" = "unverified_lang" ] || continue
    lang_container_supported "$path" || continue
    disk_name=$(resolve_disk "$path") || disk_name=""
    if [ -n "$disk_name" ]; then
      case "$path" in
        /movies/*) disk_read_path="/${disk_name}/Movies/${path#/movies/}" ;;
        /tv/*)     disk_read_path="/${disk_name}/TV/${path#/tv/}" ;;
        *)         disk_read_path="$path" ;;
      esac
    else
      disk_read_path="$path"
    fi
    [ -f "$disk_read_path" ] || continue
    release_admission "$path" "$disk_read_path"
    # Write a priority .job (same naming as tc-queue-job.sh).
    local job_svc="$svc" hash ns job_path
    case "$job_svc" in radarr|sonarr) ;; *) job_svc="sonarr" ;; esac
    case "$path" in /movies/*) job_svc="radarr" ;; /tv/*) job_svc="sonarr" ;; esac
    hash=$(printf '%s' "$path" | md5sum | cut -d' ' -f1)
    ns=$(date +%s%N)
    job_path="${TRANSCODARR_QUEUE_DIR:-/queue}/${hash}_${ns}.job"
    printf '%s\n%s\n' "$job_svc" "$path" > "$job_path" 2>/dev/null || true
    released=$((released + 1))
  done < "$tsv"
  echo "$released"
}

# Existing-backlog startup backfill (spec §8.1). Capability-gated,
# MKV-only, admission-only: only no_eng_audio rows are released
# automatically. Rows that already reached a terminal detection verdict
# are not auto-released on every restart.
lang_backfill_no_eng_audio() {
  _lang_release_failed_language_rows "no_eng_audio"
}

# Manual language rescan from the GUI/API. This is broader than startup:
# after changing model/device/settings, an operator should be able to retry
# language-detection failures that previously ended as unknown/tag errors,
# plus visible unverified_lang flags. Foreign verdicts stay terminal unless
# the source is otherwise changed.
lang_rescan_language_failures() {
  local failed flagged
  if ! _lang_language_release_available; then
    echo 0; return 0
  fi
  failed=$(_lang_release_failed_language_rows "no_eng_audio|lang_undetected|lang_tag_failed|lang_requeue_failed" "supported")
  flagged=$(_lang_release_flagged_unverified_language_rows)
  echo $(( ${failed:-0} + ${flagged:-0} ))
}

# ── Fingerprint / dedup ────────────────────────────────────────────────────

fingerprint() {
  local f="$1"
  if [ -f "$f" ]; then
    local sz ino
    sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
    ino=$(stat -c%i "$f" 2>/dev/null || echo 0)
    echo "${sz}:${ino}"
  else
    echo "0:0"
  fi
}

is_processed() {
  local f="$1" fp="$2"
  local db="${TRANSCODARR_STATE_DIR:-/state}/processed.tsv"
  grep -qF "${f}	${fp}	" "$db" 2>/dev/null
}

# ── classify_file_probe — pure probe+classify, no side effects ─────────────
#
# Probes a file with ffprobe and classifies based on codec, resolution,
# channels, tracks, and language. Returns a single result line to stdout.
# Does NOT write to queue files, check processed.tsv, or touch shared state.
#
# Args: filepath service arr_id
# Stdout: filepath<TAB>service<TAB>arr_id<TAB>result<TAB>vcodec<TAB>ach<TAB>acount<TAB>verdict
#   where result is: gpu, cpu, or skip
#   and verdict is: verified:aac_lc (when audio probe proved LC) or none
#
# verdict is independent of result. A file with result=cpu (because the
# resolution exceeds max) can still emit verdict=verified:aac_lc if its
# audio probe confirmed LC — the cache row records the AAC-LC fact,
# while the entrypoint's whole-skip gate is responsible for deciding
# whether that fact is sufficient to bypass the classifier (per the
# Phase 1 design: only when queue reasons == "aac_profile_unknown").

classify_file_probe() {
  local filepath="$1" service="${2:-radarr}" arr_id="${3:-0}"
  # Optional 4th arg: I/O path used for every ffprobe + stat. When
  # non-empty, all probing reads from this path; identity (the path in
  # the emitted result line, used as the downstream cache key) stays
  # on $filepath. Lets the caller route I/O directly to /diskN/...
  # while keeping the canonical library path (/movies/..., /tv/...)
  # for consumers. Phase 7.
  local read_path="${4:-}"
  local io_path="${read_path:-$filepath}"

  [ -f "$io_path" ] || return 0

  # Probe streams.
  # NOTE: ffprobe `-show_entries stream=A,B,C` does NOT guarantee CSV
  # column order matches the arg list — it uses ffprobe's internal
  # field-display order. The historical 3-column probe
  # (codec_type,codec_name,channels) worked because the parser reads
  # by-position and the empirical column order was stable for those
  # three fields. Adding pix_fmt + field_order to the same call broke
  # the position mapping, so they're back to dedicated probes below.
  local probe
  probe=$(ffprobe -v quiet -show_entries stream=codec_type,codec_name,channels \
    -of csv=p=0 "$io_path" 2>/dev/null || true)

  if [ -z "$probe" ]; then
    echo "[queue] WARN: probe failed: $(basename "$filepath")" >&2
    return 0
  fi

  local vcodec acount ach acodec aprofile vwidth vheight alang alang_raw pix_fmt field_order
  vcodec=$(echo "$probe"      | awk -F',' '$2=="video"{print $1; exit}')
  acount=$(echo "$probe"      | awk -F',' 'BEGIN{n=0} $2=="audio"{n++} END{print n}')
  ach=$(echo "$probe"         | awk -F',' '$2=="audio"{print $3; exit}')
  acodec=$(echo "$probe"      | awk -F',' '$2=="audio"{print $1; exit}')

  # Dedicated per-field probes so the parser is by-name, not by-position.
  # +2 ffprobe round-trips vs the broken merged probe; cheap on direct
  # disk post-Phase 7.
  pix_fmt=$(ffprobe -v quiet -select_streams v:0 -show_entries stream=pix_fmt \
    -of default=noprint_wrappers=1:nokey=1 "$io_path" 2>/dev/null | sed -n '1p' || true)
  field_order=$(ffprobe -v quiet -select_streams v:0 -show_entries stream=field_order \
    -of default=noprint_wrappers=1:nokey=1 "$io_path" 2>/dev/null | sed -n '1p' || true)

  read -r vwidth vheight < <(probe_video_dimensions "$io_path")
  vwidth="${vwidth:-0}"
  vheight="${vheight:-0}"

  # Capture both raw (for `tag=<raw>` flag detail) and normalized (for
  # the classifier's language-mismatch logic below) language values.
  alang_raw=$(ffprobe -v quiet -select_streams a:0 -show_entries stream_tags=language \
    -of default=noprint_wrappers=1:nokey=1 "$io_path" 2>/dev/null | sed -n '1p' || true)
  alang_raw=$(echo "$alang_raw" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')
  alang=$(normalize_audio_language_tag "$alang_raw")

  aprofile=$(ffprobe -v quiet -select_streams a:0 -show_entries stream=profile \
    -of default=noprint_wrappers=1:nokey=1 "$io_path" 2>/dev/null | sed -n '1p' || true)

  # One new ffprobe round-trip for duration; cheap on direct disk
  # post-Phase 7. Truncated to integer seconds to match the flag
  # detector's arithmetic.
  local duration_sec file_size_bytes
  duration_sec=$(ffprobe -v quiet -show_entries format=duration \
    -of default=noprint_wrappers=1:nokey=1 "$io_path" 2>/dev/null | sed -n '1p' \
    | awk '{printf "%.0f", $1 + 0}')
  duration_sec="${duration_sec:-0}"
  file_size_bytes=$(stat -c%s "$io_path" 2>/dev/null || echo 0)

  [ -z "$vcodec" ] && return 0

  # ── Classification ──
  local needs_gpu=false needs_cpu=false
  local vcodec_lower
  vcodec_lower=$(echo "$vcodec" | tr '[:upper:]' '[:lower:]')

  # Video codec
  case "$vcodec_lower" in
    h264|x264|avc|h.264) ;;
    *) needs_gpu=true ;;
  esac

  # Resolution
  local max_width="${TRANSCODARR_MAX_WIDTH:-1920}"
  local max_height="${TRANSCODARR_MAX_HEIGHT:-1080}"
  if [ "${vwidth:-0}" -gt "$max_width" ] 2>/dev/null || [ "${vheight:-0}" -gt "$max_height" ] 2>/dev/null; then
    needs_gpu=true
  fi

  # Audio passthrough compatibility.
  local target_codec="${TRANSCODARR_AUDIO_CODEC:-aac}"
  local acodec_lower
  acodec_lower=$(echo "${acodec:-}" | tr '[:upper:]' '[:lower:]')
  if [ -n "$acodec_lower" ] && ! audio_passthrough_ok "$target_codec" "$acodec_lower" "$aprofile"; then
    needs_cpu=true
  fi

  # Audio channels
  local max_channels="${TRANSCODARR_MAX_CHANNELS:-6}"
  if [ "${ach:-0}" -gt "$max_channels" ] 2>/dev/null; then
    needs_cpu=true
  fi

  # Multiple audio tracks
  if [ "${acount:-1}" -gt 1 ] 2>/dev/null; then
    needs_cpu=true
  fi

  # Wrong audio language
  local audio_lang
  audio_lang=$(normalize_audio_language_tag "${TRANSCODARR_AUDIO_LANG:-eng}")
  case "$alang" in
    ""|und|unknown) ;;
    *)      [ "$alang" = "$audio_lang" ] || needs_cpu=true ;;
  esac

  # Determine result
  local result="skip"
  if [ "$needs_gpu" = true ]; then
    result="gpu"
  elif [ "$needs_cpu" = true ]; then
    result="cpu"
  fi

  # Verdict — independent of result. Emitted when the audio probe
  # proved AAC-LC. Whitespace-trimmed + lowercased profile check
  # mirrors audio_passthrough_ok's policy so the two agree on what
  # "LC" means.
  local verdict="none"
  if [ "$acodec_lower" = "aac" ]; then
    local aprofile_norm
    aprofile_norm=$(printf '%s' "$aprofile" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')
    if [ "$aprofile_norm" = "lc" ]; then
      verdict="verified:aac_lc"
    fi
  fi

  # Emit result line — 15 fields per the Phase 7-followup contract.
  # The first 8 fields preserve the historical
  # contract; downstream readers that only bind 8 names will silently
  # drop the trailing 7 with no behavior change.
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$filepath" "$service" "$arr_id" "$result" \
    "$vcodec" "${ach:-0}" "${acount:-1}" "$verdict" \
    "${pix_fmt:-}" "${field_order:-}" \
    "${duration_sec:-0}" "${file_size_bytes:-0}" \
    "${alang_raw:-}" "${vwidth:-0}" "${vheight:-0}"
}

# ── classify_file — thin wrapper for single-file callers ───────────────────
#
# Checks fingerprint/processed, calls classify_file_probe for classification,
# then appends to queue files. Echoes "gpu", "cpu", or "ok" to stdout.
#
# Used by: import watcher, test mode filesystem scan.
# The parallel pipeline calls classify_file_probe() directly.
#
# Args: filepath [service] [arr_id]

classify_file() {
  local filepath="$1" service="${2:-radarr}" arr_id="${3:-0}"
  local state_dir="${TRANSCODARR_STATE_DIR:-/state}"
  local gpu_queue="$state_dir/queue-gpu.txt"
  local cpu_queue="$state_dir/queue-cpu.txt"

  [ -f "$filepath" ] || return

  local fp
  fp=$(fingerprint "$filepath")
  if is_processed "$filepath" "$fp"; then
    return
  fi

  # Probe and classify (pure function, no side effects)
  local result_line
  result_line=$(classify_file_probe "$filepath" "$service" "$arr_id")
  [ -z "$result_line" ] && return

  # Parse result line: filepath<TAB>service<TAB>arr_id<TAB>result<TAB>vcodec<TAB>ach<TAB>acount<TAB>verdict
  # _verdict is discarded — single-file callers don't need the AAC-LC cache
  # marker (entrypoint pipeline handles verified_hash_record directly).
  local _fp _svc _aid result vcodec ach acount _verdict
  local _result_read_line="${result_line//$'\t'/$'\037'}"
  IFS=$'\037' read -r _fp _svc _aid result vcodec ach acount _verdict <<< "$_result_read_line"

  # Commit to queue files and echo result for callers
  case "$result" in
    gpu)
      printf '%s\t%s\tgpu\t%s\t%sch×%s\t%s\n' \
        "$service" "$filepath" "$vcodec" "${ach:-?}" "${acount:-1}" "$arr_id" >> "$gpu_queue"
      echo "gpu"
      ;;
    cpu)
      printf '%s\t%s\tcpu\t%s\t%sch×%s\t%s\n' \
        "$service" "$filepath" "$vcodec" "${ach:-?}" "${acount:-1}" "$arr_id" >> "$cpu_queue"
      echo "cpu"
      ;;
    *)
      # "skip" maps to "ok" for backward compatibility with callers
      echo "ok"
      ;;
  esac
}

# ── Encoder capability probe (shared — boot sweep + worker cache-miss) ──
# Runs a capability probe on (encoder, profile, pixfmt) with up to
# PROBE_N_ATTEMPTS (default 3) retries and exponential backoff (1s, 2s, 4s).
#
# Shared by both:
#   - the boot-time sweep in transcodarr-probe-capabilities.sh
#   - the worker's cache-miss inline fallback in transcodarr-worker.sh
#
# Same retry policy in both paths ensures a transient device-busy /
# driver-startup / GPU-contention failure cannot cache a permanent
# false-negative. Real hardware-capability failures fail fast on every
# attempt; transients typically recover by attempt 2-3.
#
# Args:
#   1: encoder (h264_nvenc / hevc_nvenc / av1_nvenc / h264_qsv / ...)
#   2: profile (high / main10 / main / ...)
#   3: pixfmt  (nv12 / p010le / yuv420p / ...)
#
# Returns: 0 if any attempt succeeded, 1 if all PROBE_N_ATTEMPTS failed.
# Logs progress to stderr via the caller's log() if defined, else
# echoes to stderr directly.
probe_encoder_runtime() {
  local enc=$1 profile=$2 pixfmt=$3
  local n_attempts=${PROBE_N_ATTEMPTS:-3}
  local probe_w=${PROBE_W:-160}
  local probe_h=${PROBE_H:-128}

  local _log_fn
  if declare -F log >/dev/null 2>&1; then
    _log_fn() { log "$@"; }
  else
    _log_fn() { echo "[probe] $*" >&2; }
  fi

  local attempt=1 delay=1
  while (( attempt <= n_attempts )); do
    if ffmpeg -hide_banner -loglevel error \
        -f lavfi -i "color=c=black:size=${probe_w}x${probe_h}:rate=10:duration=0.1,format=${pixfmt}" \
        -c:v "$enc" -profile:v "$profile" -pix_fmt "$pixfmt" -frames:v 1 \
        -f null - </dev/null >/dev/null 2>&1; then
      if (( attempt == 1 )); then
        _log_fn "probe: $enc ($profile/$pixfmt) → available"
      else
        _log_fn "probe: $enc ($profile/$pixfmt) → available (after $attempt attempts — transient recovered)"
      fi
      return 0
    fi
    if (( attempt < n_attempts )); then
      _log_fn "probe: $enc ($profile/$pixfmt) attempt $attempt failed, retrying in ${delay}s..."
      sleep "$delay"
      delay=$((delay * 2))
    fi
    attempt=$((attempt + 1))
  done
  _log_fn "probe: $enc ($profile/$pixfmt) → UNAVAILABLE after $n_attempts attempts"
  return 1
}

# ── HDR static metadata probe ─────────────────────────────────────────────
# Extracts SMPTE 2086 mastering-display + SMPTE 2094-40 Content Light Level
# side-data from the FIRST VIDEO FRAME of a source file. Used to forward
# HDR10 metadata into the re-encoded stream so Plex clients tone-map
# against the source's actual mastering luminance (instead of the 1000-nit
# default guess, which crushes highlights on OLED targeted at higher peaks).
#
# Output (stdout): two pipe-separated values, either "md|cll" or empty:
#   md  — ffmpeg master_display format:
#         G(x,y)B(x,y)R(x,y)WP(x,y)L(max,min)
#         with x/y in 1/50000 chromaticity units and L in 1/10000 cd/m²
#   cll — ffmpeg max_cll format: "maxCLL,maxFALL" (both in cd/m²)
#
# Both missing → echoes "|" (empty). Caller checks each half and
# conditionally emits encoder flags.
#
# Requires: ffprobe + jq. jq is installed in the Dockerfile.
probe_hdr_metadata() {
  local file=$1
  local probe md cll
  # `-read_intervals '%+#1'` restricts ffprobe to the first frame only;
  # HDR static metadata lives on the first IDR/keyframe. Prevents a
  # full-file scan which would be ~30s on a 4K Blu-ray remux.
  probe=$(ffprobe -v error -select_streams v:0 \
    -read_intervals '%+#1' \
    -show_frames \
    -of json "$file" 2>/dev/null) || { echo "|"; return 0; }

  md=$(echo "$probe" | jq -r '
    .frames[0].side_data_list[]?
    | select(.side_data_type == "Mastering display metadata")
    | "G(\(.green_x|split("/")[0]),\(.green_y|split("/")[0]))"
      + "B(\(.blue_x|split("/")[0]),\(.blue_y|split("/")[0]))"
      + "R(\(.red_x|split("/")[0]),\(.red_y|split("/")[0]))"
      + "WP(\(.white_point_x|split("/")[0]),\(.white_point_y|split("/")[0]))"
      + "L(\(.max_luminance|split("/")[0]),\(.min_luminance|split("/")[0]))"
  ' 2>/dev/null | head -1)

  cll=$(echo "$probe" | jq -r '
    .frames[0].side_data_list[]?
    | select(.side_data_type == "Content light level metadata")
    | "\(.max_content),\(.max_average)"
  ' 2>/dev/null | head -1)

  # Empty jq output becomes literal "null"; treat as unset.
  [ "$md" = "null" ] && md=""
  [ "$cll" = "null" ] && cll=""
  echo "${md}|${cll}"
}

# ── CUDA decode/filter fallback policy ────────────────────────────────────
cuda_decode_filter_fallback_reason() {
  local source_codec="${1:-}"
  local src_pix_fmt="${2:-}"
  local is_interlaced="${3:-false}"

  case "$source_codec" in
    mpeg1video|cinepak|svq1|svq3|rv10|rv20|rv30|rv40|wmv1|wmv2|flv1|h263)
      echo "codec $source_codec not supported by NVDEC"
      return 0
      ;;
  esac

  if [ "$source_codec" = "mpeg2video" ] && [ "$is_interlaced" = "true" ]; then
    echo "interlaced MPEG-2 DVD sources reinitialize the CUDA filter graph"
    return 0
  fi

  # 4:2:2 pixel formats are never supported by consumer NVDEC. 4:4:4 works
  # on some Turing+ HEVC paths but is unreliable in practice, so keep it CPU.
  case "$src_pix_fmt" in
    yuv422p|yuv422p10le|yuv422p12le|yuvj422p)
      echo "4:2:2 pixfmt $src_pix_fmt not supported by NVDEC"
      ;;
    yuv444p|yuv444p10le|yuv444p12le|yuvj444p)
      echo "4:4:4 pixfmt $src_pix_fmt unreliable on NVDEC"
      ;;
  esac
}

# ── Video filter chain builder ─────────────────────────────────────────────
# Constructs the -vf / -filter:v string for the worker's ffmpeg invocation.
# Handles deinterlace, HDR tonemap (libplacebo or zscale+hable), the
# per-backend scale/format-convert path, AND output color tagging via
# setparams. Emits the filter string on stdout. Informational log lines
# go to stderr via log() if defined by the caller.
#
# Color tagging is done at the filter level, not via encoder-level
# `-color_trc/-color_primaries/-colorspace/-color_range` flags — those
# trigger ffmpeg's auto-scaler insertion when source color metadata
# doesn't match target (typical for MPEG-2 with unknown/bt470bg tags),
# and auto_scaler is a SW filter that can't accept CUDA frames. Using
# setparams avoids the conversion-negotiation path entirely.
#
# Args:
#   1: video_width         (int)  source width from ffprobe
#   2: video_height        (int)  source height from ffprobe
#   3: max_width           (int)  configured max output width
#   4: max_height          (int)  configured max output height
#   5: target_pixfmt       (str)  nv12 | p010le | yuv420p | yuv420p10le ...
#   6: hw_decoding         (str)  cuda | qsv | none
#   7: need_tonemap        (str)  true | false
#   8: is_interlaced       (str)  true | false
#   9: src_field_order     (str)  optional, for log only (e.g. tt/bb)
#  10: src_color_transfer  (str)  optional, log + HDR preserve trc source
#  11: color_target        (str)  optional, one of:
#                                   "bt709"        — HD+/SDR (default)
#                                   "bt601"        — SD (≤576p)
#                                   "preserve-hdr" — keep BT.2020 + src trc
#                                   ""             — skip color tagging
#                                                    (test-harness raw mode)
build_scale_filter() {
  local VIDEO_WIDTH=$1 VIDEO_HEIGHT=$2
  local MAX_WIDTH=$3 MAX_HEIGHT=$4
  local PIXFMT=$5 HW_DECODING=$6
  local NEED_TONEMAP=$7 IS_INTERLACED=$8
  local SRC_FIELD_ORDER=${9:-}
  local SRC_COLOR_TRANSFER=${10:-}
  local COLOR_TARGET=${11:-}

  # Call the caller's log() if defined; silent otherwise (e.g. sourced standalone).
  local _bsf_log
  if declare -F log >/dev/null 2>&1; then
    _bsf_log() { log "$@"; }
  else
    _bsf_log() { :; }
  fi

  local -a FILTER_PARTS=()

  # Deinterlace (only if source was actually flagged interlaced)
  if [ "$IS_INTERLACED" = "true" ]; then
    case "$HW_DECODING" in
      cuda) FILTER_PARTS+=("bwdif_cuda=0:-1:0") ;;
      qsv)  FILTER_PARTS+=("vpp_qsv=deinterlace=2") ;;
      *)    FILTER_PARTS+=("bwdif=0:-1:0") ;;
    esac
    _bsf_log "Source interlaced (field_order=$SRC_FIELD_ORDER) — applying deinterlace"
  fi

  # Pre-compute even output dimensions only for libplacebo and scale_qsv
  # since neither supports force_divisible_by. scale_cuda handles even-dim
  # guarantee inline via force_divisible_by=2 (oversize) or iw-mod(iw,2)
  # (fit). CPU scale uses force_divisible_by=2 in its own expression.
  local _needs_even_hw_dims=false
  if [ "$NEED_TONEMAP" = "true" ]; then
    case "$HW_DECODING" in
      cuda|qsv) _needs_even_hw_dims=true ;;
    esac
  elif [ "$HW_DECODING" = "qsv" ]; then
    _needs_even_hw_dims=true
  fi

  local _out_w _out_h
  if [ "$_needs_even_hw_dims" = "true" ]; then
    local _ratio_w _ratio_h _ratio
    _ratio_w=$(awk -v iw="$VIDEO_WIDTH" -v mw="$MAX_WIDTH" 'BEGIN{r=mw/iw; if(r>1)r=1; printf "%.6f", r}')
    _ratio_h=$(awk -v ih="$VIDEO_HEIGHT" -v mh="$MAX_HEIGHT" 'BEGIN{r=mh/ih; if(r>1)r=1; printf "%.6f", r}')
    _ratio=$(awk -v w="$_ratio_w" -v h="$_ratio_h" 'BEGIN{printf "%.6f", (w<h?w:h)}')
    _out_w=$(awk -v iw="$VIDEO_WIDTH" -v r="$_ratio" 'BEGIN{printf "%d", int(iw*r/2)*2}')
    _out_h=$(awk -v ih="$VIDEO_HEIGHT" -v r="$_ratio" 'BEGIN{printf "%d", int(ih*r/2)*2}')
  fi

  if [ "$NEED_TONEMAP" = "true" ]; then
    # HDR tonemap path is chosen by the startup capability probe
    # (transcodarr-probe-capabilities.sh writes tc:capabilities[hdr_tonemap_path]).
    # Worker exports TRANSCODARR_HDR_TONEMAP_PATH; tests / standalone callers
    # default to libplacebo for backward compatibility.
    local _hdr_path="${TRANSCODARR_HDR_TONEMAP_PATH:-libplacebo}"

    case "$HW_DECODING" in
      cuda|qsv)
        # Bridge HW surface to system memory regardless of which tonemap
        # path follows — both libplacebo and opencl/zscale need CPU
        # frames as the input format for the next stage.
        FILTER_PARTS+=("hwdownload" "format=p010le")
        case "$_hdr_path" in
          libplacebo)
            # GPU tonemap via libplacebo. Internally uses Vulkan compute
            # for the actual tonemap. Single filter handles tonemap + scale
            # + format.
            #
            # Phase 6C-followup 2A: dynamic-HDR-aware tonemap routing.
            # The worker's 6C guard sets TRANSCODARR_DYN_HDR_KIND to one
            # of "hdr10plus" / "dolby_vision" / "vivid" / "" (empty).
            # For HDR10+ sources, switch tonemapping=spline to
            # tonemapping=st2094-40 — libplacebo applies the per-frame
            # HDR10+ metadata internally when this mode is selected
            # (FFmpeg libplacebo docs; NO tone_map_metadata option —
            # some ffmpeg builds reject that flag as "Option not found").
            #
            # For static HDR10 (no kind set) and any kind we haven't
            # explicitly wired yet (dolby_vision, vivid — those still
            # fail-closed in the worker guard), keep tonemapping=spline
            # (MPV's default for static-HDR tonemap).
            local _tonemapping="spline"
            case "${TRANSCODARR_DYN_HDR_KIND:-}" in
              hdr10plus) _tonemapping="st2094-40" ;;
            esac
            FILTER_PARTS+=("libplacebo=w=${_out_w}:h=${_out_h}:tonemapping=${_tonemapping}:colorspace=bt709:color_trc=bt709:color_primaries=bt709:format=$PIXFMT")
            _bsf_log "HDR detected (transfer=$SRC_COLOR_TRANSFER) — GPU tonemap via libplacebo (${_tonemapping} → bt709${TRANSCODARR_DYN_HDR_KIND:+, kind=$TRANSCODARR_DYN_HDR_KIND})"
            ;;
          opencl)
            # GPU tonemap via OpenCL. Worker must pass `-init_hw_device
            # opencl=ocl -filter_hw_device ocl` so hwupload here knows
            # which device to upload to. tonemap_opencl does the actual
            # HDR-to-SDR pass; scale runs separately on CPU (cheap once
            # the frame is already SDR/nv12, and tonemap_opencl doesn't
            # take output dimensions). Operator matches the CPU branch
            # for visual consistency.
            FILTER_PARTS+=("hwupload" "tonemap_opencl=tonemap=mobius:format=nv12" "hwdownload" "format=nv12")
            FILTER_PARTS+=("scale=w='min(iw\,$MAX_WIDTH)':h='min(ih\,$MAX_HEIGHT)':force_original_aspect_ratio=decrease:force_divisible_by=2")
            _bsf_log "HDR detected (transfer=$SRC_COLOR_TRANSFER) — GPU tonemap via tonemap_opencl (mobius → bt709)"
            ;;
          *)
            # Last-resort CPU zscale chain after the hwdownload. See
            # operator/desat notes on the pure-CPU branch below.
            FILTER_PARTS+=("zscale=t=linear:npl=100" "format=gbrpf32le" "zscale=p=bt709" "tonemap=tonemap=mobius:desat=0.75" "zscale=t=bt709:m=bt709:r=tv" "format=yuv420p")
            FILTER_PARTS+=("scale=w='min(iw\,$MAX_WIDTH)':h='min(ih\,$MAX_HEIGHT)':force_original_aspect_ratio=decrease:force_divisible_by=2")
            _bsf_log "HDR detected (transfer=$SRC_COLOR_TRANSFER) — CPU tonemap via zscale+mobius after HW download (no GPU tonemap available)"
            ;;
        esac
        ;;
      *)
        # Pure-CPU decode path — CPU tonemap via zscale chain, then scale
        # separately to honor max dims with even divisibility.
        #
        # Operator was `hable` historically (HandBrake default); swapped
        # to `mobius` per 2026-04 audit — hable crushes highlights on
        # bright HDR10 content, mobius preserves more highlight detail
        # and handles neon/fluorescent sources better. desat bumped from
        # 0 → 0.75 (matches libplacebo/MPV default) to prevent neon
        # skin tones on overexposed HDR.
        FILTER_PARTS+=("zscale=t=linear:npl=100" "format=gbrpf32le" "zscale=p=bt709" "tonemap=tonemap=mobius:desat=0.75" "zscale=t=bt709:m=bt709:r=tv" "format=yuv420p")
        FILTER_PARTS+=("scale=w='min(iw\,$MAX_WIDTH)':h='min(ih\,$MAX_HEIGHT)':force_original_aspect_ratio=decrease:force_divisible_by=2")
        _bsf_log "HDR detected (transfer=$SRC_COLOR_TRANSFER) — CPU tonemap via zscale+mobius (desat=0.75)"
        ;;
    esac
  else
    # No tonemap — regular per-backend scale.
    # scale_qsv still needs passthrough=0 so format conversion runs even
    # when the computed output dimensions equal the input. scale_cuda is
    # split: real downscale uses the dynamic min()/aspect path; identity-
    # size inputs use format conversion only with iw-mod(iw,2) to round
    # odd dims even, avoiding the broken hw_frames_ctx path triggered by
    # force_original_aspect_ratio on no-op resizes.
    case "$HW_DECODING" in
      qsv)
        FILTER_PARTS+=("scale_qsv=w=${_out_w}:h=${_out_h}:mode=hq:format=$PIXFMT:passthrough=0")
        ;;
      cuda)
        if (( VIDEO_WIDTH > MAX_WIDTH )) || (( VIDEO_HEIGHT > MAX_HEIGHT )); then
          FILTER_PARTS+=("scale_cuda=w='min(iw\,$MAX_WIDTH)':h='min(ih\,$MAX_HEIGHT)':force_original_aspect_ratio=decrease:force_divisible_by=2:format=$PIXFMT")
        else
          FILTER_PARTS+=("scale_cuda=w='iw-mod(iw\,2)':h='ih-mod(ih\,2)':format=$PIXFMT")
        fi
        ;;
      *)
        FILTER_PARTS+=("scale=w='min(iw\,$MAX_WIDTH)':h='min(ih\,$MAX_HEIGHT)':force_original_aspect_ratio=decrease:force_divisible_by=2" "format=$PIXFMT")
        ;;
    esac
  fi

  # Output color tagging. See the header comment on why this is a filter
  # rather than encoder flags. setparams only writes metadata — no pixel
  # processing — and is safe on CUDA / QSV / SW frames alike.
  case "$COLOR_TARGET" in
    bt709)
      FILTER_PARTS+=("setparams=color_primaries=bt709:color_trc=bt709:colorspace=bt709:range=tv")
      ;;
    bt601)
      FILTER_PARTS+=("setparams=color_primaries=smpte170m:color_trc=smpte170m:colorspace=smpte170m:range=tv")
      ;;
    preserve-hdr)
      # IS_HDR gate upstream (case smpte2084|arib-std-b67) guarantees
      # SRC_COLOR_TRANSFER is set when PRESERVE_HDR is true, but fall
      # back to smpte2084 (PQ) defensively so we never emit invalid
      # `color_trc=` syntax if that invariant ever breaks.
      local _trc="${SRC_COLOR_TRANSFER:-smpte2084}"
      FILTER_PARTS+=("setparams=color_primaries=bt2020:color_trc=${_trc}:colorspace=bt2020nc:range=tv")
      ;;
    "")
      : # skip tagging (raw-mode, or caller handles it)
      ;;
    *)
      _bsf_log "WARN: unknown color_target='$COLOR_TARGET' — skipping setparams"
      ;;
  esac

  (IFS=,; echo "${FILTER_PARTS[*]}")
}
