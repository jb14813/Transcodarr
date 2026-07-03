#!/bin/bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# transcodarr-queue.sh — API intake (Stage 1)
#
# Queries Radarr/Sonarr APIs, pre-filters candidates, pushes to Valkey.
# ffprobe validation is handled by Stage 2 (ffprobe pool in entrypoint).
#
# Production: Radarr/Sonarr APIs → rough pre-filter → q_push tc:candidates:ready
# Test mode:  find /movies /tv → all media files → q_push tc:candidates:ready
# ─────────────────────────────────────────────────────────────────────────────

# ── Environment / defaults ──────────────────────────────────────────────────

STATE_DIR="${TRANSCODARR_STATE_DIR:-/state}"
TEST_MODE="${TRANSCODARR_TEST_MODE:-false}"
DB="$STATE_DIR/processed.tsv"
PROGRESS="$STATE_DIR/progress.txt"

RADARR_URL="${RADARR_URL:-}"
RADARR_API_KEY="${RADARR_API_KEY:-}"
SONARR_URL="${SONARR_URL:-}"
SONARR_API_KEY="${SONARR_API_KEY:-}"

# ── Setup ───────────────────────────────────────────────────────────────────

mkdir -p "$STATE_DIR"
touch "$DB"

# Shared helpers: fingerprint, is_processed, classify_file_probe, q_* Valkey functions
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/transcodarr-lib.sh"

# ── Helpers ─────────────────────────────────────────────────────────────────

log() { echo "[queue] $*" >&2; }

update_progress() {
  cat > "$PROGRESS" <<EOF
phase:       $1
status:      $2
updated:     $(date '+%Y-%m-%d %H:%M:%S')
EOF
}

# Phase 5C: batch size for queue-build push pipeline. Each pipeline
# subshell accumulates payloads in a BATCH array; when it reaches this
# size, _flush_push_batch issues one Lua EVAL that LPUSHes+INCRBYs
# atomically. A final flush after the read-loop drains the remainder.
# 500 keeps individual EVAL latency well under 100ms even for large
# payload sizes and stays far below Valkey's argv-count limits.
QUEUE_PUSH_BATCH_SIZE="${TRANSCODARR_PUSH_BATCH_SIZE:-500}"

# Internal API-row delimiter for Perl -> Bash pipes. Do not use tab here:
# Bash treats IFS whitespace as a run, which collapses empty middle fields
# like mediaInfo.audioLanguages and shifts resolution/audioCodec left.
QUEUE_ROW_IFS=$'\037'

# _flush_push_batch <label>
# Flushes the caller's BATCH array via the Lua helper, resets it to
# empty, and logs warnings on partial pushes or EVAL failures. BATCH is
# read/written by name — the function must be invoked from the same
# subshell that owns the array. <label> is a free-form tag for logs.
_flush_push_batch() {
  local label="$1"
  (( ${#BATCH[@]} > 0 )) || return 0
  local expected=${#BATCH[@]} accepted
  if accepted=$(q_push_batch_lua tc:candidates:ready tc:scan:pushed_total "${BATCH[@]}" 2>/dev/null); then
    if [ "$accepted" != "$expected" ]; then
      log "WARN: $label batch flush returned $accepted of $expected"
    fi
  else
    log "WARN: $label batch flush EVAL failed"
  fi
  BATCH=()
}

# fingerprint, is_processed, classify_file_probe — sourced from transcodarr-lib.sh

# ── Stage 1a: API source (production) ──────────────────────────────────────

# Shared Perl JSON parser — written to temp file, required by Perl invocations.
# /tmp is ephemeral inside the container (cleared on recreate), so a SIGTERM
# mid-run doesn't leave an orphan on the /state bind mount. EXIT trap is a
# second safety net so the file goes away even on set -e aborts.
init_perl_json() {
  PERL_JSON_LIB=$(mktemp /tmp/transcodarr-json-parser.XXXXXX.pl)
  chmod 600 "$PERL_JSON_LIB"
  trap 'rm -f "$PERL_JSON_LIB" 2>/dev/null' EXIT

  cat > "$PERL_JSON_LIB" << 'PERL_JSON'
use strict;
use warnings;

our $json;
our $pos;

sub init_json {
    local $/;
    $json = <STDIN>;
    $pos = 0;
}

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
            $s =~ s/\\n/\n/g;
            $s =~ s/\\t/\t/g;
            $s =~ s/\\r/\r/g;
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
    $pos++; # skip {
    skip_ws();
    my %obj;
    if (peek() ne '}') {
        while (1) {
            skip_ws();
            my $key = parse_string();
            skip_ws();
            die "expected colon at pos $pos" unless substr($json, $pos, 1) eq ':';
            $pos++;
            $obj{$key} = parse_value();
            skip_ws();
            last if peek() eq '}';
            die "expected comma or } at pos $pos" unless substr($json, $pos, 1) eq ',';
            $pos++;
        }
    }
    $pos++; # skip }
    return \%obj;
}

sub parse_array {
    $pos++; # skip [
    skip_ws();
    my @arr;
    if (peek() ne ']') {
        while (1) {
            push @arr, parse_value();
            skip_ws();
            last if peek() eq ']';
            die "expected comma or ] at pos $pos" unless substr($json, $pos, 1) eq ',';
            $pos++;
        }
    }
    $pos++; # skip ]
    return \@arr;
}

1; # module return
PERL_JSON
}

# Query Radarr API → push candidate paths to Valkey
query_radarr() {
  if [ -z "$RADARR_URL" ] || [ -z "$RADARR_API_KEY" ]; then
    log "Radarr not configured, skipping"
    return 0
  fi
  local radarr_url="${RADARR_URL%/}"
  if ! integration_url_allowed_shell "$radarr_url"; then
    log "WARN: Radarr URL invalid, skipping"
    return 0
  fi

  log "Querying Radarr API..."
  update_progress "queue" "querying radarr"

  set +e
  curl -sf --max-time 120 \
    -H "X-Api-Key: ${RADARR_API_KEY}" \
    -- "${radarr_url}/api/v3/movie" 2>/dev/null \
  | perl -e '
      require "'"$PERL_JSON_LIB"'";
      init_json();
      skip_ws();
      my $movies = parse_value();
      for my $m (@$movies) {
          next unless ref $m eq "HASH";
          my $mf = $m->{movieFile};
          next unless ref $mf eq "HASH";
          my $path = $mf->{path};
          next unless defined $path && $path ne "";
          my $mi = $mf->{mediaInfo};
          next unless ref $mi eq "HASH";
          my $vc = $mi->{videoCodec}       // "";
          my $ac = $mi->{audioChannels}    // 0;
          my $as = $mi->{audioStreamCount} // 1;
          my $acodec = $mi->{audioCodec}   // "";
          next if $vc eq "";
          my $mid = $m->{id} // 0;
          my $al = $mi->{audioLanguages} // "";
          my $vr = $mi->{resolution} // "";
          print join("\x1f", $path, $vc, $ac, $as, $mid, $al, $vr, $acodec) . "\n";
      }
    ' 2>&1 \
  | {
      # Phase 5C batched push: accumulate payloads, flush once per
      # QUEUE_PUSH_BATCH_SIZE rows via Lua EVAL. The brace-group at the
      # tail of the pipe is the subshell that owns BATCH; _flush_push_batch
      # reads/writes it by name from inside the same subshell.
      BATCH=()
      while IFS="$QUEUE_ROW_IFS" read -r path vcodec ach acount arr_id alang vres acodec; do
        # Centralized API pre-filter via build_queue_reasons (transcodarr-lib.sh).
        # Single source of truth shared with query_sonarr. Empty CSV → don't queue.
        # Non-empty CSV → 4-tuple payload preserves WHY this file was queued so
        # the verified-hash gate at admission can whole-skip only when the sole
        # reason is `aac_profile_unknown`.
        reasons_csv=$(build_queue_reasons "$vcodec" "$acodec" "$vres" "$ach" "$acount" "$alang")
        [ -z "$reasons_csv" ] && continue
        BATCH+=("radarr|$path|$arr_id|$reasons_csv")
        (( ${#BATCH[@]} >= QUEUE_PUSH_BATCH_SIZE )) && _flush_push_batch radarr
      done
      _flush_push_batch radarr
    }

  local _pipe=("${PIPESTATUS[@]}")
  set -e

  if [ "${_pipe[0]}" -ne 0 ]; then
    log "WARN: Radarr API request failed (curl exit ${_pipe[0]})"
  elif [ "${_pipe[1]}" -ne 0 ]; then
    log "WARN: Radarr JSON parse failed (perl exit ${_pipe[1]})"
  elif [ "${_pipe[2]:-0}" -ne 0 ]; then
    log "WARN: Radarr filter loop failed (exit ${_pipe[2]})"
  else
    log "Radarr complete"
  fi
}

# ── Sonarr fast bulk endpoint ──────────────────────────────────────────
# Tries `GET /api/v3/episodefile/transcodarr`, a Sonarr-side aggregator
# that returns ALL episodefiles in one call. In practice, the
# response shape carries every field this script consumed in the legacy
# per-series loop: top-level `path` + `seriesId`, plus
# `mediaInfo.{videoCodec,audioCodec,audioChannels,audioStreamCount,
# audioLanguages,resolution}`.
#
# `arr_id` MUST be `seriesId`, not the top-level `id` (which is the
# episode-file id) — the worker's Sonarr rescan command expects a
# series id.
#
# Return codes drive automatic fallback:
#   0 — endpoint succeeded (parse OK, payload pushed; empty result OK)
#   1 — endpoint failed (curl exit, JSON parse fail, or unconfigured)
# Caller (query_sonarr) treats non-zero as "fall back to legacy logic".
query_sonarr_fast() {
  if [ -z "$SONARR_URL" ] || [ -z "$SONARR_API_KEY" ]; then
    return 1
  fi
  local sonarr_url="${SONARR_URL%/}"
  if ! integration_url_allowed_shell "$sonarr_url"; then
    return 1
  fi

  log "Sonarr: querying fast episodefile endpoint"
  update_progress "queue" "sonarr fast endpoint"

  set +e
  local response
  response=$(curl -sf --max-time 120 \
    -H "X-Api-Key: ${SONARR_API_KEY}" \
    -- "${sonarr_url}/api/v3/episodefile/transcodarr" 2>/dev/null)
  local curl_exit=$?
  set -e

  if [ "$curl_exit" -ne 0 ]; then
    return 1
  fi

  # Parse + push in a single pipe. PIPESTATUS lets us check perl's exit
  # separately from the while-loop's (we care if perl failed; per-row
  # q_push failures should not roll back the fast-path decision).
  set +e
  printf '%s' "$response" \
  | perl -e '
      require "'"$PERL_JSON_LIB"'";
      init_json();
      skip_ws();
      # Treat empty body and non-array root as PARSE FAILURE (exit non-zero
      # so query_sonarr_fast returns 1 and the caller falls back to the
      # per-series legacy logic). A Sonarr/patch returning 200 with an
      # empty body or `{}` would otherwise silently push zero candidates
      # and never trigger fallback. An empty array `[]` is the legitimate
      # "empty library" case — handled by the for-loop doing nothing,
      # exit 0.
      my $c = peek();
      die "empty response body\n" if $c eq "";
      my $rows = parse_value();
      die "expected JSON array, got " . (ref($rows) || "scalar") . "\n" unless ref $rows eq "ARRAY";
      for my $r (@$rows) {
          next unless ref $r eq "HASH";
          my $path = $r->{path};
          next unless defined $path && $path ne "";
          my $sid = $r->{seriesId};
          next unless defined $sid && $sid ne "";
          my $mi = $r->{mediaInfo};
          next unless ref $mi eq "HASH";
          my $vc = $mi->{videoCodec}       // "";
          my $ac = $mi->{audioChannels}    // 0;
          my $as = $mi->{audioStreamCount} // 1;
          my $acodec = $mi->{audioCodec}   // "";
          next if $vc eq "";
          my $al = $mi->{audioLanguages} // "";
          my $vr = $mi->{resolution} // "";
          print join("\x1f", $path, $vc, $ac, $as, $sid, $al, $vr, $acodec) . "\n";
      }
    ' 2>/dev/null \
  | {
      # Phase 5C batched push (see query_radarr for the pattern).
      BATCH=()
      while IFS="$QUEUE_ROW_IFS" read -r path vcodec ach acount arr_id alang vres acodec; do
        reasons_csv=$(build_queue_reasons "$vcodec" "$acodec" "$vres" "$ach" "$acount" "$alang")
        [ -z "$reasons_csv" ] && continue
        BATCH+=("sonarr|$path|$arr_id|$reasons_csv")
        (( ${#BATCH[@]} >= QUEUE_PUSH_BATCH_SIZE )) && _flush_push_batch "sonarr fast"
      done
      _flush_push_batch "sonarr fast"
    }
  local _pipe=("${PIPESTATUS[@]}")
  set -e

  # _pipe = (printf, perl, while). printf is essentially always 0; the
  # while loop's exit isn't load-bearing. perl is the only failure mode
  # that should trigger fallback.
  if [ "${_pipe[1]:-0}" -ne 0 ]; then
    log "WARN: Sonarr fast endpoint JSON parse failed (perl exit ${_pipe[1]})"
    return 1
  fi

  log "Sonarr fast endpoint complete"
  return 0
}

# Query Sonarr API → push candidate paths to Valkey. Tries the fast
# aggregator endpoint first; falls back to the per-series loop if the
# fast endpoint is missing or fails (Sonarr versions without it return
# a 404 / curl exit 22).
query_sonarr() {
  if [ -z "$SONARR_URL" ] || [ -z "$SONARR_API_KEY" ]; then
    log "Sonarr not configured, skipping"
    return 0
  fi
  local sonarr_url="${SONARR_URL%/}"
  if ! integration_url_allowed_shell "$sonarr_url"; then
    log "WARN: Sonarr URL invalid, skipping"
    return 0
  fi

  # Fast path first — one HTTP call covers the whole library.
  if query_sonarr_fast; then
    return 0
  fi

  log "WARN: Sonarr fast endpoint unavailable; falling back to per-series API scan"

  log "Querying Sonarr API for series list..."
  update_progress "queue" "querying sonarr series"

  set +e
  local series_ids
  series_ids=$(curl -sf --max-time 60 \
    -H "X-Api-Key: ${SONARR_API_KEY}" \
    -- "${sonarr_url}/api/v3/series" 2>/dev/null \
  | perl -e '
      require "'"$PERL_JSON_LIB"'";
      init_json();
      skip_ws();
      my $series = parse_value();
      for my $s (@$series) {
          next unless ref $s eq "HASH" && defined $s->{id};
          print $s->{id} . "\n";
      }
    ' 2>/dev/null | sort -n) || true
  set -e

  if [ -z "$series_ids" ]; then
    log "WARN: Sonarr returned no series (API down or empty library)"
    return 0
  fi

  local total
  total=$(echo "$series_ids" | wc -l | tr -d ' ')
  local count=0

  log "Sonarr: querying episode files for $total series"

  for sid in $series_ids; do
    count=$((count + 1))
    if [ $((count % 50)) -eq 0 ]; then
      log "Sonarr: $count/$total series"
      update_progress "queue" "sonarr $count/$total series"
    fi

    set +e
    curl -sf --max-time 30 \
      -H "X-Api-Key: ${SONARR_API_KEY}" \
      -- "${sonarr_url}/api/v3/episodefile?seriesId=${sid}" 2>/dev/null \
    | perl -e '
        require "'"$PERL_JSON_LIB"'";
        init_json();
        skip_ws();
        my $c = peek();
        exit 0 if $c eq "";
        my $episodes = parse_value();
        exit 0 unless ref $episodes eq "ARRAY";
        for my $ep (@$episodes) {
            next unless ref $ep eq "HASH";
            my $path = $ep->{path};
            next unless defined $path && $path ne "";
            my $mi = $ep->{mediaInfo};
            next unless ref $mi eq "HASH";
            my $vc = $mi->{videoCodec}       // "";
            my $ac = $mi->{audioChannels}    // 0;
            my $as = $mi->{audioStreamCount} // 1;
            my $acodec = $mi->{audioCodec}   // "";
            next if $vc eq "";
            my $al = $mi->{audioLanguages} // "";
            my $vr = $mi->{resolution} // "";
            print join("\x1f", $path, $vc, $ac, $as, '"$sid"', $al, $vr, $acodec) . "\n";
        }
      ' 2>&1 \
    | {
        # Phase 5C batched push (per-series). Typical series has < 50
        # episodes so the final flush usually fires once. Still saves
        # the per-row valkey-cli fork cost.
        BATCH=()
        while IFS="$QUEUE_ROW_IFS" read -r path vcodec ach acount arr_id alang vres acodec; do
          # Centralized API pre-filter via build_queue_reasons (same helper as
          # query_radarr). Reasons CSV preserves WHY this candidate was queued
          # so the verified-hash gate can only whole-skip when the sole reason
          # is `aac_profile_unknown`.
          reasons_csv=$(build_queue_reasons "$vcodec" "$acodec" "$vres" "$ach" "$acount" "$alang")
          [ -z "$reasons_csv" ] && continue
          BATCH+=("sonarr|$path|$arr_id|$reasons_csv")
          (( ${#BATCH[@]} >= QUEUE_PUSH_BATCH_SIZE )) && _flush_push_batch "sonarr legacy"
        done
        _flush_push_batch "sonarr legacy"
      }
    local _spipe=("${PIPESTATUS[@]}")
    set -e
    if [ "${_spipe[0]}" -ne 0 ]; then
      log "WARN: Sonarr series $sid fetch failed (curl exit ${_spipe[0]})"
    elif [ "${_spipe[1]:-0}" -ne 0 ]; then
      log "WARN: Sonarr series $sid parse failed (perl exit ${_spipe[1]})"
    elif [ "${_spipe[2]:-0}" -ne 0 ]; then
      log "WARN: Sonarr series $sid filter failed (exit ${_spipe[2]})"
    fi
  done

  log "Sonarr complete ($total series)"
}

# ── Stage 1b: Filesystem source (test mode) ────────────────────────────────

scan_filesystem() {
  log "Scanning filesystem for media files (test mode)"
  update_progress "queue" "scanning filesystem"

  local filelist
  filelist=$(mktemp "$STATE_DIR/filelist.XXXXXX")

  find /movies /tv -type f \( -name '*.mkv' -o -name '*.mp4' -o -name '*.avi' -o -name '*.m4v' \) \
    -not -name '*.transcode.tmp.*' -not -name '*.replace.tmp.*' \
    > "$filelist" 2>/dev/null || true

  local total
  total=$(wc -l < "$filelist" | tr -d ' ')
  log "Found $total media files"

  local scanned=0
  # Phase 5C batched push for test mode. Same pattern as the production
  # sites but the while-loop is fed by `< $filelist` (no pipeline), so
  # BATCH lives in the main shell — no subshell semantics to worry about.
  BATCH=()
  while IFS= read -r filepath; do
    scanned=$((scanned + 1))
    if [ $((scanned % 25)) -eq 0 ]; then
      log "Pushing $scanned/$total"
      update_progress "queue" "pushing $scanned/$total"
    fi
    local service="radarr"
    case "$filepath" in /tv/*) service="sonarr" ;; esac
    # 4-tuple payload with empty reasons — test mode bypasses the API
    # pre-filter; reasons would be unknown anyway. Empty reasons
    # disqualify whole-skip at admission, so every test-mode file gets
    # the full ffprobe classifier.
    BATCH+=("$service|$filepath|0|")
    (( ${#BATCH[@]} >= QUEUE_PUSH_BATCH_SIZE )) && _flush_push_batch "test mode"
  done < "$filelist"
  _flush_push_batch "test mode"

  rm -f "$filelist"
  log "Pushed $scanned candidates to Valkey"
}

# ── Main ────────────────────────────────────────────────────────────────────

log "Building work queues"
# Reset monotonic push counter. Total pushed is what the UI needs for
# scan progress %; final LLEN of :ready is "remaining now" because the
# ffprobe pool drains concurrently. Counter is INCR'd at each successful
# q_push site below.
$QUEUE_CLI SET tc:scan:pushed_total 0 > /dev/null
update_progress "queue" "starting"

if [[ "$TEST_MODE" == "true" ]]; then
  # Test: scan filesystem, push each file as candidate
  scan_filesystem
else
  # Production: query APIs, push candidates directly to Valkey
  init_perl_json
  query_radarr
  query_sonarr
  rm -f "$PERL_JSON_LIB" 2>/dev/null
fi

# Total pushed comes from the monotonic counter, not LLEN :ready — the
# ffprobe pool drains :ready concurrently while we push, so a final
# LLEN would understate the real total (sometimes dramatically). The
# counter is INCR'd at every successful q_push above.
CANDIDATE_COUNT=$($QUEUE_CLI GET tc:scan:pushed_total)
[ -z "$CANDIDATE_COUNT" ] && CANDIDATE_COUNT=0
log "Queue build complete — $CANDIDATE_COUNT candidates pushed to Valkey"
update_progress "queued" "${CANDIDATE_COUNT} candidates pushed"
