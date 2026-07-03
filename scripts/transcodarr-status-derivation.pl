# transcodarr-status-derivation.pl — Pure scan-status derivation
#
# Required by transcodarr-api.pl for the /api/status `scan` sub-object
# (Phase 2 UI — Scan Activity panel). Extracted as a separate file so it
# can be unit-tested without spinning up the API server or Valkey.
#
# compute_scan_status(\%inputs) → \%scan
#
# Inputs (all required, pass numeric/string values; no Valkey/file I/O
# happens inside this sub):
#   pushed_total      monotonic counter from queue.sh (tc:scan:pushed_total)
#   ready_len         LLEN tc:candidates:ready
#   processing_len    LLEN tc:candidates:processing
#   pf_phase          progress-file `phase:` line value (may be "")
#   pf_status         progress-file `status:` line value (may be "")
#
# Output hashref:
#   phase             user-facing label ("Building queue" / "Classifying" / "Idle" / "Starting")
#   status            user-facing status text
#   pending           ready_len + processing_len
#   pushed_total      passed through unchanged
#   progress_pct      number 0..100, one decimal precision
#   indeterminate     1 if no measurable fraction available, else 0
#
# Cache row counts and skip-hit counters are computed and emitted by the
# caller (transcodarr-api.pl) directly — they're pure passthrough and
# don't need to round-trip through this derivation sub.
#
# The caller is responsible for json_escape'ing string fields before
# emitting JSON.

use strict;
use warnings;

sub compute_scan_status {
  my ($in) = @_;
  $in ||= {};

  my $pushed_total   = ($in->{pushed_total}   || 0) + 0;
  my $ready_len      = ($in->{ready_len}      || 0) + 0;
  my $processing_len = ($in->{processing_len} || 0) + 0;
  my $pf_phase       = defined($in->{pf_phase})  ? $in->{pf_phase}  : '';
  my $pf_status      = defined($in->{pf_status}) ? $in->{pf_status} : '';

  my $pending = $ready_len + $processing_len;

  my ($phase_label, $status_label, $progress_pct, $indeterminate);

  if ($pf_phase eq 'queue') {
    # Builder is actively running. Progress bar is ALWAYS indeterminate
    # during this phase — the Sonarr `N/402 series` ratio measures the
    # API enumeration step, which is a small fraction of total restart
    # work (the probe-pool drain that follows is much bigger). Showing
    # 100% when Sonarr finishes only to drop to ~20% when classification
    # begins is misleading. Status text still surfaces the N/M counts so
    # the user can see enumeration progress; only the bar is animated.
    $phase_label   = 'Building queue';
    $status_label  = $pf_status;
    $progress_pct  = 0;
    $indeterminate = 1;
  }
  elsif ($pending > 0) {
    # Items in flight — could be mid-build (with builder still pushing
    # concurrently) OR post-build with probe pool still draining.
    # Either way, the right answer is "Classifying" with a measurable %.
    $phase_label = 'Classifying';
    my $processed = $pushed_total - $pending;
    $processed = 0 if $processed < 0;   # clamp for imports-after-freeze case
    $status_label = sprintf('%d of %d processed', $processed, $pushed_total);
    if ($pushed_total > 0) {
      $progress_pct  = 100 * $processed / $pushed_total;
      $indeterminate = 0;
    } else {
      # Imports pending but no bulk scan in progress — % isn't meaningful
      $progress_pct  = 0;
      $indeterminate = 1;
    }
  }
  elsif ($pf_phase eq 'queued' || $pushed_total > 0) {
    # Builder finished AND drained, OR pushed_total > 0 but :ready/:processing
    # are now both empty. A "queued, 0 candidates" scan also lands here —
    # it's not "Starting", the scan IS done, just no work to do.
    $phase_label   = 'Idle';
    $status_label  = 'Waiting for imports';
    $progress_pct  = 100;
    $indeterminate = 0;
  }
  else {
    # No builder activity recorded yet. Fresh boot before queue.sh writes
    # the progress file, or external DEL of tc:scan:pushed_total.
    $phase_label   = 'Starting';
    $status_label  = '...';
    $progress_pct  = 0;
    $indeterminate = 1;
  }

  return {
    phase          => $phase_label,
    status         => $status_label,
    pending        => $pending,
    pushed_total   => $pushed_total,
    progress_pct   => sprintf('%.1f', $progress_pct) + 0,
    indeterminate  => $indeterminate ? 1 : 0,
  };
}

1;
