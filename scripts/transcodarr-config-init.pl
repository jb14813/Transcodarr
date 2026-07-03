#!/usr/bin/perl
# transcodarr-config-init.pl — Seed config.json from environment variables
# Called once by entrypoint when config.json does not exist.
# Writes to $ENV{CONFIG_FILE} (e.g. /state/config.json).

use strict;
use warnings;

my %c = (
  general => {
    start_paused      => ($ENV{TRANSCODARR_AUTO_START} || "false") eq "true" ? \0 : \1,
    streams_per_disk  => int($ENV{TRANSCODARR_STREAMS_PER_DISK} || 2),
    timeout           => int($ENV{TRANSCODARR_TIMEOUT} || 14400),
    output_container  => $ENV{TRANSCODARR_OUTPUT_CONTAINER} || "auto",
    subtitle_mode     => $ENV{TRANSCODARR_SUBTITLE_MODE} || "copy_matching",
    stderr_logging    => ($ENV{TRANSCODARR_STDERR_LOGGING} || "true") eq "true" ? \1 : \0,
    flag_short_radarr_runtime => ($ENV{TRANSCODARR_FLAG_SHORT_RADARR_RUNTIME} || "false") eq "true" ? \1 : \0,
    # Dry-Run mode: when true, worker transcodes + validates as normal
    # but writes the output as a `.replace.tmp` file next to the source
    # and SKIPS the mv step that would replace the original. Original
    # stays on disk, tmp sits alongside for manual comparison before
    # committing. Tracked in /state/almosthome-copies.tsv (legacy
    # internal name — the feature was called ALMOSTHOME in test mode
    # and is aliased to TRANSCODARR_ALMOSTHOME_TEST=true in the worker).
    dry_run           => ($ENV{TRANSCODARR_DRY_RUN} || "false") eq "true" ? \1 : \0,
  },
  gpu => {
    workers       => int($ENV{TRANSCODARR_GPU_WORKERS} || 8),
    # Top-level quality preset — fast / balanced / transparent / custom.
    # When non-custom, overrides quality_tier/encoder_speed/extras/
    # film_grain at runtime (see worker preset-application logic).
    preset        => $ENV{TRANSCODARR_PRESET} || "balanced",
    target_codec  => $ENV{TRANSCODARR_TARGET_CODEC} || "h264",
    quality_tier  => $ENV{TRANSCODARR_QUALITY_TIER} || "excellent",
    encoder_speed => $ENV{TRANSCODARR_ENCODER_SPEED} || "medium",
    hw_decoding   => $ENV{TRANSCODARR_HW_DECODING} || "cuda",
    max_width     => int($ENV{TRANSCODARR_MAX_WIDTH} || 1920),
    max_height    => int($ENV{TRANSCODARR_MAX_HEIGHT} || 1080),
    nvenc_extras  => ($ENV{TRANSCODARR_NVENC_EXTRAS} || "false") eq "true" ? \1 : \0,
    qsv_extras    => ($ENV{TRANSCODARR_QSV_EXTRAS} || "false") eq "true" ? \1 : \0,
    # SVT-AV1 film-grain synthesis level (0=off, 1-50; 8=HandBrake default).
    # Only applied when ENCODER=libsvtav1 (target_codec=av1 + hw_decoding=none).
    film_grain    => int($ENV{TRANSCODARR_AV1_FILM_GRAIN} // 8),
    # HDR handling when source is HDR (smpte2084 / arib-std-b67):
    #   auto     — preserve if target is 10-bit, else tonemap to BT.709
    #   preserve — require 10-bit target; fail otherwise (no silent data loss)
    #   tonemap  — always tonemap to BT.709 even if target supports 10-bit
    # GUI forces/greys this to "tonemap" when target_codec=h264 since h264
    # cannot carry BT.2020/PQ metadata.
    hdr_handling  => $ENV{TRANSCODARR_HDR_HANDLING} || "auto",
    # HEVC UHQ tune — opt-in quality boost for hevc_nvenc. Per NVENC
    # programming guide §9 + Scott Laird's VMAF benchmark (2025-03),
    # `-tune uhq` produces measurably better quality per bit at the cost
    # of ~2x VRAM (lookahead + temporal filter forced on) AND shifts the
    # CQ-vs-quality curve by ~+7 points. Worker applies +7 offset to
    # the resolved CQ when this toggle is on to preserve equivalent
    # quality to the baseline ladder. Only meaningful for hevc_nvenc +
    # hw_decoding=cuda; hidden in GUI otherwise.
    hevc_uhq_tune => ($ENV{TRANSCODARR_HEVC_UHQ_TUNE} || "false") eq "true" ? \1 : \0,
  },
  audio => {
    workers          => int($ENV{TRANSCODARR_CPU_WORKERS} || 2),
    codec            => $ENV{TRANSCODARR_AUDIO_CODEC} || "aac",
    language         => $ENV{TRANSCODARR_AUDIO_LANG} || "eng",
    max_channels     => int($ENV{TRANSCODARR_MAX_CHANNELS} || 6),
    sub_language     => $ENV{TRANSCODARR_SUB_LANG} || $ENV{TRANSCODARR_AUDIO_LANG} || "eng",
    bitrate_mono     => $ENV{TRANSCODARR_AUDIO_BITRATE_MONO} || "",
    bitrate_stereo   => $ENV{TRANSCODARR_AUDIO_BITRATE_STEREO} || "",
    bitrate_surround_51 => $ENV{TRANSCODARR_AUDIO_BITRATE_SURROUND_51} || "",
    bitrate_surround_71 => $ENV{TRANSCODARR_AUDIO_BITRATE_SURROUND_71} || "",
  },
  pipeline => {
    validate_workers => int($ENV{TRANSCODARR_VALIDATE_WORKERS} || 16),
    wrangler_workers => int($ENV{TRANSCODARR_WRANGLER_WORKERS} || 16),
  },
  disks => {
    # CSV of disk slugs (e.g. "disk3,disk7") that the load balancer should
    # not dispatch. Scanning, probing, queue membership, imports, and Direct
    # Queue rows are left untouched.
    ignored => $ENV{TRANSCODARR_IGNORED_DISKS} // '',
  },
  integrations => {
    plex_url              => $ENV{PLEX_URL} // '',
    plex_token            => $ENV{PLEX_TOKEN} // '',
    plex_movie_section_id => int($ENV{PLEX_MOVIE_SECTION_ID} || 1),
    plex_tv_section_id    => int($ENV{PLEX_TV_SECTION_ID} || 2),
    plex_movie_path_root  => $ENV{PLEX_MOVIE_PATH_ROOT} // '/movies',
    plex_tv_path_root     => $ENV{PLEX_TV_PATH_ROOT} // '/tv',
    radarr_url            => $ENV{RADARR_URL} // '',
    radarr_api_key        => $ENV{RADARR_API_KEY} // '',
    sonarr_url            => $ENV{SONARR_URL} // '',
    sonarr_api_key        => $ENV{SONARR_API_KEY} // '',
  },
  failure_policy => {
    enabled => ($ENV{TRANSCODARR_FAILURE_POLICY_ENABLED} || "false") eq "true" ? \1 : \0,
    dry_run => ($ENV{TRANSCODARR_FAILURE_POLICY_DRY_RUN} || "false") eq "true" ? \1 : \0,
    rules => {
      commentary_only => {
        enabled => ($ENV{TRANSCODARR_POLICY_COMMENTARY_ONLY_ENABLED} || "true") eq "true" ? \1 : \0,
        failure_class => "policy_skip",
        actions => ["arr_blocklist", "delete_file", "arr_rescan"],
      },
      wrong_language_policy_skip => {
        enabled => \0,
        failure_class => "policy_skip",
        match => { reason_prefix => "wrong_lang_" },
        destructive => \1,
        actions => ["arr_blocklist", "delete_file", "arr_rescan"],
      },
      missing_preferred_audio_policy_skip => {
        enabled => \0,
        failure_class => "policy_skip",
        match => { reason_prefix => "no_", reason_suffix => "_audio" },
        destructive => \1,
        actions => ["arr_blocklist", "delete_file", "arr_rescan"],
      },
      validation_duration_mismatch => {
        enabled => ($ENV{TRANSCODARR_POLICY_DURATION_MISMATCH_ENABLED} || "true") eq "true" ? \1 : \0,
        failure_class => "validation_failure",
        destructive => \0,
        actions => ["record_diagnostic", "mark_needs_review"],
        match => { reason => "validation_duration_mismatch" },
      },
      validation_failure => {
        enabled => \1,
        failure_class => "validation_failure",
        destructive => \0,
        actions => ["record_diagnostic", "mark_needs_review"],
      },
      stream_map_invalid => {
        enabled => \1,
        failure_class => "stream_map_invalid",
        destructive => \0,
        actions => ["record_diagnostic", "mark_needs_review"],
      },
      input_invalid => {
        enabled => \0,
        failure_class => "input_invalid",
        destructive => \1,
        actions => ["arr_blocklist", "delete_file", "arr_rescan"],
      },
      corrupt_source => {
        enabled => \0,
        failure_class => ["corrupt_input", "mkv_corrupt", "dts_non_monotonic", "illegal_reordering"],
        destructive => \1,
        actions => ["arr_blocklist", "delete_file", "arr_rescan"],
      },
      config_error => {
        enabled => \1,
        failure_class => "config_error",
        destructive => \0,
        actions => ["record_diagnostic", "mark_needs_review"],
      },
      worker_crash => {
        enabled => \1,
        failure_class => "worker_crash",
        destructive => \0,
        actions => ["record_diagnostic", "mark_needs_review"],
      },
      output_missing => {
        enabled => \1,
        failure_class => "output_missing",
        destructive => \0,
        actions => ["record_diagnostic", "mark_needs_review"],
      },
      unknown => {
        enabled => \1,
        failure_class => "unknown",
        destructive => \0,
        actions => ["record_diagnostic", "mark_needs_review"],
      },
      environment => {
        enabled => \1,
        failure_class => ["input_missing", "quarantine", "quarantine_failed", "disk_full", "permission_denied", "file_not_found", "out_of_memory", "encoder_open_failed", "hw_init_failed", "codec_not_supported", "filter_format_mismatch", "filter_reinit_failed", "no_packets_written", "mux_queue_overflow", "container_limit", "subtitle_incompatible", "conversion_failed"],
        destructive => \0,
        actions => ["record_diagnostic", "mark_needs_review"],
      },
    },
  },
  language => {
    # Opt-in audio-language detection & tagging. Restart-applied: the
    # lang_detect pool and the classifier divert are bound at startup
    # from these values + the capability probe. min_confidence is a
    # float — emitted via to_json's string branch (the \d+ branch only
    # matches integers), so it serializes as the string "0.85".
    enabled        => ($ENV{TRANSCODARR_LANGUAGE_ENABLED} || "false") eq "true" ? \1 : \0,
    device         => $ENV{TRANSCODARR_LANGUAGE_DEVICE} || "auto",
    workers        => int($ENV{TRANSCODARR_LANGUAGE_WORKERS} || 2),
    model          => $ENV{TRANSCODARR_LANGUAGE_MODEL} || "base",
    model_dir      => $ENV{TRANSCODARR_LANGUAGE_MODEL_DIR} || "/models",
    min_confidence => ($ENV{TRANSCODARR_LANGUAGE_MIN_CONFIDENCE} || "0.85") + 0,
    sample_coverage_pct => int($ENV{TRANSCODARR_LANGUAGE_SAMPLE_COVERAGE_PCT} || 10),
    samples        => int($ENV{TRANSCODARR_LANGUAGE_SAMPLES} || 3),
    sample_sec     => int($ENV{TRANSCODARR_LANGUAGE_SAMPLE_SEC} || 90),
    head_skip      => int($ENV{TRANSCODARR_LANGUAGE_HEAD_SKIP} || 60),
    tail_skip      => int($ENV{TRANSCODARR_LANGUAGE_TAIL_SKIP} || 60),
  },
  space => {
    disk_warn_kb   => int($ENV{TRANSCODARR_DISK_WARN_KB} || 10485760),
    check_interval => int($ENV{TRANSCODARR_SPACE_CHECK_INTERVAL} || 30),
    dest_retry_timeout => int($ENV{TRANSCODARR_DEST_SPACE_RETRY_TIMEOUT} || $ENV{TRANSCODARR_SPACE_RETRY_TIMEOUT} || 0),
    ssd_retry_timeout  => int($ENV{TRANSCODARR_SSD_SPACE_RETRY_TIMEOUT}  || $ENV{TRANSCODARR_SPACE_RETRY_TIMEOUT} || 0),
    tmp_max_kb     => int($ENV{TRANSCODARR_TMP_MAX_KB} || 1048576000),
    # Use temp dir (SSD-backed cache pool at /tmp-transcode when
    # compose bind-mounts it). When enabled AND the mount exists,
    # worker writes transcode outputs to the pool and atomic-moves
    # back to the source disk — saves write bandwidth on slow HDDs
    # and enables the SSD lease subsystem. Off by default; flip via
    # GUI Storage → "Use Temp Dir" after confirming the mount is
    # present (API status reports tmp_dir_available).
    tmp_dir_enabled => ($ENV{TRANSCODARR_TMP_DIR_ENABLED} || "false") eq "true" ? \1 : \0,
  },
);

# Minimal JSON serializer — no CPAN dependencies
sub to_json {
  my ($ref, $indent) = @_;
  $indent //= 0;
  my $pad = "  " x $indent;
  my $inner = "  " x ($indent + 1);

  if (ref $ref eq "HASH") {
    my @keys = sort keys %$ref;
    my @lines;
    for my $k (@keys) {
      push @lines, qq{${inner}"$k": } . to_json($ref->{$k}, $indent + 1);
    }
    return "{\n" . join(",\n", @lines) . "\n${pad}}";
  } elsif (ref $ref eq "ARRAY") {
    return "[" . join(", ", map { to_json($_, $indent + 1) } @$ref) . "]";
  } elsif (ref $ref eq "SCALAR") {
    return $$ref ? "true" : "false";
  } elsif ($ref =~ /^\d+$/) {
    return $ref;
  } else {
    my $s = $ref;
    $s =~ s/\\/\\\\/g;
    $s =~ s/"/\\"/g;
    return qq{"$s"};
  }
}

my $out = $ENV{CONFIG_FILE} or die "CONFIG_FILE not set";
open my $fh, ">", $out or die "Cannot write $out: $!";
print $fh to_json(\%c, 0) . "\n";
close $fh;
