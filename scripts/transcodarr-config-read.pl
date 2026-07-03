#!/usr/bin/perl
# transcodarr-config-read.pl — Read config.json, output TRANSCODARR_* env exports
# Called by transcodarr-config.sh on every startup.
# Reads $ENV{CONFIG_FILE}, prints export statements to stdout.

use strict;
use warnings;
use JSON::PP qw(decode_json);

my $file = $ENV{CONFIG_FILE} or die "CONFIG_FILE not set";
open my $fh, "<", $file or die "Cannot read $file: $!";
local $/;
my $json = <$fh>;
close $fh;

my $cfg = eval { decode_json($json) };
die "Cannot parse $file: $@" if $@ || ref($cfg) ne 'HASH';

my %vals;

sub flatten_config_values {
  my ($prefix, $node) = @_;
  return unless ref($node) eq 'HASH';

  for my $key (sort keys %{$node}) {
    next unless $key =~ /^\w+\z/;
    my $path = length($prefix) ? "$prefix.$key" : $key;
    my $value = $node->{$key};

    if (JSON::PP::is_bool($value)) {
      $vals{$path} = $value ? 'true' : 'false';
    } elsif (ref($value) eq 'HASH') {
      flatten_config_values($path, $value);
    } elsif (!ref($value)) {
      $vals{$path} = defined($value) ? "$value" : '';
    }
  }
}

flatten_config_values('', $cfg);

# Map config.json keys → TRANSCODARR_* env var names
my %map = (
  "general.start_paused"      => "TRANSCODARR_AUTO_START",
  "general.streams_per_disk"  => "TRANSCODARR_STREAMS_PER_DISK",
  "general.timeout"           => "TRANSCODARR_TIMEOUT",
  "general.output_container"  => "TRANSCODARR_OUTPUT_CONTAINER",
  "general.subtitle_mode"     => "TRANSCODARR_SUBTITLE_MODE",
  "general.stderr_logging"    => "TRANSCODARR_STDERR_LOGGING",
  "general.flag_short_radarr_runtime" => "TRANSCODARR_FLAG_SHORT_RADARR_RUNTIME",
  "general.dry_run"           => "TRANSCODARR_DRY_RUN",
  "gpu.workers"               => "TRANSCODARR_GPU_WORKERS",
  "gpu.preset"                => "TRANSCODARR_PRESET",
  "gpu.target_codec"          => "TRANSCODARR_TARGET_CODEC",
  "gpu.quality_tier"          => "TRANSCODARR_QUALITY_TIER",
  "gpu.encoder_speed"         => "TRANSCODARR_ENCODER_SPEED",
  "gpu.hw_decoding"           => "TRANSCODARR_HW_DECODING",
  "gpu.max_width"             => "TRANSCODARR_MAX_WIDTH",
  "gpu.max_height"            => "TRANSCODARR_MAX_HEIGHT",
  "gpu.nvenc_extras"          => "TRANSCODARR_NVENC_EXTRAS",
  "gpu.qsv_extras"            => "TRANSCODARR_QSV_EXTRAS",
  "gpu.film_grain"            => "TRANSCODARR_AV1_FILM_GRAIN",
  "gpu.hdr_handling"          => "TRANSCODARR_HDR_HANDLING",
  "gpu.hevc_uhq_tune"         => "TRANSCODARR_HEVC_UHQ_TUNE",
  "audio.workers"             => "TRANSCODARR_CPU_WORKERS",
  "audio.codec"               => "TRANSCODARR_AUDIO_CODEC",
  "audio.language"            => "TRANSCODARR_AUDIO_LANG",
  "audio.max_channels"        => "TRANSCODARR_MAX_CHANNELS",
  "audio.sub_language"        => "TRANSCODARR_SUB_LANG",
  "audio.bitrate_mono"        => "TRANSCODARR_AUDIO_BITRATE_MONO",
  "audio.bitrate_stereo"      => "TRANSCODARR_AUDIO_BITRATE_STEREO",
  "audio.bitrate_surround_51" => "TRANSCODARR_AUDIO_BITRATE_SURROUND_51",
  "audio.bitrate_surround_71" => "TRANSCODARR_AUDIO_BITRATE_SURROUND_71",
  "pipeline.validate_workers" => "TRANSCODARR_VALIDATE_WORKERS",
  "pipeline.wrangler_workers" => "TRANSCODARR_WRANGLER_WORKERS",
  "disks.ignored"             => "TRANSCODARR_IGNORED_DISKS",
  "language.enabled"        => "TRANSCODARR_LANGUAGE_ENABLED",
  "language.device"         => "TRANSCODARR_LANGUAGE_DEVICE",
  "language.workers"        => "TRANSCODARR_LANGUAGE_WORKERS",
  "language.model"          => "TRANSCODARR_LANGUAGE_MODEL",
  "language.model_dir"      => "TRANSCODARR_LANGUAGE_MODEL_DIR",
  "language.min_confidence" => "TRANSCODARR_LANGUAGE_MIN_CONFIDENCE",
  "language.sample_coverage_pct" => "TRANSCODARR_LANGUAGE_SAMPLE_COVERAGE_PCT",
  "language.samples"        => "TRANSCODARR_LANGUAGE_SAMPLES",
  "language.sample_sec"     => "TRANSCODARR_LANGUAGE_SAMPLE_SEC",
  "language.head_skip"      => "TRANSCODARR_LANGUAGE_HEAD_SKIP",
  "language.tail_skip"      => "TRANSCODARR_LANGUAGE_TAIL_SKIP",
  "integrations.plex_url"              => "PLEX_URL",
  "integrations.plex_token"            => "PLEX_TOKEN",
  "integrations.plex_movie_section_id" => "PLEX_MOVIE_SECTION_ID",
  "integrations.plex_tv_section_id"    => "PLEX_TV_SECTION_ID",
  "integrations.plex_movie_path_root"  => "PLEX_MOVIE_PATH_ROOT",
  "integrations.plex_tv_path_root"     => "PLEX_TV_PATH_ROOT",
  "integrations.radarr_url"            => "RADARR_URL",
  "integrations.radarr_api_key"        => "RADARR_API_KEY",
  "integrations.sonarr_url"            => "SONARR_URL",
  "integrations.sonarr_api_key"        => "SONARR_API_KEY",
  "space.disk_warn_kb"        => "TRANSCODARR_DISK_WARN_KB",
  "space.check_interval"      => "TRANSCODARR_SPACE_CHECK_INTERVAL",
  "space.dest_retry_timeout"  => "TRANSCODARR_DEST_SPACE_RETRY_TIMEOUT",
  "space.ssd_retry_timeout"   => "TRANSCODARR_SSD_SPACE_RETRY_TIMEOUT",
  "space.retry_timeout"       => "TRANSCODARR_SPACE_RETRY_TIMEOUT",
  "space.tmp_max_kb"          => "TRANSCODARR_TMP_MAX_KB",
  "space.tmp_dir_enabled"     => "TRANSCODARR_TMP_DIR_ENABLED",
);

for my $config_key (sort keys %map) {
  my $env_name = $map{$config_key};
  if (exists $vals{$config_key}) {
    my $val = $vals{$config_key};
    # Invert start_paused → AUTO_START (true→false, false→true)
    if ($config_key eq "general.start_paused") {
      $val = $val eq "true" ? "false" : "true";
    }
    # Shell-safe: quote the value
    $val =~ s/'/'\\''/g;
    print "export ${env_name}='${val}'\n";
  }
}

# Dry Run → TRANSCODARR_ALMOSTHOME_TEST alias. The feature is exposed in
# config.json + GUI as the user-facing "dry_run" toggle, but it drives
# the existing ALMOSTHOME test-mode code path in the worker (writes
# .replace.tmp next to source, skips the mv that would replace the
# original). Exporting both vars here keeps the worker untouched.
if (exists $vals{"general.dry_run"} && $vals{"general.dry_run"} eq "true") {
  print "export TRANSCODARR_ALMOSTHOME_TEST='true'\n";
}

# TMP_DIR resolution — governed by the GUI's `Use Temp Dir` toggle
# (space.tmp_dir_enabled) AND actual mount presence. Worker reads
# TRANSCODARR_TMP_DIR to decide whether to use the SSD pool; we only
# set it when BOTH the toggle is on AND /tmp-transcode exists as a
# directory (proxy for "compose wired the bind mount"). Explicitly
# export empty when either condition fails to override any compose-
# set value in the container env.
my $tmp_dir_path = '/tmp-transcode';
if (exists $vals{"space.tmp_dir_enabled"}
    && $vals{"space.tmp_dir_enabled"} eq "true"
    && -d $tmp_dir_path) {
  print "export TRANSCODARR_TMP_DIR='${tmp_dir_path}'\n";
} else {
  print "export TRANSCODARR_TMP_DIR=''\n";
}
