#!/usr/bin/perl
# transcodarr-api.pl — HTTP API + GUI server for Transcodarr
# Forking Perl server — handles concurrent requests.
# Replaces the netcat FIFO server.

use strict;
use warnings;
use IO::Socket::INET;
use POSIX qw(:sys_wait_h);
use Fcntl qw(:flock);
use JSON::PP;
use Digest::MD5 qw(md5_hex);
use FindBin qw($Bin);

my $PORT      = $ENV{TRANSCODARR_API_PORT} || 7879;
my $STATE_DIR = $ENV{TRANSCODARR_STATE_DIR} || '/state';
my $SCRIPT_DIR = $ENV{TRANSCODARR_SCRIPT_DIR} || '/scripts';
my $GUI_FILE  = "$SCRIPT_DIR/transcodarr-gui.html";
my $CLI       = 'valkey-cli';

# Pure scan-status derivation.
# Loaded at startup (before the request loop) so /api/status has the
# compute_scan_status() sub available. Pure function — no Valkey/file
# I/O happens inside; the caller assembles inputs from this script.
require "$SCRIPT_DIR/transcodarr-status-derivation.pl";
require "$SCRIPT_DIR/transcodarr-plex-path.pl";

$SIG{CHLD} = sub { while (waitpid(-1, WNOHANG) > 0) {} };

# ── Helpers ──────────────────────────────────────────────────────────────

sub vcli { my @args = @_; my $out = `$CLI @args 2>/dev/null`; chomp $out; return $out; }

# vcli_safe — list-form piped open, NO shell. Mandatory for any argument
# that comes from client body or is derived from it (filepaths, payloads,
# file hashes, lane names). Also used by /api/direct/* endpoints
# unconditionally — simpler review-gate rule. Retains the existing `vcli`
# for unchanged callers with hardcoded arguments.
sub vcli_safe {
  my @args = @_;
  open(my $fh, '-|', $CLI, @args) or return '';
  my $out = do { local $/; <$fh> };
  close $fh;
  chomp $out if defined $out;
  return $out // '';
}

sub vcli_checked {
  my @args = @_;
  open(my $fh, '-|', $CLI, @args) or return (0, '');
  my $out = do { local $/; <$fh> };
  # The server's SIGCHLD reaper can consume the child status before close()
  # sees it, so command success is validated by the expected output instead.
  close $fh;
  chomp $out if defined $out;
  return (defined($out) && $out ne '' ? 1 : 0, $out // '');
}

sub run_lib_helper {
  my ($helper) = @_;
  return (0, 'missing_helper') unless defined($helper) && $helper =~ /^[a-zA-Z0-9_]+$/;

  local $ENV{TRANSCODARR_STATE_DIR} = $STATE_DIR;
  my $cmd = 'source "' . $Bin . '/transcodarr-lib.sh" || exit 1; ' . $helper;
  my $rc = system('bash', '-c', $cmd);
  return ($rc == 0 ? (1, '') : (0, "helper_failed:$helper"));
}

sub run_lib_helper_capture {
  # Like run_lib_helper, but returns the helper's stdout (trimmed). Used by
  # routes that need a value (e.g. a released-count) rather than just rc.
  my ($helper) = @_;
  return '' unless defined($helper) && $helper =~ /^[a-zA-Z0-9_]+$/;
  local $ENV{TRANSCODARR_STATE_DIR} = $STATE_DIR;
  my $cmd = 'source "' . $Bin . '/transcodarr-lib.sh" || exit 1; ' . $helper;
  my $out = `bash -c '$cmd' 2>/dev/null`;
  chomp $out if defined $out;
  return defined($out) ? $out : '';
}

sub policy_runner_path {
  return "$Bin/transcodarr-failure-policy.pl";
}

sub run_policy_runner_json {
  my (@args) = @_;
  my $policy_runner = policy_runner_path();
  return '{"ok":false,"error":"runner_missing"}' unless -x $policy_runner;
  local $ENV{TRANSCODARR_STATE_DIR} = $STATE_DIR;
  open(my $fh, '-|', $policy_runner, @args)
    or return '{"ok":false,"error":"runner_spawn_failed"}';
  my $out = do { local $/; <$fh> };
  close $fh;
  return $out && $out =~ /^\s*\{/ ? $out : '{"ok":false,"error":"runner_invalid_json"}';
}

# decode_body — tolerant JSON decoder for POST bodies. Returns {} on
# empty/invalid input so endpoint handlers can treat missing fields as
# simple validation errors.
sub decode_body {
  my $b = shift // '';
  return {} if $b eq '';
  my $decoded = eval { decode_json($b) };
  return (ref($decoded) eq 'HASH') ? $decoded : {};
}

sub decode_json_object {
  my $b = shift // '';
  my $decoded = eval { decode_json($b) };
  return (undef, 'invalid_json') if $@ || ref($decoded) ne 'HASH';
  return ($decoded, undef);
}

# Preload Lua scripts at startup via $Bin (script's own dir), which is
# cwd-independent — the daemon may be launched with any working dir by
# the entrypoint. Both scripts live next to the Perl file in /scripts/lua/.
my $LUA_DIRECT_TAG   = do {
  local $/;
  open(my $fh, '<', "$Bin/lua/direct_tag.lua")
    or die "cannot load direct_tag.lua: $!";
  <$fh>;
};
my $LUA_DIRECT_UNTAG = do {
  local $/;
  open(my $fh, '<', "$Bin/lua/direct_untag.lua")
    or die "cannot load direct_untag.lua: $!";
  <$fh>;
};

sub json_escape {
  my $s = shift // '';
  $s =~ s/\\/\\\\/g;
  $s =~ s/"/\\"/g;
  $s =~ s/\n/\\n/g;
  $s =~ s/\r/\\r/g;
  $s =~ s/\t/\\t/g;
  return $s;
}

sub disk_sort {
  return sort {
    my ($an) = $a =~ /^disk(\d+)$/;
    my ($bn) = $b =~ /^disk(\d+)$/;
    defined($an) && defined($bn) ? $an <=> $bn : $a cmp $b
  } @_;
}

sub disk_csv_list {
  my $raw = shift // '';
  my %seen;
  my @out;
  for my $disk (split /,/, $raw) {
    $disk =~ s/^\s+|\s+$//g;
    next unless $disk =~ /^disk\d+$/;
    next if $seen{$disk}++;
    push @out, $disk;
  }
  return disk_sort(@out);
}

sub validate_disk_csv {
  my $raw = shift // '';
  return 'not_string' if ref($raw);
  for my $disk (split /,/, $raw) {
    $disk =~ s/^\s+|\s+$//g;
    next if $disk eq '';
    return "invalid_disk:$disk" unless $disk =~ /^disk\d+$/;
  }
  return '';
}

sub read_config_object_from {
  my $file = shift || "$STATE_DIR/config.json";
  return {} unless -f $file;
  open(my $fh, '<', $file) or return {};
  local $/;
  my $raw = <$fh>;
  close $fh;
  my $decoded = eval { decode_json($raw) };
  return ref($decoded) eq 'HASH' ? $decoded : {};
}

sub read_config_object {
  return read_config_object_from("$STATE_DIR/config.json");
}

sub apply_runtime_integration_defaults {
  my $cfg = shift || {};
  $cfg->{integrations} = {} unless ref($cfg->{integrations}) eq 'HASH';
  my %defaults = (
    plex_url              => $ENV{PLEX_URL} // '',
    plex_token            => $ENV{PLEX_TOKEN} // '',
    plex_movie_section_id => $ENV{PLEX_MOVIE_SECTION_ID} // '1',
    plex_tv_section_id    => $ENV{PLEX_TV_SECTION_ID} // '2',
    plex_movie_path_root  => $ENV{PLEX_MOVIE_PATH_ROOT} // '/movies',
    plex_tv_path_root     => $ENV{PLEX_TV_PATH_ROOT} // '/tv',
    radarr_url            => $ENV{RADARR_URL} // '',
    radarr_api_key        => $ENV{RADARR_API_KEY} // '',
    sonarr_url            => $ENV{SONARR_URL} // '',
    sonarr_api_key        => $ENV{SONARR_API_KEY} // '',
  );
  for my $key (sort keys %defaults) {
    $cfg->{integrations}->{$key} = $defaults{$key}
      unless exists $cfg->{integrations}->{$key};
  }
  return $cfg;
}

sub scalar_config_value {
  my $value = shift;
  return '' if ref($value);
  $value = '' unless defined $value;
  $value =~ s/^\s+|\s+$//g;
  return $value;
}

sub integration_url {
  my $url = scalar_config_value(shift);
  $url =~ s{/+\z}{};
  return $url;
}

sub integration_url_allowed {
  my $url = shift;
  return 0 unless length($url);
  return 0 unless $url =~ m{\Ahttps?://}i;

  my ($host) = $url =~ m{\Ahttps?://(\[[^\]]+\]|[^/:?#]+)}i;
  return 0 unless defined($host) && length($host);
  $host =~ s/\A\[//;
  $host =~ s/\]\z//;
  $host = lc($host);

  return 0 if $host eq 'localhost';
  return 0 if $host eq '::1';
  return 0 if $host eq '0.0.0.0';
  return 0 if $host =~ /\A127\./;
  return 0 if $host =~ /\A169\.254\./;
  return 0 if $host =~ /\Afe80:/;
  return 1;
}

sub curl_probe {
  my ($url, @args) = @_;
  return 0 unless length($url);
  open(my $fh, '-|',
    'curl', '-fsS', '--max-time', '10', '-o', '/dev/null', '-w', '%{http_code}',
    @args, '--', $url
  )
    or return 0;
  my $out = do { local $/; <$fh> };
  close $fh;
  $out = '' unless defined $out;
  $out =~ s/\s+//g;
  return $out =~ /^2\d\d$/ ? 1 : 0;
}

sub curl_capture {
  my ($url, @args) = @_;
  return (0, '') unless length($url);
  open(my $fh, '-|', 'curl', '-fsS', '--max-time', '10', @args, '--', $url)
    or return (0, '');
  my $out = do { local $/; <$fh> };
  close $fh;
  return (defined($out) && length($out) ? (1, $out) : (0, ''));
}

sub valid_section_id {
  my $section_id = scalar_config_value(shift);
  return $section_id =~ /^\d+$/ && int($section_id) > 0 ? $section_id : '';
}

sub plex_section_probe_target {
  my ($sections_xml, $integrations, $section_key, $path_key, $section_type) = @_;
  my $hint_section_id = valid_section_id($integrations->{$section_key});
  my $hint_path_root = scalar_config_value($integrations->{$path_key});
  my ($err, $section_id, $path_root) =
    tc_plex_section_target_for_type($sections_xml, $section_type, $hint_section_id, $hint_path_root);
  return ($err, '', '') if length($err);
  $section_id = valid_section_id($section_id);
  return ('missing_section_id', '', '') unless length($section_id);

  return ('path_mismatch', '', '')
    unless tc_plex_section_contains_path($sections_xml, $section_id, $path_root);
  return ('', $section_id, $path_root);
}

sub probe_plex_target_refresh {
  my ($url, $token, $section_id, $path_root) = @_;
  my $probe_path = tc_plex_join_root_suffix($path_root, '/.transcodarr-target-check');
  return curl_probe(
    "$url/library/sections/$section_id/refresh",
    '-X', 'POST',
    '--get',
    '--data-urlencode', "path=$probe_path",
    '--data-urlencode', "X-Plex-Token=$token"
  );
}

sub test_integration_app {
  my ($app, $integrations) = @_;
  $app = lc scalar_config_value($app);
  $integrations = {} unless ref($integrations) eq 'HASH';

  if ($app eq 'plex') {
    my $url = integration_url($integrations->{plex_url});
    my $token = scalar_config_value($integrations->{plex_token});
    return (0, 'missing_url') unless length($url);
    return (0, 'invalid_url') unless integration_url_allowed($url);
    return (0, 'missing_token') unless length($token);

    my ($sections_ok, $sections_xml) = curl_capture(
      "$url/library/sections",
      '--get', '--data-urlencode', "X-Plex-Token=$token"
    );
    return (0, 'request_failed') unless $sections_ok;

    my $detected = {};

    my ($err, $movie_section_id, $movie_path_root) =
      plex_section_probe_target($sections_xml, $integrations, 'plex_movie_section_id', 'plex_movie_path_root', 'movie');
    return (0, $err) if length($err);
    return (0, 'target_refresh_failed')
      unless probe_plex_target_refresh($url, $token, $movie_section_id, $movie_path_root);
    $detected->{plex_movie_section_id} = 0 + $movie_section_id;
    $detected->{plex_movie_path_root} = $movie_path_root;

    my ($tv_err, $tv_section_id, $tv_path_root) =
      plex_section_probe_target($sections_xml, $integrations, 'plex_tv_section_id', 'plex_tv_path_root', 'show');
    $err = $tv_err;
    return (0, $err) if length($err);
    return (0, 'target_refresh_failed')
      unless probe_plex_target_refresh($url, $token, $tv_section_id, $tv_path_root);
    $detected->{plex_tv_section_id} = 0 + $tv_section_id;
    $detected->{plex_tv_path_root} = $tv_path_root;

    return (1, '', $detected);
  }

  if ($app eq 'radarr' || $app eq 'sonarr') {
    my $url_key = $app . '_url';
    my $api_key = $app . '_api_key';
    my $url = integration_url($integrations->{$url_key});
    my $key = scalar_config_value($integrations->{$api_key});
    return (0, 'missing_url') unless length($url);
    return (0, 'invalid_url') unless integration_url_allowed($url);
    return (0, 'missing_api_key') unless length($key);
    return (curl_probe("$url/api/v3/system/status", '-H', "X-Api-Key: $key") ? (1, '') : (0, 'request_failed'));
  }

  return (0, 'invalid_app');
}

sub runtime_ignored_disks {
  my @raw = split /\n/, (vcli_safe('SMEMBERS', 'tc:disk:ignored') || '');
  my %seen;
  my @out;
  for my $disk (@raw) {
    next unless defined $disk && $disk =~ /^disk\d+$/;
    next if $seen{$disk}++;
    push @out, $disk;
  }
  return disk_sort(@out);
}

sub sync_ignored_disks_from_config {
  my $cfg = shift || {};
  my $raw = '';
  if (ref($cfg->{disks}) eq 'HASH') {
    $raw = $cfg->{disks}->{ignored} // '';
  }
  my @disks = disk_csv_list($raw);
  my ($ok, $out) = vcli_checked('EVAL',
    'redis.call("DEL", KEYS[1]); for i,v in ipairs(ARGV) do redis.call("SADD", KEYS[1], v); end; return #ARGV',
    '1', 'tc:disk:ignored', @disks);
  return ($ok && $out =~ /^\d+$/ && int($out) == scalar(@disks)) ? 1 : 0;
}

sub prepare_config_save {
  my $body = shift // '';
  my ($cfg, $err) = decode_json_object($body);
  return (undef, undef, 'invalid_json') if $err;
  return (undef, undef, 'invalid_disks_section')
    if exists($cfg->{disks}) && ref($cfg->{disks}) ne 'HASH';
  return (undef, undef, 'invalid_integrations_section')
    if exists($cfg->{integrations}) && ref($cfg->{integrations}) ne 'HASH';

  if (!exists($cfg->{disks}) || !exists($cfg->{disks}->{ignored})) {
    my $existing = read_config_object();
    my $existing_ignored = '';
    if (ref($existing->{disks}) eq 'HASH') {
      $existing_ignored = $existing->{disks}->{ignored} // '';
    }
    $cfg->{disks} ||= {};
    $cfg->{disks}->{ignored} = $existing_ignored;
    $body = encode_json($cfg);
  }

  {
    my $existing = apply_runtime_integration_defaults(read_config_object());
    $cfg->{integrations} ||= {};
    for my $key (sort keys %{$existing->{integrations}}) {
      $cfg->{integrations}->{$key} = $existing->{integrations}->{$key}
        unless exists $cfg->{integrations}->{$key};
    }
    $body = encode_json($cfg);
  }

  my $disk_err = validate_disk_csv($cfg->{disks}->{ignored});
  return (undef, undef, $disk_err) if $disk_err;
  return ($cfg, $body, '');
}

sub send_response {
  my ($client, $status, $type, $body) = @_;
  my $len = length($body);
  print $client "HTTP/1.1 $status\r\nContent-Type: $type\r\nContent-Length: $len\r\nConnection: close\r\n\r\n$body";
}

sub send_json {
  my ($client, $body) = @_;
  send_response($client, '200 OK', 'application/json', $body);
}

sub uri_decode {
  my $s = shift // '';
  $s =~ tr/+/ /;
  $s =~ s/%([0-9A-Fa-f]{2})/chr(hex $1)/ge;
  return $s;
}

sub parse_query {
  my $qs = shift || '';
  my %q;
  for (split /&/, $qs) {
    my ($k, $v) = split /=/, $_, 2;
    $q{$k} = uri_decode($v // '') if defined $k;
  }
  return %q;
}

# ── Queue reader (paginated) ────────────────────────────────────────────

# Build JSON for a single queue item line.
# Accepts (line, lane) so row carries its source lane for tag/untag actions.
# Emits route (field 11), payload (full line), lane, file_hash in addition
# to existing display fields.
#
# Payload can contain arbitrary filepath characters (spaces, quotes,
# backslashes) — MUST go through json_escape.
sub queue_item_json {
  my ($line, $lane) = @_;
  my @f = split /\|/, $line;
  my $svc     = json_escape($f[0] // '');
  my $path    = json_escape($f[1] // '');
  my $vcodec  = json_escape($f[3] // '');
  my $ach     = $f[4] // 0;
  my $acount  = $f[5] // 1;
  my $disk    = json_escape($f[6] // '');
  my $size_kb = $f[8] // 0;
  my $route   = $f[10] // 'bulk';
  my ($fname) = ($f[1] // '') =~ m{([^/]+)$};
  $fname = json_escape($fname // '');
  my $payload_esc = json_escape($line);
  my $route_esc   = json_escape($route);
  my $lane_esc    = json_escape($lane // '');
  my $file_hash   = '';
  if (($f[1] // '') ne '') {
    # md5 of filepath — matches .job filenames and tc:worker:phase:<hash>.
    # Digest::MD5 from Task 7; no shell interpolation.
    $file_hash = md5_hex($f[1]);
  }
  return qq({"service":"$svc","file":"$fname","path":"$path","disk":"$disk","vcodec":"$vcodec","channels":$ach,"tracks":$acount,"size_kb":$size_kb,"route":"$route_esc","lane":"$lane_esc","payload":"$payload_esc","file_hash":"$file_hash"});
}

sub serve_queue {
  my ($queues_ref, $offset, $limit, $disk_filter, $q_filter) = @_;
  my @queues = ref $queues_ref ? @$queues_ref : ($queues_ref);

  # Collect (lane, line) tuples from each queue in priority order.
  # Use vcli_safe throughout — lane names are caller-allow-listed, but the
  # /api/direct/* "no shell" rule is easier to audit if serve_queue (which
  # /api/direct GET routes through) uses the safe helper unconditionally.
  my @tuples;
  for my $q (@queues) {
    for my $line (split /\n/, (vcli_safe('LRANGE', $q, '0', '-1') || '')) {
      next unless $line;
      push @tuples, [ $q, $line ];
    }
  }

  # Case-insensitive substring search across the whole payload string —
  # matches filepath (most useful) but also disk name, codec, etc. so users
  # can search by anything visible.
  my $q_lc = (defined $q_filter && length $q_filter) ? lc $q_filter : '';

  # Apply disk + search filters if set
  my @filtered;
  for my $t (@tuples) {
    my ($lane, $line) = @$t;
    if ($disk_filter && $disk_filter ne 'all') {
      my @f = split /\|/, $line;
      next unless ($f[6] // '') eq $disk_filter;
    }
    if ($q_lc ne '') {
      next unless index(lc $line, $q_lc) >= 0;
    }
    push @filtered, $t;
  }

  my $total = scalar @filtered;
  my @page = splice(@filtered, $offset, $limit);
  my @items = map { queue_item_json($_->[1], $_->[0]) } @page;
  my $items_json = join(',', @items);
  return qq({"items":[$items_json],"total":$total,"offset":$offset,"limit":$limit});
}

# ── TSV reader (paginated, newest first) ─────────────────────────────────

sub serve_tsv {
  my ($file, $offset, $limit, $disk_filter, $disk_col, $q_filter, $reason_filter, $reason_col) = @_;
  unless (-f $file && -s $file) {
    return qq({"items":[],"total":0,"offset":$offset,"limit":$limit,"disk_counts":{},"reason_counts":{}});
  }

  # Read reversed (newest first)
  my @all = `tac "$file" 2>/dev/null`;
  chomp @all;

  # Compute unfiltered disk + reason counts for the filter bars (computed
  # against ALL rows pre-filter so the pickers show true counts regardless
  # of any active search query).
  my %dcounts;
  if (defined $disk_col) {
    for my $line (@all) {
      my @f = split /\t/, $line;
      my $dd = $f[$disk_col] // '';
      $dcounts{$dd}++ if $dd ne '';
    }
  }
  my @dc_pairs;
  for my $d (sort keys %dcounts) {
    push @dc_pairs, qq("@{[json_escape($d)]}":$dcounts{$d});
  }
  my $dc_json = join(',', @dc_pairs);

  my %rcounts;
  if (defined $reason_col) {
    for my $line (@all) {
      my @f = split /\t/, $line;
      my $rr = $f[$reason_col] // '';
      $rcounts{$rr}++ if $rr ne '';
    }
  }
  my @rc_pairs;
  for my $r (sort keys %rcounts) {
    push @rc_pairs, qq("@{[json_escape($r)]}":$rcounts{$r});
  }
  my $rc_json = join(',', @rc_pairs);

  my $q_lc = (defined $q_filter && length $q_filter) ? lc $q_filter : '';

  # Apply disk + reason + search filters
  my @lines;
  for my $line (@all) {
    if ($disk_filter && $disk_filter ne 'all' && defined $disk_col) {
      my @f = split /\t/, $line;
      next unless ($f[$disk_col] // '') eq $disk_filter;
    }
    if ($reason_filter && $reason_filter ne 'all' && defined $reason_col) {
      my @f = split /\t/, $line;
      next unless ($f[$reason_col] // '') eq $reason_filter;
    }
    if ($q_lc ne '') {
      next unless index(lc $line, $q_lc) >= 0;
    }
    push @lines, $line;
  }

  my $total = scalar @lines;
  my @slice = splice(@lines, $offset, $limit);
  my @items;
  for my $line (@slice) {
    my @fields = split /\t/, $line;
    my $fields_json = join(',', map { '"' . json_escape($_) . '"' } @fields);
    push @items, qq({"fields":[$fields_json]});
  }
  my $items_json = join(',', @items);
  return qq({"items":[$items_json],"total":$total,"offset":$offset,"limit":$limit,"disk_counts":{$dc_json},"reason_counts":{$rc_json}});
}

sub failed_policy_key {
  my (@f) = @_;
  return md5_hex(join "\t",
    $f[0] // '',
    $f[1] // '',
    $f[2] // '',
    $f[3] // '',
    $f[8] // '');
}

# Keep byte-for-byte in sync with outcome_for_state in transcodarr-failure-policy.pl.
sub policy_outcome_for_state {
  my ($state) = @_;
  $state //= '';
  return 'handled'      if $state eq 'complete';
  return 'blocked'      if $state eq 'blocked';
  return 'needs_review' if $state eq 'needs_review';
  return 'dry_run'      if $state eq 'dry_run';
  return 'in_progress'  if $state eq 'pending'
                        || $state eq 'resolving'
                        || $state eq 'blocklisted'
                        || $state eq 'deleted';
  return 'unknown';
}

sub policy_actions_by_failed_key {
  my $file = "$STATE_DIR/failure-policy-actions.tsv";
  my %by_key;
  return %by_key unless -f $file;
  open(my $fh, '<', $file) or return %by_key;
  while (my $line = <$fh>) {
    chomp $line;
    next unless length $line;
    my @f = split /\t/, $line, -1;
    next unless @f >= 11;
    next if ($f[6] // '') eq 'dry_run';
    my $proof = eval { decode_json($f[10] || '{}') };
    $proof = {} unless ref($proof) eq 'HASH';
    $by_key{$f[1]} = {
      policy_id => $f[0],
      state => $f[6],
      outcome => policy_outcome_for_state($f[6]),
      attempt_count => int($f[7] || 0),
      next_attempt_ts => int($f[8] || 0),
      updated_ts => $f[9],
      proof => $proof,
    };
  }
  close $fh;
  return %by_key;
}

sub serve_failed_tsv {
  my ($offset, $limit, $disk_filter, $q_filter) = @_;
  my $json = serve_tsv("$STATE_DIR/failed-files.tsv", $offset, $limit, $disk_filter, 7, $q_filter);
  my $decoded = eval { decode_json($json) };
  return $json if $@ || ref($decoded) ne 'HASH';
  my %policy = policy_actions_by_failed_key();
  for my $item (@{ $decoded->{items} || [] }) {
    next unless ref($item) eq 'HASH';
    my @fields = @{ $item->{fields} || [] };
    my $key = failed_policy_key(@fields);
    $item->{policy} = $policy{$key} if $policy{$key};
  }
  return encode_json($decoded);
}

sub count_file_lines {
  my $file = shift;
  return 0 unless -f $file;
  open(my $fh, '<', $file) or return 0;
  my $count = 0;
  $count++ while <$fh>;
  close $fh;
  return $count;
}

sub clear_failed_tsv {
  my $file = "$STATE_DIR/failed-files.tsv";
  my $hashes_file = "$STATE_DIR/failed-hashes.tsv";
  my $lock_file = "$STATE_DIR/failed-files.clear.lock";

  open(my $lock_fh, '>>', $lock_file) or return (0, 'cannot_lock', 0);
  unless (flock($lock_fh, LOCK_EX)) {
    close $lock_fh;
    return (0, 'cannot_lock', 0);
  }

  my $cleared = count_file_lines($file);
  my $fh;
  unless (open($fh, '>', $file)) {
    close $lock_fh;
    return (0, 'cannot_write', 0);
  }
  close $fh;

  # Hash-gate extension (spec r9): clear the hash list AND tc:seen so
  # post-Clear retries can actually re-enter the probe pool. Without
  # the tc:seen DEL, q_try_mark would silently dedupe re-injected
  # .job files against pre-Clear path:size:inode entries. Best-effort
  # — log a warning on failure but don't fail the whole operation;
  # the visible failed-files truncate (above) already succeeded.
  if (open(my $hash_fh, '>', $hashes_file)) {
    close $hash_fh;
  } else {
    warn "clear_failed_tsv: cannot truncate $hashes_file: $!";
  }
  system("valkey-cli DEL tc:seen >/dev/null 2>&1") == 0
    or warn "clear_failed_tsv: valkey-cli DEL tc:seen failed (rc=$?)";

  close $lock_fh;
  return (1, '', $cleared);
}

sub clear_flagged_runtime_state {
  system("valkey-cli DEL tc:idx:flagged:seen >/dev/null 2>&1") == 0
    or warn "clear_flagged_tsv: valkey-cli DEL tc:idx:flagged:seen failed (rc=$?)";
  system("valkey-cli DEL tc:flags:by_path >/dev/null 2>&1") == 0
    or warn "clear_flagged_tsv: valkey-cli DEL tc:flags:by_path failed (rc=$?)";
  system("valkey-cli DEL tc:flags:tsv_dirty >/dev/null 2>&1") == 0
    or warn "clear_flagged_tsv: valkey-cli DEL tc:flags:tsv_dirty failed (rc=$?)";
}

sub clear_flagged_tsv {
  my $file = "$STATE_DIR/flagged-files.tsv";
  my $lock_file = "$STATE_DIR/flagged-files.clear.lock";

  # Best-effort Valkey reset, run only AFTER the TSV is successfully
  # truncated so the on-disk and in-memory views stay aligned.
  #
  # tc:idx:flagged:seen — Phase 6 dedupe set; no longer populated
  # (Phase 7-followup deleted record_flag_once) but DEL'd here as a
  # defensive no-op for upgrades from older builds.
  #
  # tc:flags:by_path  — Phase 7-followup classifier-owned current
  # flag index. Clearing the Flagged tab MUST wipe this hash so the
  # next API read returns zero rows.
  #
  # tc:flags:tsv_dirty — wipe is a fresh-start; clearing this is
  # unconditional (not snapshot-result), so the dirty bit is gone too.
  open(my $lock_fh, '>>', $lock_file) or return (0, 'cannot_lock', 0);
  unless (flock($lock_fh, LOCK_EX)) {
    close $lock_fh;
    return (0, 'cannot_lock', 0);
  }

  my $cleared = count_file_lines($file);
  my $fh;
  unless (open($fh, '>', $file)) {
    close $lock_fh;
    return (0, 'cannot_write', 0);
  }
  close $fh;
  clear_flagged_runtime_state();
  close $lock_fh;
  return (1, '', $cleared);
}

sub reset_cache_tsvs_for_rescan {
  my %counts = (
    failed  => count_file_lines("$STATE_DIR/failed-files.tsv"),
    flagged => count_file_lines("$STATE_DIR/flagged-files.tsv"),
  );

  for my $file ("$STATE_DIR/failed-files.tsv", "$STATE_DIR/flagged-files.tsv") {
    open(my $fh, '>', $file) or return (0, "cannot_write:$file", \%counts);
    close $fh;
  }

  my ($ok, $err) = run_lib_helper('reset_cache_rail_tsvs_for_rescan');
  return (0, $err, \%counts) unless $ok;
  return (1, '', \%counts);
}

sub clear_runtime_state_for_rescan {
  clear_flagged_runtime_state();

  vcli_safe('DEL',
    'tc:lb:gpu:ready', 'tc:lb:gpu:import:ready', 'tc:lb:gpu:direct:ready',
    'tc:lb:cpu:ready', 'tc:lb:cpu:import:ready', 'tc:lb:cpu:direct:ready',
    'tc:dispatch:gpu:ready', 'tc:dispatch:cpu:ready',
    'tc:candidates:ready', 'tc:candidates:import:ready',
    'tc:candidates:processing', 'tc:candidates:import:processing',
    'tc:candidates:resolved:ready', 'tc:candidates:resolved:import:ready',
    'tc:candidates:resolved:processing', 'tc:candidates:resolved:import:processing',
    'tc:lang:ready', 'tc:lang:processing',
    'tc:seen', 'tc:direct:active',
    'tc:skip:verified_aac_lc', 'tc:skip:verified_fully_classified', 'tc:skip:failed_hash'
  );

  # Sweep tc:direct:meta:* sidecar keys (orphaned tag metadata).
  my $scan_out = vcli_safe('--scan', '--pattern', 'tc:direct:meta:*') // '';
  for my $k (split /\n/, $scan_out) {
    next unless length $k;
    vcli_safe('DEL', $k);
  }
}

sub fork_queue_rescan {
  system("$Bin/transcodarr-queue.sh </dev/null >/dev/null &");
}

sub clear_cache_and_rescan {
  # Load-bearing order:
  # 1. Reset TSVs first.
  # 2. Rebuild empty rail indexes next so stale :paths membership cannot
  #    be observed by a new probe pass.
  # 3. Clear flagged runtime, queue, direct, dedupe, and skip-counter state.
  # 4. Fork queue.sh last.
  my ($ok, $err, $counts) = reset_cache_tsvs_for_rescan();
  return (0, $err, $counts) unless $ok;

  ($ok, $err) = run_lib_helper('rebuild_cache_rails_for_rescan');
  return (0, $err, $counts) unless $ok;

  clear_runtime_state_for_rescan();
  fork_queue_rescan();
  return (1, '', $counts);
}

# ── Request handler ──────────────────────────────────────────────────────

sub handle_request {
  my ($client) = @_;

  # Read request line
  my $req_line = <$client>;
  return unless $req_line;
  $req_line =~ s/\r?\n$//;
  my ($method, $uri) = split /\s+/, $req_line;
  return unless $method && $uri;

  # Read headers
  my $content_length = 0;
  while (my $header = <$client>) {
    $header =~ s/\r?\n$//;
    last if $header eq '';
    if ($header =~ /^content-length:\s*(\d+)/i) {
      $content_length = $1;
    }
  }

  # Read body
  my $body = '';
  if ($content_length > 0) {
    read($client, $body, $content_length);
  }

  # Split route and query
  my ($route, $qs) = split /\?/, $uri, 2;
  my %q = parse_query($qs);
  my $offset = int($q{offset} || 0);
  my $limit  = int($q{limit}  || 50);
  $limit = 200 if $limit > 200;

  # ── Routes ───────────────────────────────────────────────────────────

  if ($method eq 'GET' && $route eq '/') {
    if (-f $GUI_FILE) {
      open my $fh, '<', $GUI_FILE or do { send_response($client, '500 Error', 'text/plain', 'Cannot read GUI'); return; };
      local $/;
      my $html = <$fh>;
      close $fh;
      send_response($client, '200 OK', 'text/html; charset=utf-8', $html);
    } else {
      send_response($client, '404 Not Found', 'text/plain', 'GUI not found');
    }
  }

  elsif ($method eq 'GET' && ($route eq '/icon.png' || $route eq '/favicon.png')) {
    my $fname = $route eq '/favicon.png' ? 'favicon.png' : 'icon.png';
    my $icon_file = "$SCRIPT_DIR/$fname";
    if (-f $icon_file) {
      open my $fh, '<:raw', $icon_file or do { send_response($client, '500 Error', 'text/plain', 'Cannot read icon'); return; };
      local $/;
      my $data = <$fh>;
      close $fh;
      send_response($client, '200 OK', 'image/png', $data);
    } else {
      send_response($client, '404 Not Found', 'text/plain', 'Icon not found');
    }
  }

  elsif ($method eq 'GET' && $route eq '/api/status') {
    my $gpu_q  = (vcli('LLEN', 'tc:lb:gpu:ready') || 0) + (vcli('LLEN', 'tc:lb:gpu:import:ready') || 0);
    my $cpu_q  = (vcli('LLEN', 'tc:lb:cpu:ready') || 0) + (vcli('LLEN', 'tc:lb:cpu:import:ready') || 0);
    my $gpu_w  = vcli('LLEN', 'tc:dispatch:gpu:processing') || 0;
    my $cpu_w  = vcli('LLEN', 'tc:dispatch:cpu:processing') || 0;
    my $probe_w = vcli('GET', 'tc:pool:probe:active') || 0;
    my $disk_w  = vcli('GET', 'tc:pool:disk:active') || 0;
    my $paused_raw = vcli('GET', 'tc:pause') || '';
    my $paused = $paused_raw eq '1' ? 1 : 0;

    my $processed = 0;
    if (-f "$STATE_DIR/processed.tsv") {
      $processed = `wc -l < "$STATE_DIR/processed.tsv" 2>/dev/null`;
      chomp $processed; $processed =~ s/\s//g; $processed ||= 0;
    }
    my $failed = 0;
    if (-f "$STATE_DIR/failed-files.tsv") {
      $failed = `wc -l < "$STATE_DIR/failed-files.tsv" 2>/dev/null`;
      chomp $failed; $failed =~ s/\s//g; $failed ||= 0;
    }
    my $flagged = 0;
    if (-f "$STATE_DIR/flagged-files.tsv") {
      $flagged = `wc -l < "$STATE_DIR/flagged-files.tsv" 2>/dev/null`;
      chomp $flagged; $flagged =~ s/\s//g; $flagged ||= 0;
    }

    # Active jobs
    my @active;
    for my $pq ('tc:dispatch:gpu:processing', 'tc:dispatch:cpu:processing') {
      my $wtype = $pq =~ /cpu/ ? 'cpu' : 'gpu';
      my @items = split /\n/, (vcli('LRANGE', $pq, 0, -1) || '');
      for my $item (@items) {
        next unless $item;
        my @f = split /\|/, $item;
        my $svc   = json_escape($f[0] // '');
        my ($fname) = ($f[1] // '') =~ m{([^/]+)$};
        $fname = json_escape($fname // '');
        my $disk  = json_escape($f[6] // '?');
        # Route (field 11) tells the GUI whether this came from a .job
        # file (import) vs bulk scan vs user-tagged direct, so it can
        # render the same arr-origin badge as the queue tabs.
        my $route = json_escape($f[10] // 'bulk');
        # Look up phase
        my $path = $f[1] // '';
        my $phash = md5_hex($path);
        my $phase = 'queued';
        if ($phash) {
          my $p = vcli('GET', "tc:worker:phase:$phash");
          $phase = $p if $p;
        }
        push @active, qq({"service":"$svc","file":"$fname","disk":"$disk","type":"$wtype","phase":"$phase","route":"$route"});
      }
    }
    for my $item (split /\n/, (vcli('LRANGE', 'tc:lang:processing', 0, -1) || '')) {
      next unless $item;
      my @f = split /\|/, $item;
      my $svc = json_escape($f[0] // '');
      my ($fname) = ($f[1] // '') =~ m{([^/]+)$};
      $fname = json_escape($fname // '');
      my $disk = json_escape($f[3] // '?');
      push @active, qq({"service":"$svc","file":"$fname","disk":"$disk","type":"lang","phase":"detecting language","route":"language"});
    }
    my $active_json = join(',', @active);

    # Disks (names from space monitor keys)
    my @disk_keys = split /\n/, (vcli('KEYS', 'tc:disk:*:space_ok') || '');
    my @disks;
    for my $dk (@disk_keys) {
      if ($dk =~ /^tc:disk:(.+):space_ok$/) {
        my $disk = $1;
        push @disks, $disk if $disk =~ /^disk\d+$/;
      }
    }
    @disks = disk_sort(@disks);
    my $disks_json = join(',', map { qq("$_") } @disks);
    my @ignored_disks = runtime_ignored_disks();
    my $ignored_disks_json = join(',', map { qq("$_") } @ignored_disks);

    # Per-disk queue counts for the requested queue (gpu, cpu, or both)
    my $qfilter = $q{queue} || '';
    my @count_queues;
    if ($qfilter eq 'gpu')    { @count_queues = ('tc:lb:gpu:ready', 'tc:lb:gpu:import:ready'); }
    elsif ($qfilter eq 'cpu') { @count_queues = ('tc:lb:cpu:ready', 'tc:lb:cpu:import:ready'); }
    else                      { @count_queues = ('tc:lb:gpu:ready', 'tc:lb:gpu:import:ready', 'tc:lb:cpu:ready', 'tc:lb:cpu:import:ready'); }

    my %dcounts;
    for my $cq (@count_queues) {
      my @citems = split /\n/, (vcli('LRANGE', $cq, 0, -1) || '');
      for my $ci (@citems) {
        my @cf = split /\|/, $ci;
        my $dd = $cf[6] // '';
        $dcounts{$dd}++ if $dd;
      }
    }
    my @dc_pairs;
    for my $d (sort keys %dcounts) {
      push @dc_pairs, qq("$d":$dcounts{$d});
    }
    my $dc_json = join(',', @dc_pairs);

    my $w_queue   = "${gpu_q}  |  ${cpu_q}";
      my $w_workers = "${gpu_w}  |  ${cpu_w}";
      my $w_stats   = "${processed}  |  ${failed}";

      # Temp-dir availability — the GUI uses this to hide the whole
      # tmp-dir settings cluster when no bind mount is present. A
      # missing directory means compose didn't wire /tmp-transcode, so
      # enabling the toggle in the GUI would be meaningless.
      my $tmp_dir_path = '/tmp-transcode';
      my $tmp_dir_available = (-d $tmp_dir_path) ? 'true' : 'false';

    # Direct queue state (new in v2.0)
    my $direct_active = (vcli('GET', 'tc:direct:active') || '') eq '1' ? 1 : 0;
    my $gpu_direct_queue = (vcli('LLEN', 'tc:lb:gpu:direct:ready') || 0) + 0;
    my $cpu_direct_queue = (vcli('LLEN', 'tc:lb:cpu:direct:ready') || 0) + 0;

    # ── Scan Activity (Phase 2 UI) ────────────────────────────────────────
    # Reads the progress file written by queue.sh + Valkey counters and
    # delegates the presentation logic to compute_scan_status() (pure,
    # unit-tested in tests/scan-status-derivation-unit.sh).
    my ($scan_pf_phase, $scan_pf_status) = ("", "");
    my $scan_progress_file = "$STATE_DIR/progress.txt";
    if (-f $scan_progress_file) {
      if (open my $sfh, '<', $scan_progress_file) {
        while (my $line = <$sfh>) {
          chomp $line;
          $scan_pf_phase  = $1 if $line =~ /^phase:\s*(.*)/;
          $scan_pf_status = $1 if $line =~ /^status:\s*(.*)/;
        }
        close $sfh;
      }
    }

    my $scan_pushed_total   = (vcli('GET',  'tc:scan:pushed_total')      || 0) + 0;
    # scan_ready_len is the visible "candidates remaining" depth. After
    # Phase 7, candidates traverse two stages: raw (tc:candidates:ready)
    # then resolved (tc:candidates:resolved:ready). Sum both so the UI
    # does not report "scan idle" while ffprobe has a resolved backlog.
    # Import variants stay out of the counter — no GUI surface today,
    # and including them would inflate the visible scan size for items
    # that bypass the bulk Arr scan flow.
    my $scan_ready_len = ((vcli('LLEN', 'tc:candidates:ready')          || 0) + 0)
                       + ((vcli('LLEN', 'tc:candidates:resolved:ready') || 0) + 0);
    my $scan_processing_len = ((vcli('LLEN', 'tc:candidates:processing')          || 0) + 0)
                            + ((vcli('LLEN', 'tc:candidates:resolved:processing') || 0) + 0);

    # Unified "verified" cache count — union of unique paths from BOTH
    # positive rails (narrow verified-hashes.tsv and broad
    # fully-classified-hashes.tsv). Same path with multiple historical
    # hashes counts once; a path present in both files counts once.
    # The two rails serve different gate decisions but represent the
    # same domain fact ("this file's status is known"), so the GUI
    # surfaces them as a single number. Defensive: missing files,
    # empty files, or parse failure all degrade to 0.
    my $verified_unique_count = 0;
    my $vfile  = "$STATE_DIR/verified-hashes.tsv";
    my $fcfile = "$STATE_DIR/fully-classified-hashes.tsv";
    if (-f $vfile || -f $fcfile) {
      my $vu_raw = `grep -hv '^#' "$vfile" "$fcfile" 2>/dev/null | cut -f1 | sort -u | wc -l` // '0';
      chomp $vu_raw;
      $vu_raw =~ s/\D//g;
      $verified_unique_count = ($vu_raw eq '') ? 0 : $vu_raw + 0;
      $verified_unique_count = 0 if $verified_unique_count < 0;
    }

    # Failed-hash row count (negative gate). Phase 5B added a `#`
    # policy header line to this rail's TSV (matching verified +
    # fully_classified), so the count is data rows = total lines
    # minus comment header. `grep -cv '^#'` is the same shape the
    # verified/broad rails use above.
    my $failed_hash_count = 0;
    my $fhfile = "$STATE_DIR/failed-hashes.tsv";
    if (-f $fhfile) {
      my $fhraw = `grep -cv '^#' "$fhfile" 2>/dev/null` // '0';
      chomp $fhraw;
      $fhraw =~ s/\D//g;
      $failed_hash_count = ($fhraw eq '') ? 0 : $fhraw + 0;
      $failed_hash_count = 0 if $failed_hash_count < 0;
    }

    # Skip-hit counters — INCR'd by entrypoint.sh on each successful
    # Skip-hit counters (tc:skip:verified_aac_lc / verified_fully_classified
    # / failed_hash) used to feed a "hits this restart" UI surface. The
    # display was removed once the Phase 5 cache machinery stabilized —
    # they're no longer load-bearing for anything user-visible. The
    # entrypoint still INCRs them on each gate skip (cheap fork-per-hit)
    # so debugging can resurrect the display without other plumbing.
    # See git history if you need the GET+JSON shape back.

    my $scan_result = compute_scan_status({
      pushed_total   => $scan_pushed_total,
      ready_len      => $scan_ready_len,
      processing_len => $scan_processing_len,
      pf_phase       => $scan_pf_phase,
      pf_status      => $scan_pf_status,
    });

    my $scan_phase_esc  = json_escape($scan_result->{phase});
    my $scan_status_esc = json_escape($scan_result->{status});
    my $scan_json =
      qq("scan":{) .
      qq("phase":"$scan_phase_esc",) .
      qq("status":"$scan_status_esc",) .
      qq("pending":$scan_result->{pending},) .
      qq("pushed_total":$scan_result->{pushed_total},) .
      qq("progress_pct":$scan_result->{progress_pct},) .
      qq("verified_unique_count":$verified_unique_count,) .
      qq("failed_hash_count":$failed_hash_count,) .
      qq("indeterminate":$scan_result->{indeterminate}) .
      qq(});

      send_json($client,
      "{\"gpu_queue\":$gpu_q,\"cpu_queue\":$cpu_q,\"gpu_workers\":$gpu_w,\"cpu_workers\":$cpu_w,\"probe_workers\":$probe_w,\"disk_workers\":$disk_w,\"paused\":$paused,\"processed\":$processed,\"failed\":$failed,\"flagged\":$flagged,\"widget_queue\":\"$w_queue\",\"widget_workers\":\"$w_workers\",\"widget_stats\":\"$w_stats\",\"disks\":[$disks_json],\"ignored_disks\":[$ignored_disks_json],\"disk_counts\":{$dc_json},\"active\":[$active_json],\"tmp_dir_available\":$tmp_dir_available,\"tmp_dir_path\":\"$tmp_dir_path\",\"direct_active\":$direct_active,\"gpu_direct_queue\":$gpu_direct_queue,\"cpu_direct_queue\":$cpu_direct_queue,$scan_json}");
  }

  elsif ($method eq 'GET' && $route eq '/api/queue/gpu') {
    send_json($client, serve_queue(['tc:lb:gpu:import:ready', 'tc:lb:gpu:ready'], $offset, $limit, $q{disk}, $q{q}));
  }

  elsif ($method eq 'GET' && $route eq '/api/queue/cpu') {
    send_json($client, serve_queue(['tc:lb:cpu:import:ready', 'tc:lb:cpu:ready'], $offset, $limit, $q{disk}, $q{q}));
  }

  elsif ($method eq 'GET' && $route eq '/api/direct') {
    my $offset = ($q{offset} // 0) + 0;
    my $limit  = ($q{limit}  // 50) + 0;
    # GPU direct first (display priority — matches existing import-first ordering)
    send_json($client, serve_queue(
      ['tc:lb:gpu:direct:ready', 'tc:lb:cpu:direct:ready'],
      $offset, $limit, undef
    ));
  }

  elsif ($method eq 'GET' && $route eq '/api/tsv/processed') {
    # processed.tsv columns: path, svc, mode, vcodec, ch, origSize, outSize, ts, disk
    send_json($client, serve_tsv("$STATE_DIR/processed.tsv", $offset, $limit, $q{disk}, 8, $q{q}));
  }

  elsif ($method eq 'GET' && $route eq '/api/tsv/failed') {
    # failed-files.tsv columns: ts, svc, reason, path, vcodec, ch, origSize, disk, failure_class
    send_json($client, serve_failed_tsv($offset, $limit, $q{disk}, $q{q}));
  }

  elsif ($method eq 'POST' && $route eq '/api/tsv/failed/clear') {
    my ($ok, $err, $cleared) = clear_failed_tsv();
    if ($ok) {
      send_json($client, qq({"ok":1,"cleared":$cleared}));
    } else {
      send_response($client, '500 Internal Server Error', 'application/json',
        '{"error":"' . json_escape($err) . '"}');
    }
  }

  elsif ($method eq 'GET' && $route eq '/api/tsv/flagged') {
    # flagged-files.tsv columns: ts, svc, reason, path, detail (no disk
    # column, so no disk filter applies). Reason filters are generated
    # dynamically from the TSV's reason column.
    #
    # Phase 7-followup: the durable TSV is regenerated from the Valkey
    # tc:flags:by_path hash on first read after any mutation. Chain:
    #   1. ready = GET tc:flags:ready
    #   2. if ready, dirty = GET tc:flags:tsv_dirty
    #   3. if dirty, success-gated snapshot HSET -> TSV, then DEL dirty
    #   4. serve_tsv as before
    # Empty HSET is a valid "zero flagged files" state — never fall
    # back when ready=1. Only fall back to the on-disk TSV as-is when
    # ready != "1" (boot pre-rebuild, Valkey error).
    my $flag_ready = vcli('GET', 'tc:flags:ready') // '';
    if ($flag_ready eq '1') {
      my $flag_dirty = vcli('GET', 'tc:flags:tsv_dirty') // '';
      if ($flag_dirty eq '1') {
        # Race-safe dirty-bit dance: CLEAR dirty BEFORE running the
        # snapshot. Any worker mutation that lands during HVALS will
        # re-SET dirty=1 in set_path_flags/clear_path_flags, and the
        # next read picks up the change. If we instead cleared AFTER
        # success, a mutation between HVALS and the DEL would be
        # silently dropped — its dirty bit overwritten by our DEL.
        vcli_safe('DEL', 'tc:flags:tsv_dirty');
        my $snap_cmd = 'source "' . $Bin . '/transcodarr-lib.sh" 2>/dev/null && snapshot_flag_index_to_tsv';
        my $rc       = system('bash', '-c', $snap_cmd);
        if ($rc != 0) {
          # Snapshot failed (e.g. Valkey blip). Restore the dirty
          # bit so the next read retries; durable TSV is unchanged
          # because snapshot_flag_index_to_tsv is success-gated.
          vcli_safe('SET', 'tc:flags:tsv_dirty', '1');
        }
      }
    }
    send_json($client, serve_tsv("$STATE_DIR/flagged-files.tsv", $offset, $limit, undef, undef, $q{q}, $q{reason}, 2));
  }

  elsif ($method eq 'POST' && $route eq '/api/flagged/snapshot') {
    # Admin endpoint — force a HSET -> TSV snapshot regardless of the
    # dirty bit. Same race-safe dirty-bit handling as the lazy path:
    # DEL dirty first, snapshot, restore dirty on failure. Concurrent
    # worker mutations re-SET dirty during the HVALS window and are
    # picked up by the next read.
    vcli_safe('DEL', 'tc:flags:tsv_dirty');
    my $snap_cmd = 'source "' . $Bin . '/transcodarr-lib.sh" 2>/dev/null && snapshot_flag_index_to_tsv';
    my $rc       = system('bash', '-c', $snap_cmd);
    if ($rc != 0) {
      vcli_safe('SET', 'tc:flags:tsv_dirty', '1');
    }
    send_json($client, sprintf('{"ok":%d}', $rc == 0 ? 1 : 0));
  }

  elsif ($method eq 'POST' && $route eq '/api/tsv/flagged/clear') {
    my ($ok, $err, $cleared) = clear_flagged_tsv();
    if ($ok) {
      send_json($client, qq({"ok":1,"cleared":$cleared}));
    } else {
      send_response($client, '500 Internal Server Error', 'application/json',
        '{"error":"' . json_escape($err) . '"}');
    }
  }

  elsif ($method eq 'GET' && $route eq '/api/config') {
    my $file = "$STATE_DIR/config.json";
    if (-f $file) {
      my $cfg = apply_runtime_integration_defaults(read_config_object_from($file));
      send_json($client, encode_json($cfg));
    } else {
      send_json($client, '{"error":"config.json not found"}');
    }
  }

  elsif ($method eq 'GET' && $route eq '/api/config/boot') {
    my $file = "$STATE_DIR/config.boot.json";
    if (-f $file) {
      my $cfg = apply_runtime_integration_defaults(read_config_object_from($file));
      send_json($client, encode_json($cfg));
    } else {
      send_json($client, '{"error":"boot config not found"}');
    }
  }

  elsif ($method eq 'POST' && $route eq '/api/config') {
    if ($body eq '') {
      send_response($client, '400 Bad Request', 'application/json', '{"error":"empty body"}');
    } else {
      my $lock_file = "$STATE_DIR/config.save.lock";
      open my $lock_fh, '>>', $lock_file or do {
        send_response($client, '500 Internal Server Error', 'application/json', '{"error":"cannot lock config"}');
        return;
      };
      flock($lock_fh, LOCK_EX) or do {
        close $lock_fh;
        send_response($client, '500 Internal Server Error', 'application/json', '{"error":"cannot lock config"}');
        return;
      };

      my ($cfg, $save_body, $err) = prepare_config_save($body);
      if ($err) {
        close $lock_fh;
        send_response($client, '400 Bad Request', 'application/json',
          '{"error":"invalid config","detail":"' . json_escape($err) . '"}');
        return;
      }
      my $old_cfg = read_config_object();

      my $tmp_file = "$STATE_DIR/config.json.tmp.$$";
      open my $fh, '>', $tmp_file or do {
        close $lock_fh;
        send_response($client, '500 Internal Server Error', 'application/json', '{"error":"cannot write"}');
        return;
      };
      print $fh $save_body;
      close $fh;

      unless (sync_ignored_disks_from_config($cfg)) {
        unlink $tmp_file;
        close $lock_fh;
        send_response($client, '503 Service Unavailable', 'application/json', '{"error":"cannot sync ignored disks"}');
        return;
      }

      unless (rename $tmp_file, "$STATE_DIR/config.json") {
        unlink $tmp_file;
        sync_ignored_disks_from_config($old_cfg);
        close $lock_fh;
        send_response($client, '500 Internal Server Error', 'application/json', '{"error":"cannot write"}');
        return;
      }

      close $lock_fh;
      send_response($client, '200 OK', 'application/json', $save_body);
    }
  }

  elsif ($method eq 'POST' && $route eq '/api/integrations/test') {
    my ($req, $json_err) = decode_json_object($body);
    if ($json_err) {
      send_response($client, '400 Bad Request', 'application/json', '{"error":"invalid_json"}');
      return;
    }

    my $app = scalar_config_value($req->{app});
    my $integrations = ref($req->{integrations}) eq 'HASH'
      ? $req->{integrations}
      : apply_runtime_integration_defaults(read_config_object())->{integrations};
    my ($ok, $err, $detected) = test_integration_app($app, $integrations);
    send_json($client, encode_json({
      ok => $ok ? 1 : 0,
      app => lc($app),
      error => $err,
      detected => $detected,
    }));
  }

  # /api/disks removed — disk counts are now in /api/status?queue=gpu|cpu

  elsif ($method eq 'POST' && $route eq '/api/pause') {
    vcli('SET', 'tc:pause', 1);
    send_json($client, '{"paused":1}');
  }

  elsif ($method eq 'POST' && $route eq '/api/resume') {
    vcli('DEL', 'tc:pause');
    send_json($client, '{"paused":0}');
  }

  elsif ($method eq 'GET' && $route eq '/api/failure-policy/status') {
    send_response($client, '200 OK', 'application/json', run_policy_runner_json('status'));
  }

  elsif ($method eq 'POST' && $route eq '/api/failure-policy/run') {
    send_response($client, '200 OK', 'application/json', run_policy_runner_json('run'));
  }

  elsif ($method eq 'GET' && $route eq '/api/failure-policy/actions') {
    send_response($client, '200 OK', 'application/json', run_policy_runner_json('actions-json'));
  }

  elsif ($method eq 'GET' && $route eq '/api/failure-policy/preview') {
    send_response($client, '200 OK', 'application/json', run_policy_runner_json('preview-json'));
  }

  elsif ($method eq 'POST' && $route eq '/api/restart') {
    send_json($client, '{"restarting":true}');
    kill 'TERM', 1;
  }

  elsif ($method eq 'POST' && $route eq '/api/rescan') {
    my ($ok, $err, $counts) = clear_cache_and_rescan();
    if ($ok) {
      my $failed = $counts->{failed} // 0;
      my $flagged = $counts->{flagged} // 0;
      send_json($client, qq({"ok":1,"rescanning":1,"cleared_failed":$failed,"cleared_flagged":$flagged}));
    } else {
      send_response($client, '500 Internal Server Error', 'application/json',
        '{"error":"' . json_escape($err) . '"}');
    }
  }

  elsif ($method eq 'POST' && $route eq '/api/lang/rescan') {
    # Release eligible language failures through language detection.
    # Capability + enabled gated inside the helper (returns 0 released when
    # lang_backend is unusable). Admission-only: display rows stay until
    # detection resolves them. Startup remains no_eng_audio-only; manual
    # rescan also retries lang_undetected/lang_tag_failed/lang_requeue_failed after the operator
    # changes model/device/settings.
    my $released = run_lib_helper_capture('lang_rescan_language_failures');
    $released = 0 unless defined($released) && $released =~ /^\d+$/;
    send_json($client, qq({"ok":1,"released":$released}));
  }

  elsif ($method eq 'POST' && $route eq '/api/direct/tag') {
    my $req = decode_body($body);
    my $source_lane = $req->{source_lane} // '';
    my $payload     = $req->{payload}     // '';

    my %valid_source = map { $_ => 1 } (
      'tc:lb:gpu:ready', 'tc:lb:gpu:import:ready',
      'tc:lb:cpu:ready', 'tc:lb:cpu:import:ready'
    );
    unless ($valid_source{$source_lane} && length($payload)) {
      send_response($client, '400 Bad Request', 'application/json',
        '{"error":"invalid_request"}');
      return;
    }

    # Payload shape validation — reject malformed client input before
    # rewriting. Field count must be 11 or 12 (optional SSD lease); field
    # 11 (index 10) must be "bulk" or "import"; route must match source
    # lane kind (import lane ↔ import route, bulk lane ↔ bulk route).
    my @f = split /\|/, $payload;
    unless (@f == 11 || @f == 12) {
      send_response($client, '400 Bad Request', 'application/json',
        '{"error":"invalid_payload_fields"}');
      return;
    }
    my $current_route = $f[10] // '';
    my $expected_route = ($source_lane =~ /:import:ready$/) ? 'import' : 'bulk';
    unless ($current_route eq $expected_route) {
      send_response($client, '400 Bad Request', 'application/json',
        '{"error":"route_mismatch"}');
      return;
    }

    my $filepath = $f[1] // '';
    unless (length($filepath)) {
      send_response($client, '400 Bad Request', 'application/json',
        '{"error":"invalid_payload"}');
      return;
    }
    my $file_hash = md5_hex($filepath);
    my $meta_key  = "tc:direct:meta:$file_hash";

    # Derive direct_lane (gpu ↔ gpu, cpu ↔ cpu)
    my $direct_lane = ($source_lane =~ /^tc:lb:gpu:/)
      ? 'tc:lb:gpu:direct:ready'
      : 'tc:lb:cpu:direct:ready';

    # Rewrite field 11 (index 10) = "direct". Field 12 (if present) rides
    # through unchanged.
    $f[10] = 'direct';
    my $rewritten = join('|', @f);

    # EVAL preloaded Lua. All args client-derived → vcli_safe only.
    my $rv = vcli_safe('EVAL', $LUA_DIRECT_TAG, '3',
      $source_lane, $direct_lane, $meta_key,
      $payload, $rewritten, $source_lane);

    if ($rv eq '1') {
      send_json($client, '{"ok":1}');
    } else {
      send_response($client, '404 Not Found', 'application/json',
        '{"error":"not_found"}');
    }
  }

  elsif ($method eq 'POST' && $route eq '/api/direct/untag') {
    my $req = decode_body($body);
    my $direct_lane = $req->{direct_lane} // '';
    my $payload     = $req->{payload}     // '';

    my %valid_direct = map { $_ => 1 } (
      'tc:lb:gpu:direct:ready', 'tc:lb:cpu:direct:ready'
    );
    unless ($valid_direct{$direct_lane} && length($payload)) {
      send_response($client, '400 Bad Request', 'application/json',
        '{"error":"invalid_request"}');
      return;
    }

    # Payload shape validation — same checks as tag, inverted: field 11
    # must be "direct" (that's what's in the direct lane).
    my @f = split /\|/, $payload;
    unless (@f == 11 || @f == 12) {
      send_response($client, '400 Bad Request', 'application/json',
        '{"error":"invalid_payload_fields"}');
      return;
    }
    unless (($f[10] // '') eq 'direct') {
      send_response($client, '400 Bad Request', 'application/json',
        '{"error":"route_mismatch"}');
      return;
    }

    my $filepath = $f[1] // '';
    unless (length($filepath)) {
      send_response($client, '400 Bad Request', 'application/json',
        '{"error":"invalid_payload"}');
      return;
    }
    my $file_hash = md5_hex($filepath);
    my $meta_key  = "tc:direct:meta:$file_hash";

    # Read meta to find restore lane; NEVER guess.
    my $restore_lane = vcli_safe('GET', $meta_key);
    my %valid_restore = map { $_ => 1 } (
      'tc:lb:gpu:ready', 'tc:lb:gpu:import:ready',
      'tc:lb:cpu:ready', 'tc:lb:cpu:import:ready'
    );
    unless ($valid_restore{$restore_lane}) {
      send_response($client, '404 Not Found', 'application/json',
        '{"error":"meta_missing"}');
      return;
    }

    # Derive original route from restore lane: ":import:ready" → import, else bulk.
    # Field 12 (SSD lease) if present rides through unchanged.
    my $original_route = ($restore_lane =~ /:import:ready$/) ? 'import' : 'bulk';
    $f[10] = $original_route;
    my $rewritten = join('|', @f);

    my $rv = vcli_safe('EVAL', $LUA_DIRECT_UNTAG, '3',
      $direct_lane, $restore_lane, $meta_key,
      $payload, $rewritten);

    if ($rv eq '1') {
      send_json($client, '{"ok":1}');
    } else {
      send_response($client, '404 Not Found', 'application/json',
        '{"error":"not_found"}');
    }
  }

  elsif ($method eq 'POST' && $route eq '/api/direct/start') {
    # 409 if both direct LB queues are empty — never set the flag on an
    # empty list. Uses vcli_safe throughout per the /api/direct/* review gate.
    my $gpu_len = (vcli_safe('LLEN', 'tc:lb:gpu:direct:ready') || 0) + 0;
    my $cpu_len = (vcli_safe('LLEN', 'tc:lb:cpu:direct:ready') || 0) + 0;
    if ($gpu_len == 0 && $cpu_len == 0) {
      send_response($client, '409 Conflict', 'application/json',
        '{"error":"empty"}');
      return;
    }
    vcli_safe('SET', 'tc:direct:active', '1');
    send_json($client, '{"ok":1}');
  }

  elsif ($method eq 'POST' && $route eq '/api/direct/stop') {
    vcli_safe('DEL', 'tc:direct:active');
    send_json($client, '{"ok":1}');
  }

  elsif ($method eq 'GET' && $route eq '/health') {
    send_response($client, '200 OK', 'text/plain', 'OK');
  }

  # ── Encoder capability probe results ─────────────────────────────────────
  # Returned as JSON object whose keys are encoder names and values are
  # "1" (available) or "0" (unavailable). Populated by the boot-time probe
  # (transcodarr-probe-capabilities.sh). The GUI uses this to hide
  # target_codec options that would fail at encode time.
  #
  # Endpoint is cheap — single HGETALL against a small hash. GUI fetches
  # it once on page load, doesn't poll.
  elsif ($method eq 'GET' && $route eq '/api/capabilities') {
    my $raw = `valkey-cli HGETALL tc:capabilities 2>/dev/null`;
    my @lines = split /\n/, $raw;
    my @pairs;
    my $json_pair = sub {
      my ($k, $v) = @_;
      return '"' . json_escape($k) . '":"' . json_escape($v) . '"';
    };
    for (my $i = 0; $i + 1 < @lines; $i += 2) {
      my $k = $lines[$i];
      my $v = $lines[$i + 1];
      push @pairs, $json_pair->($k, $v);
    }

    my $model_dir = $ENV{TRANSCODARR_LANGUAGE_MODEL_DIR} || '/models';
    my $model_host_dir = $ENV{TRANSCODARR_LANGUAGE_MODEL_HOST_DIR} || $model_dir;
    my @models;
    if (opendir(my $dh, $model_dir)) {
      while (defined(my $entry = readdir($dh))) {
        next unless $entry =~ /^ggml-(.+)\.bin$/;
        push @models, $1;
      }
      closedir($dh);
    }
    my @lang_model_order = qw(tiny tiny.en base base.en small small.en medium medium.en large-v1 large-v2 large-v3 large large-v3-turbo turbo);
    my %lang_model_rank = map { $lang_model_order[$_] => $_ } 0 .. $#lang_model_order;
    @models = sort {
      ($lang_model_rank{$a} // 1000) <=> ($lang_model_rank{$b} // 1000)
        || lc($a) cmp lc($b)
    } @models;
    push @pairs, $json_pair->('lang_models', join(',', @models));
    push @pairs, $json_pair->('lang_model_dir', $model_dir);
    push @pairs, $json_pair->('lang_model_host_dir', $model_host_dir);
    send_json($client, '{' . join(',', @pairs) . '}');
  }

  else {
    send_response($client, '404 Not Found', 'application/json', '{"error":"not found"}');
  }
}

# ── Server ───────────────────────────────────────────────────────────────

my $server = IO::Socket::INET->new(
  LocalPort => $PORT,
  Type      => SOCK_STREAM,
  Reuse     => 1,
  Listen    => 20,
) or die "Cannot start server on port $PORT: $!\n";

print STDERR "[api] Starting on port $PORT\n";

while (1) {
  my $client = $server->accept();
  unless ($client) {
    next if $!{EINTR};  # Interrupted by SIGCHLD — retry
    warn "Accept failed: $!";
    next;
  }
  my $pid = fork();
  if (!defined $pid) {
    warn "Fork failed: $!";
    close $client;
    next;
  }
  if ($pid == 0) {
    # Child
    close $server;
    eval { handle_request($client); };
    warn "Request error: $@" if $@;
    close $client;
    exit 0;
  }
  # Parent
  close $client;
}
