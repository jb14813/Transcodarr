#!/usr/bin/perl
use strict;
use warnings;
use JSON::PP qw(decode_json encode_json);
use Digest::MD5 qw(md5_hex);
use Fcntl qw(:flock);
use POSIX qw(strftime);

my $STATE_DIR = $ENV{TRANSCODARR_STATE_DIR} || '/state';
my $CONFIG_FILE = "$STATE_DIR/config.json";
my $FAILED_FILE = "$STATE_DIR/failed-files.tsv";
my $ACTIONS_FILE = "$STATE_DIR/failure-policy-actions.tsv";
my $ACTIONS_TMP_FILE = $ENV{TRANSCODARR_FAILURE_POLICY_ACTIONS_TMP} || '';
my $LOCK_FILE = "$STATE_DIR/failure-policy-actions.lock";
my $NOW = $ENV{TRANSCODARR_POLICY_TEST_NOW} || time;

sub json_bool {
  my ($v) = @_;
  return $v ? JSON::PP::true : JSON::PP::false;
}

sub slurp_json {
  my ($file) = @_;
  return {} unless -f $file;
  open(my $fh, '<', $file) or return {};
  local $/;
  my $raw = <$fh>;
  close $fh;
  my $decoded = eval { decode_json($raw || '{}') };
  return ref($decoded) eq 'HASH' ? $decoded : {};
}

sub now_iso {
  return strftime('%Y-%m-%dT%H:%M:%S%z', localtime($NOW));
}

sub default_policy_config {
  return {
    enabled => JSON::PP::false,
    dry_run => JSON::PP::false,
    rules => {
      commentary_only => { enabled => JSON::PP::true, failure_class => 'policy_skip', match => { reason => 'commentary_only' }, destructive => JSON::PP::true, actions => ['arr_blocklist','delete_file','arr_rescan'] },
      wrong_language_policy_skip => { enabled => JSON::PP::false, failure_class => 'policy_skip', match => { reason_prefix => 'wrong_lang_' }, destructive => JSON::PP::true, actions => ['arr_blocklist','delete_file','arr_rescan'] },
      missing_preferred_audio_policy_skip => { enabled => JSON::PP::false, failure_class => 'policy_skip', match => { reason_prefix => 'no_', reason_suffix => '_audio' }, destructive => JSON::PP::true, actions => ['arr_blocklist','delete_file','arr_rescan'] },
      input_invalid => { enabled => JSON::PP::false, failure_class => 'input_invalid', destructive => JSON::PP::true, actions => ['arr_blocklist','delete_file','arr_rescan'] },
      corrupt_source => { enabled => JSON::PP::false, failure_class => ['corrupt_input','mkv_corrupt','dts_non_monotonic','illegal_reordering'], destructive => JSON::PP::true, actions => ['arr_blocklist','delete_file','arr_rescan'] },
      validation_duration_mismatch => { enabled => JSON::PP::true, failure_class => 'validation_failure', match => { reason => 'validation_duration_mismatch' }, destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
      validation_failure => { enabled => JSON::PP::true, failure_class => 'validation_failure', destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
      stream_map_invalid => { enabled => JSON::PP::true, failure_class => 'stream_map_invalid', destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
      config_error => { enabled => JSON::PP::true, failure_class => 'config_error', destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
      worker_crash => { enabled => JSON::PP::true, failure_class => 'worker_crash', destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
      output_missing => { enabled => JSON::PP::true, failure_class => 'output_missing', destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
      output_failure => { enabled => JSON::PP::true, failure_class => 'output_failure', destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
      rename_failure => { enabled => JSON::PP::true, failure_class => 'rename_failure', destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
      unknown => { enabled => JSON::PP::true, failure_class => 'unknown', destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
      environment => { enabled => JSON::PP::true, failure_class => ['input_missing','quarantine','quarantine_failed','disk_full','permission_denied','file_not_found','out_of_memory','encoder_open_failed','hw_init_failed','codec_not_supported','filter_format_mismatch','filter_reinit_failed','no_packets_written','mux_queue_overflow','container_limit','subtitle_incompatible','conversion_failed'], destructive => JSON::PP::false, actions => ['record_diagnostic','mark_needs_review'] },
    },
  };
}

sub config_bool_checked {
  my ($value, $default, $explicit) = @_;
  return (1, $default) unless $explicit;
  return (0, 0) unless defined $value;
  if (ref($value) eq 'JSON::PP::Boolean') {
    return (1, $value ? 1 : 0);
  }
  return (0, 0);
}

sub config_bool {
  my ($value, $default, $explicit) = @_;
  my ($ok, $bool) = config_bool_checked($value, $default, $explicit);
  return $ok ? $bool : 0;
}

sub config_bool_valid {
  my ($value, $default, $explicit) = @_;
  my ($ok, $bool) = config_bool_checked($value, $default, $explicit);
  return $ok;
}

sub configured_actions {
  my ($raw, $explicit) = @_;
  my %known = map { $_ => 1 } qw(arr_blocklist delete_file arr_rescan record_diagnostic mark_needs_review);
  my @default = qw(arr_blocklist delete_file arr_rescan);
  return (1, @default) unless $explicit;
  return (0, 'invalid_actions_config') unless ref($raw) eq 'ARRAY';
  my %seen;
  my @out;
  for my $action (@$raw) {
    return (0, 'invalid_actions_config') unless defined $action && $known{$action};
    next if $seen{$action}++;
    push @out, $action;
  }
  return (1, @out);
}

sub rule_is_destructive {
  my ($rule) = @_;
  return config_bool($rule->{destructive}, 0, exists($rule->{destructive})) ? 1 : 0;
}

sub rule_has_reason_match {
  my ($rule) = @_;
  my $match = ref($rule->{match}) eq 'HASH' ? $rule->{match} : {};
  return 1 if defined $match->{reason} && !ref($match->{reason}) && length($match->{reason});
  return 1 if defined $match->{reason_prefix} && !ref($match->{reason_prefix}) && length($match->{reason_prefix});
  return 1 if defined $match->{reason_suffix} && !ref($match->{reason_suffix}) && length($match->{reason_suffix});
  return 0;
}

sub destructive_match_ok {
  my ($rule_id, $rule) = @_;
  return 1 if rule_has_reason_match($rule);
  # Class-only destructive is intentionally limited to source-bad rules exposed
  # in Bad File Cleanup. Our-output/environment catch-alls stay observe-only even
  # if config.json is hand-edited, because they can cover many unrelated rows.
  my %class_only_destructive_allowed = map { $_ => 1 } qw(input_invalid corrupt_source);
  return 0 unless $class_only_destructive_allowed{$rule_id};
  my @classes = rule_failure_classes($rule);
  return 0 unless @classes;
  my $defaults = default_policy_config();
  my $default_rule = $defaults->{rules}->{$rule_id};
  return 0 unless ref($default_rule) eq 'HASH';
  return 0 if rule_has_reason_match($default_rule);
  my %ok = map { $_ => 1 } rule_failure_classes($default_rule);
  return 0 unless %ok;
  for my $c (@classes) { return 0 unless $ok{$c}; }
  return 1;
}

sub rule_failure_classes {
  my ($rule) = @_;
  my $fc = $rule->{failure_class};
  if (ref($fc) eq 'ARRAY') {
    return grep { defined($_) && !ref($_) && length($_) } @$fc;
  }
  return (defined($fc) && !ref($fc) && length($fc)) ? ($fc) : ();
}

sub validate_rule {
  my ($rule_id, $rule, $default_rule) = @_;
  if (ref($default_rule) eq 'HASH') {
    for my $key (qw(enabled failure_class match destructive actions)) {
      $rule->{$key} = $default_rule->{$key} unless exists $rule->{$key} || !defined $default_rule->{$key};
    }
  }
  unless (config_bool_valid($rule->{enabled}, 1, exists($rule->{enabled}))) {
    return "invalid_${rule_id}_enabled";
  }
  my $fc = $rule->{failure_class};
  if (ref($fc) eq 'ARRAY') {
    return "invalid_${rule_id}_failure_class" unless @$fc;
    for my $c (@$fc) {
      return "invalid_${rule_id}_failure_class" unless defined($c) && !ref($c) && length($c);
    }
  } else {
    return "invalid_${rule_id}_failure_class" unless defined($fc) && !ref($fc) && length($fc);
  }
  if (exists($rule->{match})) {
    return "invalid_${rule_id}_match" unless ref($rule->{match}) eq 'HASH';
    my $match = $rule->{match};
    if (exists($match->{reason})
        && !(defined($match->{reason}) && !ref($match->{reason}) && length($match->{reason}))) {
      return "invalid_${rule_id}_match_reason";
    }
    if (exists($match->{reason_prefix})
        && !(defined($match->{reason_prefix}) && !ref($match->{reason_prefix}) && length($match->{reason_prefix}))) {
      return "invalid_${rule_id}_match_reason_prefix";
    }
    if (exists($match->{reason_suffix})
        && !(defined($match->{reason_suffix}) && !ref($match->{reason_suffix}) && length($match->{reason_suffix}))) {
      return "invalid_${rule_id}_match_reason_suffix";
    }
  }
  if (exists($rule->{destructive}) && !config_bool_valid($rule->{destructive}, 0, 1)) {
    return "invalid_${rule_id}_destructive";
  }
  if (rule_is_destructive($rule) && !destructive_match_ok($rule_id, $rule)) {
    return "invalid_${rule_id}_destructive_match";
  }
  return undef;
}

sub policy_config {
  my $cfg = slurp_json($CONFIG_FILE);
  my $defaults = default_policy_config();
  my $policy = ref($cfg->{failure_policy}) eq 'HASH' ? $cfg->{failure_policy} : {};

  for my $key (qw(enabled dry_run)) {
    $policy->{$key} = $defaults->{$key} unless exists $policy->{$key};
  }
  if (exists($policy->{rules}) && ref($policy->{rules}) ne 'HASH') {
    $policy->{_invalid_rules} = 'invalid_rules_config';
    $policy->{rules} = {};
    return $policy;
  }
  $policy->{rules} = {} unless ref($policy->{rules}) eq 'HASH';

  my %rule_ids = map { $_ => 1 } (keys %{ $defaults->{rules} }, keys %{ $policy->{rules} });
  for my $rule_id (sort keys %rule_ids) {
    if (exists($policy->{rules}->{$rule_id}) && ref($policy->{rules}->{$rule_id}) ne 'HASH') {
      $policy->{_invalid_rules} = "invalid_${rule_id}_rule_config";
      return $policy;
    }
    $policy->{rules}->{$rule_id} = {} unless ref($policy->{rules}->{$rule_id}) eq 'HASH';
    my $err = validate_rule($rule_id, $policy->{rules}->{$rule_id}, $defaults->{rules}->{$rule_id});
    if ($err) {
      $policy->{_invalid_rules} = $err;
      return $policy;
    }
  }
  return $policy;
}

sub failed_key {
  my ($row) = @_;
  return md5_hex(join "\t",
    $row->{ts} // '',
    $row->{service} // '',
    $row->{reason} // '',
    $row->{path} // '',
    $row->{failure_class} // '');
}

sub policy_id_for {
  my ($key) = @_;
  return 'fp-' . substr($key, 0, 20);
}

sub read_failed_rows {
  return () unless -f $FAILED_FILE;
  open(my $fh, '<', $FAILED_FILE) or return ();
  my @rows;
  while (my $line = <$fh>) {
    chomp $line;
    next unless length $line;
    my @f = split /\t/, $line, -1;
    next unless @f >= 9;
    my %row = (
      ts => $f[0],
      service => $f[1],
      reason => $f[2],
      path => $f[3],
      vcodec => $f[4],
      channels => $f[5],
      original_size => $f[6],
      disk => $f[7],
      failure_class => $f[8],
    );
    $row{failed_key} = failed_key(\%row);
    push @rows, \%row;
  }
  close $fh;
  return @rows;
}

sub action_from_fields {
  my (@f) = @_;
  return undef unless @f >= 11;
  my $proof = eval { decode_json($f[10] || '{}') };
  $proof = {} unless ref($proof) eq 'HASH';
  return {
    policy_id => $f[0],
    failed_key => $f[1],
    failed_ts => $f[2],
    service => $f[3],
    reason => $f[4],
    path => $f[5],
    state => $f[6],
    attempt_count => int($f[7] || 0),
    next_attempt_ts => int($f[8] || 0),
    updated_ts => $f[9],
    proof => $proof,
  };
}

sub read_actions {
  return () unless -f $ACTIONS_FILE;
  open(my $fh, '<', $ACTIONS_FILE) or die "cannot read $ACTIONS_FILE: $!";
  my @actions;
  my $line_no = 0;
  while (my $line = <$fh>) {
    $line_no++;
    chomp $line;
    next unless length $line;
    my @f = split /\t/, $line, -1;
    my $action = action_from_fields(@f);
    die "invalid action row $line_no in $ACTIONS_FILE" unless $action;
    next if ($action->{state} // '') eq 'dry_run';
    push @actions, $action;
  }
  close $fh;
  return @actions;
}

sub action_line {
  my ($a) = @_;
  my $proof = encode_json($a->{proof} || {});
  $proof =~ s/[\r\n\t]/ /g;
  return join("\t",
    $a->{policy_id} // '',
    $a->{failed_key} // '',
    $a->{failed_ts} // '',
    $a->{service} // '',
    $a->{reason} // '',
    $a->{path} // '',
    $a->{state} // '',
    int($a->{attempt_count} || 0),
    int($a->{next_attempt_ts} || 0),
    $a->{updated_ts} // '',
    $proof);
}

sub write_actions {
  my (@actions) = @_;
  my $tmp = $ACTIONS_TMP_FILE || "$ACTIONS_FILE.tmp.$$";
  open(my $fh, '>', $tmp) or die "cannot write $tmp: $!";
  for my $a (@actions) {
    print $fh action_line($a), "\n";
  }
  close($fh) or die "cannot flush $tmp: $!";
  rename $tmp, $ACTIONS_FILE or die "cannot replace $ACTIONS_FILE: $!";
}

sub with_lock {
  my ($cb) = @_;
  open(my $lock_fh, '>>', $LOCK_FILE) or die "cannot lock $LOCK_FILE: $!";
  flock($lock_fh, LOCK_EX) or die "cannot lock $LOCK_FILE: $!";
  my $result = $cb->();
  close $lock_fh;
  return $result;
}

sub action_list_has_destructive {
  my (@actions) = @_;
  my %destructive = map { $_ => 1 } qw(arr_blocklist delete_file arr_rescan);
  return (grep { $destructive{$_} } @actions) ? 1 : 0;
}

sub rule_match_score {
  my ($rule, $row) = @_;
  my @classes = rule_failure_classes($rule);
  return 0 unless @classes;
  my $row_class = $row->{failure_class} // '';
  return 0 unless grep { $_ eq $row_class } @classes;
  my $match = ref($rule->{match}) eq 'HASH' ? $rule->{match} : {};
  my $reason = $row->{reason} // '';
  if (defined $match->{reason} && !ref($match->{reason}) && length($match->{reason})) {
    return ($reason eq $match->{reason}) ? 3 : 0;
  }
  my $has_reason_constraint = 0;
  if (defined $match->{reason_prefix} && !ref($match->{reason_prefix}) && length($match->{reason_prefix})) {
    $has_reason_constraint = 1;
    return 0 unless index($reason, $match->{reason_prefix}) == 0;
  }
  if (defined $match->{reason_suffix} && !ref($match->{reason_suffix}) && length($match->{reason_suffix})) {
    $has_reason_constraint = 1;
    return 0 unless length($reason) >= length($match->{reason_suffix});
    return 0 unless substr($reason, -length($match->{reason_suffix})) eq $match->{reason_suffix};
  }
  return $has_reason_constraint ? 2 : 1;
}

sub select_rule_for_row {
  my ($policy, $row) = @_;
  my $best;
  for my $rule_id (sort keys %{ $policy->{rules} }) {
    my $rule = $policy->{rules}->{$rule_id};
    next unless ref($rule) eq 'HASH';
    next unless config_bool($rule->{enabled}, 1, exists($rule->{enabled}));
    my $score = rule_match_score($rule, $row);
    next unless $score;
    if (rule_is_destructive($rule)) {
      next unless ($row->{service} // '') eq 'radarr' || ($row->{service} // '') eq 'sonarr';
    }
    $best = { id => $rule_id, rule => $rule, score => $score }
      if !$best || $score > $best->{score};
  }
  return $best ? ($best->{id}, $best->{rule}) : ();
}

sub matching_rows_with_rules {
  my ($policy) = @_;
  return () if $policy->{_invalid_rules};
  my @out;
  for my $row (read_failed_rows()) {
    my ($rule_id, $rule) = select_rule_for_row($policy, $row);
    next unless defined $rule_id;
    push @out, [$row, $rule_id, $rule];
  }
  return @out;
}

sub preview_matches {
  my ($policy) = @_;
  my @items;
  return \@items if $policy->{_invalid_rules};
  for my $row (read_failed_rows()) {
    my ($rule_id, $rule) = select_rule_for_row($policy, $row);
    next unless defined $rule_id;
    my ($actions_ok, @actions_or_err) = configured_actions($rule->{actions}, exists($rule->{actions}));
    push @items, {
      failed_key => $row->{failed_key}, failed_ts => $row->{ts}, service => $row->{service},
      reason => $row->{reason}, path => $row->{path}, failure_class => $row->{failure_class}, rule_id => $rule_id,
      enabled => json_bool(config_bool($rule->{enabled}, 1, exists($rule->{enabled}))),
      destructive => json_bool(rule_is_destructive($rule)),
      actions => $actions_ok ? \@actions_or_err : [], actions_valid => json_bool($actions_ok),
      actions_error => $actions_ok ? '' : $actions_or_err[0], diagnostic => action_diagnostic($row),
    };
  }
  return \@items;
}

sub action_diagnostic {
  my ($row) = @_;
  return {
    failure_class => $row->{failure_class} // '',
    vcodec        => $row->{vcodec} // '',
    channels      => $row->{channels} // '',
    original_size => $row->{original_size} // '',
    disk          => $row->{disk} // '',
  };
}

sub create_action_for_row {
  my ($row, $rule, $rule_id) = @_;
  my ($actions_ok, @actions_or_err) = configured_actions($rule->{actions}, exists($rule->{actions}));
  my @actions = $actions_ok ? @actions_or_err : ();
  my $proof = {
    rule_id    => $rule_id,
    diagnostic => action_diagnostic($row),
    actions    => \@actions,
  };
  my $state = 'pending';
  if (!$actions_ok) {
    $state = 'blocked';
    $proof->{message} = $actions_or_err[0];
  } elsif (action_list_has_destructive(@actions) && !rule_is_destructive($rule)) {
    $state = 'blocked';
    $proof->{message} = 'destructive_actions_not_allowed';
  }
  return {
    policy_id => policy_id_for($row->{failed_key}),
    failed_key => $row->{failed_key},
    failed_ts => $row->{ts},
    service => $row->{service},
    reason => $row->{reason},
    path => $row->{path},
    state => $state,
    attempt_count => 0,
    next_attempt_ts => 0,
    updated_ts => now_iso(),
    proof => $proof,
  };
}

sub curl_json {
  my (@args) = @_;
  my $url = pop @args;
  return (0, undef) unless defined($url) && length($url);
  open(my $fh, '-|', 'curl', '-sf', '--max-time', '60', @args, '--', $url) or return (0, undef);
  local $/;
  my $raw = <$fh>;
  my $ok = close($fh);
  return (0, undef) unless $ok;
  my $decoded = eval { decode_json($raw || '') };
  return $@ ? (0, undef) : (1, $decoded);
}

sub curl_ok {
  my (@args) = @_;
  my $url = pop @args;
  return 0 unless defined($url) && length($url);
  open(my $fh, '-|', 'curl', '-sf', '--max-time', '60', @args, '--', $url) or return 0;
  1 while <$fh>;
  return close($fh) ? 1 : 0;
}

sub arr_url_allowed {
  my ($url) = @_;
  return 0 unless defined($url) && length($url);
  return 0 unless $url =~ m{\Ahttps?://}i;
  my ($host) = $url =~ m{\Ahttps?://(\[[^\]]+\]|[^/:?#]+)}i;
  return 0 unless defined($host) && length($host);
  $host =~ s/\A\[//;
  $host =~ s/\]\z//;
  $host = lc($host);
  return 0 if $host eq 'localhost';
  return 0 if $host eq '0.0.0.0';
  return 0 if $host eq '::1';
  return 0 if $host =~ /\A127\./;
  return 0 if $host =~ /\A::ffff:127\./;
  return 0 if $host =~ /\A169\.254\./;
  return 0 if $host =~ /\Afe80:/;
  return 1;
}

sub arr_config {
  my ($service) = @_;
  my ($url, $key) = ('', '');
  if ($service eq 'radarr') {
    ($url, $key) = ($ENV{RADARR_URL} || '', $ENV{RADARR_API_KEY} || '');
  } elsif ($service eq 'sonarr') {
    ($url, $key) = ($ENV{SONARR_URL} || '', $ENV{SONARR_API_KEY} || '');
  } else {
    return ('', '');
  }
  $url =~ s{/+\z}{};
  return ('', '') unless arr_url_allowed($url);
  return ($url, $key);
}

sub same_path {
  my ($a, $b) = @_;
  return 0 unless defined $a && defined $b;
  $a =~ s{\\}{/}g;
  $b =~ s{\\}{/}g;
  return $a eq $b ? 1 : 0;
}

sub resolve_radarr {
  my ($action, $url, $key) = @_;
  my ($ok, $movies) = curl_json('-H', "X-Api-Key: $key", "$url/api/v3/movie");
  return (0, 'radarr_movie_query_failed') unless $ok && ref($movies) eq 'ARRAY';
  for my $movie (@$movies) {
    next unless ref($movie) eq 'HASH';
    my $mf = $movie->{movieFile};
    next unless ref($mf) eq 'HASH';
    next unless same_path($mf->{path}, $action->{path});
    return (1, {
      service => 'radarr',
      movieId => $movie->{id},
      mediaFileId => $mf->{id},
      mediaPath => $mf->{path},
    }) if $movie->{id} && $mf->{id};
  }
  return (0, 'radarr_media_file_not_found_by_path');
}

sub resolve_sonarr {
  my ($action, $url, $key) = @_;
  my ($ok, $rows) = curl_json('-H', "X-Api-Key: $key", "$url/api/v3/episodefile/transcodarr");
  if ($ok && ref($rows) eq 'ARRAY') {
    for my $row (@$rows) {
      next unless ref($row) eq 'HASH';
      next unless same_path($row->{path}, $action->{path});
      return (1, {
        service => 'sonarr',
        seriesId => $row->{seriesId},
        mediaFileId => $row->{id},
        mediaPath => $row->{path},
      }) if $row->{id} && $row->{seriesId};
    }
  }

  my ($series_ok, $series) = curl_json('-H', "X-Api-Key: $key", "$url/api/v3/series");
  return (0, 'sonarr_series_query_failed') unless $series_ok && ref($series) eq 'ARRAY';
  for my $s (@$series) {
    next unless ref($s) eq 'HASH' && $s->{id};
    my ($ef_ok, $episode_files) = curl_json('-H', "X-Api-Key: $key", "$url/api/v3/episodefile?seriesId=$s->{id}");
    next unless $ef_ok && ref($episode_files) eq 'ARRAY';
    for my $ef (@$episode_files) {
      next unless ref($ef) eq 'HASH';
      next unless same_path($ef->{path}, $action->{path});
      return (1, {
        service => 'sonarr',
        seriesId => $s->{id},
        mediaFileId => $ef->{id},
        mediaPath => $ef->{path},
      }) if $ef->{id};
    }
  }
  return (0, 'sonarr_media_file_not_found_by_path');
}

sub find_history_record {
  my ($action, $url, $key, $resolved) = @_;
  my $query = $resolved->{service} eq 'radarr'
    ? "movieIds=$resolved->{movieId}"
    : "seriesIds=$resolved->{seriesId}";
  my ($ok, $history) = curl_json('-H', "X-Api-Key: $key", "$url/api/v3/history?page=1&pageSize=100&sortKey=date&sortDirection=descending&$query");
  return (0, 'history_query_failed') unless $ok && ref($history) eq 'HASH';
  my $records = $history->{records};
  return (0, 'history_records_missing') unless ref($records) eq 'ARRAY';
  for my $rec (@$records) {
    next unless ref($rec) eq 'HASH' && $rec->{id};
    my $data = ref($rec->{data}) eq 'HASH' ? $rec->{data} : {};
    my @paths = grep { defined && length } (
      $data->{importedPath}, $data->{droppedPath}, $data->{path}, $data->{sourcePath},
    );
    for my $p (@paths) {
      return (1, {
        id => $rec->{id},
        sourceTitle => $rec->{sourceTitle} // '',
        downloadId => $rec->{downloadId} // '',
        date => $rec->{date} // '',
      }) if same_path($p, $action->{path});
    }
  }
  return (0, 'matching_history_record_not_found');
}

sub blocklist_record_matches_history {
  my ($rec, $history, $resolved) = @_;
  return 0 unless ref($rec) eq 'HASH' && ref($history) eq 'HASH' && ref($resolved) eq 'HASH';
  if (($resolved->{service} || '') eq 'radarr') {
    return 0 unless ($rec->{movieId} || 0) == ($resolved->{movieId} || -1);
  } elsif (($resolved->{service} || '') eq 'sonarr') {
    return 0 unless ($rec->{seriesId} || 0) == ($resolved->{seriesId} || -1);
  } else {
    return 0;
  }
  my $history_title = $history->{sourceTitle} // '';
  return 0 unless length $history_title;
  return 0 unless defined $rec->{id} && length "$rec->{id}";
  return ($rec->{sourceTitle} // '') eq $history_title ? 1 : 0;
}

sub fetch_blocklist_records {
  my ($url, $key, $resolved) = @_;
  my $query = $resolved->{service} eq 'radarr'
    ? "movieIds=$resolved->{movieId}"
    : "seriesIds=$resolved->{seriesId}";
  my ($ok, $blocklist) = curl_json('-H', "X-Api-Key: $key", "$url/api/v3/blocklist?page=1&pageSize=100&$query");
  return (0, 'blocklist_verify_query_failed') unless $ok && ref($blocklist) eq 'HASH';
  my $records = $blocklist->{records};
  return (0, 'blocklist_records_missing') unless ref($records) eq 'ARRAY';
  return (1, $records);
}

sub matching_blocklist_ids {
  my ($records, $history, $resolved) = @_;
  my %ids;
  for my $rec (@$records) {
    next unless ref($rec) eq 'HASH';
    next unless blocklist_record_matches_history($rec, $history, $resolved);
    $ids{"$rec->{id}"} = 1;
  }
  return %ids;
}

sub prove_blocklist {
  my ($action, $url, $key, $resolved) = @_;
  my ($hist_ok, $history_or_err) = find_history_record($action, $url, $key, $resolved);
  return (0, $history_or_err) unless $hist_ok;
  my $history = $history_or_err;
  my $history_id = $history->{id};
  return (0, 'history_source_title_missing') unless length($history->{sourceTitle} // '');

  my ($before_ok, $before_records_or_err) = fetch_blocklist_records($url, $key, $resolved);
  return (0, $before_records_or_err) unless $before_ok;
  my %before_ids = matching_blocklist_ids($before_records_or_err, $history, $resolved);

  my $posted = curl_ok('-X', 'POST', '-H', "X-Api-Key: $key", "$url/api/v3/history/failed/$history_id");
  return (0, 'history_failed_post_failed') unless $posted;

  my ($after_ok, $after_records_or_err) = fetch_blocklist_records($url, $key, $resolved);
  return (0, $after_records_or_err) unless $after_ok;
  for my $rec (@$after_records_or_err) {
    next unless ref($rec) eq 'HASH';
    next unless blocklist_record_matches_history($rec, $history, $resolved);
    my $id = "$rec->{id}";
    next if $before_ids{$id};
    return (1, {
      %$history,
      blocklistId => $id,
      blocklistDate => $rec->{date} // '',
    });
  }
  return (0, 'blocklist_proof_not_found_after_history_failed');
}

sub delete_media_file {
  my ($url, $key, $resolved) = @_;
  my $path = $resolved->{service} eq 'radarr'
    ? "moviefile/$resolved->{mediaFileId}"
    : "episodefile/$resolved->{mediaFileId}";
  return curl_ok('-X', 'DELETE', '-H', "X-Api-Key: $key", "$url/api/v3/$path")
    ? (1, 'deleted')
    : (0, 'arr_media_delete_failed');
}

sub trigger_rescan {
  my ($url, $key, $resolved) = @_;
  my ($name, $id_field, $id_value) = $resolved->{service} eq 'radarr'
    ? ('RescanMovie', 'movieId', $resolved->{movieId})
    : ('RescanSeries', 'seriesId', $resolved->{seriesId});
  my $body = encode_json({ name => $name, $id_field => $id_value });
  my ($ok, $resp) = curl_json('-X', 'POST', '-H', 'Content-Type: application/json',
    '-H', "X-Api-Key: $key", '-d', $body, "$url/api/v3/command");
  return $ok ? (1, (ref($resp) eq 'HASH' ? ($resp->{id} || 0) : 0)) : (0, 'arr_rescan_command_failed');
}

sub block_action {
  my ($action, $message) = @_;
  $action->{state} = 'blocked';
  $action->{attempt_count} = int($action->{attempt_count} || 0) + 1;
  $action->{next_attempt_ts} = 0;
  $action->{updated_ts} = now_iso();
  $action->{proof}->{message} = $message;
}

sub advance_action {
  my ($action) = @_;
  my ($url, $key) = arr_config($action->{service});
  return block_action($action, 'arr_api_not_configured') unless $url && $key;

  my ($actions_ok, @steps_or_err) = configured_actions($action->{proof}->{actions}, 1);
  return block_action($action, $steps_or_err[0]) unless $actions_ok;
  my @steps = @steps_or_err;
  unless (@steps) {
    $action->{state} = 'complete';
    $action->{next_attempt_ts} = 0;
    $action->{updated_ts} = now_iso();
    $action->{proof}->{message} = 'no_actions_configured';
    return;
  }

  my ($resolved, $blocklisted, $deleted);
  my $resolve = sub {
    return (1, $resolved) if $resolved;
    my ($resolved_ok, $resolved_or_err) = $action->{service} eq 'radarr'
      ? resolve_radarr($action, $url, $key)
      : resolve_sonarr($action, $url, $key);
    return (0, $resolved_or_err) unless $resolved_ok;
    $resolved = $resolved_or_err;
    $action->{state} = 'resolving';
    $action->{proof}->{resolved} = $resolved;
    return (1, $resolved);
  };

  for my $step (@steps) {
    if ($step eq 'arr_blocklist') {
      my ($resolved_ok, $resolved_or_err) = $resolve->();
      return block_action($action, $resolved_or_err) unless $resolved_ok;
      my ($block_ok, $block_proof_or_err) = prove_blocklist($action, $url, $key, $resolved);
      return block_action($action, $block_proof_or_err) unless $block_ok;
      $blocklisted = 1;
      $action->{state} = 'blocklisted';
      $action->{proof}->{blocklistHistoryId} = $block_proof_or_err->{id};
      $action->{proof}->{blocklistId} = $block_proof_or_err->{blocklistId};
      $action->{proof}->{blocklistSourceTitle} = $block_proof_or_err->{sourceTitle};
    } elsif ($step eq 'delete_file') {
      return block_action($action, 'delete_requires_blocklist_proof') unless $blocklisted;
      my ($delete_ok, $delete_err) = delete_media_file($url, $key, $resolved);
      return block_action($action, $delete_err) unless $delete_ok;
      $deleted = 1;
      $action->{state} = 'deleted';
      $action->{proof}->{deleteProof} = $delete_err;
    } elsif ($step eq 'arr_rescan') {
      my ($resolved_ok, $resolved_or_err) = $resolve->();
      return block_action($action, $resolved_or_err) unless $resolved_ok;
      my %requested = map { $_ => 1 } @steps;
      return block_action($action, 'rescan_requires_delete_proof') if $requested{delete_file} && !$deleted;
      my ($rescan_ok, $rescan_id_or_err) = trigger_rescan($url, $key, $resolved);
      return block_action($action, $rescan_id_or_err) unless $rescan_ok;
      $action->{proof}->{rescanCommandId} = $rescan_id_or_err;
    }
  }

  $action->{state} = 'complete';
  $action->{next_attempt_ts} = 0;
  $action->{updated_ts} = now_iso();
}

sub counts_for {
  my (@actions) = @_;
  my %counts;
  for my $a (@actions) {
    $counts{$a->{state} || 'unknown'}++;
  }
  return \%counts;
}

sub recent_action {
  my (@actions) = @_;
  return undef unless @actions;
  return $actions[-1];
}

# Keep byte-for-byte in sync with policy_outcome_for_state in transcodarr-api.pl.
sub outcome_for_state {
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

sub with_outcome {
  my ($a) = @_;
  return $a unless ref($a) eq 'HASH';
  return { %$a, outcome => outcome_for_state($a->{state}) };
}

sub status_json {
  my @actions = map { with_outcome($_) } read_actions();
  my @recent = reverse @actions;
  splice @recent, 10 if @recent > 10;
  print encode_json({
    ok => JSON::PP::true,
    counts => counts_for(@actions),
    total => scalar(@actions),
    recent_action => recent_action(@actions),
    recent => \@recent,
  }) . "\n";
}

sub actions_json {
  my @actions = map { with_outcome($_) } read_actions();
  print encode_json({ ok => JSON::PP::true, items => \@actions, total => scalar(@actions) }) . "\n";
}

sub preview_json {
  my $policy = policy_config();
  if ($policy->{_invalid_rules}) {
    print encode_json({ ok => JSON::PP::true, total => 0, items => [], reason => $policy->{_invalid_rules}, persisted => JSON::PP::false }) . "\n";
    return;
  }
  my $items = preview_matches($policy);
  print encode_json({ ok => JSON::PP::true, total => scalar(@$items), items => $items, persisted => JSON::PP::false }) . "\n";
}

sub rule_id_for_action {
  my ($action) = @_;
  my $p = $action->{proof};
  if (ref($p) eq 'HASH' && defined $p->{rule_id} && !ref($p->{rule_id}) && length($p->{rule_id})) {
    return $p->{rule_id};
  }
  return 'commentary_only' if ($action->{reason} // '') eq 'commentary_only';
  return undef;
}

sub advance_observe_action {
  my ($action, @steps) = @_;
  for my $step (@steps) {
    if ($step eq 'record_diagnostic') {
      $action->{proof}->{diagnostic} ||= {};
    } elsif ($step eq 'mark_needs_review') {
      $action->{state} = 'needs_review';
    } elsif (action_list_has_destructive($step)) {
      return block_action($action, 'destructive_action_on_observe_path');
    }
  }
  $action->{state} = 'needs_review' unless ($action->{state} // '') eq 'needs_review';
  $action->{next_attempt_ts} = 0;
  $action->{updated_ts} = now_iso();
}

sub run_once {
  my $policy = policy_config();
  unless (config_bool($policy->{enabled}, 0, exists($policy->{enabled}))) {
    print encode_json({ ok => JSON::PP::true, ran => JSON::PP::false, reason => 'disabled' }) . "\n";
    return;
  }
  unless (config_bool_valid($policy->{dry_run}, 0, exists($policy->{dry_run}))) {
    print encode_json({ ok => JSON::PP::true, ran => JSON::PP::false, reason => 'invalid_dry_run' }) . "\n";
    return;
  }
  if ($policy->{_invalid_rules}) {
    print encode_json({ ok => JSON::PP::true, ran => JSON::PP::false, reason => $policy->{_invalid_rules} }) . "\n";
    return;
  }

  # A rule may advance actions only if it is enabled AND its actions config is valid.
  my %rule_ok;
  for my $rule_id (keys %{ $policy->{rules} }) {
    my $rule = $policy->{rules}->{$rule_id};
    next unless ref($rule) eq 'HASH';
    my ($actions_ok) = configured_actions($rule->{actions}, exists($rule->{actions}));
    my $enabled = config_bool($rule->{enabled}, 1, exists($rule->{enabled}));
    $rule_ok{$rule_id} = ($enabled && $actions_ok) ? 1 : 0;
  }

  my $dry = config_bool($policy->{dry_run}, 0, exists($policy->{dry_run}));

  if ($dry) {
    my $items = preview_matches($policy);
    print encode_json({ ok => JSON::PP::true, ran => JSON::PP::true, persisted => JSON::PP::false, total => scalar(@$items), items => $items }) . "\n";
    return;
  }

  my $result = with_lock(sub {
    my @actions = read_actions();
    my %seen = map { ($_->{failed_key} => 1) } @actions;
    my $created = 0;
    for my $pair (matching_rows_with_rules($policy)) {
      my ($row, $rule_id, $rule) = @$pair;
      next if $seen{$row->{failed_key}};
      push @actions, create_action_for_row($row, $rule, $rule_id);
      $seen{$row->{failed_key}} = 1;
      $created++;
    }

    my $advanced = 0;
    for my $action (@actions) {
      next unless ($action->{state} // '') eq 'pending';
      my $rule_id = rule_id_for_action($action);
      next unless defined $rule_id;
      next unless $rule_ok{$rule_id};

      my ($steps_ok, @steps) = configured_actions(
        (ref($action->{proof}) eq 'HASH' ? $action->{proof}->{actions} : undef), 1);
      if (!$steps_ok) {
        block_action($action, $steps[0]);
      } elsif (action_list_has_destructive(@steps)) {
        my $rule = $policy->{rules}->{$rule_id};
        if (ref($rule) eq 'HASH' && rule_is_destructive($rule)) {
          advance_action($action);
        } else {
          block_action($action, 'destructive_actions_not_allowed');
        }
      } else {
        advance_observe_action($action, @steps);
      }
      $advanced++;
    }
    write_actions(@actions);
    return { created => $created, advanced => $advanced, counts => counts_for(@actions) };
  });

  print encode_json({
    ok => JSON::PP::true,
    ran => JSON::PP::true,
    created => $result->{created},
    advanced => $result->{advanced},
    counts => $result->{counts},
  }) . "\n";
}

my $cmd = shift @ARGV || 'status';
if ($cmd eq 'status') {
  status_json();
} elsif ($cmd eq 'run') {
  run_once();
} elsif ($cmd eq 'actions-json') {
  actions_json();
} elsif ($cmd eq 'preview-json') {
  preview_json();
} else {
  print encode_json({ ok => JSON::PP::false, error => 'unknown_command' }) . "\n";
  exit 2;
}
