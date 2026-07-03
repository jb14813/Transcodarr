#!/usr/bin/perl
use strict;
use warnings;
use Encode qw(decode_utf8);

binmode STDIN,  ':encoding(UTF-8)';
binmode STDOUT, ':encoding(UTF-8)';
binmode STDERR, ':encoding(UTF-8)';

sub tc_plex_scalar {
  my $value = shift;
  return '' if ref($value);
  $value = '' unless defined $value;
  $value = decode_utf8($value) unless utf8::is_utf8($value);
  $value =~ s/^\s+|\s+$//g;
  return $value;
}

sub tc_plex_normalize_path {
  my $path = tc_plex_scalar(shift);
  $path =~ s{/+\z}{} unless $path eq '/';
  return $path;
}

sub tc_plex_library_root_for_service {
  my $service = lc tc_plex_scalar(shift);
  return '/movies' if $service eq 'radarr';
  return '/tv'     if $service eq 'sonarr';
  return '';
}

sub tc_plex_path_root_for_service {
  my ($service, $integrations) = @_;
  $integrations = {} unless ref($integrations) eq 'HASH';
  my $service_lc = lc tc_plex_scalar($service);
  my $key = $service_lc eq 'radarr' ? 'plex_movie_path_root'
    : $service_lc eq 'sonarr' ? 'plex_tv_path_root'
    : '';
  return ('', 'invalid_service') unless length($key);

  my $root = tc_plex_scalar($integrations->{$key});
  $root = $ENV{uc($key)} // '' unless length($root);
  $root = tc_plex_library_root_for_service($service_lc) unless length($root);
  $root = tc_plex_normalize_path($root);
  return ('', 'invalid_plex_path_root') unless $root =~ m{^/};
  return ($root, '');
}

sub tc_plex_join_root_suffix {
  my ($root, $suffix) = @_;
  $root = tc_plex_normalize_path($root);
  $suffix = '' unless defined $suffix;
  return $root if $suffix eq '';
  $suffix = "/$suffix" unless $suffix =~ m{^/};
  return $root eq '/' ? $suffix : "$root$suffix";
}

sub tc_plex_path_for_service {
  my ($service, $input_path, $integrations) = @_;
  my $library_root = tc_plex_library_root_for_service($service);
  return ('', 'invalid_service') unless length($library_root);

  my $input = tc_plex_normalize_path($input_path);
  return ('', 'missing_path') unless length($input);
  return ('', 'broad_path')
    if $input eq $library_root;
  return ('', 'path_mismatch')
    unless $input =~ m{^\Q$library_root\E/};

  my ($plex_root, $root_err) = tc_plex_path_root_for_service($service, $integrations);
  return ('', $root_err) if length($root_err);
  my $suffix = substr($input, length($library_root));
  return (tc_plex_join_root_suffix($plex_root, $suffix), '');
}

sub tc_plex_xml_unescape {
  my $value = shift // '';
  $value =~ s/&#x([0-9a-fA-F]+);/chr(hex($1))/ge;
  $value =~ s/&#([0-9]+);/chr($1)/ge;
  $value =~ s/&quot;/"/g;
  $value =~ s/&apos;/'/g;
  $value =~ s/&lt;/</g;
  $value =~ s/&gt;/>/g;
  $value =~ s/&amp;/&/g;
  return $value;
}

sub tc_plex_section_locations {
  my ($xml, $section_id) = @_;
  $xml = '' unless defined $xml;
  $section_id = tc_plex_scalar($section_id);
  my @locations;
  while ($xml =~ /<Directory\b([^>]*)>(.*?)<\/Directory>/sg) {
    my ($attrs, $body) = ($1, $2);
    my ($key) = $attrs =~ /\bkey="([^"]+)"/;
    next unless defined($key) && tc_plex_xml_unescape($key) eq $section_id;
    while ($body =~ /<Location\b[^>]*\bpath="([^"]+)"/g) {
      push @locations, tc_plex_normalize_path(tc_plex_xml_unescape($1));
    }
  }
  return @locations;
}

sub tc_plex_xml_attr {
  my ($attrs, $name) = @_;
  return '' unless defined($attrs) && defined($name);
  my ($value) = $attrs =~ /\b\Q$name\E="([^"]*)"/;
  return defined($value) ? tc_plex_xml_unescape($value) : '';
}

sub tc_plex_section_targets_for_type {
  my ($xml, $type) = @_;
  $xml = '' unless defined $xml;
  $type = tc_plex_scalar($type);
  my @targets;
  while ($xml =~ /<Directory\b([^>]*)>(.*?)<\/Directory>/sg) {
    my ($attrs, $body) = ($1, $2);
    next unless tc_plex_xml_attr($attrs, 'type') eq $type;
    my $key = tc_plex_xml_attr($attrs, 'key');
    next unless length($key);

    my @locations;
    while ($body =~ /<Location\b([^>]*)/g) {
      my $path = tc_plex_normalize_path(tc_plex_xml_attr($1, 'path'));
      push @locations, $path if length($path);
    }
    next unless @locations;
    push @targets, { key => $key, locations => \@locations };
  }
  return @targets;
}

sub tc_plex_section_target_for_type {
  my ($xml, $type, $hint_section_id, $hint_path_root) = @_;
  my @targets = tc_plex_section_targets_for_type($xml, $type);
  return ('missing_section', '', '') unless @targets;

  $hint_section_id = tc_plex_scalar($hint_section_id);
  $hint_path_root = tc_plex_normalize_path($hint_path_root);

  if (length($hint_section_id)) {
    for my $target (@targets) {
      next unless $target->{key} eq $hint_section_id;
      for my $location (@{$target->{locations}}) {
        return ('', $target->{key}, $location)
          if length($hint_path_root) && $location eq $hint_path_root;
      }
      return ('', $target->{key}, $target->{locations}->[0]);
    }
  }

  if (length($hint_path_root)) {
    for my $target (@targets) {
      for my $location (@{$target->{locations}}) {
        return ('', $target->{key}, $location) if $location eq $hint_path_root;
      }
    }
  }

  return ('', $targets[0]->{key}, $targets[0]->{locations}->[0]);
}

sub tc_plex_path_within_locations {
  my ($path, @locations) = @_;
  my $target = tc_plex_normalize_path($path);
  return 0 unless length($target);
  for my $location (@locations) {
    my $root = tc_plex_normalize_path($location);
    next unless length($root);
    return 1 if $root eq '/';
    return 1 if $target eq $root || $target =~ m{^\Q$root\E/};
  }
  return 0;
}

sub tc_plex_section_contains_path {
  my ($xml, $section_id, $path) = @_;
  my @locations = tc_plex_section_locations($xml, $section_id);
  return tc_plex_path_within_locations($path, @locations);
}

sub tc_plex_rating_key_for_file {
  my ($xml, $path) = @_;
  $xml = '' unless defined $xml;
  my $target = tc_plex_normalize_path($path);
  return '' unless length($target) && $target ne '/';

  while ($xml =~ /<Video\b([^>]*)>(.*?)<\/Video>/sg) {
    my ($attrs, $body) = ($1, $2);
    my ($rating_key) = $attrs =~ /\bratingKey="([^"]+)"/;
    next unless defined($rating_key) && $rating_key =~ /^\d+$/;

    while ($body =~ /<Part\b[^>]*\bfile="([^"]+)"/g) {
      my $file = tc_plex_normalize_path(tc_plex_xml_unescape($1));
      return $rating_key if $file eq $target;
    }
  }

  return '';
}

sub tc_plex_cli_arg {
  my ($args, $name) = @_;
  for (my $i = 0; $i < @$args - 1; $i++) {
    return $args->[$i + 1] if $args->[$i] eq $name;
  }
  return '';
}

sub tc_plex_cli {
  my @args = @_;
  my $cmd = shift @args // '';

  if ($cmd eq 'map') {
    my $service = tc_plex_cli_arg(\@args, '--service');
    my $path = tc_plex_cli_arg(\@args, '--path');
    my %integrations = (
      plex_movie_path_root => $ENV{PLEX_MOVIE_PATH_ROOT} // '',
      plex_tv_path_root    => $ENV{PLEX_TV_PATH_ROOT} // '',
    );
    my ($mapped, $err) = tc_plex_path_for_service($service, $path, \%integrations);
    if (length($err)) {
      print STDERR "$err\n";
      return 2;
    }
    print "$mapped\n";
    return 0;
  }

  if ($cmd eq 'validate-section') {
    my $section_id = tc_plex_cli_arg(\@args, '--section-id');
    my $path = tc_plex_cli_arg(\@args, '--path');
    my $xml = do { local $/; <STDIN> };
    return tc_plex_section_contains_path($xml, $section_id, $path) ? 0 : 2;
  }

  if ($cmd eq 'rating-key') {
    my $path = tc_plex_cli_arg(\@args, '--path');
    my $xml = do { local $/; <STDIN> };
    my $rating_key = tc_plex_rating_key_for_file($xml, $path);
    if (length($rating_key)) {
      print "$rating_key\n";
      return 0;
    }
    print STDERR "rating_key_not_found\n";
    return 2;
  }

  print STDERR "usage: $0 map|validate-section|rating-key\n";
  return 2;
}

exit tc_plex_cli(@ARGV) unless caller;
1;
