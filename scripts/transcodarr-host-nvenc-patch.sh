#!/bin/bash
set -euo pipefail

log() {
  printf '[transcodarr-nvenc-patch] %s\n' "$*"
}

declare -A patch_list=(
  ["575.51.02"]='s/\xe8\xb5\x2f\xfe\xff\x85\xc0\x41\x89\xc4/\xe8\xb5\x2f\xfe\xff\x29\xc0\x41\x89\xc4/g'
  ["575.57.08"]='s/\xe8\xb5\x2f\xfe\xff\x85\xc0\x41\x89\xc4/\xe8\xb5\x2f\xfe\xff\x29\xc0\x41\x89\xc4/g'
  ["575.64"]='s/\xe8\xb5\x2f\xfe\xff\x85\xc0\x41\x89\xc4/\xe8\xb5\x2f\xfe\xff\x29\xc0\x41\x89\xc4/g'
  ["575.64.03"]='s/\xe8\xb5\x2f\xfe\xff\x85\xc0\x41\x89\xc4/\xe8\xb5\x2f\xfe\xff\x29\xc0\x41\x89\xc4/g'
  ["575.64.05"]='s/\xe8\xb5\x2f\xfe\xff\x85\xc0\x41\x89\xc4/\xe8\xb5\x2f\xfe\xff\x29\xc0\x41\x89\xc4/g'
)

driver_locations_for_version() {
  local driver_version="$1"
  local driver_major="${driver_version%%.*}"

  printf '%s\n' \
    '/usr/lib/x86_64-linux-gnu' \
    '/usr/lib/x86_64-linux-gnu/nvidia/current' \
    '/usr/lib/x86_64-linux-gnu/nvidia/tesla' \
    "/usr/lib/x86_64-linux-gnu/nvidia/tesla-${driver_major}" \
    '/usr/lib64' \
    '/usr/lib' \
    "/usr/lib/nvidia-${driver_major}"
}

detect_driver_version() {
  local nvidia_smi

  nvidia_smi="$(command -v nvidia-smi || true)"
  if [[ -z "$nvidia_smi" ]]; then
    return 1
  fi

  "$nvidia_smi" --query-gpu=driver_version --format=csv,noheader,nounits 2>/dev/null | head -n 1
}

wait_for_driver_version() {
  local attempts="${TRANSCODARR_NVENC_WAIT_ATTEMPTS:-120}"
  local sleep_seconds="${TRANSCODARR_NVENC_WAIT_SECONDS:-2}"
  local attempt
  local driver_version

  for attempt in $(seq 1 "$attempts"); do
    driver_version="$(detect_driver_version || true)"
    if [[ -n "$driver_version" ]]; then
      printf '%s\n' "$driver_version"
      return 0
    fi
    sleep "$sleep_seconds"
  done

  return 1
}

find_driver_file() {
  local driver_version="$1"
  local driver_dir

  while IFS= read -r driver_dir; do
    if [[ -e "$driver_dir/libnvidia-encode.so.$driver_version" ]]; then
      printf '%s\n' "$driver_dir/libnvidia-encode.so.$driver_version"
      return 0
    fi
  done < <(driver_locations_for_version "$driver_version")

  return 1
}

file_contains_bytes() {
  local bytes="$1"
  local file="$2"

  LC_ALL=C grep -qaP "$bytes" "$file"
}

main() {
  local driver_version
  local driver_file
  local backup_dir="${TRANSCODARR_NVENC_BACKUP_DIR:-/opt/nvidia/libnvidia-encode-backup}"
  local backup_file
  local patch
  local original_bytes
  local patched_bytes
  local tmp_file

  driver_version="$(wait_for_driver_version)" || {
    log "Timed out waiting for nvidia-smi to report a driver version"
    exit 1
  }

  patch="${patch_list[$driver_version]-}"
  if [[ -z "$patch" ]]; then
    log "Driver $driver_version is not in the tracked NVENC patch list"
    exit 1
  fi

  driver_file="$(find_driver_file "$driver_version")" || {
    log "Could not locate libnvidia-encode.so.$driver_version on disk"
    exit 1
  }

  original_bytes="$(awk -F / '$2 { print $2 }' <<< "$patch")"
  patched_bytes="$(awk -F / '$3 { print $3 }' <<< "$patch")"

  if file_contains_bytes "$patched_bytes" "$driver_file"; then
    log "Driver $driver_version already patched"
    exit 0
  fi

  if ! file_contains_bytes "$original_bytes" "$driver_file"; then
    log "Expected byte pattern not found in $driver_file"
    exit 1
  fi

  mkdir -p "$backup_dir"
  backup_file="$backup_dir/libnvidia-encode.so.$driver_version"

  if [[ ! -f "$backup_file" ]]; then
    cp -p "$driver_file" "$backup_file"
    log "Saved backup to $backup_file"
  elif ! cmp -s "$backup_file" "$driver_file"; then
    log "Backup exists and live driver differs from backup; refusing to overwrite"
    exit 1
  fi

  tmp_file="$(mktemp "${driver_file}.XXXXXX")"
  trap 'rm -f "$tmp_file"' EXIT

  sed "$patch" "$backup_file" > "$tmp_file"
  cp "$tmp_file" "$driver_file"
  chmod 0755 "$driver_file"
  ldconfig

  if ! file_contains_bytes "$patched_bytes" "$driver_file"; then
    log "Patch verification failed for $driver_file"
    exit 1
  fi

  rm -f "$tmp_file"
  trap - EXIT

  log "Patched $driver_file for driver $driver_version"
}

main "$@"
