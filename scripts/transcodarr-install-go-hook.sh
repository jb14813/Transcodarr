#!/bin/bash
set -euo pipefail

log() {
  printf '[transcodarr-go-hook] %s\n' "$*"
}

go_file="${1:-/boot/config/go}"
patch_script="${2:-/boot/config/transcodarr-host-nvenc-patch.sh}"
marker_begin='# BEGIN Transcodarr NVENC patch'
marker_end='# END Transcodarr NVENC patch'

# If the patch script isn't at the target location, try to copy it from alongside this script
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [[ ! -f "$patch_script" && -f "$SCRIPT_DIR/transcodarr-host-nvenc-patch.sh" ]]; then
  cp "$SCRIPT_DIR/transcodarr-host-nvenc-patch.sh" "$patch_script"
  chmod +x "$patch_script"
  log "Copied patch script to $patch_script"
fi

if [[ ! -f "$go_file" ]]; then
  log "go file not found: $go_file"
  exit 1
fi

tmp_file="$(mktemp)"
trap 'rm -f "$tmp_file"' EXIT

awk -v begin="$marker_begin" -v end="$marker_end" '
  $0 == begin { skip = 1; next }
  $0 == end { skip = 0; next }
  !skip { print }
' "$go_file" > "$tmp_file"

cat <<EOF >> "$tmp_file"
$marker_begin
(
  for attempt in \$(seq 1 120); do
    if [ -x "$patch_script" ]; then
      /bin/bash "$patch_script" >> /var/log/transcodarr-nvenc-patch.log 2>&1
      exit_code=\$?
      logger -t transcodarr-nvenc-patch "boot patch helper exited with status \$exit_code"
      exit "\$exit_code"
    fi
    sleep 2
  done
  logger -t transcodarr-nvenc-patch "patch helper not found at $patch_script after boot"
) &
$marker_end
EOF

if cmp -s "$go_file" "$tmp_file"; then
  log "No changes required in $go_file"
  exit 0
fi

cp "$tmp_file" "$go_file"
chmod +x "$go_file"
log "Updated $go_file with managed NVENC patch hook"
