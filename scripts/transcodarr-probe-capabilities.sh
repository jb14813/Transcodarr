#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# transcodarr-probe-capabilities.sh — probe runtime encoder availability
#
# Why: the ffmpeg encoders list (`ffmpeg -encoders`) says WHICH encoders the
# build supports. It does NOT say whether the hardware + driver combination
# can actually USE them. Classic case: Ampere (RTX 30-series) has
# `av1_nvenc` compiled into ffmpeg, but the hardware itself lacks an AV1
# encoder — invoking it fails at runtime with an opaque error.
#
# This script test-encodes a 1-frame synthesized clip with each HW encoder
# and records the result (pass/fail) in Valkey as hash `tc:capabilities`.
# The API server exposes that hash via /api/capabilities; the GUI uses it
# to hide target_codec options that would fail.
#
# Runtime: ~0.3-1s per encoder probe, ~6-9s total for all six HW encoders
# on a healthy system. CPU encoders (libx264/x265/svtav1) are always
# listed as available since ffmpeg builds include them unconditionally.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Shared retry-probe helper lives in transcodarr-lib.sh so the worker's
# cache-miss inline path uses identical policy. Stub QUEUE_CLI before
# sourcing so we don't require valkey-cli-aware lib functions at probe
# script load time.
QUEUE_CLI="${QUEUE_CLI:-valkey-cli}"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/transcodarr-lib.sh"

capability_hset() {
  local field="$1" value="$2"
  if ! $QUEUE_CLI HSET tc:capabilities "$field" "$value" > /dev/null 2>&1; then
    echo "[probe] WARN: failed to record capability $field=$value"
  fi
  return 0
}

# Record result + never propagate non-zero (would trip set -e at the
# top-level call site and abort the sweep on the first expected
# unavailable encoder).
probe_and_record() {
  local label=$1 enc=$2 profile=$3 pixfmt=$4
  if probe_encoder_runtime "$enc" "$profile" "$pixfmt"; then
    capability_hset "$label" 1
  else
    capability_hset "$label" 0
  fi
  return 0
}

echo "[probe] Starting encoder capability probe"

# HW-backed encoders probed with the EXACT (profile, pixfmt) the worker
# uses at runtime. These MUST match ENCODER_PROFILE + PIX_FMT in
# scripts/transcodarr-codec-tables.sh:56-67. Keep in sync.
probe_and_record h264_nvenc h264_nvenc high   nv12
probe_and_record hevc_nvenc hevc_nvenc main10 p010le
probe_and_record av1_nvenc  av1_nvenc  main   p010le
probe_and_record h264_qsv   h264_qsv   high   nv12
probe_and_record hevc_qsv   hevc_qsv   main10 p010le
probe_and_record av1_qsv    av1_qsv    main   p010le

record_cap() {
  local enc=$1 result=$2
  capability_hset "$enc" "$result"
}

# CPU encoders: ffmpeg builds we ship always include them. List-based
# presence check via `ffmpeg -encoders` catches the edge case of a build
# missing libsvtav1 (e.g. a minimal ffmpeg build); otherwise they're on.
# Do the cheap list check rather than an expensive probe.
_encoders_list=$(ffmpeg -hide_banner -encoders 2>/dev/null || true)
for enc in libx264 libx265 libsvtav1; do
  if printf '%s\n' "$_encoders_list" | grep -qE "^[ VADS.FBSXL]+ ${enc}\b"; then
    record_cap "$enc" 1
    echo "[probe] $enc: available"
  else
    record_cap "$enc" 0
    echo "[probe] $enc: unavailable (not in ffmpeg build)"
  fi
done

# Audio encoders the worker cares about — list-check only; audio encoders
# don't have "hardware can't drive it" failure modes.
for enc in libfdk_aac aac ac3 eac3 libopus; do
  if printf '%s\n' "$_encoders_list" | grep -qE "^[ VADS.FBSXL]+ ${enc}\b"; then
    record_cap "$enc" 1
  else
    record_cap "$enc" 0
  fi
done

# HDR tonemap path probe. Tried in preference order:
#
#   1. libplacebo (Vulkan compute) — preferred. Best quality + can passthrough
#      Dolby Vision metadata. Requires the host to ship an NVIDIA Vulkan ICD
#      that exports `vk_icdGetInstanceProcAddr`. Some distros (Unraid's
#      ich777 plugin in particular) ship the driver without the desktop
#      Vulkan ICD library, so libplacebo's vf_libplacebo loads but
#      vkCreateInstance fails with "Found no suitable device".
#
#   2. tonemap_opencl — GPU fallback. Works on any host with a functional
#      OpenCL ICD. NVIDIA's OpenCL ICD (`/etc/OpenCL/vendors/nvidia.icd`)
#      ships with the driver and works even when desktop Vulkan doesn't,
#      so this is the right path on Unraid hosts.
#
#   3. CPU zscale+mobius — last-resort. Slow at 4K but works anywhere
#      ffmpeg runs. Reserved for hosts where both GPU paths fail.
#
# Each probe encodes 1 frame against a synthetic HDR-tagged input. Exit 0
# means the path is usable; anything else and we drop to the next tier.
echo "[probe] HDR tonemap path:"

_hdr_probe_err="$(ffmpeg -hide_banner -v error \
  -f lavfi -i "color=c=white:size=64x64:rate=1,format=p010le" \
  -vf "libplacebo=w=64:h=64:tonemapping=spline:colorspace=bt709:color_trc=bt709:color_primaries=bt709:format=nv12" \
  -frames:v 1 -f null - 2>&1)" || true

if [ -z "$_hdr_probe_err" ]; then
  capability_hset hdr_tonemap_path libplacebo
  echo "[probe]   libplacebo: available (Vulkan ICD working)"
else
  echo "[probe]   libplacebo: unavailable — $(printf '%s' "$_hdr_probe_err" | head -1)"
  # Try OpenCL next. Synthetic-input probe tags the source as smpte2084 +
  # bt2020 so tonemap_opencl accepts it (otherwise it rejects "unsupported
  # transfer function characteristic").
  _opencl_probe_err="$(ffmpeg -hide_banner -v error \
    -init_hw_device opencl=ocl -filter_hw_device ocl \
    -f lavfi -i "color=c=white:size=64x64:rate=1,format=yuv420p10le" \
    -vf "format=yuv420p10le,setparams=color_trc=smpte2084:colorspace=bt2020nc:color_primaries=bt2020,hwupload,tonemap_opencl=tonemap=mobius:format=nv12,hwdownload,format=nv12" \
    -frames:v 1 -f null - 2>&1)" || true
  if [ -z "$_opencl_probe_err" ]; then
    capability_hset hdr_tonemap_path opencl
    echo "[probe]   tonemap_opencl: available — using OpenCL for HDR tonemap"
  else
    capability_hset hdr_tonemap_path cpu
    echo "[probe]   tonemap_opencl: unavailable — $(printf '%s' "$_opencl_probe_err" | head -1)"
    echo "[probe]   falling back to CPU zscale+mobius chain for HDR tonemap"
  fi
fi

# ── Language-detection backend probe ─────────────────────────────────────
# whisper.cpp's whisper-cli is built with a GPU backend (CUDA on nvidia,
# Vulkan on intel, SYCL) or CPU/OpenBLAS. As with the HDR ladder above, the
# build supporting a backend does NOT prove the host can use it at runtime
# (the Vulkan-ICD-absent case is the same trap). So we test-detect a
# 1-second synthetic mono 16 kHz WAV and record the backend that worked.
#
# Classification is PINNED to whisper.cpp v1.7.4 backend-init log strings
# (spec §6/§10). whisper-cli prints a device-found line on a working GPU:
#   CUDA:   "ggml_cuda_init: found N CUDA devices:"
#   Vulkan: "ggml_vulkan: Found N Vulkan devices:"
#   SYCL:   "ggml_sycl_init: found N SYCL devices:"
# We key off the DEVICE-FOUND line (deterministic), NOT a bare token —
# and only when the run exits 0. The rc==0 guard is LOAD-BEARING: a CUDA
# build on a GPU-less host prints "ggml_cuda_init: no CUDA-capable device
# is detected" (contains 'cuda') but exits non-zero, so the guard is what
# stops a false 'cuda' marker. The detected language itself is irrelevant;
# we only care that the run succeeded and which backend it loaded.
#
# Falls back through:
#   GPU run rc=0 + device-found marker → that backend
#   GPU run rc!=0 OR no device marker  → retry with -ng (CPU); rc=0 → cpu
#   CPU -ng also fails                         → lang_backend UNSET (fail-closed)
#   no tag support / whisper-cli / model       → lang_backend UNSET (fail-closed)
echo "[probe] Language-detection backend:"

_lang_model_dir="${TRANSCODARR_LANGUAGE_MODEL_DIR:-/models}"
_lang_model_name="${TRANSCODARR_LANGUAGE_MODEL:-base}"
_lang_model="${TRANSCODARR_LANG_MODEL:-${_lang_model_dir}/ggml-${_lang_model_name}.bin}"
_lang_wav="$(mktemp /tmp/tc-lang-probe-XXXXXX.wav)"
# 1s mono 16 kHz WAV — the format whisper.cpp expects. Sine over silence
# so the file is non-empty; content doesn't matter for a backend probe.
# NOTE: the .wav path MUST stay the literal last arg (-f wav "$_lang_wav");
# the unit-test ffmpeg stub keys off the last positional arg.
ffmpeg -hide_banner -v error \
  -f lavfi -i "sine=frequency=440:duration=1" \
  -ar 16000 -ac 1 -f wav "$_lang_wav" 2>/dev/null || : > "$_lang_wav"

# Classify a whisper-cli stderr log to a backend token via the pinned
# v1.7.4 device-found lines, else empty.
_lang_classify_backend() {
  local log=$1
  if printf '%s\n' "$log" | grep -qE 'ggml_cuda_init: found [0-9]+ CUDA device'; then
    echo cuda
  elif printf '%s\n' "$log" | grep -qE 'ggml_vulkan: Found [0-9]+ Vulkan device'; then
    echo vulkan
  elif printf '%s\n' "$log" | grep -qE 'ggml_sycl_init: found [0-9]+ SYCL device'; then
    echo sycl
  else
    echo ""
  fi
}

_lang_detect_device_label() {
  local backend="${1:-}"
  local log="${2:-}"
  local label=""
  case "$backend" in
    cuda)
      if command -v nvidia-smi >/dev/null 2>&1; then
        label="$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 || true)"
      fi
      [ -n "$label" ] || label="$(printf '%s\n' "$log" | sed -nE 's/^ggml_cuda_init:.*device[^:]*:?[[:space:]]*(.+)$/\1/p' | head -1)"
      [ -n "$label" ] || label="NVIDIA GPU"
      ;;
    vulkan)
      label="$(printf '%s\n' "$log" | sed -nE 's/^ggml_vulkan:[[:space:]]*[0-9]+ = (.+)$/\1/p' | head -1)"
      [ -n "$label" ] || label="Vulkan GPU"
      ;;
    sycl)
      label="$(printf '%s\n' "$log" | sed -nE 's/^ggml_sycl_init:.*device[^:]*:?[[:space:]]*(.+)$/\1/p' | head -1)"
      [ -n "$label" ] || label="SYCL GPU"
      ;;
    cpu)
      label="CPU"
      ;;
  esac
  printf '%s\n' "$label" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
}

_lang_backend=""    # empty == no usable detector (fail-closed)
_lang_device_label=""
if ! command -v mkvpropedit >/dev/null 2>&1; then
  echo "[probe]   mkvpropedit not found — lang_backend UNSET (language tag support unavailable, fail-closed)"
elif ! command -v ffmpeg >/dev/null 2>&1; then
  echo "[probe]   ffmpeg not found — lang_backend UNSET (language tag support unavailable, fail-closed)"
elif ! command -v ffprobe >/dev/null 2>&1; then
  echo "[probe]   ffprobe not found — lang_backend UNSET (language tag support unavailable, fail-closed)"
elif ! command -v whisper-cli >/dev/null 2>&1; then
  echo "[probe]   whisper-cli not found — lang_backend UNSET (feature build absent, fail-closed)"
elif [ ! -f "$_lang_model" ]; then
  echo "[probe]   model $_lang_model missing — lang_backend UNSET (fail-closed)"
else
  # GPU attempt (no -ng): whisper-cli loads its compiled backend.
  _lang_gpu_log="$(whisper-cli -m "$_lang_model" -f "$_lang_wav" -dl 2>&1)" \
    && _lang_gpu_rc=0 || _lang_gpu_rc=$?
  _lang_detected_backend="$(_lang_classify_backend "$_lang_gpu_log")"
  # rc==0 guard is load-bearing — see header comment.
  if [ "$_lang_gpu_rc" -eq 0 ] && [ -n "$_lang_detected_backend" ]; then
    _lang_backend="$_lang_detected_backend"
    _lang_device_label="$(_lang_detect_device_label "$_lang_backend" "$_lang_gpu_log")"
    echo "[probe]   GPU backend usable — lang_backend=$_lang_backend"
  else
    echo "[probe]   GPU backend unusable (rc=$_lang_gpu_rc: $(printf '%s' "$_lang_gpu_log" | head -1)) — trying CPU (-ng)"
    if whisper-cli -m "$_lang_model" -f "$_lang_wav" -dl -ng >/dev/null 2>&1; then
      _lang_backend="cpu"
      _lang_device_label="CPU"
      echo "[probe]   CPU backend usable — lang_backend=cpu"
    else
      echo "[probe]   CPU detect failed too — lang_backend UNSET (no usable detector, fail-closed)"
    fi
  fi
fi
rm -f "$_lang_wav"
if [ -n "$_lang_backend" ]; then
  capability_hset lang_backend "$_lang_backend"
  capability_hset lang_device_label "$_lang_device_label"
  echo "[probe]   lang_backend recorded: $_lang_backend"
else
  # Fail-closed: clear any stale value so Plan C's non-empty gate treats
  # the feature as unavailable (no divert, no backfill, no pool).
  $QUEUE_CLI HDEL tc:capabilities lang_backend > /dev/null 2>&1 || true
  $QUEUE_CLI HDEL tc:capabilities lang_device_label > /dev/null 2>&1 || true
  echo "[probe]   lang_backend UNSET — language processing fail-closed (no usable detector)"
fi

# Record probe timestamp for debugging / cache-invalidation if needed.
capability_hset probed_at "$(date -Iseconds)"

echo "[probe] Capabilities written to Valkey: tc:capabilities"
