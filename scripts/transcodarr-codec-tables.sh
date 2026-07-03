#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# transcodarr-codec-tables.sh — Lookup tables for codec/container decisions
#
# Pure data: associative arrays sourced by transcodarr-worker.sh at startup.
# No functions, no logic — just tables.
#
# Sources: Tdarr (324 plugins), Unmanic, HandBrake, Don Melton, ab-av1, ffmpeg docs
# ─────────────────────────────────────────────────────────────────────────────

# ── Container remap ──────────────────────────────────────────────────────
# Exotic containers → safe output container. Unlisted extensions pass through.
# asf joins the remap list — its muxer is poorly maintained and frequently
# fails on re-encoded video; mkv is a safer destination.
declare -A CONTAINER_REMAP=(
  [divx]=mkv [avi]=mkv [wmv]=mkv [ts]=mkv [flv]=mkv [asf]=mkv
  [vob]=mkv [ogv]=mkv [mpg]=mkv [mpeg]=mkv [m4v]=mp4
  # Blu-ray / AVCHD transport streams (m2ts, mts) — TS-family, same
  # timestamp pathologies, need mkv. QuickTime captures (mov) come from
  # iPhones / dashcams / DSLR rigs and benefit from mp4 repackaging
  # (strictly safer cross-player than .mov).
  [m2ts]=mkv [mts]=mkv [mov]=mp4
)

# Containers that need -fflags +genpts (missing or non-monotonic timestamps).
# flv and asf added: both routinely have DTS issues that mux-fail without it.
# m2ts/mts: TS-family, inherit all DTS issues ts has. mov: QuickTime
# captures (GoPro, iPhone slo-mo, dashcams) frequently have non-monotonic
# DTS and benefit from both +genpts and -avoid_negative_ts make_zero.
declare -A GENPTS_CONTAINERS=(
  [divx]=1 [avi]=1 [wmv]=1 [ts]=1 [flv]=1 [asf]=1 [vob]=1 [mpg]=1 [mpeg]=1
  [m2ts]=1 [mts]=1 [mov]=1
)

# ── Subtitle codec compatibility ─────────────────────────────────────────
declare -A MKV_SUB_SUPPORTED=(
  [subrip]=1 [ass]=1 [ssa]=1 [hdmv_pgs_subtitle]=1
  [dvd_subtitle]=1 [dvdsub]=1 [dvb_subtitle]=1 [dvbsub]=1 [webvtt]=1
)
declare -A MKV_SUB_BLOCKED=( [mov_text]=1 [eia_608]=1 [timed_id3]=1 )

declare -A MP4_SUB_SUPPORTED=( [mov_text]=1 [webvtt]=1 )
# Text subs convertible to mov_text for MP4 output
declare -A MP4_TEXT_CONVERTIBLE=( [subrip]=1 [ass]=1 [ssa]=1 )

# ── Encoder name lookup ──────────────────────────────────────────────────
declare -A ENCODER_NAME=(
  [h264:cuda]=h264_nvenc   [h264:qsv]=h264_qsv   [h264:none]=libx264
  [hevc:cuda]=hevc_nvenc   [hevc:qsv]=hevc_qsv   [hevc:none]=libx265
  [av1:cuda]=av1_nvenc     [av1:qsv]=av1_qsv     [av1:none]=libsvtav1
)

# ── Quality flag per encoder ─────────────────────────────────────────────
declare -A QUALITY_FLAG=(
  [h264_nvenc]=-cq     [hevc_nvenc]=-cq     [av1_nvenc]=-cq
  [h264_qsv]=-global_quality  [hevc_qsv]=-global_quality  [av1_qsv]=-global_quality
  [libx264]=-crf       [libx265]=-crf       [libsvtav1]=-crf
)

# ── Encoders needing -b:v 0 for true CQ/CRF mode ────────────────────────
declare -A NEEDS_UNCAPPED=( [h264_nvenc]=1 [hevc_nvenc]=1 [av1_nvenc]=1 [libx264]=1 )

# ── Profile per encoder ─────────────────────────────────────────────────
declare -A ENCODER_PROFILE=(
  [h264_nvenc]=high    [hevc_nvenc]=main10  [av1_nvenc]=main
  [h264_qsv]=high      [hevc_qsv]=main10   [av1_qsv]=main
  [libx264]=high        [libx265]=main10    [libsvtav1]=main
)

# ── Pixel format per encoder ────────────────────────────────────────────
declare -A PIX_FMT=(
  [h264_nvenc]=nv12   [hevc_nvenc]=p010le  [av1_nvenc]=p010le
  [h264_qsv]=nv12     [hevc_qsv]=p010le   [av1_qsv]=p010le
  [libx264]=yuv420p   [libx265]=yuv420p10le [libsvtav1]=yuv420p10le
)

# ── Scale filter per backend ────────────────────────────────────────────
declare -A SCALE_FILTER_NAME=(
  [cuda]="scale_cuda"  [qsv]="scale_qsv"  [none]="scale"
)

# ── HW decode args per backend ──────────────────────────────────────────
declare -A HW_DECODE_ARGS=(
  [cuda]="-hwaccel cuda -hwaccel_output_format cuda"
  [qsv]="-hwaccel qsv -hwaccel_output_format qsv"
  [none]=""
)

# ── Quality tier → CRF/CQ mapping (encoder:tier:resolution_bucket) ─────
# Per-encoder tables because CRF values are NOT comparable across encoders.
# Per codec-database-addendum.md:20 + :84-92, hevc_nvenc needs ~+2 CQ over
# libx265 for equivalent visual quality; h264_nvenc tracks libx264 closely;
# libsvtav1 has its own scale (transparent=20-25, good=28-32, fast=35-43,
# default=35).
# Resolution buckets: sd (<=576p), hd (<=720p), fhd (<=1080p), uhd (>1080p).
declare -A QUALITY_TIERS=(
  # === H.264 ===
  # libx264  — addendum:84  transparent=18  good=22-24  fast=26-28
  [libx264:transparent:sd]=18    [libx264:transparent:hd]=18    [libx264:transparent:fhd]=18   [libx264:transparent:uhd]=20
  [libx264:excellent:sd]=20      [libx264:excellent:hd]=21      [libx264:excellent:fhd]=22     [libx264:excellent:uhd]=24
  [libx264:good:sd]=22           [libx264:good:hd]=23           [libx264:good:fhd]=24          [libx264:good:uhd]=26
  [libx264:fast:sd]=24           [libx264:fast:hd]=26           [libx264:fast:fhd]=28          [libx264:fast:uhd]=30
  # h264_nvenc — addendum:87  transparent=18  good=20-24  fast=26-30
  [h264_nvenc:transparent:sd]=18 [h264_nvenc:transparent:hd]=18 [h264_nvenc:transparent:fhd]=18 [h264_nvenc:transparent:uhd]=20
  [h264_nvenc:excellent:sd]=20   [h264_nvenc:excellent:hd]=21   [h264_nvenc:excellent:fhd]=22   [h264_nvenc:excellent:uhd]=24
  [h264_nvenc:good:sd]=22        [h264_nvenc:good:hd]=23        [h264_nvenc:good:fhd]=24        [h264_nvenc:good:uhd]=26
  [h264_nvenc:fast:sd]=26        [h264_nvenc:fast:hd]=28        [h264_nvenc:fast:fhd]=29        [h264_nvenc:fast:uhd]=30
  # h264_qsv  — Intel QSV behaves close to NVENC on h264; mirror nvenc values
  [h264_qsv:transparent:sd]=18   [h264_qsv:transparent:hd]=18   [h264_qsv:transparent:fhd]=18   [h264_qsv:transparent:uhd]=20
  [h264_qsv:excellent:sd]=20     [h264_qsv:excellent:hd]=21     [h264_qsv:excellent:fhd]=22     [h264_qsv:excellent:uhd]=24
  [h264_qsv:good:sd]=22          [h264_qsv:good:hd]=23          [h264_qsv:good:fhd]=24          [h264_qsv:good:uhd]=26
  [h264_qsv:fast:sd]=26          [h264_qsv:fast:hd]=28          [h264_qsv:fast:fhd]=29          [h264_qsv:fast:uhd]=30

  # === HEVC ===
  # libx265   — addendum:85  transparent=22  good=24-26  fast=28-30
  [libx265:transparent:sd]=22    [libx265:transparent:hd]=22    [libx265:transparent:fhd]=22   [libx265:transparent:uhd]=24
  [libx265:excellent:sd]=23      [libx265:excellent:hd]=23      [libx265:excellent:fhd]=24     [libx265:excellent:uhd]=25
  [libx265:good:sd]=24           [libx265:good:hd]=25           [libx265:good:fhd]=26          [libx265:good:uhd]=27
  [libx265:fast:sd]=26           [libx265:fast:hd]=27           [libx265:fast:fhd]=28          [libx265:fast:uhd]=30
  # hevc_nvenc — addendum:88  transparent=24  good=28-31  fast=33-36 (KEY: +2 vs libx265)
  [hevc_nvenc:transparent:sd]=24 [hevc_nvenc:transparent:hd]=24 [hevc_nvenc:transparent:fhd]=24 [hevc_nvenc:transparent:uhd]=26
  [hevc_nvenc:excellent:sd]=26   [hevc_nvenc:excellent:hd]=27   [hevc_nvenc:excellent:fhd]=28   [hevc_nvenc:excellent:uhd]=29
  [hevc_nvenc:good:sd]=28        [hevc_nvenc:good:hd]=29        [hevc_nvenc:good:fhd]=30        [hevc_nvenc:good:uhd]=31
  [hevc_nvenc:fast:sd]=31        [hevc_nvenc:fast:hd]=33        [hevc_nvenc:fast:fhd]=34        [hevc_nvenc:fast:uhd]=36
  # hevc_qsv — Intel QSV mirrors NVENC for hevc
  [hevc_qsv:transparent:sd]=24   [hevc_qsv:transparent:hd]=24   [hevc_qsv:transparent:fhd]=24   [hevc_qsv:transparent:uhd]=26
  [hevc_qsv:excellent:sd]=26     [hevc_qsv:excellent:hd]=27     [hevc_qsv:excellent:fhd]=28     [hevc_qsv:excellent:uhd]=29
  [hevc_qsv:good:sd]=28          [hevc_qsv:good:hd]=29          [hevc_qsv:good:fhd]=30          [hevc_qsv:good:uhd]=31
  [hevc_qsv:fast:sd]=31          [hevc_qsv:fast:hd]=33          [hevc_qsv:fast:fhd]=34          [hevc_qsv:fast:uhd]=36

  # === AV1 ===
  # libsvtav1 — ladder widened per 2026-04 audit. Previous transparent=25
  # @ 1080p was too tight vs cross-encoder VMAF equivalence
  # (SVT-AV1 CRF 30 ≈ x265 CRF 21 ≈ x264 CRF 16); ab-av1 + ffmpeg.party +
  # StreamingLearningCenter all center transparent in the 28-30 zone for
  # 1080p. Widening saves ~40-60% bitrate on AV1 encodes with no visible
  # quality loss. UHD bumped proportionally.
  [libsvtav1:transparent:sd]=26  [libsvtav1:transparent:hd]=27  [libsvtav1:transparent:fhd]=28  [libsvtav1:transparent:uhd]=30
  [libsvtav1:excellent:sd]=28    [libsvtav1:excellent:hd]=30    [libsvtav1:excellent:fhd]=30    [libsvtav1:excellent:uhd]=33
  [libsvtav1:good:sd]=31         [libsvtav1:good:hd]=32         [libsvtav1:good:fhd]=33         [libsvtav1:good:uhd]=35
  [libsvtav1:fast:sd]=36         [libsvtav1:fast:hd]=37         [libsvtav1:fast:fhd]=38         [libsvtav1:fast:uhd]=41
  # av1_nvenc / av1_qsv — mirror libsvtav1 values. These encoders broadly
  # track AV1 reference quality at similar numeric CQ.
  [av1_nvenc:transparent:sd]=26  [av1_nvenc:transparent:hd]=27  [av1_nvenc:transparent:fhd]=28  [av1_nvenc:transparent:uhd]=30
  [av1_nvenc:excellent:sd]=28    [av1_nvenc:excellent:hd]=30    [av1_nvenc:excellent:fhd]=30    [av1_nvenc:excellent:uhd]=33
  [av1_nvenc:good:sd]=31         [av1_nvenc:good:hd]=32         [av1_nvenc:good:fhd]=33         [av1_nvenc:good:uhd]=35
  [av1_nvenc:fast:sd]=36         [av1_nvenc:fast:hd]=37         [av1_nvenc:fast:fhd]=38         [av1_nvenc:fast:uhd]=41
  [av1_qsv:transparent:sd]=26    [av1_qsv:transparent:hd]=27    [av1_qsv:transparent:fhd]=28    [av1_qsv:transparent:uhd]=30
  [av1_qsv:excellent:sd]=28      [av1_qsv:excellent:hd]=30      [av1_qsv:excellent:fhd]=30      [av1_qsv:excellent:uhd]=33
  [av1_qsv:good:sd]=31           [av1_qsv:good:hd]=32           [av1_qsv:good:fhd]=33           [av1_qsv:good:uhd]=35
  [av1_qsv:fast:sd]=36           [av1_qsv:fast:hd]=37           [av1_qsv:fast:fhd]=38           [av1_qsv:fast:uhd]=41
)

# ── Preset normalization (codec:backend:normalized_speed → vendor preset) ─
declare -A PRESET_MAP=(
  # h264 NVENC
  [h264:cuda:slowest]=p7  [h264:cuda:slower]=p7  [h264:cuda:slow]=p6
  [h264:cuda:medium]=p5   [h264:cuda:fast]=p4    [h264:cuda:faster]=p3  [h264:cuda:fastest]=p2
  # h264 QSV
  [h264:qsv:slowest]=veryslow [h264:qsv:slower]=slower [h264:qsv:slow]=slow
  [h264:qsv:medium]=medium    [h264:qsv:fast]=fast     [h264:qsv:faster]=faster [h264:qsv:fastest]=veryfast
  # h264 CPU
  [h264:none:slowest]=veryslow [h264:none:slower]=slower [h264:none:slow]=slow
  [h264:none:medium]=medium    [h264:none:fast]=fast     [h264:none:faster]=faster [h264:none:fastest]=veryfast
  # hevc NVENC
  [hevc:cuda:slowest]=p7  [hevc:cuda:slower]=p7  [hevc:cuda:slow]=p6
  [hevc:cuda:medium]=p5   [hevc:cuda:fast]=p4    [hevc:cuda:faster]=p3  [hevc:cuda:fastest]=p2
  # hevc QSV
  [hevc:qsv:slowest]=veryslow [hevc:qsv:slower]=slower [hevc:qsv:slow]=slow
  [hevc:qsv:medium]=medium    [hevc:qsv:fast]=fast     [hevc:qsv:faster]=faster [hevc:qsv:fastest]=veryfast
  # hevc CPU
  [hevc:none:slowest]=veryslow [hevc:none:slower]=slower [hevc:none:slow]=slow
  [hevc:none:medium]=medium    [hevc:none:fast]=fast     [hevc:none:faster]=faster [hevc:none:fastest]=veryfast
  # av1 NVENC (RTX 40+ only)
  [av1:cuda:slowest]=p7  [av1:cuda:slower]=p7  [av1:cuda:slow]=p6
  [av1:cuda:medium]=p5   [av1:cuda:fast]=p4    [av1:cuda:faster]=p3  [av1:cuda:fastest]=p2
  # av1 QSV (Intel Arc only)
  [av1:qsv:slowest]=veryslow [av1:qsv:slower]=slower [av1:qsv:slow]=slow
  [av1:qsv:medium]=medium    [av1:qsv:fast]=fast     [av1:qsv:faster]=faster [av1:qsv:fastest]=veryfast
  # av1 CPU (SVT-AV1 — numeric presets, capped at practical max 10 per
  # codec-database-addendum.md:56 — presets 11-13 are debug/experimental)
  [av1:none:slowest]=4  [av1:none:slower]=5  [av1:none:slow]=6
  [av1:none:medium]=8   [av1:none:fast]=9    [av1:none:faster]=10 [av1:none:fastest]=10
)

# ── Per-encoder quality extras (toggled via Settings) ────────────────────
# Stored as space-separated strings; worker uses `read -ra` to expand.
# Per-encoder so AV1 paths drop flags that don't apply (NVENC's b_ref_mode
# is a no-op on AV1 but harmless; QSV's -rdo and -mbbrc are HARD ERRORS
# on av1_qsv — that combination would kill the encode and classify as
# `unknown` since no stderr pattern matches).
# NVENC extras = optional quality knobs. Note: -multipass fullres is now
# ALWAYS-ON in the worker baseline (codec-database-addendum.md:17 says ffmpeg
# docs recommend it for quality; community tools all leave it disabled which
# is the wrong default for a quality-first transcoder). It used to live here.
declare -A NVENC_EXTRAS_PER_ENCODER=(
  [h264_nvenc]="-rc-lookahead 32 -spatial_aq:v 1 -temporal_aq:v 1 -aq-strength:v 8 -b_ref_mode middle"
  [hevc_nvenc]="-rc-lookahead 32 -spatial_aq:v 1 -temporal_aq:v 1 -aq-strength:v 8 -b_ref_mode middle"
  [av1_nvenc]="-rc-lookahead 32 -spatial_aq:v 1 -temporal_aq:v 1 -aq-strength:v 8"
)
# -a53cc 0 used to be on every row. Dropped per 2026-04 audit: input SEI
# 608/708 closed captions are stripped during the NVENC re-encode
# regardless of -a53cc value (encoder decodes frames, captions don't
# travel). Setting -a53cc 0 was a no-op for our workflow — it only
# matters for -c:v copy paths where we AREN'T re-encoding.

declare -A QSV_EXTRAS_PER_ENCODER=(
  [h264_qsv]="-look_ahead 1 -extbrc 1 -look_ahead_depth 100 -async_depth 4 -rdo 1 -mbbrc 1"
  [hevc_qsv]="-look_ahead 1 -extbrc 1 -look_ahead_depth 100 -async_depth 4 -rdo 1 -mbbrc 1"
  [av1_qsv]="-look_ahead 1 -extbrc 1 -look_ahead_depth 100 -async_depth 4"
)

# ── Per-codec max channels ──────────────────────────────────────────────
# AC3 (Dolby Digital) is a 5.1-era codec and cannot encode >6 channels;
# ffmpeg either errors or silently downmixes. We cap in the worker so
# the user gets a clean downmix + log rather than a confusing failure.
declare -A AUDIO_CODEC_MAX_CHANNELS=(
  [aac]=8   [ac3]=6   [eac3]=8   [opus]=8
)

# ── Per-preset / per-codec / per-channel audio bitrate defaults ─────────
# Keyed on (preset : codec : output_channel_layout). Output channels are
# post-downmix + post-codec-cap. Lookup falls back to "balanced" when
# preset=custom or a per-preset value is missing.
#
# fast        — ~25% below balanced; saves space, still acceptable
# balanced    — research-cited transparent-tier defaults; the "right" values
# transparent — bumped where the codec has headroom (AC3 5.1 maxes at 448k
#               so transparent matches balanced there; EAC3 / Opus / AAC
#               accept higher rates for max quality)
#
# Per-codec rationale carried over from the previous flat table:
#   AAC (libfdk_aac): industry-standard LC-AAC rates (~64k/channel).
#   AC3:             Dolby reference 448k for 5.1 (codec maxes at 5.1).
#   EAC3:            448k for 5.1, 640k for 7.1 (Atmos-adjacent spec).
#   Opus (libopus):  per RFC 7587 — 128k stereo transparent, ~56k/ch surround.
declare -A AUDIO_BITRATE_DEFAULTS=(
  # === fast preset ===
  # Opus 7.1 fast bumped 320k → 384k (40k/ch → 48k/ch — prev was too thin
  # for 8-channel music per audit).
  [fast:aac:mono]=64k     [fast:aac:stereo]=96k     [fast:aac:surround_51]=320k   [fast:aac:surround_71]=384k
  [fast:ac3:mono]=80k     [fast:ac3:stereo]=128k    [fast:ac3:surround_51]=384k   [fast:ac3:surround_71]=384k
  [fast:eac3:mono]=80k    [fast:eac3:stereo]=128k   [fast:eac3:surround_51]=384k  [fast:eac3:surround_71]=448k
  [fast:opus:mono]=48k    [fast:opus:stereo]=96k    [fast:opus:surround_51]=256k  [fast:opus:surround_71]=384k

  # === balanced preset (research-cited defaults) ===
  # AAC 7.1 balanced bumped 512k → 576k. 512k = 64k/ch = Apple's floor
  # for AAC 7.1, not the "balanced" target. 576k = 72k/ch lands mid-tier.
  [balanced:aac:mono]=80k    [balanced:aac:stereo]=128k    [balanced:aac:surround_51]=384k   [balanced:aac:surround_71]=576k
  [balanced:ac3:mono]=96k    [balanced:ac3:stereo]=192k    [balanced:ac3:surround_51]=448k   [balanced:ac3:surround_71]=448k
  [balanced:eac3:mono]=96k   [balanced:eac3:stereo]=192k   [balanced:eac3:surround_51]=448k  [balanced:eac3:surround_71]=640k
  [balanced:opus:mono]=64k   [balanced:opus:stereo]=128k   [balanced:opus:surround_51]=384k  [balanced:opus:surround_71]=448k

  # === transparent preset (codec headroom bumps) ===
  [transparent:aac:mono]=128k   [transparent:aac:stereo]=192k    [transparent:aac:surround_51]=512k   [transparent:aac:surround_71]=640k
  [transparent:ac3:mono]=128k   [transparent:ac3:stereo]=224k    [transparent:ac3:surround_51]=448k   [transparent:ac3:surround_71]=448k
  [transparent:eac3:mono]=128k  [transparent:eac3:stereo]=256k   [transparent:eac3:surround_51]=576k  [transparent:eac3:surround_71]=768k
  # Opus trimmed per RFC 6716 / Xiph listening tests — previous values
  # (stereo=160k, 5.1=448k, 7.1=512k) overshot diminishing returns:
  #   stereo 128k is already transparent (Xiph)
  #   5.1  384k = 64k/ch matches Xiph reference
  #   7.1  448k = 56k/ch stays transparent at scale
  [transparent:opus:mono]=96k   [transparent:opus:stereo]=128k   [transparent:opus:surround_51]=384k  [transparent:opus:surround_71]=448k
)

# ── Quality presets (top-level user choice) ────────────────────────────
# Three named presets bundle quality_tier + encoder_speed + extras
# toggles + film_grain into a single user-facing decision. The fourth
# value "custom" means: skip the preset application, use individual
# field values as-is (advanced mode).
#
# Worker reads $TRANSCODARR_PRESET; if not "custom", these values
# overwrite the individual settings. GUI hides the individual fields
# when a preset is active.
declare -A PRESETS=(
  # fast — speed > quality. For one-off views or bulk processing where
  # quality concerns are minor. Disables extras for max throughput.
  [fast:quality_tier]=fast
  [fast:encoder_speed]=fastest
  [fast:nvenc_extras]=false
  [fast:qsv_extras]=false
  [fast:film_grain]=0

  # balanced — good quality at reasonable speed. Sensible default for
  # library encoding. Enables extras for quality wins, keeps encoder
  # at medium speed (canonical default across all encoders).
  [balanced:quality_tier]=excellent
  [balanced:encoder_speed]=medium
  [balanced:nvenc_extras]=true
  [balanced:qsv_extras]=true
  [balanced:film_grain]=8

  # transparent — highest practical quality. Slower encode but archival-
  # grade output. CRF set to research's "transparent" tier per encoder.
  [transparent:quality_tier]=transparent
  [transparent:encoder_speed]=slow
  [transparent:nvenc_extras]=true
  [transparent:qsv_extras]=true
  [transparent:film_grain]=8
)

# Fields controlled by presets (used by GUI to hide them when preset
# is active). Keep this list in sync with PRESETS keys above.
PRESET_CONTROLLED_FIELDS="quality_tier encoder_speed nvenc_extras qsv_extras film_grain"

# ── Encoder-specific tuning params ──────────────────────────────────────
# libx265 — HandBrake's production-tuned x265 params, codec-database-
# addendum.md:60-70. "These are HandBrake's production-tuned x265 params
# after years of testing... psy-rd=0.75 and psy-rdoq=4.0 are the key
# perceptual optimization knobs." Optimizes perceptual quality over PSNR.
LIBX265_PARAMS="psy-rd=0.75:psy-rdoq=4.0:aq-mode=1:rd=4:rect=0:strong-intra-smoothing=0:rskip=2"

# libsvtav1 — film-grain synthesis (addendum:51-52,183) saves 20-40%
# bitrate on grainy content. tune=3 is iq/perceptual mode (addendum:53).
# Film-grain level is built dynamically in worker.sh from the user's
# TRANSCODARR_AV1_FILM_GRAIN setting (default 8 = HandBrake's). 0 means
# synthesis disabled. This template is no longer used directly — kept
# for documentation/reference only.
# SVTAV1_PARAMS="film-grain=8:tune=3"  # legacy template

# ── Container-specific muxer flags ──────────────────────────────────────
# MP4 production set:
#   faststart              — moves moov atom to file start for streaming
#   disable_chpl           — prevents Nero chapter-list corruption in some
#                            players (older LG/Samsung TVs, PS5 Media Player)
#   write_colr             — embeds color metadata (primaries/transfer/
#                            matrix) in the 'colr' atom; Plex/Jellyfin read
#                            this for HDR and BT.2020 tagging
#   negative_cts_offsets   — enables ISOBMFF version-1 CTTS, which permits
#                            negative CTS offsets and avoids the edit-list
#                            workaround that skips the first 1-2 frames on
#                            Apple TV / older Roku
#   -brand mp42            — declares MP4 base media file format (ffmpeg
#                            defaults to "isom"; mp42 is what QuickTime,
#                            Plex, tvOS, and Infuse expect)
#   -write_tmcd 0          — suppresses the QuickTime timecode track that
#                            some non-Apple players expose as a stray
#                            "track 3" Plex sometimes mis-identifies
declare -a MP4_MUXER_FLAGS=(
  -movflags +faststart+disable_chpl+write_colr+negative_cts_offsets
  -brand mp42
  -write_tmcd 0
)

# ── Stderr failure pattern classifications ───────────────────────────────
# Parallel indexed arrays — ordered specific-to-generic, first match wins.
# Keep specific HW-filter patterns at the top since they're actionable
# (signal a codec-table or filter-chain bug) and a generic "Error
# initializing" below would otherwise swallow them as hw_init_failed.
STDERR_PATTERNS=(
  "Impossible to convert between the formats supported by the filter"
  "Error reinitializing filters"
  "Could not open encoder before EOF"
  "Nothing was written into output file"
  "too many packets buffered"
  "Unknown encoder"
  "No space left on device"
  "No such file"
  "Permission denied"
  "Element exceeds containing master element"
  "Non-monotonic DTS"
  "illegal reordering"
  "Stream map .* matches no streams"
  "Invalid data found"
  "Avi capacity exceeded"
  "Subtitle codec.*not compatible"
  "Error initializing"
  "out of memory"
  "Conversion failed"
)
STDERR_CLASSES=(
  "filter_format_mismatch"
  "filter_reinit_failed"
  "encoder_open_failed"
  "no_packets_written"
  "mux_queue_overflow"
  "codec_not_supported"
  "disk_full"
  "file_not_found"
  "permission_denied"
  "mkv_corrupt"
  "dts_non_monotonic"
  "illegal_reordering"
  "stream_map_invalid"
  "corrupt_input"
  "container_limit"
  "subtitle_incompatible"
  "hw_init_failed"
  "out_of_memory"
  "conversion_failed"
)
