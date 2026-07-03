# Changelog

All notable changes to Transcodarr will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.2.1] - 2026-07-03

Security and runtime-hardening patch release. No new features — hardens the
3.2.0 Language-Processing + rename-on-convert pipeline and closes the findings
from a full-project security review.

### Security
- **Settings stored-XSS.** `esc()` now escapes `"` and `'`, so config values
  (Plex/Radarr/Sonarr URLs and tokens) can no longer break out of a
  double-quoted HTML attribute in the Settings page.
- **Command injection in `/api/status`.** The active-jobs builder no longer
  shells out `echo "$path" | md5sum` on a media path (command substitution
  embedded in a crafted filename could execute in-container); it now uses
  `Digest::MD5::md5_hex`.
- **Destructive failure-policy arming.** `destructive_match_ok` whitelists only
  the source-bad rule ids (`input_invalid`, `corrupt_source`); observe-only
  catch-alls (`environment`, `unknown`, `output_*`, …) can no longer be armed
  for class-wide blocklist+delete by a hand-edited config.
- **SSRF / curl arg-injection.** Integration-test and saved-config Plex/*arr URLs
  are validated (http(s)-only; loopback/link-local rejected) and every `curl`
  call gained a `--` argument terminator.

### Fixed
- Rename-on-convert overwrite/delete race: a destination lock
  (promote-after-acquire) stops two workers racing the same output path.
- Language pipeline: atomic priority-requeue after an English tag; per-file tag
  lock with quiet duplicate handling; language workers honor `tc:pause`;
  empty-field-safe (`\037`) parsing in the language-release loops.
- Fail loudly / fail closed: copy-back failures record
  `copy_back_failed`/`output_failure`; unreadable destination free space
  requeues (exit 75) instead of bypassing the space check; capability-probe
  writes no longer abort the sweep on a transient Valkey error.
- Pipeline supervision: the ffprobe, wrangler, language, load-balancer, and
  space-monitor stages now restart on unexpected exit.
- Failure policy: `rename_failure` and `output_failure` registered as
  observe-only classes (previously unmatched by the engine).
- Commentary detection uses word-bounded tokens via a single shared helper.
- GUI: config-save failures are surfaced; number-field values are escaped;
  Failed and Flagged tabs reconcile mutable rows (stale-card removal).

### Housekeeping
- `models/` added to `.gitignore`.
- Cache reset now also clears the language queues.
- Filter-chain unit test extracts source blocks by semantic markers instead of
  hardcoded line ranges.

## [3.2.0] - 2026-07-01

### Added
- **Language Processing (opt-in, `language.enabled`).** A whisper.cpp-based
  audio-language detection stage that fixes the `no_eng_audio`
  false-positive problem: multi-track files whose audio is merely
  *untagged* (common on BluRay remuxes) were previously indistinguishable
  from genuinely foreign-audio files and could be blocklisted/deleted by
  the destructive failure-policy path. Untagged multi-track MKV/MP4-family
  files now divert to a `lang_detect` worker pool, which samples the audio
  (coverage-based sampling with a retry boundary), runs whisper.cpp per
  candidate, takes a per-candidate vote/median verdict, and either tags the
  track `eng` in place (`mkvpropedit` for MKV, an `ffmpeg -map 0 -c copy`
  remux + `ffprobe` verify for MP4/M4V/MOV) or leaves genuinely
  foreign/unknown audio correctly failed. Cross-vendor GPU with CPU
  fallback, concurrency configurable (`TRANSCODARR_LANGUAGE_WORKERS`),
  whisper model auto-selected and checksummed at build time. A new
  Settings card shows the detected backend, device, and model, with a
  manual `POST /api/lang/rescan`. A startup backfill sweeps the existing
  `no_eng_audio` failure backlog once the feature is enabled. Strict
  verification extends diversion to single-track unknown-language files
  too, so `unverified_lang`-flagged media no longer silently passes as
  playable-but-unverified.
- **Container rename-on-convert.** When a transcode's output container
  differs from the source file's extension (e.g. re-encoding a `.mp4`
  whose content becomes Matroska under `output_container=mkv`), the
  worker now writes the output with the correct extension, removes the
  stale original, and reconciles Radarr/Sonarr + Plex against the renamed
  path — including the corrective API calls needed when a plain
  rescan/refresh doesn't pick up the rename cleanly. All path-keyed Redis
  state (admission, hashes, processed/failed rows) migrates to the new
  path atomically, guarded by a collision check and an mv-exit gate.
  Ownership (`chown`) is applied to the renamed final path, not the
  removed original.

### Fixed
- **Plex path matching now survives XML-escaped UTF-8.** The Plex
  post-transcode path matcher (`transcodarr-plex-path.pl`) previously
  failed to match library paths containing non-ASCII characters that Plex
  XML-escapes; matching is now byte-correct and the UTF-8 helper output
  no longer gets mangled in the process.
- **MP4/M4V/MOV audio tag writes.** The language tagger now supports the
  MP4 family (previously MKV-only), and correctly drops MP4 `data`
  streams instead of choking on them during the tag-and-remux.

## [3.1.0] - 2026-06-24

### Added
- **Per-disk ignore policy (`disks.ignored`).** New `TRANSCODARR_IGNORED_DISKS`
  env var (CSV of disk slugs, e.g. `disk3,disk7`) seeds the runtime set
  `tc:disk:ignored` at startup. The load balancer's `dispatch_eligible`
  checks SISMEMBER and skips items targeting ignored disks: they stay in
  the queue lane, just don't dispatch. Settings now includes a dynamic
  disk chip selector that saves `disks.ignored` and mirrors it into
  `tc:disk:ignored` live, without changing scan/probe/import/direct queue
  behavior.
- **Disk filter bar on Failed and Done tabs.** The disk chip filter that
  already existed on the GPU/CPU queue tabs now applies to the Failed and
  Done TSV tabs too. `/api/tsv/{processed,failed}` accept a `disk` query
  parameter and `serve_tsv` computes unfiltered disk counts directly from
  the TSV so the chip labels reflect TSV state, not queue state. The
  filter row hides itself on the Settings tab.
- **In-place UI refresh that preserves "Load more" expansion.** The 10s
  poll no longer wipes-and-rerenders the visible list. Queue tabs (gpu,
  cpu) reconcile against the API response — appendChild reorders existing
  DOM elements to match the new queue order, new keys get rendered in
  place, departed keys get removed. TSV tabs (processed, failed) detect
  newly-appended rows in the top-PAGE response and prepend them, leaving
  every row below untouched. Per-row click-to-expand state survives all
  refreshes because elements are reused, never destroyed. Tab switches
  also preserve expansion as long as the disk filter hasn't changed.
- **Clear Failed list action.** The Failed tab now has a confirmed clear
  button backed by `POST /api/tsv/failed/clear`, which truncates only
  `failed-files.tsv`. That TSV is a reporting list, not a processing gate.

### Fixed
- **Plex section IDs and path roots are now detected.** The Plex
  connection test reads `/library/sections`, derives the movie/show
  section IDs and Plex-visible roots, and records them into config
  automatically. These values remain internal worker settings, but are no
  longer shown as manual Settings fields or public setup variables.
- **Plex post-transcode analysis for in-place replacements.** After the
  existing targeted folder refresh, the worker now resolves the exact
  Plex metadata item by Plex-visible file path and calls
  `/library/metadata/{ratingKey}/analyze`, so Plex re-reads stream
  metadata after Transcodarr replaces a file at the same path.
- **Plex notify no longer silently no-ops on partial config.** The config
  reader (`transcodarr-config-read.pl`) now parses `config.json` with
  `JSON::PP` and recursively flattens scalar values, so a nested section
  appearing before `integrations` (e.g. `failure_policy.rules`) no longer
  stops it from exporting `PLEX_*` / `*_URL` / `*_API_KEY`. Previously the
  one-level scanner left `PLEX_URL` empty, so the post-transcode Plex
  analyze above never actually ran. The worker also logs a redacted warning
  when Plex is only partially configured, so this can't fail silently again.
- **Destination-disk space failures no longer dead-end in `tc:parked:space:*`.**
  Worker exit 75 with `space_fail_kind=dest` now records a
  `dest_space_retry` attempt, releases any SSD temp-pool lease, and requeues
  the item through its original load-balancer lane (bulk, import, or direct).
  The load balancer now also tracks `tc:disk:<disk>:free_kb` and holds a
  requeued item until that disk has enough free space for the item, avoiding
  the old park-or-hot-loop tradeoff. Non-zero destination retry caps still
  park as `parked_dest_space_timeout`; the default remains retry forever.
- **Load balancer priority gate stalled bulk when imports were blocked
  by per-disk cap.** The LB dispatch loop used an exclusive `if/elif`
  between the import queue and the bulk queue: if `tc:lb:*:import:ready`
  had any items, the bulk fall-through was never tried. That looks
  correct in the happy case (priority preserved), but if every queued
  import was blocked on `STREAMS_PER_DISK` (e.g. one import on a
  disk that had already reached its cap from other work), the LB
  stalled — it kept re-trying the same blocked imports every tick
  and never dispatched bulk for that mode. Observed symptom: 1 CPU
  import on a disk at 4/4 cap with 11k+ bulk items across the other
  9 disks, and only 5-6 workers active instead of the configured 16.
  Fix: track whether the import dispatch actually placed an item
  this tick, and fall through to bulk dispatch if it didn't.
  Priority is still preserved when imports are dispatchable; bulk
  only sneaks in when imports are blocked. Latent since v1.0.0, only
  visible under specific conjunctions of import presence + disk
  saturation + bulk-waiting-on-different-disks.
- **Fallback failed-row schema mismatch.** The three entrypoint paths
  that wrote 4-column rows to `failed-files.tsv` (`parked_dest_space`,
  `parked_space_timeout`, `worker_exit_<rc>`) now emit the same 8-column
  shape as `worker.sh::record_failed`, so every failed row carries the
  disk column and the new disk filter applies uniformly.

### Changed
- **Storage settings now distinguish destination disks from the SSD temp pool.**
  The old ambiguous `TRANSCODARR_SPACE_RETRY_TIMEOUT` remains as a legacy
  fallback, but new configs and the GUI use
  `TRANSCODARR_DEST_SPACE_RETRY_TIMEOUT` for destination disks and
  `TRANSCODARR_SSD_SPACE_RETRY_TIMEOUT` for the optional SSD temp pool.
  GUI labels now say "Destination Disk" or "SSD Temp Pool" where scope matters.
- **Host mount restructure (deployment-config only, no app-behavior
  change).** The host-side bind sources for the tmp pool and the
  Sonarr/Radarr queue rendezvous were consolidated into a single
  location. Container-side paths are unchanged (`/state`, `/queue`,
  `/tmp-transcode`), and the bundled `docker-compose.yml` template is
  unaffected — it ships placeholder paths. If you wire Sonarr/Radarr to
  the `/queue` volume, make sure both sides point at the same host path.

## [1.9.0] - 2026-04-14

> Rolls up every user-facing change since `1.0.0`. See the entries
> below for the full breakdown of features, fixes, and changes.

### Added
- **Intel QSV and CPU-only encoding support.** The worker's GPU pipeline
  now branches on `TRANSCODARR_HW_DECODING` (`cuda` / `qsv` / `none`).
  `cuda` remains the production default via NVENC; `qsv` uses Intel
  QuickSync via `h264_qsv`; `none` falls back to libx264 on CPU for
  hosts without a supported GPU. Each branch has its own scale filter
  and codec flag set; the Dockerfile picks up the right media-driver
  dependencies via the `GPU_TYPE` build arg.
- **Priority job lifecycle via `.job` files.** Sonarr/Radarr Custom
  Scripts drop a hash-prefixed `<md5(filepath)>_<ns>.job` file into a
  shared `/queue` volume on download completion. A blocking
  `startup_job_bridge` ingests any pending `.job` files before the bulk
  queue builder starts, so imports always claim their `tc:seen` dedupe
  slot first. A background `job_bridge` polls the directory every 2s
  for new arrivals. Priority items get dedicated import lanes at every
  pipeline stage (`tc:candidates:import:ready` → `tc:gpu:import:ready`
  → `tc:lb:gpu:import:ready` → load balancer import-first dispatch),
  so a freshly-downloaded file never has to wait behind a large
  bulk backlog. `.job` files persist through failures and restarts;
  worker success or "already at spec" classification cleans them up.
- **Standalone deployment.** Transcodarr is now built and deployed as
  a self-contained image. Scripts are also baked into the image via
  `COPY scripts/ /scripts/` so production builds run the exact code
  they were built with; the bundled `docker-compose.yml` template
  still bind-mounts `./scripts:/scripts:ro` for developer convenience
  while iterating on a clone. The `queue-import.sh` shim for
  Sonarr/Radarr Custom Scripts ships in the same `scripts/` tree and
  can be copied directly onto the Sonarr/Radarr hosts.
- **Web dashboard enhancements.** Queue list items are now expandable
  with per-item detail (disk, codec, channels, phase), the `processed`
  and `failed` TSV schemas include richer metadata (output size, out
  codec, disk name), and the `/api/queue/*` endpoints return queue
  metadata alongside the paginated list.
- **SSD reservation atomic lease subsystem** (off by default — only
  active when `TRANSCODARR_TMP_DIR` is set). Replaces the legacy
  global `tc:ssd:reserved_kb` counter with per-reservation lease keys
  (`tc:ssd:lease:<file_hash>:<nonce>`) indexed via `tc:ssd:leases`,
  managed by an atomic admission Lua script, reconciled every cycle by
  `space_monitor`, and released via a three-layer safety net (worker
  EXIT trap → consumer fallthrough release → TTL + reconciliation
  sweep). Closes the SIGKILL leak path that would surface the moment
  anyone enabled the SSD tmp pool.

### Changed
- **Icon references canonicalized.** The Unraid Docker label, the
  Unraid CA template, the web UI header, the empty-state image, and
  the Homepage widget README example all now resolve to
  `https://github.com/jb14813/Transcodarr/raw/main/icon.png`. The
  self-hosted `/icon.png` endpoint is still served by the API for
  backward compatibility but nothing in the stack points at it
  anymore.

### Fixed
- **Priority imports silently demoted to bulk lane on SSD-space retry**
  (latent pre-v1.0 correctness bug). The exit-75 retry branch in
  `worker_consumer` was using `${item%|*}|${first_space_fail_ts}` to
  "update" the retry timestamp, but that pattern actually stripped
  `item_route` (field 11) and replaced it with the timestamp. On the
  next `worker_consumer` pass the item would parse as `item_route=<ts>`,
  fail the `= "import"` check, and get routed to the bulk lane. Invisible
  today because `TRANSCODARR_SPACE_RETRY_TIMEOUT` defaults to `0` (no
  retries ever happen), but would have surfaced the moment anyone
  enabled retries. Fixed via explicit field rebuild that preserves
  both `item_route` and the SSD lease key.
- **Duration validation: replaced end-pts check with span-based per-stream
  comparison, fixed three latent probe bugs, removed skip-and-trust fallback**
  The original validation compared
  container-level `format=duration`, which in MKV is `max()` across all
  streams including subtitles — so a source with a 60-second PGS subtitle
  track extending past the video end reported a container duration 60s
  longer than the real content, and a correct encode that dropped the
  subtitle overhang was flagged as truncated. The rewrite does per-stream
  span comparison (`span = last_packet_pts - first_packet_pts`) for the
  video stream and the selected audio stream, which is independent of
  subtitle timelines, independent of container `format=duration`, and
  normalized for sources with non-zero `start_time` (e.g. files cut with
  `-copyts -ss N`). The helper lives at
  `scripts/transcodarr-lib.sh::get_stream_span` and takes a `seek_from`
  argument; when the caller passes `seek_from=0` as a sentinel (source's
  `format=duration` is unreadable or N/A), the helper skips the seek-
  based fast path and goes straight to a full-stream packet scan. Three
  helper bugs were caught and fixed during validation of the
  rewrite:
    1. **ffprobe `-of csv=p=0` trailing comma.** Output of single-column
       `packet=pts_time` probes ends with `9.985000,\n` — a literal comma
       appended by the CSV writer even for a single field. The helper's
       numeric-validation case pattern rejected that as non-numeric,
       cleared the value, and returned 0. Fixed by switching output format
       to `-of default=noprint_wrappers=1:nokey=1`, which writes bare
       numeric lines.
    2. **Negative `pts_time` rejected as non-numeric.** AAC encoders
       (libfdk_aac and native) emit priming samples at the start of the
       output with a negative `pts_time` like `-0.043000`. The helper's
       case pattern `*[!0-9.]*` flagged the leading minus as an invalid
       character. Fixed by expanding the character class to
       `*[!-0-9.]*` (POSIX bracket semantics treat `-` at position 1
       after `!` as literal).
    3. **First-packet probe seek-skipped negative-pts packets.** The
       fast-path `-read_intervals "0%+5"` told ffprobe to seek to
       `pts=0` and read forward, which skipped any packet at `pts<0` —
       so for an AAC re-encoded output, the reported "first" packet was
       `pts=0.02x` instead of `-0.043`, off by the encoder delay.
       Fixed by removing `-read_intervals` from the first-packet probe
       entirely and letting ffprobe read from the stream start into a
       `head -n 1` pipe.
  The previously-shipped "skip validation if any span returned 0" fallback
  has been removed. A span of 0 now means the stream is genuinely empty or
  corrupt and the encode is rejected.
- **Duplicate `failed-files.tsv` entries on validation failure.**
  Every validation failure logged two lines: one with
  the specific reason (`validation_duration_mismatch` etc.) and one
  generic `failed` line. Root cause was a second writer in
  `transcodarr-entrypoint.sh:652` that wrote the generic line
  unconditionally whenever the worker exited non-zero, on top of the
  specific-reason line the worker had already written via
  `record_failed`. The API's `wc -l`-based `failed` counter was
  reporting 2× the real failure count. Fixed via a per-encode
  `TRANSCODARR_FAILURE_MARKER` handshake: `record_failed` touches the
  marker, and the entrypoint's fallback branch only writes a line if
  the marker is absent (crash / OOM / timeout SIGKILL case). Fallback
  reason changed from bare `failed` to `worker_exit_<rc>` to surface
  the exit code.
- **`.job` file cleanup gaps**. Two paths in
  the worker exited cleanly without removing the `.job` file: the
  `MODE=ok` branch (when full inspection found the file already met
  spec even though the ffprobe pool's quick classifier routed it as
  `cpu`/`gpu`) and the `quarantine()` function (when a corrupt file
  was moved to `/state/quarantine/`). The first path would re-ingest
  the same `.job` on every container restart, hogging priority slots
  forever. The second would leave orphan `.job` files pointing at
  paths that no longer existed. Both now call
  `cleanup_job_files_for_path` on their terminal exit.
- **Disk-slot DECR / EXIT trap race**. Worker consumer's
  explicit `DECR tc:disk:N:active` ran before `current_disk=""` was
  cleared at three release sites (exit-75 branch, dry-run path, success
  fall-through). A SIGKILL landing between those two statements would
  cause the EXIT trap to DECR a second time, driving the counter
  negative and bypassing the streams-per-disk cap on that disk.
  Extremely narrow window (microseconds between consecutive bash
  commands with no I/O) but real. Fixed by swapping the order — a leaked
  slot is strictly better than a negative counter that silently breaks
  the concurrency cap.
- **Worker log tag showed `Bulk` for priority imports** (cosmetic).
  `transcodarr-entrypoint.sh:596` hardcoded the literal
  string `"Bulk"` as the worker's third arg (the `$EVENT_TYPE` that
  feeds the `=== Worker start (X) ===` log line) instead of using
  `$item_route`. The parallel `TRANSCODARR_ITEM_ROUTE` env var was
  already correct; only the log label was wrong. Fixed by passing
  `"${item_route^}"` (title-cased) instead of the literal.
- **Restart button could return a 502 during the restart window.** The
  `/api/restart` handler sent the wrong signal, and clients (for example,
  when running behind a reverse proxy) could get a 502 while the container
  bounced. Fixed by using `SIGTERM` on PID 1 (which the shutdown trap
  handles cleanly) and making the restart page resilient to the 502 while
  the container comes back up.
- **Entrypoint hard-fails on startup when `/movies` or `/tv` aren't
  mounted.** The orphan-tmp-files sweep at
  `transcodarr-entrypoint.sh:71` was unconditionally listing those
  paths in `find`, which errored when they didn't exist — and
  `set -o pipefail` surfaced the error as a whole-script exit.
  Now guarded by per-path `-d` checks so deployments that mount only
  one media root start cleanly.

## [1.0.0] - 2025

Initial public release.
