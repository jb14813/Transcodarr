# Transcodarr — automated media transcoder
# Supports NVIDIA NVENC, Intel QSV, and CPU-only encoding
ARG GPU_TYPE=nvidia
FROM lscr.io/linuxserver/ffmpeg@sha256:8b7f2d546f28761e4eb5d6ed611f8cab3bd842667f7cea379c196040eed8f254

LABEL org.opencontainers.image.title="Transcodarr"
LABEL org.opencontainers.image.description="Automated media transcoder with GPU-accelerated encoding"
LABEL org.opencontainers.image.authors="jb14813"

# Install runtime dependencies.
# jq is used by probe_hdr_metadata() in transcodarr-lib.sh to extract
# SMPTE 2086 mastering-display + MaxCLL/MaxFALL side-data from ffprobe
# JSON so we can forward them to the HDR-preserve encoder path.
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends \
      curl perl valkey-server valkey-tools jq mkvtoolnix && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Rebuild libplacebo with explicit vkGetInstanceProcAddr binding.
# linuxserver/ffmpeg ships libplacebo built without -Dvk-proc-addr=enabled
# (verified 2026-05-15 via nm -D; preserved through the 2026-05-16 base
# bump). vf_libplacebo then fails on every HDR source with:
#   "No vkGetInstanceProcAddr function provided, and libplacebo built
#    without linking against this function!"
# Match the base image's libplacebo version (v7.360.1, SOVERSION 360) so
# ffmpeg's dynamic-link ABI stays intact.
#
# libdovi (Dolby Vision metadata library) is built from Rust source first —
# Ubuntu/Debian don't ship a libdovi-dev apt package, so we install cargo
# + cargo-c, compile libdovi via the dovi_tool repo's dolby_vision crate
# (matches the linuxserver/docker-ffmpeg base image's recipe), then build
# libplacebo with -Dlibdovi=enabled so vf_libplacebo can passthrough DV
# metadata. Without libdovi our libplacebo would still link/load, but
# Dolby Vision sources (transfer=dvhe) would lose metadata handling.
#
# libshaderc1 is pulled in as a dep of libshaderc-dev. Our libplacebo
# rebuild links against /usr/lib/x86_64-linux-gnu/libshaderc.so.1 (the
# Debian-packaged shaderc), NOT the base image's libshaderc_shared.so.1.
# Mark it as manually installed so the apt-get autoremove below doesn't
# drag it out — without this, ffmpeg fails with "libshaderc.so.1: cannot
# open shared object file". libvulkan1 isn't needed at runtime since
# libplacebo resolves libvulkan.so.1 from the base image's /usr/local/lib.
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends \
      git build-essential meson ninja-build pkg-config \
      libvulkan-dev libshaderc-dev libssl-dev ca-certificates && \
    apt-mark manual libshaderc1 && \
    # Install latest stable Rust via rustup. Ubuntu 24.04 ships rustc 1.75
    # but recent dolby_vision crates require >= 1.79. We use --profile
    # minimal to skip docs/source and keep the layer small (~250MB
    # toolchain), and cargo install cargo-c separately since it needs the
    # newer rustc too.
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path && \
    export PATH="/root/.cargo/bin:$PATH" && \
    cargo install cargo-c --locked && \
    # Build libdovi (Rust) first so libplacebo's meson can find it.
    git clone --depth 1 --branch 2.1.3 \
      https://github.com/quietvoid/dovi_tool.git /tmp/dovi_tool && \
    cd /tmp/dovi_tool/dolby_vision && \
    cargo cinstall --release --prefix=/usr/local --library-type=cdylib && \
    ldconfig && \
    git clone --depth 1 --branch v7.360.1 --recursive \
      https://code.videolan.org/videolan/libplacebo.git /tmp/libplacebo && \
    cd /tmp/libplacebo && \
    PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:/usr/local/lib/x86_64-linux-gnu/pkgconfig \
    meson setup build --buildtype=release \
      -Dvulkan=enabled -Dvk-proc-addr=enabled \
      -Dshaderc=enabled -Dlibdovi=enabled && \
    ninja -C build && \
    install -m644 build/src/libplacebo.so.360 \
      /usr/local/lib/x86_64-linux-gnu/libplacebo.so.360 && \
    ldconfig && \
    # Uninstall rustup toolchain + clean apt build deps + scratch dirs.
    /root/.cargo/bin/rustup self uninstall -y && \
    apt-get purge -y git build-essential meson ninja-build pkg-config \
      libvulkan-dev libshaderc-dev libssl-dev && \
    apt-get autoremove -y && \
    rm -rf /tmp/libplacebo /tmp/dovi_tool /root/.cargo /root/.rustup \
           /var/lib/apt/lists/*

# Intel QSV: install media driver (only for intel builds)
ARG GPU_TYPE
RUN if [ "$GPU_TYPE" = "intel" ]; then \
      apt-get update -qq && \
      apt-get install -y -qq --no-install-recommends \
        intel-media-va-driver-non-free libmfx1 libva-drm2 libva2 && \
      apt-get clean && \
      rm -rf /var/lib/apt/lists/*; \
    fi

# ── whisper.cpp (audio-language detection) ───────────────────────────────
# Build whisper.cpp from pinned source (tag v1.7.4) the same way we build
# libdovi/libplacebo above: git clone a fixed tag, compile, install the
# single binary we need (whisper-cli). Backend is selected by GPU_TYPE
# (re-declared below; nvidia→CUDA, intel→Vulkan, else CPU/OpenBLAS).
#
# RUNTIME-LINK SAFETY (differs from the libplacebo purge above): a CUDA
# build links whisper-cli against the CUDA *runtime* (libcudart.so), and a
# Vulkan build needs the Vulkan loader at runtime. Those runtime libs are
# NEEDED after build — unlike libplacebo's build deps. So we purge only
# BUILD-ONLY packages (compilers/cmake/dev headers) and apt-mark the
# runtime libraries manual so `autoremove` cannot drag them out. Purging
# the whole nvidia-cuda-toolkit would delete libcudart.so and silently
# degrade the nvidia binary to CPU. We do NOT do that.
#
# The default ggml model (base) is fetched from Hugging Face and verified
# against a pinned SHA-256 — a corrupted/truncated download must fail the
# build, never silently ship a broken model. It lives outside /models so
# the runtime user-mounted model folder can hide /models without losing
# the bundled seed model.
ARG GPU_TYPE
ARG WHISPER_CUDA_ARCHITECTURES="52;61;70;75;80;86;89"
RUN set -eux; \
    apt-get update -qq; \
    _whisper_build_deps="git build-essential cmake pkg-config ca-certificates"; \
    _whisper_backend_build_deps=""; \
    _whisper_runtime_keep=""; \
    _whisper_cmake_flags="-DWHISPER_BUILD_TESTS=OFF -DWHISPER_BUILD_EXAMPLES=ON"; \
    case "$GPU_TYPE" in \
      nvidia) \
        # nvidia-cuda-toolkit brings nvcc + headers (build-only) AND the
        # CUDA runtime libs (runtime-needed). We install it, then below
        # apt-mark the runtime libs manual so autoremove keeps them while
        # the toolkit meta/headers are purged.
        _whisper_backend_build_deps="nvidia-cuda-toolkit"; \
        _whisper_runtime_keep="libcudart12 libcublas12"; \
        _whisper_cmake_flags="$_whisper_cmake_flags -DGGML_CUDA=1 -DCMAKE_CUDA_ARCHITECTURES=${WHISPER_CUDA_ARCHITECTURES}"; \
        ;; \
      intel) \
        # Vulkan: glslc/glslang + dev headers are build-only; the loader
        # (libvulkan1) is runtime-needed.
        _whisper_backend_build_deps="libvulkan-dev glslc glslang-tools"; \
        _whisper_runtime_keep="libvulkan1"; \
        _whisper_cmake_flags="$_whisper_cmake_flags -DGGML_VULKAN=1"; \
        ;; \
      *) \
        # OpenBLAS: libopenblas0 (runtime) is needed; the -dev is build-only.
        _whisper_backend_build_deps="libopenblas-dev"; \
        _whisper_runtime_keep="libopenblas0"; \
        _whisper_cmake_flags="$_whisper_cmake_flags -DGGML_BLAS=1 -DGGML_BLAS_VENDOR=OpenBLAS"; \
        ;; \
    esac; \
    apt-get install -y -qq --no-install-recommends \
      $_whisper_build_deps $_whisper_backend_build_deps $_whisper_runtime_keep; \
    # Pin the runtime libs as manual BEFORE building/purging so autoremove
    # cannot remove libcudart/libvulkan/libopenblas the binary links to.
    if [ -n "$_whisper_runtime_keep" ]; then apt-mark manual $_whisper_runtime_keep; fi; \
    git clone --depth 1 --branch v1.7.4 \
      https://github.com/ggml-org/whisper.cpp.git /tmp/whisper.cpp; \
    cmake -S /tmp/whisper.cpp -B /tmp/whisper.cpp/build \
      -DCMAKE_BUILD_TYPE=Release $_whisper_cmake_flags; \
    cmake --build /tmp/whisper.cpp/build --config Release -j"$(nproc)" --target whisper-cli; \
    install -m755 /tmp/whisper.cpp/build/bin/whisper-cli /usr/local/bin/whisper-cli; \
    find -L /tmp/whisper.cpp/build -type f \
      \( -name 'libwhisper.so*' -o -name 'libggml*.so*' \) \
      -exec cp -a {} /usr/local/lib/ \;; \
    ldconfig; \
    mkdir -p /opt/transcodarr/models; \
    curl -fsSL -o /opt/transcodarr/models/ggml-base.bin \
      https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base.bin; \
    echo "60ed5bc3dd14eea856493d334349b405782ddcaf0028d4b5df4088345fba2efe  /opt/transcodarr/models/ggml-base.bin" \
      | sha256sum -c -; \
    # Purge BUILD-ONLY packages only. Runtime libs were apt-marked manual
    # above so autoremove leaves them. ldd at Step 5 verifies the link.
    apt-get purge -y $_whisper_build_deps $_whisper_backend_build_deps; \
    apt-get autoremove -y; \
    ldconfig; \
    rm -rf /tmp/whisper.cpp /var/lib/apt/lists/*

COPY scripts/ /scripts/
COPY tests/ /tests/
RUN chmod +x /scripts/*.sh /scripts/*.pl /tests/*.sh
