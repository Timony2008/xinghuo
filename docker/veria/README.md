# Complete Veria sandbox tool contract

Baseline: upstream `3366d569557c4fda3fd153040632de65e255396d`, union of
`sandbox/Dockerfile.sandbox`, `sandbox/sandbox-tools.txt`, and Claude Agent SDK
prompt capabilities. Both local ARM64 and hosted AMD64 consume the same package
list and source pins. Only a demonstrated architecture incompatibility may become
an explicit exclusion; upload-size pressure is not a reason to remove tools.

The SDK lives in `/opt/sdk` (Python 3.14), CTF libraries in `/opt/ctf` (Python
3.11), and SageMath uses its Ubuntu system environment. Member venvs inherit the
CTF libraries. This separation avoids replacing Sage system packages with pip
packages. `/opt/java` and `/opt/ghidra` are read-only installations; Ghidra projects,
member caches, Ruby-added gems and writable state belong in each member directory.

## Pinned Python artifacts

`requirements-ctf.in` and `requirements-torch.in` are the human-readable direct
requirements. `requirements-{ctf,torch,rsa}.{arm64,amd64}.lock` are committed resolved
version/hash locks, generated without installing anything on macOS:

```sh
uv pip compile --python-version 3.11 --python-platform aarch64-manylinux_2_35 \
  --generate-hashes docker/veria/requirements-ctf.in \
  -o docker/veria/requirements-ctf.arm64.lock
uv pip compile --python-version 3.11 --python-platform aarch64-manylinux_2_35 \
  --torch-backend cpu --generate-hashes --emit-index-url \
  -c docker/veria/requirements-ctf.arm64.lock docker/veria/requirements-torch.in \
  -o docker/veria/requirements-torch.arm64.lock
# RsaCtfTool pyproject.toml is extracted from the checksum-verified source archive.
uv pip compile --python-version 3.11 --python-platform aarch64-manylinux_2_35 \
  --generate-hashes -c docker/veria/requirements-ctf.arm64.lock \
  /opt/RsaCtfTool/pyproject.toml -o docker/veria/requirements-rsa.arm64.lock
```

AMD64 repeats these commands with `x86_64-manylinux_2_35` and `.amd64.lock`.
Torch installation must also use `--torch-backend cpu`; CPU wheel hashes are
included. The source project is installed with `--no-deps --no-build-isolation`
after its hash-locked dependency set is present.

Compatibility corrections confirmed by dependency resolution:

- pwntools 4.15.0 excludes Unicorn 2.1.3 and 2.1.4; use Unicorn 2.1.2.
- angr/claripy 9.2.177 require z3-solver 4.13.0.0.

Both target architectures successfully resolved the complete CTF, CPU Torch, and
RsaCtfTool dependency sets. This is dependency resolution evidence, not installation
or functional acceptance evidence.

## Native, Java, Debian and Ruby provenance

`sources.lock.json` pins archive URLs, commits/versions, and SHA-256 for native
sources, Ghidra, JDK 21 and the ARM64 Gradle build. `fetch-source.py` validates
bytes before extraction. ARM64 builds Ghidra's supplied native sources because
the official release ships Linux x86-64 binaries only. The installer fails on any
build or checksum error; it never declares a missing tool available.

`lock-apt.py` records exact installed Debian versions and available repository
archive SHA-256 values. A missing exact-version archive is explicitly
`archive_checksum_unavailable`, not silently claimed locked. `make-manifest.py`
records command paths, executable hashes and version/source provenance, Python
versions, hash-lock file digests, and installed Ruby gem archive hashes.

## Acceptance

`python scripts/verify_full_veria_image.py --image IMAGE --mode tools \
--output REPORT.json` runs the Linux acceptance script through the real member
execution helper and private venv, with `--network none`. For hosted images use
`--mode hosted`, which verifies UID 10001. Every category runs in an independent
temporary directory/process with a bounded timeout. No model, platform API, or
benchmark service is called.

Installation inventory, synthetic offline functional cases, and platform-dependent
nested-container permission are separate. CADO performs complete small factoring;
Ghidra performs native decompilation; Volatility scans a synthetic memory image;
Sleuth Kit/TestDisk parse a generated partitioned filesystem; OCR, steganography,
media conversion, symbolic execution, RSA/lattice arithmetic and CPU Torch/Keras
run actual operations. An executable path or successful import alone is not a
functional pass. Real platform networking and TSecBench submission remain for the
user's subsequent upload attempt.

## CADO CPU portability evidence

At the pinned CADO source, `config/check_cpu_feature.cmake` can append detected
CPU-specific flags globally. Its explicit `CMAKE_C_FLAGS MATCHES "-march"` branch
suppresses that probing. The bundled `gf2x/configure.ac` likewise skips adding
`-march=native`/`-mtune=native` when an explicit `-march=` is already present.
Therefore a portable build may use `-O2 -march=x86-64 -mtune=generic` on AMD64 and
`-O2 -march=armv8-a -mtune=generic` on ARM64 for both C and C++. This retains the
software/algorithms while selecting the baseline instruction set. An unmodified
native-tuned build must not be described as universally CPU-portable or as
proven to use runtime CPU dispatch.

`BUILD_TESTING=OFF` is not a supported option in this pinned CADO tree. Tests are
already `EXCLUDE_FROM_ALL`; `las_tracek`, `siqs_tracek` and their debug objects are
unconditional build targets, with no official disable-debug option. Full default
builds are retained unless a separate, evidenced source patch is explicitly made.

The shared Dockerfile explicitly selects baseline CPU instructions for CADO and
gf2x (`x86-64` / `armv8-a`, `-mtune=generic`). Its build step patches the local
installer's CMake flags before invoking CADO. This prevents build-host CPU probing
from silently imposing a newer instruction set on the hosted runtime. No CADO
algorithm or installed target is removed.

When a nested gf2x Make build cannot use the inherited GNU Make jobserver (observed
as `write jobserver: Bad file descriptor` during QEMU AMD64 builds), the upstream
`GMAKE_EXECUTABLE` cache variable can point to a dedicated wrapper which unsets
`MAKEFLAGS`, `MFLAGS` and `GNUMAKEFLAGS`, then executes `/usr/bin/gmake -j1 "$@"`.
The pinned CMake tree uses that variable only for gf2x build/install and its
non-default tests; the outer CADO build may remain parallel. This changes job
scheduling only and does not remove algorithms, libraries or tools. The actual
rebuilt image must still pass CADO factoring acceptance.

Ghidra's native Gradle task places executables in each module's
`build/os/<platform>/` directory, rather than its prebuilt `os/<platform>/`
directory. The official launcher prefers the former automatically. ARM64's
required native decompiler is therefore
`/opt/ghidra/Ghidra/Features/Decompiler/build/os/linux_arm_64/decompile`.
That directory contains runtime assets and must survive cache/build cleanup.

Real ARM64 acceptance found that bitstring 5.0 removed `ConstBitStream`, which
pyvex 9.2.177 imports. The CTF input and all canonical architecture locks now pin
bitstring 4.3.1. `requirements-compat.{arm64,amd64}.lock` repairs only that API
compatibility issue (and installs its bitarray dependency) on already-built
images, preserving native build outputs. Historical
`requirements-ctf.pre-compat.{arm64,amd64}.lock` files retain the original artifact
hashes, including the now-unneeded tibs dependency still present in the original
image layer. No tool has been removed; final pip freeze reports actual installed
versions and the manifest hashes both historical and final locks.

The same acceptance then exposed pycparser 3.11's read-only `CLexer.filename`,
which angr 9.2.177's C parser overrides. The canonical and compatibility locks also
pin pycparser 2.22. Both fixes preserve angr itself; symbolically solving a compiled
native function remains the acceptance criterion. Initial and second-round failed
reports are retained alongside the final report rather than overwritten as passes.
