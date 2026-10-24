#!/bin/bash
set -euo pipefail
JOBS=${BUILD_JOBS:-2}
fetch() { /usr/bin/python3 /opt/xinghuo/build/fetch-source.py "$1" "/tmp/$1.tar.gz"; mkdir -p "/tmp/$1"; tar -xzf "/tmp/$1.tar.gz" --strip-components=1 -C "/tmp/$1"; }
case "$1" in
radare2)
  fetch radare2
  cd /tmp/radare2
  ./configure --prefix=/usr/local
  make -j"$JOBS"
  make install
  /usr/bin/python3 - <<'PY'
import hashlib, json, pathlib, subprocess
build = pathlib.Path('/opt/xinghuo/build')
sources = json.loads((build / 'sources.lock.json').read_text())
records = {}
for component in ('sdb', 'qjs', 'otezip', 'capstone-v5'):
    directory = pathlib.Path('/tmp/radare2/subprojects', component)
    pin = sources['r2-' + component]
    commit = subprocess.check_output(['git', '-C', str(directory), 'rev-parse', 'HEAD'], text=True).strip()
    if commit != pin['commit']:
        raise SystemExit('Radare2 subproject commit mismatch: ' + component + ' ' + commit)
    archive = subprocess.check_output(['git', '-C', str(directory), 'archive', 'HEAD'])
    changes = subprocess.check_output(['git', '-C', str(directory), 'diff', '--binary', 'HEAD'])
    records[component] = {'commit': commit, 'upstream_ref': pin['upstream_ref'],
        'git_archive_sha256': hashlib.sha256(archive).hexdigest(),
        'upstream_patches_sha256': hashlib.sha256(changes).hexdigest(),
        'independently_verified_codeload_sha256': pin['sha256']}
(build / 'radare-subprojects.lock.json').write_text(json.dumps(records, indent=2) + '\n')
PY
  ldconfig
  r2 -v
  ;;
flatter|stegseek)
  fetch "$1"
  cmake -S "/tmp/$1" -B "/tmp/$1/build" -DCMAKE_BUILD_TYPE=Release
  cmake --build "/tmp/$1/build" -j"$JOBS"
  cmake --install "/tmp/$1/build"
  ldconfig
  ;;
cado-nfs)
  fetch cado-nfs
  cmake -S /tmp/cado-nfs -B /tmp/cado-build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/cado-nfs -DBUILD_TESTING=OFF -DCMAKE_C_FLAGS='-O2' -DCMAKE_CXX_FLAGS='-O2'
  cmake --build /tmp/cado-build -j"$JOBS"
  cmake --install /tmp/cado-build
  # Use the installed layout; no runtime writes or hostname symlinks under /opt.
  printf '#!/bin/sh\nexec /opt/ctf/bin/python3 /opt/cado-nfs/bin/cado-nfs.py "$@"\n' > /usr/local/bin/cado-nfs
  chmod 755 /usr/local/bin/cado-nfs
  rm -rf /tmp/cado-build
  ;;
rsactftool)
  fetch rsactftool
  mv /tmp/rsactftool /opt/RsaCtfTool
  arch=$(dpkg --print-architecture)
  uv pip install --python /opt/ctf/bin/python3 --require-hashes \
    -r "/opt/xinghuo/build/requirements-rsa.$arch.lock"
  # The project archive itself is commit- and checksum-pinned. Its dependencies
  # have already been installed from the hash lock above.
  uv pip install --python /opt/ctf/bin/python3 --no-deps --no-build-isolation /opt/RsaCtfTool
  ;;
*) exit 2;;
esac
rm -rf "/tmp/$1" "/tmp/$1.tar.gz"
