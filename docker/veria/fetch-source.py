"""Fetch only locked public build inputs, validating bytes before extraction."""
import hashlib
import json
import pathlib
import subprocess
import sys

name, output = sys.argv[1:]
record = json.loads(pathlib.Path('/opt/xinghuo/build/sources.lock.json').read_text())[name]
subprocess.run(['curl', '--fail', '--location', '--retry', '5', '--output', output, record['url']], check=True)
hasher = hashlib.sha256()
with open(output, 'rb') as stream:
    for block in iter(lambda: stream.read(1024 * 1024), b''):
        hasher.update(block)
digest = hasher.hexdigest()
if digest != record['sha256']:
    pathlib.Path(output).unlink()
    raise SystemExit(f'Checksum mismatch: {name}')
