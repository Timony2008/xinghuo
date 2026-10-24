#!/usr/bin/env python3
"""Compare installed release code/resources with the workspace, without a query."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


INSPECT = r'''
import hashlib, json
from pathlib import Path
import xinghuo
root = Path(xinghuo.__file__).parent
files = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
         for p in root.rglob('*') if p.is_file() and p.suffix in {'.py', '.txt', '.md'}}
extra = {name: hashlib.sha256(Path(path).read_bytes()).hexdigest() for name, path in {
    'tool_helper': '/usr/local/bin/xinghuo_exec.py',
    'tool_acceptance': '/opt/xinghuo/veria_tool_acceptance.py'}.items()}
print(json.dumps({'package': str(root), 'files': files, 'extra': extra}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    package = root / 'src/xinghuo'
    expected = {str(p.relative_to(package)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in package.rglob('*') if p.is_file() and p.suffix in {'.py', '.txt', '.md'}}
    result = subprocess.run(['docker', 'run', '--rm', '--network', 'none', '--entrypoint',
        '/opt/sdk/bin/python', args.image, '-c', INSPECT], check=True, capture_output=True, text=True)
    observed = json.loads(result.stdout)
    mismatch = sorted(name for name in expected.keys() | observed['files'].keys()
                      if expected.get(name) != observed['files'].get(name))
    for name, path in {'tool_helper': 'scripts/veria_container_exec.py',
                       'tool_acceptance': 'scripts/veria_tool_acceptance.py'}.items():
        if observed['extra'].get(name) != hashlib.sha256((root/path).read_bytes()).hexdigest():
            mismatch.append(name)
    identity = subprocess.check_output(['docker', 'image', 'inspect', '--format', '{{.Id}}', args.image], text=True).strip()
    report = {'status': 'failed' if mismatch else 'passed', 'image': args.image, 'image_id': identity,
              'installed_package': observed['package'], 'source_files': len(expected),
              'mismatches': mismatch, 'sha256': observed, 'model_calls': 0, 'platform_calls': 0}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({key: report[key] for key in ('status', 'image', 'source_files', 'mismatches')}))
    return bool(mismatch)


if __name__ == '__main__':
    raise SystemExit(main())
