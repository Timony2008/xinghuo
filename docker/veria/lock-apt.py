#!/usr/bin/env python3
"""Capture installed Debian versions plus repository archive SHA-256s before cleanup.

Absent metadata is explicitly unverified. A missing repository hash alone does
not establish that a package was present in the pinned Ubuntu base image.
"""
import json
from pathlib import Path
import subprocess

installed = subprocess.check_output(
    ['dpkg-query', '-W', '-f=${binary:Package}\t${Version}\t${db:Status-Status}\n'], text=True)
packages = {parts[0]: parts[1] for line in installed.splitlines()
            if len(parts := line.split('\t')) == 3 and parts[2] == 'installed'}
result = subprocess.run(['apt-cache', 'show', *[f'{name}={version}' for name, version in packages.items()]],
                        capture_output=True, text=True)
archives = {}
for stanza in result.stdout.split('\n\n'):
    fields = dict(line.split(': ', 1) for line in stanza.splitlines() if ': ' in line and not line.startswith(' '))
    if fields.get('SHA256') and fields.get('Version'):
        archives[(fields['Package'], fields['Version'])] = {key: fields[key] for key in ('SHA256', 'Filename', 'Size') if key in fields}
rows = {}
for name, version in sorted(packages.items()):
    archive = archives.get((name.split(':')[0], version))
    rows[name] = {'version': version, 'archive': archive,
                  'provenance': 'repository_archive_sha256' if archive else 'archive_checksum_unavailable'}
Path('/opt/xinghuo/build/apt-artifacts.lock.json').write_text(json.dumps(rows, indent=2) + '\n')
