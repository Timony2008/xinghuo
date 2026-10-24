"""Release publication guards, using fake build/check commands and no Docker."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class HostedBuildTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        source = Path(__file__).resolve().parents[1] / "scripts/build_hosted.sh"
        shutil.copyfile(source, scripts / "build_hosted.sh")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.release = self.root / "release"
        self.release.mkdir()
        self.log = self.root / "commands.jsonl"
        self._executable(self.bin / "docker", r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with pathlib.Path(os.environ['FAKE_DOCKER_LOG']).open('a') as log:
    log.write(json.dumps(args) + '\n')
mode = os.environ.get('FAKE_DOCKER_MODE', '')
if args[:2] == ['image', 'inspect']:
    if mode == 'missing':
        raise SystemExit(1)
    if args[-1] == '{{.Os}}/{{.Architecture}}':
        print('linux/arm64' if mode == 'arm64' else 'linux/amd64')
    else:
        print('sha256:' + ('b' if args[2].startswith('xinghuo-hosted') else 'a') * 64)
elif args[0] == 'run':
    print(json.dumps({'ok': True}))
elif args[0] == 'save':
    sys.stdout.buffer.write(b'synthetic archive content\n')
    if mode == 'save-fails':
        raise SystemExit(1)
elif args[:2] != ['buildx', 'build']:
    raise SystemExit('unexpected fake Docker arguments')
''')
        fake_venv = self.root / ".venv-veria/bin"
        fake_venv.mkdir(parents=True)
        self._executable(fake_venv / "python", r'''#!/usr/bin/env python3
import json, pathlib, sys
args = sys.argv[1:]
pathlib.Path(args[args.index('--output') + 1]).write_text(json.dumps({'status': 'passed'}))
''')

    @staticmethod
    def _executable(path, source):
        path.write_text(source)
        path.chmod(0o755)

    def _build(self, mode="", reuse="1"):
        # Keep only the ordinary executable search path, avoiding inherited
        # release overrides and any real model/platform credentials.
        env = {"PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
               "XINGHUO_RELEASE_DIR": str(self.release),
               "XINGHUO_REUSE_TOOLS_IMAGE": reuse,
               "FAKE_DOCKER_LOG": str(self.log), "FAKE_DOCKER_MODE": mode}
        return subprocess.run(["bash", str(self.root / "scripts/build_hosted.sh")],
                              env=env, capture_output=True, text=True, timeout=30)

    def test_reuse_validates_local_architecture_before_building(self):
        result = self._build(mode="arm64")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be linux/amd64", result.stderr)
        commands = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertFalse(any(row[:2] == ["buildx", "build"] for row in commands))
        self.assertFalse((self.release / "archive-summary.json").exists())

    def test_refuses_existing_release_without_touching_it(self):
        summary = self.release / "archive-summary.json"
        summary.write_text("old release")
        result = self._build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Refusing existing release artifact", result.stderr)
        self.assertEqual(summary.read_text(), "old release")
        self.assertFalse(self.log.exists())

    def test_failed_export_never_publishes_archive_or_summary(self):
        result = self._build(mode="save-fails")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.release / "xinghuo-hosted-linux-amd64.tar.gz").exists())
        self.assertFalse((self.release / "archive-summary.json").exists())
        self.assertFalse((self.release / ".build.lock").exists())
        self.assertEqual(len(list(self.release.glob(".build-*/source-check.json"))), 1)
        self.assertEqual(len(list(self.release.glob(".build-*/xinghuo-hosted-linux-amd64.tar.gz.tmp"))), 1)

    def test_success_publishes_matching_archive_identity_and_sha(self):
        result = self._build()
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads((self.release / "archive-summary.json").read_text())
        archive = self.release / summary["archive"]
        self.assertEqual(summary["sha256"], hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertEqual(summary["bytes"], archive.stat().st_size)
        self.assertEqual(summary["image_id"], "sha256:" + "b" * 64)
        self.assertEqual(summary["tools_image_id"], "sha256:" + "a" * 64)
        self.assertTrue(summary["tools_image_reused"])
        self.assertTrue(summary["uploadable"])
        self.assertFalse(summary["tools_removed_for_size"])
        self.assertEqual((self.release / (summary["archive"] + ".sha256")).read_text(),
                         summary["sha256"] + "  " + summary["archive"] + "\n")
        self.assertFalse(list(self.release.glob(".build*")))
        commands = [json.loads(line) for line in self.log.read_text().splitlines()]
        builds = [row for row in commands if row[:2] == ["buildx", "build"]]
        self.assertEqual(len(builds), 1)
        self.assertIn("Dockerfile.hosted", builds[0])


if __name__ == "__main__":
    unittest.main()
