#!/usr/bin/env python3
"""Recheck pinned upstream originals and export full prompt/continuation ledgers.

Offline by default. --fetch-upstream independently compares the 37 vendored
files with GitHub's fixed-commit archive, without extracting or executing it.
Never contacts a model or competition platform.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import io
import json
from pathlib import Path
import tarfile
import urllib.request

from audit_veria_prompts import export

ROOT = Path(__file__).resolve().parents[1]
COMMIT = "3366d569557c4fda3fd153040632de65e255396d"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fetch-upstream", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    vendor = ROOT / "vendor/veria-ctf-agent"
    metadata = json.loads((vendor / "UPSTREAM.json").read_text())
    assert metadata["commit"] == COMMIT
    manifest = json.loads((vendor / "SOURCE_SHA256.json").read_text())
    assert len(manifest) == 37
    archive = None
    archive_hash = None
    if args.fetch_upstream:
        url = f"https://codeload.github.com/verialabs/ctf-agent/tar.gz/{COMMIT}"
        with urllib.request.urlopen(url, timeout=40) as response:
            raw = response.read()
        archive_hash = hashlib.sha256(raw).hexdigest()
        archive = tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz")
    records = []
    for name, expected in manifest.items():
        digest = hashlib.sha256((vendor / name).read_bytes()).hexdigest()
        upstream_digest = None
        if archive:
            with archive.extractfile(f"ctf-agent-{COMMIT}/{name}") as source:
                upstream_digest = hashlib.sha256(source.read()).hexdigest()
        records.append({"file": name, "manifest_sha256": expected,
                        "local_sha256": digest, "upstream_sha256": upstream_digest,
                        "passed": digest == expected and (upstream_digest is None or upstream_digest == expected)})
    if archive:
        archive.close()

    # Only the attribution docstring differs in the copied loop detector.
    original = ast.parse((vendor / "backend/loop_detect.py").read_text())
    actual = ast.parse((ROOT / "src/xinghuo/veria/loop_detect.py").read_text())
    loop_equal = (ast.dump(ast.Module(body=original.body[1:], type_ignores=[])) ==
                  ast.dump(ast.Module(body=actual.body[1:], type_ignores=[])))
    result = {"commit": COMMIT, "source_archive": metadata["source_archive"],
              "fetched_upstream": args.fetch_upstream, "source_archive_sha256": archive_hash,
              "files": records, "loop_detector_ast_identical_except_docstring": loop_equal,
              "passed": all(item["passed"] for item in records) and loop_equal,
              "model_calls": 0, "platform_calls": 0}
    (args.output / "upstream-source-check.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    if not result["passed"]:
        raise SystemExit("Upstream integrity mismatch; see upstream-source-check.json")
    prompts = export(args.output / "upstream-prompts")
    print(json.dumps({"passed": True, "source_files": len(records),
                      "remote_source_checked": args.fetch_upstream,
                      "prompt_scenarios": len(prompts["scenarios"]), "output": str(args.output.resolve())}))


if __name__ == "__main__":
    main()
