#!/usr/bin/env python3
"""Export complete synthetic prompt snapshots and narrow-patch ledgers offline."""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from xinghuo.veria.prompts import build_role_document, continuation_document
from xinghuo.veria.upstream_baseline import SOURCE_HASHES, FIRST_PROMPT, CONTINUE_PROMPT, BUMP_PREFIX, BUMP_SUFFIX
from xinghuo.model_connection import profile_contract


ROTATION_MARKER = "## Supervisor-owned challenge rotation"
FLASH_ROTATION_POLICY = {"mode": "no_flag_timeout", "timeout_seconds": 900}


def paragraph_coverage(document) -> list[dict]:
    """Account for every baseline paragraph, including chained replacements.

    Keep source intervals as replacements are applied, so a second reviewer
    patch to a prior multi-flag patch still points to the original paragraph.
    This does not infer semantic consistency; its purpose is complete, explicit
    source-to-runtime location accounting for human review.
    """
    original = document.baseline
    current = original
    origins = [(i, i + 1) for i in range(len(original))]
    changed = [set() for _ in original]
    for patch in document.patches:
        position = 0
        for _ in range(patch.occurrences):
            start = current.index(patch.original, position)
            end = start + len(patch.original)
            source_start = min(x[0] for x in origins[start:end])
            source_end = max(x[1] for x in origins[start:end])
            for rules in changed[source_start:source_end]:
                rules.add(patch.rule)
            current = current[:start] + patch.replacement + current[end:]
            origins[start:end] = [(source_start, source_end)] * len(patch.replacement)
            position = start + len(patch.replacement)
    if current != document.adapted:
        raise ValueError("Paragraph coverage failed to reproduce adapted baseline")
    result = []
    for paragraph in re.finditer(r"\S(?:.|\n)*?(?=\n\s*\n|\Z)", original):
        start, end = paragraph.span()
        rules = sorted(set().union(*changed[start:end]))
        output_start = next((i for i, interval in enumerate(origins) if interval[1] > start), len(current))
        result.append({"baseline_start_line": original.count("\n", 0, start) + 1,
                       "baseline_end_line": original.count("\n", 0, end) + 1,
                       "system_prompt_start_line": current.count("\n", 0, output_start) + 1,
                       "status": "explicit_conflict_patch" if rules else "verbatim",
                       "conflict_rules": rules, "baseline_text": paragraph.group()})
    return result


def export(output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    local = {"architecture": "aarch64", "execution_mode": "docker", "network_mode": "bridge",
             "web_tools": ["WebFetch", "WebSearch"], "input_files": ["binary", "image.png"]}
    hosted = {"architecture": "x86_64", "execution_mode": "hosted_process", "network_mode": "hosted_shared",
              "web_tools": [], "paths": {"inputs": "/work/attempt/inputs", "members": "/work/attempt/members", "shared": "/work/attempt/shared"},
              "input_files": ["binary", "image.png"]}
    base = {"challenge": {"name": "Offline synthetic prompt audit", "category": "pwn", "points": 100,
            "description": "Synthetic audit fixture; no real platform or model calls.", "tags": ["binary", "crypto"],
            "hints": [{"content": "Preserve visible hints."}], "connection_info": "nc localhost 31337"},
            "platform": {"completed": False, "flag_count": 2, "correct_flag_count": 0}}
    scenarios = [
        ("solver-local-tcp", "solver", "pro-1", base, local),
        ("solver-hosted-tcp", "solver", "pro-1", base, hosted),
        ("solver-local-http", "solver", "pro-1", {**base, "challenge": {**base["challenge"], "category": "web", "connection_info": "https://fixture.invalid/path?x=1&y=2"}}, local),
        ("solver-files", "solver", "pro-1", {**base, "challenge": {**base["challenge"], "connection_info": ""}}, local),
        ("solver-multiple-entrances", "solver", "pro-1", {**base, "challenge": {**base["challenge"], "connection_info": "Use both listed URLs", "urls": ["https://a.invalid", "https://b.invalid"]}}, hosted),
        ("reviewer-known-flags", "reviewer", "reviewer-audit", {**base, "agent_id": "reviewer", "review": {"review_id": "audit", "confirmed_submissions": [{"request_id": "synthetic-receipt", "status": "correct"}]}}, hosted),
        ("reviewer-no-flags", "reviewer", "reviewer-empty", {**base, "agent_id": "reviewer", "review": {"review_id": "empty", "confirmed_submissions": []}}, local),
        ("reviewer-known-flags-files", "reviewer", "reviewer-files", {**base, "challenge": {**base["challenge"], "connection_info": ""}, "review": {"confirmed_submissions": [{"request_id": "synthetic-files-receipt"}]}}, local),
        ("solver-image-only", "solver", "pro-1", {"challenge": {"name": "Image fixture", "category": "forensics", "hints": [{"content": "visible"}, {"id": "hidden"}]}}, {**local, "input_files": ["image.png"]}),
        ("solver-crypto", "solver", "pro-1", {**base, "challenge": {**base["challenge"], "category": "crypto", "connection_info": ""}}, local),
        ("solver-observed-tool-exclusion", "solver", "pro-1", base, {**local, "missing_packages": ["pyghidra", "angr"], "missing_commands": ["r2"]}),
        ("solver-pyghidra-runtime-restricted", "solver", "pro-1", base, {**local, "package_inventory": {"pyghidra": {"available": True}}, "tool_results": {"python:pyghidra": {"functional": {"status": "environment_restricted", "case": "ghidra"}}}}),
        ("solver-description-remote-materials", "solver", "pro-1", {"challenge": {"code": "platform-shaped-materials", "description": "![challenge image](https://materials.invalid/image.png?token=fixture) [archive](https://materials.invalid/binary.zip) [service](https://target.invalid/)"}, "addresses": ["https://target.invalid/"], "distfile_names": []}, hosted | {"input_files": []}),
        ("solver-metadata-remote-materials", "solver", "pro-1", {"challenge": {"category": "forensics", "attachments": [{"filename": "image", "url": "https://materials.invalid/download?id=7", "content_type": "image/png"}], "distfiles": [{"name": "remote.zip", "url": "https://materials.invalid/remote.zip"}]}}, local | {"input_files": []}),
        ("coordinator", "coordinator", "coordinator", base, hosted),
        ("reporter", "reporter", "reporter", base, hosted),
    ]
    mixed_context = {**base, "team_profile": "mixed4", "solver_ids": [f"solver-{n}" for n in range(1, 5)],
        "role_providers": {"solver-1": "deepseek", "solver-2": "deepseek", "solver-3": "glm", "solver-4": "glm",
                           "coordinator": "glm", "reviewer": "glm", "reporter": "deepseek"}}
    for role, member in (("solver", "solver-1"), ("solver", "solver-3"), ("coordinator", "coordinator"),
                         ("reviewer", "reviewer"), ("reporter", "reporter")):
        context = {**mixed_context, "agent_id": member, "provider": mixed_context["role_providers"][member]}
        if role == "reviewer":
            context["review"] = {"confirmed_submissions": [], "started_at": 100, "reproduction_deadline": 700,
                "coverage_deadline": 1780, "deadline": 1900, "phase": "coverage"}
        scenarios.append((f"mixed4-{role}-{member}", role, member, context, hosted))
    flash = profile_contract({"XINGHUO_TEAM_PROFILE": "flash4"})
    for member in (*flash["solver_ids"], "coordinator", "reporter", "reviewer"):
        role = "solver" if member in flash["solver_ids"] else member
        context = {**base, "team_profile": "flash4", "solver_ids": flash["solver_ids"],
            "agent_id": member, "provider": flash["providers"][member],
            "role_providers": flash["providers"], "rotation_policy": dict(FLASH_ROTATION_POLICY)}
        if role == "reviewer":
            context["review"] = {"review_id": "flash-rotation-audit", "confirmed_submissions": [],
                "started_at": 100, "reproduction_deadline": 700, "coverage_deadline": 1780,
                "deadline": 1900, "phase": "coverage"}
        scenarios.append((f"flash4-hard900-{member}", role, member, context, hosted))
    manifest = {"upstream_commit": "3366d569557c4fda3fd153040632de65e255396d", "source_hashes": SOURCE_HASHES, "scenarios": []}
    for name, role, member, context, environment in scenarios:
        document = build_role_document(role, member, context, environment)
        if context.get("rotation_policy") and ROTATION_MARKER not in document.text:
            raise ValueError(f"Configured rotation policy is absent from the live prompt: {name}")
        data = {"role": role, "member": member, **document.audit()}
        (output / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
        (output / f"{name}.md").write_text(document.text + "\n")
        delta = "".join(difflib.unified_diff(document.baseline.splitlines(True), document.adapted.splitlines(True),
                                             fromfile="unmodified-baseline", tofile="conflict-adapted-baseline"))
        (output / f"{name}.diff").write_text(delta)
        coverage = paragraph_coverage(document)
        (output / f"{name}.paragraphs.json").write_text(json.dumps(coverage, ensure_ascii=False, indent=2) + "\n")
        rows = ["| Baseline lines | Final SDK prompt line | Disposition | Conflict rules |", "|---|---|---|---|"]
        rows.extend(f"| {item['baseline_start_line']}-{item['baseline_end_line']} | {item['system_prompt_start_line']} | {item['status']} | {', '.join(item['conflict_rules'])} |" for item in coverage)
        (output / f"{name}.paragraphs.md").write_text("\n".join(rows) + "\n")
        manifest["scenarios"].append({"name": name, "role": role, "characters": len(document.text),
            "sha256": hashlib.sha256(document.text.encode()).hexdigest(), "patches": [p.rule for p in document.patches],
            "baseline_paragraphs": len(coverage), "verbatim_paragraphs": sum(item["status"] == "verbatim" for item in coverage),
            "rotation_policy": context.get("rotation_policy"),
            "rotation_policy_present": ROTATION_MARKER in document.text})
    guidance = [{"id": "audit-bump-1", "insights": "inspect new evidence"}, {"id": "audit-bump-2", "insights": "independently verify"}]
    continuation_scenarios = {
        "first": {"has_confirmed": False, "first": True},
        "continue": {"has_confirmed": False},
        "continue_known_flags": {"has_confirmed": True},
        "bump_no_flags": {"has_confirmed": False, "insights": guidance},
        "bump_known_flags": {"has_confirmed": True, "insights": guidance},
        "first_bump_no_flags": {"has_confirmed": False, "first": True, "insights": guidance},
    }
    continuation_scenarios.update({"flash4_hard900_" + name: {**options,
        "rotation_policy": dict(FLASH_ROTATION_POLICY)}
        for name, options in list(continuation_scenarios.items())})
    continuation_documents = {}
    for name, options in continuation_scenarios.items():
        document = continuation_document(**options)
        if options.get("rotation_policy") and ROTATION_MARKER not in document.text:
            raise ValueError(f"Configured rotation policy is absent from continuation: {name}")
        continuation_documents[name] = document.audit()
    continuations = {"baseline": {"first": FIRST_PROMPT, "continue": CONTINUE_PROMPT, "bump_prefix": BUMP_PREFIX, "bump_suffix": BUMP_SUFFIX},
        "documents": continuation_documents}
    manifest["continuations"] = [{"name": name, "rotation_policy": options.get("rotation_policy"),
        "rotation_policy_present": ROTATION_MARKER in continuation_documents[name]["system_prompt"]}
        for name, options in continuation_scenarios.items()]
    (output / "continuations.json").write_text(json.dumps(continuations, ensure_ascii=False, indent=2) + "\n")
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "dist/veria-prompt-audit-20261016")
    args = parser.parse_args()
    manifest = export(args.output)
    print(json.dumps({"output": str(args.output.resolve()), "scenarios": len(manifest["scenarios"])}, ensure_ascii=False))
