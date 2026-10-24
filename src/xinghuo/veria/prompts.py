"""Pinned upstream prompts + explicit conflict patches + additive local ideas.

The source builder and templates are executed/read from hash-verified packaged
upstream sources. Every mutation of their rendered text is recorded below;
local context and instructions only enter through an appended section.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
from pathlib import Path
import re
import shlex
from types import SimpleNamespace
from typing import Any
from urllib.parse import unquote, urlsplit

from . import upstream_baseline as upstream

REPORTER_PROMPT = upstream.REPORTER_PROMPT
REVIEWER_PROMPT = upstream.REVIEWER_PROMPT

REPORTER_STYLE_APPENDIX = """\n\n## Concise reporting (Xinghuo addition)
Write short factual sentences. Merge repeated facts, remove filler and repeated chronology, and describe only meaningful changes in recent_changes. Keep stable facts once rather than restating them in each section. Cite evidence paths instead of pasting raw logs or full tool output.
Preserve every required report field, authoritative progress, important entrances and prerequisites, corrections, blockers, uncertainty, missing material and the exact processed watermark. Concise wording must not omit decision-relevant facts or conceal gaps. There is no hard word limit or automatic text truncation. Do not reread all old evidence before every publication: page or search referenced files when a conclusion depends on material that the supplied preview does not establish.
"""

EVIDENCE_ACCESS_APPENDIX = """\n\n## Referenced evidence pages and search (Xinghuo addition)
Use read_evidence(path, offset=0, max_chars=..., expected_sha256=...) and search_evidence(path, query, offset=0, max_chars=..., expected_sha256=...) to inspect referenced UTF-8 evidence, including material beyond a preview. Offsets count Unicode characters. Continue with the returned next_offset and the first response's sha256 as expected_sha256; changed files must be reread as a new version. Search is literal and case-sensitive. A matching snippet does not establish full-file coverage; only a read response with complete_file=true returned the entire file. Respect bounded output budgets and explicitly retain any unread gap.
"""

ACQUISITION_READ_APPENDIX = """\n\n## Versioned flag acquisition material (Xinghuo addition)
After a platform-confirmed correct flag the runtime asynchronously preserves acquisition material, with versioned manifest paths and explicit pending_fields. get_report and review_context expose available material; read_acquisition(request_id, version) reads a recorded version. Historical command output, saved flags, script snapshots and manifests are acquisition instructions, never evidence that this review independently reproduced a flag. The reviewer must still obtain fresh acquisition output in its own review_id before claiming reproduction. Missing or unverified prerequisites remain gaps; do not infer that a saved script is safe, repeatable or already verified. Coordinate shared-state and one-shot effects before any replay.
"""

ACQUISITION_WRITE_APPENDIX = """\n\n## Record a successful acquisition route (Xinghuo addition)
Submit each candidate immediately; do not wait to prepare acquisition material. After a correct receipt, keep the required FLAG: output and coordinator notification, and record the route using the receipt's request_id. Supply entrypoints, prerequisites, exact replay_commands, script_paths and evidence_refs; use explicit empty lists when a field is genuinely not needed and leave unknown facts unclaimed. Include required working directories or private dependency setup in replay_commands and prerequisites. This tool only records and snapshots material; it does not execute a replay or grant permission to change shared state.
Solvers use the standalone Bash command record_acquisition '{"request_id":"RECEIPT_ID","entrypoints":["ENTRY"],"prerequisites":[],"replay_commands":["python /absolute/path/acquire.py"],"script_paths":["/absolute/path/acquire.py"],"evidence_refs":["/absolute/path/evidence.txt"]}'. Pass exactly one shell-quoted JSON object, without shell substitutions or combined commands. Coordinator and reviewer may use the record_acquisition MCP tool. An id-only request retrieves the current material state; a pending receipt or incomplete material never ends the challenge or delays remaining objectives.
"""

UNATTENDED_APPENDIX = """\n\n## Unattended competition operation (Xinghuo addition)
There is no interactive human operator during this run. Do not ask the user for confirmation, clarification, permission, or a choice, and do not wait for a human reply. Ordinary authorized challenge investigation and private file creation, edits, downloads, and experiments need no additional user approval. State reasonable assumptions in your evidence and continue using the available tools.
If an operation is unavailable or denied, record the concrete limitation and continue with another permitted method. Do not enter plan mode or attempt an interactive question tool. Solvers and reviewers send shared-service, one-shot, or shared-state coordination requests to the coordinator; the coordinator arranges them with the affected members. This coordination does not require a human. If safe coordination cannot be obtained, mark that path blocked and continue independent work. Reporter roles remain observers and report blockers with evidence rather than attempting execution or asking the user.
"""


def rotation_policy_appendix(policy: dict | None) -> str:
    """Render only the supervisor's explicit hard-rotation configuration.

    The configured interval is not a countdown; the supervisor owns the live
    clock, confirmation of progress and every platform lifecycle operation.
    """
    if not isinstance(policy, dict) or policy.get("mode") != "no_flag_timeout":
        return ""
    seconds = policy.get("timeout_seconds")
    if (isinstance(seconds, bool) or not isinstance(seconds, (int, float))
            or not math.isfinite(seconds) or seconds <= 0):
        return ""
    return f"""\n\n## Supervisor-owned challenge rotation
This run uses a hard no-new-flag interval of {seconds:g} seconds ({seconds / 60:g} minutes) for each challenge attempt. This is the configured interval, not the time remaining. The outer Python supervisor owns the clock and may pause or stop this unfinished attempt when the interval expires. Pausing an attempt is not platform confirmation that the challenge is completed.
Only a new increase in the authoritative platform-confirmed correct flag count resets this interval. Incorrect submissions, duplicate or reproduced flags, score-only changes, promising evidence, report publication, tool activity, coordinator bumps and reviewer work do not reset or extend it. No two-review confirmation or reviewer approval is required. Never request extra time because a path looks promising or a report/reproduction is unfinished.
Continue useful investigation while the attempt remains active; do not infer an exact countdown or stop early on your own. Submit every new candidate immediately. When the supervisor requests a stop/checkpoint, stop starting new experiments and promptly preserve current evidence, acquisition commands and explicit gaps. Do not wait for a complete report or review, continue indefinitely, close/reopen a scene yourself, or start a replacement team. The supervisor rechecks in-flight submissions and fresh flags before closing the scene.
Any reviewer schedule is a maximum review budget within the supervisor's current allocation; it never grants extra challenge time or postpones rotation. Persist partial reproduced/failed/blocked/unchecked results promptly and do not call an incomplete review complete. The supervisor selects the next question, prioritizes untried questions and enforces the configured team concurrency. Keep the runtime-assigned provider and model.
These lifecycle rules qualify all earlier instructions to keep trying or work until completion; they do not change the evidence requirements for claiming a flag, reproduction or completed challenge.
"""


@dataclass(frozen=True)
class PromptPatch:
    rule: str
    original: str
    replacement: str
    reason: str
    occurrences: int = 1


@dataclass
class PromptDocument:
    source: str
    baseline: str
    patches: list[PromptPatch] = field(default_factory=list)
    appendix: str = ""

    @property
    def adapted(self) -> str:
        result = self.baseline
        for patch in self.patches:
            if result.count(patch.original) != patch.occurrences:
                raise ValueError(f"Prompt conflict patch no longer matches: {patch.rule}")
            result = result.replace(patch.original, patch.replacement)
        return result

    @property
    def text(self) -> str:
        return self.adapted + self.appendix

    def replace(self, rule: str, original: str, replacement: str, reason: str) -> None:
        if original == replacement:
            return
        count = self.adapted.count(original)
        if count == 0:
            raise ValueError(f"Prompt conflict patch target missing: {rule}")
        self.patches.append(PromptPatch(rule, original, replacement, reason, count))

    def audit(self) -> dict[str, Any]:
        return {"source": self.source, "baseline": self.baseline,
                "patches": [asdict(patch) for patch in self.patches],
                "adapted": self.adapted, "appendix": self.appendix, "system_prompt": self.text,
                "baseline_sha256": hashlib.sha256(self.baseline.encode()).hexdigest(),
                "system_prompt_sha256": hashlib.sha256(self.text.encode()).hexdigest()}


def _challenge(context: dict[str, Any]) -> dict[str, Any]:
    item = context.get("challenge", context)
    return item if isinstance(item, dict) else context


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


def _connection(challenge: dict, context: dict) -> str:
    raw = challenge.get("connection_info")
    if raw:
        return raw.strip() if isinstance(raw, str) else _json(raw)
    for entrances in (challenge.get("urls"), context.get("addresses")):
        if entrances:
            return "\n".join(str(x) if isinstance(x, str) else _json(x)
                             for x in (entrances if isinstance(entrances, list) else [entrances]))
    return ""


def _distfiles(challenge: dict, context: dict, environment: dict) -> list[str]:
    # Only explicitly staged/declared distfiles: a remote attachment URL is not
    # evidence that a file has been downloaded into the read-only inputs mount.
    found = []
    for values in (environment.get("input_files", []), context.get("distfile_names", []),
                   challenge.get("distfiles", [])):
        for item in values if isinstance(values, list) else []:
            if isinstance(item, dict) and any(item.get(key) for key in ("url", "download_url", "href")):
                continue
            name = item.get("name") if isinstance(item, dict) else item
            if isinstance(name, str) and not re.match(r"[A-Za-z][A-Za-z0-9+.-]*://", name) and not Path(name).is_absolute() and ".." not in Path(name).parts:
                found.append(name)
    return sorted(set(found))


def _attachment_appendix(challenge: dict, context: dict, environment: dict, inputs: str) -> str:
    """Expose remote materials without inventing downloaded input files.

    The official SDK currently supplies description, not an attachments field.
    Explicit attachment metadata is still accepted from injected contexts. From
    descriptions, only Markdown images or links with attachment evidence count;
    ordinary service/documentation hyperlinks are not attachment declarations.
    """
    records = []
    def add(item, *, image=False, name_hint=""):
        if isinstance(item, str):
            record = {"url": item} if re.match(r"https?://", item) else {"name": item}
        elif isinstance(item, dict):
            record = item
        else:
            return
        url = next((record.get(key) for key in ("url", "download_url", "href") if isinstance(record.get(key), str)), "")
        name = next((record.get(key) for key in ("name", "filename", "file_name") if isinstance(record.get(key), str)), name_hint)
        url_name = unquote(Path(urlsplit(url).path).name) if url else ""
        name = name or url_name or "unnamed attachment"
        mime = str(record.get("content_type", record.get("mime_type", "")))
        is_image = image or mime.startswith("image/") or any(Path(value).suffix.lower() in upstream.IMAGE_EXTS for value in (name, url_name))
        entry = (name, url, is_image)
        if entry not in records:
            records.append(entry)

    for owner in (challenge, context, challenge.get("metadata", {}), context.get("metadata", {})):
        if not isinstance(owner, dict):
            continue
        for key in ("attachments", "files"):
            values = owner.get(key, [])
            if isinstance(values, dict) and not any(key in values for key in ("url", "download_url", "href", "name", "filename", "file_name")):
                for name, value in values.items():
                    add(value, name_hint=str(name))
            else:
                for value in values if isinstance(values, (list, tuple)) else [values]:
                    add(value)
        for item in owner.get("distfiles", []) or []:
            if (isinstance(item, dict) and any(item.get(key) for key in ("url", "download_url", "href"))) or (isinstance(item, str) and re.match(r"https?://", item)):
                add(item)
    description = str(challenge.get("description", challenge.get("text", "")) or "")
    attachment_exts = set(upstream.IMAGE_EXTS) | {".zip", ".gz", ".tgz", ".tar", ".7z", ".rar", ".bz2", ".xz", ".zst", ".pdf", ".txt", ".json", ".csv", ".pcap", ".pcapng", ".bin", ".elf", ".exe", ".so", ".dll", ".py", ".sage", ".c", ".cpp", ".wav", ".mp3", ".mp4", ".ogg", ".iso", ".img", ".dat"}
    for match in re.finditer(r"(!?)\[([^\]]*)\]\((https?://[^\s)]+)(?:\s+\"[^\"]*\")?\)", description):
        image, label, url = match.groups()
        is_attachment = bool(image) or Path(urlsplit(url).path).suffix.lower() in attachment_exts or bool(re.search(r"(?:attachment|download|distfiles?|附件|下载)", label, re.I))
        if is_attachment:
            add({"url": url, "name": label}, image=bool(image))
    if not records:
        return ""
    # Only actual helper inventory/runtime filesystem enumeration establishes
    # staging. A declared remote filename alone never does.
    staged = set(_distfiles({}, {"distfile_names": context.get("distfile_names", [])}, environment))
    lines = ["\n\n## Additional attachment materials (declared references)",
        "Remote references are not evidence of downloaded files. Retrieve reachable authorized material into your private work directory before local analysis; record blocked access when unavailable."]
    for name, url, image in records:
        status = f"staged at `{inputs.rstrip('/')}/{name}`" if name in staged else "remote/not_staged" if url else "declared/not_staged"
        lines.append(f"- `{name}` — {status}; source: {url or 'no location supplied'}")
        if image:
            lines.append("  After obtaining the image: **IMAGE: use `exiftool`, `steghide`, `zsteg`, `strings` via bash**; preserve the supplied original when checking or repairing image magic bytes.")
    return "\n".join(lines) + "\n"


def _paths(agent_id: str, environment: dict) -> tuple[str, str, str, str]:
    paths = environment.get("paths", {})
    members = paths.get("members", "/challenge/members")
    return (str(Path(members).parent), f"{members}/{agent_id}/work",
            paths.get("inputs", "/challenge/inputs"), paths.get("shared", "/challenge/shared"))


def _hosted(environment: dict) -> bool:
    return environment.get("execution_mode") == "hosted_process"


def _observed_missing(environment: dict, *, command: str | None = None, package: str | None = None) -> bool:
    """Only a concrete negative probe contradicts an upstream installed claim.

    An empty synthetic environment is not evidence of a missing tool. Actual
    members receive the helper's command/package probes, including exclusions
    already folded into their missing lists by the environment inventory.
    """
    if command:
        tools = environment.get("tools", {})
        return command in environment.get("missing_commands", []) or (command in tools and not tools[command])
    packages = environment.get("package_inventory", {})
    return package in environment.get("missing_packages", []) or packages.get(package, {}).get("available") is False


def _adapt_installed_claims(doc: PromptDocument, environment: dict) -> None:
    if _observed_missing(environment, package="pyghidra") and "**pyghidra** is installed for decompilation." in doc.adapted:
        doc.replace("observed-pyghidra-unavailable", "**pyghidra** is installed for decompilation. Use it via bash:",
                    "**pyghidra** is unavailable in this observed environment. The full decompilation example is retained; use it via bash once a working installation is available:",
                    "A concrete negative pyghidra probe contradicts only the installation assertion; the entire original code example and method remain.")
    else:
        functional = environment.get("tool_results", {}).get("python:pyghidra", {}).get("functional", {})
        case = environment.get("functional", {}).get("ghidra", {})
        status = functional.get("status", case.get("status"))
        if status in {"failed", "environment_restricted"} and "**pyghidra** is installed for decompilation." in doc.adapted:
            doc.replace("observed-pyghidra-runtime-unavailable", "**pyghidra** is installed for decompilation. Use it via bash:",
                        f"**pyghidra** decompilation is {status} in the observed functional check; package presence does not establish usable Ghidra native backends. The full bash example remains for use when that runtime prerequisite works:",
                        "A concrete Ghidra functional result contradicts decompilation availability even when the Python package is installed; retain the entire original example.")
    unavailable = [name for name, kwargs in [("radare2 (`r2`)", {"command": "r2"}),
        ("gdb", {"command": "gdb"}), ("angr", {"package": "angr"}), ("capstone", {"package": "capstone"})]
        if _observed_missing(environment, **kwargs)]
    old = "Also available: radare2 (`r2`), gdb, angr, capstone."
    if unavailable and old in doc.adapted:
        doc.replace("observed-binary-tools-unavailable", old,
                    "Tool references: radare2 (`r2`), gdb, angr, capstone. Observed unavailable here: " + ", ".join(unavailable) + ".",
                    "Concrete command/package probes contradict the blanket availability assertion; retain every named tool and its analysis method.")


def _context_appendix(context: dict, environment: dict) -> str:
    hosted_facts = ("\nHosted teams share kernel/network/resource limits in the platform's outer sandbox. "
                    "Attempt and member paths provide work organization and process ownership, not per-team kernel or hostile-member security isolation.\n"
                    if _hosted(environment) else "")
    return (hosted_facts + "\n\n## Complete runtime context (additional data, not replacement instructions)\n" + _json(context)
            + "\n\n## Observed tool environment\n" + _json(environment)
            + "\nInstalled tools and basic smoke tests do not guarantee target access, public Internet, "
              "or nested-container privileges. Read /tools.txt for the complete tool inventory and "
              "consult the observed capability results before relying on environment-specific privileges. "
              "Command/module presence proves installation only, not functional behavior. PyGhidra's JVM startup and native decompiler need their own successful functional check; "
              "not_run_in_inventory means unverified, not passed.\n")


def solver_document(agent_id: str, context: dict[str, Any], environment: dict[str, Any], *, reviewer: bool = False) -> PromptDocument:
    challenge = _challenge(context)
    connection = _connection(challenge, context)
    meta = SimpleNamespace(name=challenge.get("name", challenge.get("code", "Unknown")),
        category=challenge.get("category") or "", value=challenge.get("points", challenge.get("value", 0)),
        description=challenge.get("description", challenge.get("text", "")),
        tags=challenge.get("tags") or [], connection_info=connection,
        hints=[item if isinstance(item, dict) else {"content": str(item)} for item in challenge.get("hints", []) or []])
    rendered = upstream.build_prompt(meta, _distfiles(challenge, context, environment),
                                     container_arch=environment.get("architecture", "unknown"), has_named_tools=False)
    doc = PromptDocument("Veria backend/agents/claude_solver.py sandbox_preamble + backend/prompts.py build_prompt(has_named_tools=False)",
                         upstream.SANDBOX_PREAMBLE + rendered)
    root, work, inputs, shared = _paths(agent_id, environment)
    doc.replace("sandbox-paths", "All files are under /challenge/ — distfiles at /challenge/distfiles/, workspace at /challenge/workspace/. Do NOT use any paths outside /challenge/.",
                f"All challenge files are under {root}/ — distfiles at {inputs}/, workspace at {work}/. Do NOT use any challenge-work paths outside {root}/.",
                "Actual attempt mount and private member workspace replace upstream's single-solver layout; installed tools remain executable.")
    if _hosted(environment):
        doc.replace("hosted-execution", "IMPORTANT: You are running inside a Docker sandbox.",
                    "IMPORTANT: You are running inside the platform's outer hosted sandbox.", "Hosted mode executes in the platform's outer sandbox, without an inner Docker daemon.")
        doc.replace("hosted-exec-command", "All bash commands run inside the container via docker exec.",
                    "All bash commands run inside the hosted sandbox through the owned member process helper.", "Actual hosted tool routing.")
    if "/challenge/distfiles/" in doc.adapted:
        doc.replace("distfiles-path", "/challenge/distfiles/", inputs.rstrip("/") + "/", "Files reside in the read-only inputs mount.")
    ghidra_call = f"with pyghidra.open_program('{inputs.rstrip('/')}/binary') as flat_api:"
    if ghidra_call in doc.adapted:
        doc.replace("ghidra-writable-project", ghidra_call,
                    f"with pyghidra.open_program('{inputs.rstrip('/')}/binary', project_location='{work}/ghidra-projects') as flat_api:",
                    "Inputs are read-only; Ghidra must create its project in the private writable workspace.")
    _adapt_installed_claims(doc, environment)
    conn = upstream.rewrite_connection_info(connection)
    if conn and conn != connection and environment.get("network_mode") != "bridge":
        doc.replace("loopback-network", conn, connection, "Only Docker bridge networking needs upstream's host.docker.internal rewrite.")
        conn = connection
    if conn:
        old = f"> Run: `{conn}` (use a heredoc or pwntools script as shown below)."
        if re.fullmatch(r"https?://\S+", conn):
            new = f"> Run: `curl -i -- {shlex.quote(conn)}` (use a heredoc or pwntools script as shown below)."
        elif re.fullmatch(r"nc\s+[A-Za-z0-9_.:\[\]-]+\s+\d{1,5}", conn):
            new = old
        else:
            new = "> Interpret the supplied connection details and connect using curl, a socket, or a pwntools script as appropriate (use a heredoc or pwntools script as shown below). Do not execute prose as a shell command."
        doc.replace("typed-connection-command", old, new, "A URL, multiple entrances, or descriptive metadata is not an executable shell command.")
    web_hint = "or use `web_fetch`."
    if web_hint in doc.adapted:
        doc.replace("sdk-web-tool-name", web_hint,
                    "or use `WebFetch`." if "WebFetch" in environment.get("web_tools", []) else "Native WebFetch is unavailable in this environment; use bash with curl/python3 requests for reachable targets.",
                    "The Claude SDK tool name and availability differ from the upstream generic web_fetch hint.")
    if _hosted(environment) or environment.get("network_mode") == "none":
        doc.replace("hosted-oob-network", "For XSS/SSRF: use `curl` to webhook.site.",
                    "For XSS/SSRF: use `curl` to an approved reachable callback endpoint. Public webhook.site is unavailable in this network; record the callback capability as blocked if no reachable endpoint exists.",
                    "Hosted or disabled networking has no public Internet; preserve the technique with an explicit reachability requirement.")
    doc.replace("multi-flag-progress", "2. Keep using tools until you have the flag.",
                "2. Keep using tools until the platform confirms all challenge objectives complete.", "One confirmed flag does not complete a multi-flag challenge.")
    review_context = context.get("review") or {}
    known_paths = bool(review_context.get("confirmed_submissions")) if isinstance(review_context, dict) else False
    if reviewer and known_paths:
        if conn:
            doc.replace("review-first-action", "> **FIRST ACTION REQUIRED**: Your very first tool call MUST connect to the service.",
                        "> **FIRST ACTION REQUIRED**: Your first execution work MUST retrace every known flag acquisition path. If there are no known flags, your very first execution tool call MUST connect to the service.",
                        "Reviewer first reproduces known acquisition paths; those may require local artifacts before a network connection.")
            doc.replace("review-first-path", "> Do NOT explore the sandbox filesystem first. The flag is on the service, not in the container.",
                        "> When there are known paths, inspect the relevant artifacts needed to retrace them first. Otherwise do NOT explore the sandbox filesystem first. The service is a challenge entry point, not the sandbox filesystem.",
                        "Known-flag reproduction can require filesystem prerequisites before connecting.")
            doc.replace("review-command-order", "1. Connect to the service now.",
                        "1. Retrace known flag acquisition paths now; if there are none, connect to the service now.", "Reviewer reproduction precedes new exploration.")
        else:
            doc.replace("review-command-order", "1. Inspect distfiles now.", "1. Retrace known flag acquisition paths now; if there are none, inspect distfiles now.", "Reviewer reproduction precedes new exploration.")
    if reviewer:
        doc.replace("review-bounded-completion", "2. Keep using tools until the platform confirms all challenge objectives complete.",
                    "2. Keep using tools until every known path has a reproduced/failed/blocked/unchecked result and the requested coverage review is complete; then deliver complete_review.", "The reviewer produces a finite review; autonomous solvers continue afterward.")
    rotation = rotation_policy_appendix(context.get("rotation_policy"))
    if rotation:
        old = ("2. Keep using tools until every known path has a reproduced/failed/blocked/unchecked result and the requested coverage review is complete; then deliver complete_review."
               if reviewer else "2. Keep using tools until the platform confirms all challenge objectives complete.")
        doc.replace("rotation-solver-stop", old,
                    ("2. Perform the requested review while this attempt remains active; persist partial outcomes promptly. Deliver complete_review only if its evidence requirements are met before the outer supervisor pauses or stops the attempt."
                     if reviewer else "2. Keep investigating while this attempt remains active, until the platform confirms completion or the outer supervisor pauses/stops it under its no-new-flag policy."),
                    "A hard no-new-flag deadline can stop an unfinished attempt; neither a solver nor a review may extend its allocation.")
    doc.appendix = ("\n\n## Xinghuo shared-work appendix\n"
        f"Your member identity is {context.get('agent_id', agent_id)}. Your private tool working directory is {work}.\n"
        "Each submit_flag or notify_coordinator command must be standalone with exactly one quoted argument; do not combine it with other shell commands.\n"
        "After CORRECT output FLAG: <value> as required above, notify_coordinator with acquisition steps/script, prerequisites and output paths, then continue remaining objectives. A correct or duplicate receipt alone does not end the team.\n"
        "You may independently explore or verify the same route as another solver; there are no route locks or fixed specialties.\n"
        "The coordinator chooses which discoveries to broadcast. Solver and active reviewer broadcasts arrive at ordinary tool boundaries every five calls and in natural continuation context.\n"
        f"Inputs at {inputs}/ are read-only. Copy useful artifacts to new immutable versioned paths under {shared}/. Do not overwrite published artifacts or another member's private files.\n"
        "Your private Python venv inherits preinstalled CTF packages. Put added dependencies and language caches in your member directory.\n"
        "Coordinate changes to shared system dependencies, services, ports, or another member's live state with the coordinator. Ordinary private experiments need no approval.\n"
        "Bumps are queued guidance for the next natural continuation; they do not interrupt this turn or require a fresh session.\n")
    if reviewer:
        doc.source += " + Xinghuo local reviewer baseline"
        doc.baseline += "\n\n## Independent reviewer responsibilities (local baseline, appended in full)\n" + REVIEWER_PROMPT
        old = "Native filesystem, web, Agent and Task tools are unavailable."
        web_tools = [name for name in ("WebFetch", "WebSearch") if name in environment.get("web_tools", [])]
        replacement = ("Native filesystem, Agent and Task tools are unavailable. "
                       + ((" and ".join(web_tools) + (" are" if len(web_tools) > 1 else " is")
                           + " available as described in the observed tool environment.") if web_tools else "Native web tools are unavailable."))
        doc.replace("review-web-availability", old, replacement, "Native web availability is environment-specific; filesystem and subagent restrictions remain.")
        doc.appendix = doc.appendix.replace("then continue remaining objectives.", "then continue the requested bounded review.")
        doc.appendix += ("For any reproduction that could reset a service, consume a one-shot opportunity, change shared state, "
            "or interfere with live solvers, notify_coordinator with the exact prerequisite and impact, and ask the coordinator "
            "to broadcast the coordination notice. Wait for a concrete coordinated arrangement and affected members to be ready before that operation. "
            "A broadcast alone does not authorize destructive execution. Do not pause, kill, or restart other solvers yourself. "
            "If coordination is unavailable, mark that path blocked and continue other independent review work; "
            "do not reset shared state on your own.\n")
        doc.appendix += ("\n## Reproduction checkpoint and subsequent coverage review (Xinghuo addition)\n"
            "After retracing all known flag acquisition paths, call record_reproductions with the fresh per-path results and evidence before investigating lateral opportunities or coverage gaps. "
            "If there are no known flags, record the empty/not-applicable reproduction checkpoint first. "
            "A reproduction may be reproduced, failed, blocked, or unchecked, but any result short of reproduction must explain the concrete gap; it never counts as successful reproduction. "
            "Then inspect lateral opportunities, coverage, missed routes, prior rejections, and member health. "
            "complete_review must include sections named lateral_opportunities, coverage, missed_routes, prior_rejections, and member_health, with findings and evidence or an explicit blocked/unchecked reason for each. "
            "Use audit_evidence_refs to cite actual tool evidence obtained after the reproduction checkpoint. "
            "Do not reuse the reproduction-only evidence as proof that this subsequent review was performed.\n"
            "The persisted review_context supplies absolute reproduction_deadline, coverage_deadline and deadline: retries and continuation never reset them. "
            "The total budget is 30 minutes, with at most the first 10 minutes for reproduction, coverage until minute 28, and the last 2 minutes for finalization. "
            "When there are no known flags the runtime immediately saves the not-applicable checkpoint; enter coverage immediately. "
            "Persist each attempted path promptly with record_review_progress(reproductions=[...]); then record_reproductions when all paths have outcomes. "
            "After the checkpoint, call record_review_progress with each section's findings and audit_evidence_refs as you investigate, so the supervisor can make an informed early decision. "
            "If the runtime ends the review early, your persisted evidence remains a partial review, never a completed reproduction or complete review.\n")
    doc.appendix += UNATTENDED_APPENDIX
    doc.appendix += ACQUISITION_WRITE_APPENDIX
    if reviewer:
        doc.appendix += EVIDENCE_ACCESS_APPENDIX + ACQUISITION_READ_APPENDIX
    doc.appendix += _attachment_appendix(challenge, context, environment, inputs)
    doc.appendix += _context_appendix(context, environment)
    if rotation:
        doc.appendix = doc.appendix.replace("then continue remaining objectives.",
                                           "then continue remaining objectives only while this attempt remains active.")
        doc.appendix = doc.appendix.replace(
            "The total budget is 30 minutes, with at most the first 10 minutes for reproduction, coverage until minute 28, and the last 2 minutes for finalization.",
            "The persisted review schedule is only a maximum review budget. Its reproduction, coverage and finalization phases must fit within the outer supervisor's current challenge allocation; an earlier supervisor stop ends this review as partial without extending the no-new-flag interval.")
    doc.appendix += rotation
    return doc


def coordinator_document(context: dict, environment: dict) -> PromptDocument:
    solvers = tuple(context.get("solver_ids") or (f"{family}-{number}" for family in ("pro", "flash") for number in (1, 2, 3)))
    count = {4: "four", 6: "six"}.get(len(solvers), str(len(solvers)))
    doc = PromptDocument("Veria backend/agents/claude_coordinator.py COORDINATOR_PROMPT", upstream.COORDINATOR_PROMPT)
    doc.replace("coordinator-single-question", "You are a CTF competition coordinator running for the ENTIRE duration of a live competition.\nYour job is to maximize the number of challenges solved.",
                "You are a CTF technical coordinator running for the ENTIRE duration of the current challenge attempt.\nYour job is to maximize the number of real flags found for this challenge.",
                "The outer Python supervisor manages the competition; this coordinator owns one challenge.")
    doc.replace("coordinator-fixed-team", "- Spawn swarms for unsolved challenges, prioritizing by solve count (easy first)",
                f"- Guide the {count} fixed autonomous solvers on this challenge, prioritizing promising opportunities (easy first)", "The runtime creates the fixed team; coordinator cannot select other challenges or spawn teams.")
    doc.replace("coordinator-completion", "The only time a swarm should die is when the flag\n  is confirmed correct.",
                "The only time this swarm should finish normally is when the outer supervisor confirms all\n  challenge objectives complete; the outer supervisor also owns deadlines, stopping and replacement.", "A first flag is not multi-flag completion; outer supervisor owns lifecycle.")
    doc.replace("coordinator-cost-scope", "Keep all swarms running.", "Keep all solvers on this challenge running.", "Narrow team scope without deleting Cost is not a concern.")
    doc.replace("coordinator-event-scope", "Respond with tool calls to manage the competition.", "Respond with tool calls to guide this challenge.", "Single-question tool interface.")
    rotation = rotation_policy_appendix(context.get("rotation_policy"))
    if rotation:
        doc.replace("rotation-coordinator-bounded",
                    "- NEVER kill a swarm. Solvers will keep trying indefinitely with different approaches.\n  Even when stuck, they often unstick themselves after several bumps. Your job is to\n  HELP them, not give up on them. The only time this swarm should finish normally is when the outer supervisor confirms all\n  challenge objectives complete; the outer supervisor also owns deadlines, stopping and replacement.",
                    "- Do not kill a team or manage platform scenes yourself. Help solvers make useful progress while the current attempt remains active. The outer Python supervisor may pause or stop this unfinished attempt when its no-new-flag interval expires, without waiting for bumps, a report or reviewer approval. Follow its checkpoint/stop request promptly; only the platform can confirm challenge completion.",
                    "Indefinite persistence and completion-only stopping conflict with the supervisor's hard no-new-flag rotation policy.")
        doc.replace("rotation-coordinator-cost", "- Cost is not a concern. Keep all solvers on this challenge running.",
                    "- Use the supervisor's current time allocation efficiently. Keep assigned solvers productive while the attempt is active, and preserve their evidence when the supervisor stops it.",
                    "The coordinator cannot promise unlimited attempt time or override the supervisor's resource and lifecycle allocation.")
    doc.appendix = """\n\n## Xinghuo coordination appendix
The runtime already starts SOLVER_COUNT fixed solver IDs: SOLVER_ROSTER. You cannot spawn more solvers, native Agent/Task subagents, or other questions. Each member must use its runtime-assigned provider and model; do not change those bindings or create alternative provider clients.
notify_coordinator messages are private to your inbox until you choose a broadcast. Prioritize reusable evidence, changed assumptions, and major corrections; do not automatically broadcast every progress note.
bump_agent persists ordered next-turn guidance. Queued/injected guidance is not proof the solver adopted it. Inspect pending state and later evidence.
A first correct flag is progress, not team completion. Immediately submit any new candidate through submit_flag.
The reporter writes the formal progress report. Answer technical questions; do not wait for, or create, a separate leader report.
Shared dependencies, services, ports and destructive reproduction prerequisites require coordination. Do not impose exclusive route ownership or stop duplicate independent exploration.
When the reviewer requests shared-state or one-shot reproduction, broadcast the concrete proposed operation and its impact to affected solvers, then coordinate the arrangement before execution. Do not treat the request itself as permission to reset services or stop solvers.
You also have Bash in the same member tool environment as the solvers. Use it for concrete technical experiments and durable evidence. Your tool events and findings reach the outer Python supervisor through the persisted evidence and reporter pipeline. To escalate significant evidence or a concrete blocker, use significant_evidence(source_event_id, summary, evidence_refs) with an existing original reportable event and its evidence paths. This sends a persisted supervisor event; it does not authorize you to close or replace the question. There is no separate platform client.
Use current tool schemas: read_solver_trace(solver_id, after), bump_agent(solver_id, insights), broadcast(message, source_message_ids), submit_flag(flag), check_swarm_status() and get_solve_status(). There is no CTFd submission fallback.
"""
    doc.appendix = doc.appendix.replace("SOLVER_COUNT", count).replace("SOLVER_ROSTER", ", ".join(solvers))
    doc.appendix += UNATTENDED_APPENDIX + EVIDENCE_ACCESS_APPENDIX + ACQUISITION_READ_APPENDIX + ACQUISITION_WRITE_APPENDIX
    doc.appendix += _context_appendix(context, environment)
    doc.appendix += rotation
    return doc


# Compatibility export; actual runtime builds the same baseline with context.
COORDINATOR_PROMPT = coordinator_document({}, {}).adapted


def build_solver_prompt(agent_id: str, context: dict[str, Any], environment: dict[str, Any]) -> str:
    return solver_document(agent_id, context, environment).text


def build_role_document(role: str, agent_id: str, context: dict[str, Any], environment: dict[str, Any]) -> PromptDocument:
    if role in {"solver", "reviewer"}:
        return solver_document(agent_id, context, environment, reviewer=role == "reviewer")
    if role == "coordinator":
        return coordinator_document(context, environment)
    if role == "reporter":
        return PromptDocument("Xinghuo local reporter baseline; no upstream equivalent", REPORTER_PROMPT,
                              appendix=UNATTENDED_APPENDIX + REPORTER_STYLE_APPENDIX + EVIDENCE_ACCESS_APPENDIX
                                       + ACQUISITION_READ_APPENDIX + _context_appendix(context, environment)
                                       + rotation_policy_appendix(context.get("rotation_policy")))
    raise ValueError(f"Unknown Veria role: {role}")


def build_role_prompt(role: str, agent_id: str, context: dict[str, Any], environment: dict[str, Any]) -> str:
    return build_role_document(role, agent_id, context, environment).text


def continuation_document(*, has_confirmed: bool, insights: list[dict[str, Any]] | None = None, first: bool = False,
                          rotation_policy: dict | None = None) -> PromptDocument:
    """Audit continuation patches just like the SDK system prompt patches."""
    if insights:
        doc = PromptDocument("Veria Claude SDK complete bump template + persisted guidance payload",
                             upstream.BUMP_PREFIX + _json(insights) + upstream.BUMP_SUFFIX)
        if has_confirmed:
            doc.replace("bump-confirmed-progress", "Your previous attempt did not find the flag.",
                        "Earlier candidates have been confirmed, but the platform has not confirmed this entire challenge complete.",
                        "Existing confirmed flags contradict the assertion that no flag was found.")
        elif first:
            doc.replace("bump-no-prior-attempt", "Your previous attempt did not find the flag.",
                        "This challenge has not yet produced a confirmed flag.",
                        "Guidance may exist before the first attempt; do not invent a previous execution.")
        doc.replace("bump-independent-verification", "Do NOT repeat what was tried.",
                    "Do NOT blindly repeat what was tried. Purposeful independent verification remains allowed.",
                    "An absolute repetition prohibition conflicts with independent verification; mechanical repetition remains prohibited.")
    else:
        doc = PromptDocument("Veria Claude SDK first/ordinary continuation template",
                             upstream.FIRST_PROMPT if first else upstream.CONTINUE_PROMPT)
    if has_confirmed:
        doc.appendix = ("\nContinue the remaining objectives while this attempt remains active; reproduction and duplicates are not new flags. Only the authoritative platform can confirm challenge completion; the outer supervisor may pause or stop an unfinished attempt."
                        if rotation_policy_appendix(rotation_policy) else
                        "\nContinue the remaining objectives; reproduction and duplicates are not new flags. Only authoritative platform completion ends the challenge.")
    doc.appendix += rotation_policy_appendix(rotation_policy)
    return doc


def continuation_prompt(*, has_confirmed: bool, insights: list[dict[str, Any]] | None = None, first: bool = False,
                        rotation_policy: dict | None = None) -> str:
    return continuation_document(has_confirmed=has_confirmed, insights=insights, first=first,
                                 rotation_policy=rotation_policy).text
