"""Protect the entire upstream baseline, conflict allowlist, and SDK-bound text."""
import ast
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from xinghuo.veria import upstream_baseline as upstream
from xinghuo.veria.prompts import (build_role_document, build_role_prompt,
                                 continuation_document, continuation_prompt, coordinator_document, solver_document)

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "vendor/veria-ctf-agent/backend"

# This list is reviewed policy: a new string replacement must be consciously
# admitted here as a real conflict, not quietly treated as stylistic cleanup.
CONFLICT_RULES = {
    "sandbox-paths", "hosted-execution", "hosted-exec-command", "distfiles-path",
    "ghidra-writable-project", "loopback-network", "typed-connection-command",
    "sdk-web-tool-name", "hosted-oob-network", "multi-flag-progress",
    "review-first-action", "review-first-path", "review-command-order",
    "review-bounded-completion", "review-web-availability",
    "coordinator-single-question", "coordinator-fixed-team", "coordinator-completion",
    "coordinator-cost-scope", "coordinator-event-scope",
    "observed-pyghidra-unavailable", "observed-binary-tools-unavailable",
    "observed-pyghidra-runtime-unavailable",
}
CONTINUATION_CONFLICT_RULES = {"bump-confirmed-progress", "bump-no-prior-attempt", "bump-independent-verification"}


class UpstreamPromptTests(unittest.TestCase):
    def test_all_37_pinned_vendor_originals_match_hash_manifest(self):
        vendor = VENDOR.parent
        self.assertEqual(json.loads((vendor / "UPSTREAM.json").read_text())["commit"],
                         "3366d569557c4fda3fd153040632de65e255396d")
        manifest = json.loads((vendor / "SOURCE_SHA256.json").read_text())
        self.assertEqual(len(manifest), 37)
        for name, digest in manifest.items():
            with self.subTest(name=name):
                self.assertEqual(hashlib.sha256((vendor / name).read_bytes()).hexdigest(), digest)

    def test_packaged_original_sources_equal_fixed_vendor_bytes(self):
        for resource, vendor in {
            "upstream_prompts.py.txt": "prompts.py",
            "upstream_coordinator.py.txt": "agents/claude_coordinator.py",
            "upstream_claude_solver.py.txt": "agents/claude_solver.py",
            "upstream_tools_core.py.txt": "tools/core.py",
            "upstream_output_types.py.txt": "output_types.py",
        }.items():
            self.assertEqual((upstream.RESOURCE_DIR / resource).read_bytes(), (VENDOR / vendor).read_bytes())
        source = (VENDOR / "prompts.py").read_text()
        function = next(x for x in ast.parse(source).body if isinstance(x, ast.FunctionDef) and x.name == "build_prompt")
        self.assertEqual(upstream.build_prompt.__code__.co_firstlineno, function.lineno)

    def test_full_solver_schema_and_loop_warning_equal_upstream(self):
        tree = ast.parse((VENDOR / "output_types.py").read_text())
        fn = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "solver_output_json_schema")
        schema = ast.literal_eval(next(node.value for node in fn.body if isinstance(node, ast.Return)))
        self.assertEqual(upstream.solver_output_json_schema(), schema)
        from xinghuo.veria.loop_detect import LOOP_WARNING_MESSAGE
        loop_tree = ast.parse((VENDOR / "loop_detect.py").read_text())
        self.assertEqual(LOOP_WARNING_MESSAGE, ast.literal_eval(upstream.assignment(loop_tree, "LOOP_WARNING_MESSAGE")))

    def test_full_original_render_is_baseline_and_only_reviewed_conflicts_change_it(self):
        for connection, environment in [
            ("", {}), ("nc localhost 1234", {"network_mode": "bridge"}),
            ("http://localhost:8080/path", {"web_tools": ["WebFetch", "WebSearch"]}),
            ("two entrances: https://a.invalid and https://b.invalid", {}),
            ("nc localhost 1234", {"execution_mode": "hosted_process", "network_mode": "hosted_shared",
                                   "paths": {"members": "/work/attempt/members", "inputs": "/work/attempt/inputs"}}),
        ]:
            with self.subTest(connection=connection, environment=environment):
                challenge = {"name": "fixture", "category": "pwn", "points": 100, "tags": ["tcp", "image"],
                             "description": "Only synthetic data.", "connection_info": connection,
                             "hints": [{"content": "visible hint"}, {"id": "hidden"}],
                             "distfiles": ["binary", "image.png"]}
                context = {"challenge": challenge}
                meta = SimpleNamespace(name="fixture", category="pwn", value=100, tags=["tcp", "image"],
                                       description="Only synthetic data.", connection_info=connection,
                                       hints=challenge["hints"])
                original = upstream.SANDBOX_PREAMBLE + upstream.build_prompt(meta, ["binary", "image.png"], "unknown", False)
                doc = solver_document("pro-1", context, environment)
                self.assertEqual(doc.baseline, original)
                self.assertTrue(doc.appendix.startswith("\n\n## Xinghuo shared-work appendix"))
                output = original
                for patch in doc.patches:
                    self.assertIn(patch.rule, CONFLICT_RULES)
                    self.assertTrue(patch.reason)
                    self.assertEqual(output.count(patch.original), patch.occurrences)
                    output = output.replace(patch.original, patch.replacement)
                self.assertEqual(doc.text, output + doc.appendix)
                self.assertEqual(build_role_prompt("solver", "pro-1", context, environment), doc.text)
                for verbatim in [
                    "**pyghidra** is installed for decompilation. Use it via bash:",
                    "    listing = flat_api.currentProgram.getListing()",
                    "    # Iterate functions, decompile, etc.",
                    "Also available: radare2 (`r2`), gdb, angr, capstone.",
                    "**IMAGE: use `exiftool`, `steghide`, `zsteg`, `strings` via bash**",
                    "For RSA: use `RsaCtfTool`, sage ECM, or `cado-nfs`.",
                    "**Tags**    : tcp, image", "## Hints\n- visible hint",
                    "6. Once CORRECT: output `FLAG: <value>` on its own line.",
                ]:
                    self.assertIn(verbatim, doc.text)

    def test_tcp_examples_and_first_action_restored(self):
        text = build_role_prompt("solver", "pro-1", {"challenge": {"connection_info": "nc target.invalid 1234"}}, {})
        self.assertIn("> **FIRST ACTION REQUIRED**: Your very first tool call MUST connect to the service.", text)
        self.assertIn("Each `bash` call is a fresh process — use a heredoc to send multiple lines in one shot:", text)
        self.assertIn("nc target.invalid 1234 <<'EOF'\ncommand1\ncommand2\nEOF", text)
        self.assertIn("Or write a Python `socket` / `pwntools` script for stateful interaction.", text)

    def test_http_command_is_quoted_and_hosted_callback_limitation_keeps_method(self):
        text = build_role_prompt("solver", "pro-1", {"challenge": {"connection_info": "https://a.invalid/?q=$(whoami)&x=1"}},
                                 {"execution_mode": "hosted_process"})
        self.assertIn("Run: `curl -i -- 'https://a.invalid/?q=$(whoami)&x=1'`", text)
        self.assertIn("For XSS/SSRF: use `curl` to an approved reachable callback endpoint.", text)
        self.assertNotIn("or use `WebFetch`", text)

    def test_local_web_tools_and_multiple_entrances_not_lost(self):
        context = {"challenge": {"connection_info": "https://a.invalid", "urls": ["https://a.invalid", "https://b.invalid"]},
                   "addresses": [{"host": "target", "port": 80}]}
        text = build_role_prompt("solver", "pro-1", context, {"web_tools": ["WebFetch", "WebSearch"]})
        self.assertIn("or use `WebFetch`.", text)
        self.assertIn("For XSS/SSRF: use `curl` to webhook.site.", text)
        self.assertIn("https://b.invalid", text)
        self.assertIn('"host": "target"', text)

    def test_reviewer_inherits_methods_before_complete_local_reproduction_policy(self):
        context = {"challenge": {"category": "pwn", "connection_info": "nc target 80"},
                   "review": {"confirmed_submissions": [{"request_id": "known-flag"}]}}
        for environment in [{}, {"web_tools": ["WebFetch", "WebSearch"]}]:
            doc = build_role_document("reviewer", "reviewer-round1", context, environment)
            self.assertTrue(doc.baseline.startswith(upstream.SANDBOX_PREAMBLE))
            self.assertTrue(doc.baseline.endswith(upstream.REVIEWER_PROMPT))
            for content in ["For RSA: use `RsaCtfTool`, sage ECM, or `cado-nfs`.",
                            "pyghidra.open_program(", "Each `bash` call is a fresh process",
                            "ACTUALLY retrace every known flag acquisition path", "then deliver complete_review.",
                            "/challenge/members/reviewer-round1/work/ghidra-projects", "known-flag"]:
                self.assertIn(content, doc.text)
            self.assertNotIn("2. Keep using tools until the platform confirms all challenge objectives complete.", doc.text)
            self.assertNotIn("then continue remaining objectives", doc.text)
            self.assertIn("ask the coordinator to broadcast the coordination notice", doc.text)
            self.assertIn("do not reset shared state on your own", doc.text)
            self.assertTrue({p.rule for p in doc.patches} <= CONFLICT_RULES)

    def test_reviewer_without_known_paths_keeps_original_solver_first_action(self):
        for connection in ["", "nc target 1234"]:
            with self.subTest(connection=connection):
                doc = build_role_document("reviewer", "reviewer-empty", {
                    "challenge": {"connection_info": connection},
                    "review": {"confirmed_submissions": []}}, {})
                self.assertFalse({"review-first-action", "review-first-path", "review-command-order"} & {p.rule for p in doc.patches})
                self.assertIn("1. Connect to the service now." if connection else "1. Inspect distfiles now.", doc.text)
                if connection:
                    self.assertIn("> **FIRST ACTION REQUIRED**: Your very first tool call MUST connect to the service.", doc.text)
                self.assertIn("then deliver complete_review", doc.text)

    def test_reviewer_partial_web_capability_claim_names_only_the_available_tool(self):
        context = {"challenge": {"connection_info": "https://fixture.invalid/"}, "review": {"confirmed_submissions": []}}
        for tool, unavailable in [("WebSearch", "WebFetch"), ("WebFetch", "WebSearch")]:
            with self.subTest(tool=tool):
                doc = build_role_document("reviewer", "reviewer-web", context, {"web_tools": [tool]})
                self.assertIn(f"{tool} is available as described in the observed tool environment.", doc.text)
                self.assertNotIn("WebFetch and WebSearch are available", doc.text)
                self.assertNotIn(f"{unavailable} is available as described", doc.text)
                self.assertEqual("or use `WebFetch`." in doc.text, tool == "WebFetch")
                self.assertEqual("Native WebFetch is unavailable" in doc.text, tool != "WebFetch")
                patch = next(patch for patch in doc.patches if patch.rule == "review-web-availability")
                self.assertIn(f"{tool} is available", patch.replacement)

    def test_unavailable_tools_change_only_installed_facts_and_keep_complete_methods(self):
        context = {"challenge": {"category": "pwn", "distfiles": ["binary", "image.png"]}}
        complete = solver_document("pro-1", context, {})
        restricted = solver_document("pro-1", context, {
            "architecture": "aarch64", "tools": {"r2": None, "gdb": "/usr/bin/gdb"},
            "package_inventory": {"pyghidra": {"available": False}, "angr": {"available": False}, "capstone": {"available": True}},
            "missing_packages": ["pyghidra", "angr"], "missing_commands": ["r2"]})
        self.assertNotIn("**pyghidra** is installed", restricted.text)
        self.assertIn("**pyghidra** is unavailable in this observed environment.", restricted.text)
        self.assertIn("Observed unavailable here: radare2 (`r2`), angr.", restricted.text)
        for snippet in ["import pyghidra", "with pyghidra.open_program(",
                        "    listing = flat_api.currentProgram.getListing()", "    # Iterate functions, decompile, etc.",
                        "For RSA: use `RsaCtfTool`, sage ECM, or `cado-nfs`.", "**IMAGE: use `exiftool`, `steghide`, `zsteg`, `strings` via bash**"]:
            self.assertIn(snippet, complete.text)
            self.assertIn(snippet, restricted.text)
        self.assertEqual({p.rule for p in restricted.patches} - {p.rule for p in complete.patches},
                         {"observed-pyghidra-unavailable", "observed-binary-tools-unavailable"})

    def test_pyghidra_package_presence_never_proves_native_decompilation(self):
        context = {"challenge": {"category": "pwn"}}
        inventory_only = solver_document("pro-1", context, {
            "package_inventory": {"pyghidra": {"available": True}},
            "functional_validation": {"status": "not_run_in_inventory"}})
        self.assertIn("not_run_in_inventory means unverified, not passed", inventory_only.text)
        for observation in [{"tool_results": {"python:pyghidra": {"functional": {"status": "environment_restricted", "case": "ghidra"}}}},
                            {"functional": {"ghidra": {"status": "failed", "stderr": "native decompiler unavailable"}}}]:
            with self.subTest(observation=observation):
                doc = solver_document("pro-1", context, {"package_inventory": {"pyghidra": {"available": True}}, **observation})
                self.assertIn("observed-pyghidra-runtime-unavailable", {p.rule for p in doc.patches})
                self.assertNotIn("**pyghidra** is installed for decompilation", doc.text)
                self.assertIn("with pyghidra.open_program(", doc.text)
                self.assertIn("    listing = flat_api.currentProgram.getListing()", doc.text)

    def test_real_platform_description_attachment_links_are_visible_without_inventing_local_files(self):
        from xinghuo.core import Challenge
        description = ("![challenge image](https://materials.invalid/diagram.png?token=fixture)\n"
                       "[binary archive](https://materials.invalid/challenge.zip)\n"
                       "[service](https://target.invalid/) [documentation](https://docs.invalid/guide)")
        context = {"challenge": Challenge("materials-fixture", description=description).to_dict(),
                   "addresses": ["https://target.invalid/"], "distfile_names": []}
        doc = solver_document("pro-1", context, {})
        materials = doc.appendix.split("## Additional attachment materials", 1)[1].split("## Complete runtime context", 1)[0]
        self.assertIn("`challenge image` — remote/not_staged; source: https://materials.invalid/diagram.png?token=fixture", materials)
        self.assertIn("`binary archive` — remote/not_staged; source: https://materials.invalid/challenge.zip", materials)
        self.assertIn("**IMAGE: use `exiftool`, `steghide`, `zsteg`, `strings` via bash**", materials)
        self.assertNotIn("target.invalid", materials)
        self.assertNotIn("docs.invalid", materials)
        self.assertNotIn("## Attached Files", doc.baseline)
        self.assertNotIn("/challenge/inputs/diagram.png", doc.text)
        self.assertIn(description, doc.baseline)
        self.assertIn('"distfile_names": []', doc.text)

    def test_explicit_remote_metadata_and_staged_materials_are_separate(self):
        context = {"challenge": {"category": "forensics", "attachments": [
            {"filename": "secret-image", "url": "https://materials.invalid/download?id=7", "content_type": "image/png"}],
            "distfiles": [{"name": "remote.zip", "url": "https://materials.invalid/remote.zip"}, "https://materials.invalid/raw.bin"]},
            "distfile_names": ["actual.png"]}
        doc = solver_document("pro-1", context, {})
        self.assertIn("- `/challenge/inputs/actual.png`", doc.text)
        self.assertNotIn("/challenge/inputs/remote.zip", doc.text)
        self.assertNotIn("/challenge/inputs/https:", doc.text)
        self.assertIn("`secret-image` — remote/not_staged", doc.text)
        self.assertIn("`remote.zip` — remote/not_staged", doc.text)
        self.assertIn("`raw.bin` — remote/not_staged", doc.text)
        self.assertIn('"content_type": "image/png"', doc.text)

    def test_category_branches_match_complete_upstream_builder(self):
        for category in ["reverse", "reversing", "re", "pwn", "binary", "misc", "", "crypto", "web", "forensics"]:
            with self.subTest(category=category):
                context = {"challenge": {"category": category}}
                doc = solver_document("pro-1", context, {})
                meta = SimpleNamespace(name="Unknown", category=category, value=0, description="", tags=[], connection_info="", hints=[])
                self.assertEqual(doc.baseline, upstream.SANDBOX_PREAMBLE + upstream.build_prompt(meta, [], "unknown", False))

    def test_coordinator_keeps_anti_give_up_and_cost_rules_in_full(self):
        doc = coordinator_document({}, {})
        self.assertEqual(doc.baseline, upstream.COORDINATOR_PROMPT)
        self.assertIn("- NEVER kill a swarm. Solvers will keep trying indefinitely with different approaches.\n  Even when stuck, they often unstick themselves after several bumps. Your job is to\n  HELP them, not give up on them.", doc.text)
        self.assertIn("- Cost is not a concern.", doc.text)
        self.assertIn("You also have Bash in the same member tool environment", doc.text)
        self.assertIn("persisted evidence and reporter pipeline", doc.text)
        self.assertIn("broadcast the concrete proposed operation and its impact", doc.text)
        self.assertTrue({p.rule for p in doc.patches} <= CONFLICT_RULES)

    def test_reporter_has_complete_explicit_local_baseline(self):
        doc = build_role_document("reporter", "reporter", {}, {})
        self.assertEqual(doc.baseline, upstream.REPORTER_PROMPT)
        self.assertEqual(doc.patches, [])
        self.assertIn("no upstream equivalent", doc.source)

    def test_every_role_is_explicitly_unattended_without_replacing_its_baseline(self):
        for role in ["solver", "coordinator", "reviewer", "reporter"]:
            with self.subTest(role=role):
                doc = build_role_document(role, role, {}, {})
                self.assertIn("Do not ask the user for confirmation, clarification, permission, or a choice", doc.appendix)
                self.assertIn("do not wait for a human reply", doc.appendix)
                self.assertIn("This coordination does not require a human", doc.appendix)
                self.assertIn("mark that path blocked and continue independent work", doc.appendix)
                self.assertNotIn("## Unattended competition operation", doc.baseline)

    def test_mixed4_coordinator_uses_actual_roster_without_forbidding_assigned_providers(self):
        roster = [f"solver-{index}" for index in range(1, 5)]
        doc = build_role_document("coordinator", "coordinator", {"solver_ids": roster, "team_profile": "mixed4"}, {})
        self.assertIn("Guide the four fixed autonomous solvers", doc.text)
        self.assertIn("four fixed solver IDs: " + ", ".join(roster), doc.text)
        self.assertNotIn("six fixed solver", doc.text)
        self.assertIn("runtime-assigned provider and model", doc.text)
        self.assertIn("Cost is not a concern", doc.text)

    def test_reviewer_requires_reproduction_checkpoint_before_additional_review(self):
        for paths in [[], [{"request_id": "known-path"}]]:
            with self.subTest(paths=paths):
                doc = build_role_document("reviewer", "reviewer-audit", {
                    "review": {"confirmed_submissions": paths}}, {})
                self.assertTrue(doc.baseline.endswith(upstream.REVIEWER_PROMPT))
                self.assertIn("call record_reproductions", doc.appendix)
                self.assertIn("before investigating lateral opportunities or coverage gaps", doc.appendix)
                self.assertIn("record the empty/not-applicable reproduction checkpoint first", doc.appendix)
                self.assertIn("audit_evidence_refs", doc.appendix)
                for section in ["lateral_opportunities", "coverage", "missed_routes", "prior_rejections", "member_health"]:
                    self.assertIn(section, doc.appendix)

    def test_continuation_uses_full_original_templates_and_narrow_conflicts(self):
        self.assertEqual(continuation_prompt(has_confirmed=False, first=True), upstream.FIRST_PROMPT)
        self.assertEqual(continuation_prompt(has_confirmed=False), upstream.CONTINUE_PROMPT)
        bumped = continuation_prompt(has_confirmed=False, insights=[{"id": "bump-1", "insights": "inspect nonce"}])
        self.assertTrue(bumped.startswith("Your previous attempt did not find the flag. Insights from other agents:\n\n"))
        self.assertIn('"id": "bump-1"', bumped)
        self.assertIn("Try a different approach. Do NOT blindly repeat what was tried.", bumped)
        confirmed = continuation_prompt(has_confirmed=True, insights=[{"id": "bump-2"}])
        self.assertNotIn("previous attempt did not find the flag", confirmed)
        self.assertIn("remaining objectives", confirmed)

    def test_every_continuation_patch_has_a_conflict_ledger_including_first_turn_bumps(self):
        for confirmed in [True, False]:
            for first in [True, False]:
                for insights in [[], [{"id": "guide-1", "insights": "retain full payload"}, {"id": "guide-2", "insights": "retry independently"}]]:
                    with self.subTest(confirmed=confirmed, first=first, insights=insights):
                        doc = continuation_document(has_confirmed=confirmed, first=first, insights=insights)
                        self.assertTrue({p.rule for p in doc.patches} <= CONTINUATION_CONFLICT_RULES)
                        rendered = doc.baseline
                        for patch in doc.patches:
                            self.assertTrue(patch.reason)
                            self.assertEqual(rendered.count(patch.original), patch.occurrences)
                            rendered = rendered.replace(patch.original, patch.replacement)
                        self.assertEqual(doc.text, rendered + doc.appendix)
                        self.assertEqual(doc.text, continuation_prompt(has_confirmed=confirmed, first=first, insights=insights))
                        if insights:
                            self.assertEqual(doc.baseline, upstream.BUMP_PREFIX + json.dumps(insights, ensure_ascii=False, indent=2) + upstream.BUMP_SUFFIX)
                            for item in insights:
                                self.assertIn(item["id"], doc.text)
                        if not confirmed and first:
                            self.assertNotIn("Your previous attempt", doc.text)


if __name__ == "__main__":
    unittest.main()
