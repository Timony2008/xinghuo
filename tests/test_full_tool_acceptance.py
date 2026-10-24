"""Failure-path tests for offline acceptance; no heavy tools are mocked as verified."""
import importlib.util
from contextlib import chdir
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("tool_acceptance", ROOT / "scripts/veria_tool_acceptance.py")
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)
OFFLINE_NETWORK = {"interfaces": {"lo": {"flags": 9, "up": True, "loopback": True}}, "errors": {}, "active_non_loopback": [], "ipv4_routes": ""}


class AcceptanceFailureTests(unittest.TestCase):
    def test_nonzero_command_and_timeout_are_failures(self):
        with self.assertRaises(RuntimeError):
            acceptance.execute([sys.executable, "-c", "raise SystemExit(7)"])
        with self.assertRaises(RuntimeError):
            acceptance.execute([sys.executable, "-c", "import time; time.sleep(30)"], timeout=.05)
        result = acceptance.execute([sys.executable, "-c", "raise SystemExit(7)"], check=False)
        self.assertEqual(result["returncode"], 7)

    def test_missing_command_and_missing_python_distribution_fail_inventory(self):
        manifest = {"commands": ["missing-upstream-command"],
                    "python_modules": {"not-a-real-ctf-distribution": "also_missing"}}
        result = acceptance.inventory(manifest)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["commands"]["missing-upstream-command"]["status"], "failed")
        self.assertEqual(result["python_modules"]["not-a-real-ctf-distribution"]["status"], "failed")

    def test_nested_runtime_restriction_is_not_reported_as_success(self):
        def execute(argv, **_):
            # Compose config and helper version probes do not need container
            # namespaces. Only actual kernel/storage operations are denied.
            if argv[0] == "podman-compose":
                return {"returncode": 0, "stdout": "services:\n  fixture: {}\n", "stderr": ""}
            if argv[0] == "fuse-overlayfs" and "-o" in argv:
                return {"returncode": 1, "stdout": "", "stderr": "fuse: device not found, try 'modprobe fuse' first"}
            if argv[0] in {"fuse-overlayfs", "slirp4netns"}:
                return {"returncode": 0, "stdout": "version fixture", "stderr": ""}
            return {"returncode": 125, "stdout": "", "stderr": "cannot clone: Operation not permitted"}
        with patch.object(acceptance.shutil, "which", return_value="/usr/bin/podman"), patch.object(acceptance, "execute", side_effect=execute):
            result = acceptance.nested_container_probe()
        self.assertEqual(result["status"], "environment_restricted")
        with patch.object(acceptance.shutil, "which", return_value="/usr/bin/podman"), patch.object(acceptance, "execute", return_value={"returncode": 125, "stdout": "", "stderr": "invalid configuration"}):
            self.assertEqual(acceptance.nested_container_probe()["status"], "failed")

    def test_empty_trace_requires_independent_translation_evidence(self):
        def execute(argv, **_):
            Path(argv[2]).write_text("+++ exited with 0 +++\n")
            return {"returncode": 0 if argv[0] == "strace" else 1, "stdout": "VERIA_OFFLINE_FIXTURE\n",
                    "stderr": "" if argv[0] == "strace" else '"/proc/123/exe" is ELF from incompatible architecture'}
        for translation, expected in ((False, "failed"), (True, "environment_restricted")):
            with self.subTest(translation=translation), tempfile.TemporaryDirectory() as directory, chdir(directory), patch.object(acceptance, "compile_fixture", return_value="/synthetic-fixture"), patch.object(acceptance, "execute", side_effect=execute), patch.object(acceptance, "traced_exec_architecture", return_value={"architecture_translation_observed": translation}):
                result = acceptance.tracing()
            self.assertEqual(result["status"], expected)
            self.assertEqual(set(result["subchecks"]), {"strace", "ltrace"})

    def test_unknown_trace_error_is_not_hidden_by_translation(self):
        def execute(argv, **_):
            Path(argv[2]).write_text("")
            return {"returncode": 1, "stdout": "", "stderr": "invalid configuration"}
        with tempfile.TemporaryDirectory() as directory, chdir(directory), patch.object(acceptance, "compile_fixture", return_value="/synthetic-fixture"), patch.object(acceptance, "execute", side_effect=execute), patch.object(acceptance, "traced_exec_architecture", return_value={"architecture_translation_observed": True}):
            self.assertEqual(acceptance.tracing()["status"], "failed")

    def test_fuse_and_slirp_prerequisites_record_real_denials(self):
        denied = {"returncode": 1, "stdout": "", "stderr": "Operation not permitted"}
        with patch.object(acceptance, "execute", return_value=denied) as run:
            self.assertEqual(acceptance.fuse_overlay_probe()["status"], "environment_restricted")
            self.assertEqual(run.call_args.args[0][0], "fuse-overlayfs")
            self.assertIn("-o", run.call_args.args[0])
            self.assertEqual(acceptance.slirp_network_probe()["status"], "environment_restricted")
            self.assertEqual(run.call_args.args[0][0], "unshare")

    def test_empty_manifest_cannot_pass(self):
        self.assertEqual(acceptance.inventory({"commands": [], "python_modules": {}})["status"], "failed")

    def test_missing_tools_document_is_a_reported_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            result = acceptance.inventory({"commands": [], "python_modules": {}}, Path(directory, "missing-tools.txt"))
        self.assertEqual(result["tools_documentation"]["status"], "failed")
        self.assertIsNotNone(result["tools_documentation"]["error"])

    def test_uninstalled_virtual_package_uses_actual_installed_provider(self):
        manifest = {"commands": [], "python_modules": {}, "apt_packages": ["exiftool"],
                    "command_provenance": {"exiftool": {"package": "libimage-exiftool-perl"}}}
        def execute(argv, **_):
            return {"returncode": 0, "stdout": "12.40\tinstall ok installed" if argv[-1] == "libimage-exiftool-perl" else "\tunknown ok not-installed", "stderr": ""}
        with patch.object(acceptance, "execute", side_effect=execute):
            result = acceptance.inventory(manifest)
        self.assertEqual(result["apt_packages"]["exiftool"]["status"], "passed")
        self.assertEqual(result["apt_packages"]["exiftool"]["resolved_package"], "libimage-exiftool-perl")

    def test_missing_manifest_is_a_reported_failure(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(acceptance.sys, "platform", "linux"), patch.dict(acceptance.os.environ, {"XINGHUO_MEMBER": "pro-1"}), patch.object(acceptance, "network_state", return_value=OFFLINE_NETWORK):
            result = acceptance.verify(Path(directory, "missing.json"), phase="functional")
        self.assertEqual(result["status"], "failed")
        self.assertIn("manifest_error", result)

    def test_real_member_and_offline_network_are_required(self):
        with patch.object(acceptance.sys, "platform", "linux"), patch.dict(acceptance.os.environ, {}, clear=True):
            with self.assertRaisesRegex(AssertionError, "actual member"):
                acceptance.verify(phase="functional")

    def test_down_kernel_tunnels_do_not_imply_external_network(self):
        state = {"interfaces": {**OFFLINE_NETWORK["interfaces"], "gre0": {"flags": 128, "up": False, "loopback": False}},
                 "active_non_loopback": [], "errors": {}, "ipv4_routes": ""}
        with tempfile.TemporaryDirectory() as directory, patch.object(acceptance.sys, "platform", "linux"), patch.dict(acceptance.os.environ, {"XINGHUO_MEMBER": "pro-1"}), patch.object(acceptance, "network_state", return_value=state):
            # Reaching the explicit missing-manifest failure demonstrates that
            # the offline network check accepted only this down tunnel.
            result = acceptance.verify(Path(directory, "absent.json"), phase="functional")
            self.assertIn("manifest_error", result)
            state["interfaces"]["gre0"]["up"] = True
            state["active_non_loopback"] = ["gre0"]
            with self.assertRaisesRegex(AssertionError, "network none"):
                acceptance.verify(phase="functional")
        with patch.object(acceptance.sys, "platform", "linux"), patch.dict(acceptance.os.environ, {"XINGHUO_MEMBER": "pro-1"}), patch.object(acceptance.Path, "glob", return_value=[Path("lo"), Path("eth0")]):
            with self.assertRaisesRegex(AssertionError, "network none"):
                acceptance.verify(phase="functional")

    def test_partial_case_failure_does_not_stop_or_pass_remaining_cases(self):
        outputs = [{"returncode": 1, "stdout": "", "stderr": "missing Ghidra library"},
                   {"returncode": 0, "stdout": "{}", "stderr": ""}]
        with tempfile.TemporaryDirectory() as directory, patch.object(acceptance.sys, "platform", "linux"), patch.dict(acceptance.os.environ, {"XINGHUO_MEMBER": "pro-1"}), patch.object(acceptance, "network_state", return_value=OFFLINE_NETWORK), patch.dict(acceptance.CASES, {"ghidra": None, "binary": None}, clear=True), patch.object(acceptance, "execute", side_effect=outputs) as command, patch.object(acceptance, "nested_container_probe", return_value={"status": "environment_restricted"}):
            manifest = Path(directory, "manifest.json")
            manifest.write_text(json.dumps({"commands": ["gcc"], "python_modules": {"pyghidra": "pyghidra"}}))
            result = acceptance.verify(manifest, phase="functional")
        self.assertEqual(command.call_count, 2)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["functional"]["ghidra"]["status"], "failed")
        self.assertEqual(result["functional"]["binary"]["status"], "passed")


if __name__ == "__main__":
    unittest.main()
