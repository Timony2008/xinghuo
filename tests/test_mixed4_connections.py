"""Member routing and persisted profile safety; all credentials are synthetic."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from xinghuo.config import Config, VERIA_LEGACY_SOLVERS, VERIA_MIXED4_SOLVERS
from xinghuo.hosted import hosted_config, configure_model_environment, prepare_hosted_run
from xinghuo.model_connection import connection, profile_contract
from xinghuo.store import Store
from xinghuo.veria.driver import client_environment, write_sdk_launcher, DriverError


class Mixed4RoutingTests(unittest.TestCase):
    def fixture_env(self):
        return {"XINGHUO_TEAM_PROFILE": "mixed4", "XINGHUO_DEPLOYMENT": "hosted",
                "DEEPSEEK_API_KEY": "fake-deepseek-key", "GLM_API_KEY": "fake-glm-key",
                "DEEPSEEK_BASE_URL": "http://api.deepseek.com.tsecbench.gw/anthropic",
                "GLM_BASE_URL": "http://console.cloudrouter.com.cn.tsecbench.gw",
                "BENCHMARK_TOKEN": "fake-platform-token"}

    def test_explicit_mixed4_has_glm_coordinator_reviewer_and_deepseek_strategy_reporter(self):
        config = hosted_config({"XINGHUO_TEAM_PROFILE": "mixed4"})
        self.assertEqual(config.veria_team_profile, "mixed4")
        self.assertEqual(config.veria_solver_ids, list(VERIA_MIXED4_SOLVERS))
        self.assertEqual(len(config.veria_models), 7)
        self.assertEqual(config.veria_models, {"solver-1": "deepseek-v4-pro", "solver-2": "deepseek-flash",
            "solver-3": "glm-5.3", "solver-4": "glm-5.3-flash", "coordinator": "glm-5.3",
            "reviewer": "glm-5.3", "reporter": "deepseek-flash"})
        self.assertEqual(config.veria_providers["solver-3"], "glm")
        self.assertEqual(config.veria_providers["coordinator"], "glm")
        self.assertEqual(config.veria_providers["reviewer"], "glm")
        self.assertEqual(config.model, "deepseek-v4-pro")
        self.assertEqual(config.veria_providers["reporter"], "deepseek")
        self.assertEqual((config.budget, config.parallel), (28800, 3))
        self.assertEqual(Config().validate().veria_solver_ids, list(VERIA_LEGACY_SOLVERS))

    def test_glm_coordinator_reviewer_override_does_not_move_deepseek_strategy(self):
        env = self.fixture_env() | {"DEEPSEEK_STRONG_MODEL": "ds-group/pro", "GLM_STRONG_MODEL": "glm-group/5.3"}
        config = hosted_config(env)
        self.assertEqual(config.model, "ds-group/pro")
        self.assertEqual(config.veria_models["reviewer"], "glm-group/5.3")
        self.assertEqual(config.veria_models["coordinator"], "glm-group/5.3")
        self.assertEqual(config.veria_providers["coordinator"], "glm")
        self.assertEqual(config.veria_providers["reviewer"], "glm")
        summary = configure_model_environment(env, config)
        self.assertEqual(summary["provider"], "deepseek")
        self.assertEqual(summary["strategy_model"], "ds-group/pro")
        self.assertEqual(env["ANTHROPIC_API_KEY"], "fake-deepseek-key")

    def test_strategy_environment_without_model_override_stays_deepseek(self):
        env = self.fixture_env() | {"GLM_STRONG_MODEL": "private/glm-review", "DEEPSEEK_STRONG_MODEL": "private/ds-strategy"}
        summary = configure_model_environment(env)
        self.assertEqual(summary["provider"], "deepseek")
        self.assertEqual(summary["strategy_model"], "private/ds-strategy")
        self.assertEqual(env["ANTHROPIC_API_KEY"], "fake-deepseek-key")
        self.assertNotEqual(env["ANTHROPIC_MODEL"], "private/glm-review")

    def test_provider_is_explicit_even_when_model_name_and_global_aliases_overlap(self):
        env = self.fixture_env() | {"ANTHROPIC_API_KEY": "stale-alias", "XINGHUO_PROVIDER": "glm"}
        before = dict(env)
        one = client_environment("same-group/model", Path("/tmp/one"), provider="deepseek", environ=env)
        two = client_environment("same-group/model", Path("/tmp/two"), provider="glm", environ=env)
        self.assertEqual(one["ANTHROPIC_API_KEY"], "fake-deepseek-key")
        self.assertEqual(two["ANTHROPIC_API_KEY"], "fake-glm-key")
        self.assertEqual(one["ANTHROPIC_BASE_URL"], env["DEEPSEEK_BASE_URL"])
        self.assertEqual(two["ANTHROPIC_BASE_URL"], env["GLM_BASE_URL"])
        self.assertEqual(env, before)
        self.assertNotIn("BENCHMARK_TOKEN", one)
        self.assertNotIn("GLM_API_KEY", one)
        self.assertNotIn("DEEPSEEK_API_KEY", two)

    def test_missing_glm_key_cannot_use_deepseek_or_selected_anthropic_alias(self):
        env = self.fixture_env()
        del env["GLM_API_KEY"]
        env["ANTHROPIC_API_KEY"] = "fake-deepseek-key"
        with self.assertRaisesRegex(DriverError, "Missing GLM_API_KEY"):
            client_environment("glm-5.3", Path("/tmp/glm"), provider="glm", environ=env)
        with self.assertRaisesRegex(ValueError, "Missing GLM_API_KEY"):
            configure_model_environment(env, hosted_config(env))

    def test_cloudrouter_anthropic_url_is_domain_root_and_group_names_are_preserved(self):
        env = self.fixture_env()
        for path in ("/v1", "/v1/messages", "/v1/chat/completions", "/anthropic"):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "domain root"):
                connection(env | {"GLM_BASE_URL": env["GLM_BASE_URL"] + path}, provider="glm")
        config = hosted_config(env | {"GLM_STRONG_MODEL": "hackathon/glm-5.3",
                                      "GLM_FAST_MODEL": "private/glm-5.3-flash"})
        self.assertEqual(config.veria_models["solver-3"], "hackathon/glm-5.3")
        self.assertEqual(config.veria_models["coordinator"], "hackathon/glm-5.3")
        self.assertEqual(config.veria_models["reviewer"], "hackathon/glm-5.3")
        self.assertEqual(config.veria_models["solver-4"], "private/glm-5.3-flash")
        summary = configure_model_environment(env, config)
        self.assertEqual(summary["strategy_model"], "deepseek-v4-pro")
        self.assertEqual(summary["provider_hosts"]["glm"], "console.cloudrouter.com.cn.tsecbench.gw")
        self.assertNotIn("fake-", json.dumps(summary) + json.dumps(config.to_dict()))

    def test_actual_launcher_drops_raw_provider_platform_and_unrelated_credentials(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            child = root / "capture"
            child.write_text("#!" + sys.executable + "\nimport json,os\nprint(json.dumps(dict(os.environ)))\n")
            child.chmod(0o700)
            launcher = root / "launch-sdk"
            write_sdk_launcher(launcher, child)
            env = self.fixture_env() | {"AWS_SECRET_ACCESS_KEY": "fake-aws-secret", "UNRELATED_SECRET": "fake-other"}
            env.update(client_environment("glm-5.3", root / ".claude", provider="glm", environ=env))
            result = subprocess.run([str(launcher)], env=env, capture_output=True, text=True, check=True)
            observed = json.loads(result.stdout)
            self.assertEqual(observed["ANTHROPIC_API_KEY"], "fake-glm-key")
            self.assertEqual(observed["ANTHROPIC_AUTH_TOKEN"], "fake-glm-key")
            for name in ("DEEPSEEK_API_KEY", "GLM_API_KEY", "BENCHMARK_TOKEN", "AWS_SECRET_ACCESS_KEY", "UNRELATED_SECRET"):
                self.assertNotIn(name, observed)

    def test_resume_inherits_profile_and_aliases_but_refuses_any_immutable_change(self):
        with tempfile.TemporaryDirectory() as temp:
            root, directory = Path(temp), Path(temp) / "run"
            config = hosted_config(self.fixture_env() | {"GLM_STRONG_MODEL": "group/glm-5.3"})
            store = Store(directory)
            for name, value in {"config": config.to_dict(), "started_at": 10, "deadline": 28810,
                                "run_id": "synthetic-run"}.items():
                store.set_meta(name, value)
            store.close()
            database = directory / "state.sqlite3"
            before = database.read_bytes()
            env = {"XINGHUO_RUN_MODE": "resume", "XINGHUO_RUN_DIR": str(directory), "GLM_API_KEY": "rotated-key"}
            with prepare_hosted_run(env, work_root=root) as run:
                self.assertEqual(run.config.veria_solver_ids, list(VERIA_MIXED4_SOLVERS))
                self.assertEqual(run.config.veria_models["solver-3"], "group/glm-5.3")
                self.assertEqual(run.store.get_meta("deadline"), 28810)
            for change in ({"XINGHUO_TEAM_PROFILE": "legacy6"}, {"GLM_STRONG_MODEL": "glm-5.3"},
                           {"GLM_FAST_MODEL": "other"}, {"DEEPSEEK_STRONG_MODEL": "other"},
                           {"XINGHUO_PROVIDER": "glm"}):
                with self.subTest(change=change), self.assertRaises(ValueError):
                    with prepare_hosted_run(env | change, work_root=root):
                        self.fail("Changed immutable mixed4 configuration accepted")
                self.assertEqual(database.read_bytes(), before)

    def test_old_config_without_new_fields_keeps_six_solver_profile(self):
        config = hosted_config({"XINGHUO_TEAM_PROFILE": "legacy6"}).to_dict()
        for name in ("veria_team_profile", "veria_solver_ids", "veria_providers"):
            config.pop(name)
        old = Config(**config).validate()
        self.assertEqual(old.veria_team_profile, "legacy6")
        self.assertEqual(old.veria_solver_ids, list(VERIA_LEGACY_SOLVERS))
