"""All-Flash hosted profile, exact routing, and immutable resume policy."""
import json
from pathlib import Path
import tempfile
import unittest

from xinghuo.config import Config, VERIA_FLASH4_MODELS, VERIA_FLASH4_PROVIDERS, VERIA_MIXED4_SOLVERS
from xinghuo.hosted import configure_model_environment, hosted_config, prepare_hosted_run
from xinghuo.model_connection import profile_contract
from xinghuo.store import Store
from xinghuo.veria.driver import client_environment


class Flash4RoutingTests(unittest.TestCase):
    def fixture_env(self):
        return {"XINGHUO_TEAM_PROFILE": "flash4", "XINGHUO_PROVIDER": "deepseek",
                "DEEPSEEK_API_KEY": "synthetic-deepseek-key",
                "DEEPSEEK_BASE_URL": "http://api.deepseek.com.tsecbench.gw/anthropic"}

    def test_hosted_defaults_are_all_flash_with_fifteen_minute_switch(self):
        config = hosted_config({})
        self.assertEqual(config.veria_team_profile, "flash4")
        self.assertEqual(config.veria_solver_ids, list(VERIA_MIXED4_SOLVERS))
        self.assertEqual(config.veria_models, VERIA_FLASH4_MODELS)
        self.assertEqual(config.veria_providers, VERIA_FLASH4_PROVIDERS)
        self.assertEqual(config.veria_provider, "deepseek")
        self.assertEqual(config.model, "deepseek-flash")
        self.assertEqual((config.budget, config.parallel, config.no_flag_timeout), (28800, 3, 900))
        self.assertEqual(Config().validate().no_flag_timeout, 0)

    def test_every_role_and_strategy_use_only_deepseek_credentials(self):
        env = self.fixture_env() | {"GLM_API_KEY": "unused-glm-key",
            "GLM_BASE_URL": "https://unused.invalid", "ANTHROPIC_API_KEY": "stale-alias",
            "ANTHROPIC_MODEL": "stale-model", "BENCHMARK_TOKEN": "synthetic-platform-token"}
        config = hosted_config(env)
        report = configure_model_environment(env, config)
        self.assertEqual(report["strategy_model"], "deepseek-flash")
        self.assertEqual(report["provider_hosts"], {"deepseek": "api.deepseek.com.tsecbench.gw"})
        for member, model in config.veria_models.items():
            with self.subTest(member=member):
                child = client_environment(model, Path("/tmp") / member,
                                           provider=config.veria_providers[member], environ=env)
                self.assertEqual(child["ANTHROPIC_API_KEY"], "synthetic-deepseek-key")
                self.assertEqual(child["ANTHROPIC_MODEL"], "deepseek-flash")
                self.assertEqual(child["ANTHROPIC_BASE_URL"], env["DEEPSEEK_BASE_URL"])
                self.assertNotIn("GLM_API_KEY", child)
                self.assertNotIn("BENCHMARK_TOKEN", child)
        self.assertEqual(env["ANTHROPIC_MODEL"], "deepseek-flash")
        for name in ("ANTHROPIC_DEFAULT_OPUS_MODEL", "ANTHROPIC_DEFAULT_SONNET_MODEL",
                     "ANTHROPIC_DEFAULT_HAIKU_MODEL", "ANTHROPIC_SMALL_FAST_MODEL", "CLAUDE_CODE_SUBAGENT_MODEL"):
            self.assertEqual(env[name], "deepseek-flash")
        self.assertNotIn("synthetic-deepseek-key", json.dumps(report) + json.dumps(config.to_dict()))

    def test_no_glm_key_required_and_glm_cannot_supply_missing_deepseek_key(self):
        env = self.fixture_env()
        self.assertEqual(configure_model_environment(env)["provider"], "deepseek")
        del env["DEEPSEEK_API_KEY"]
        with self.assertRaisesRegex(ValueError, "Missing DEEPSEEK_API_KEY"):
            configure_model_environment(env | {"GLM_API_KEY": "synthetic-glm-key"})

    def test_conflicting_model_provider_overrides_are_rejected(self):
        overrides = [{"XINGHUO_PROVIDER": "glm"}] + [{name: "deepseek-v4-pro"} for name in (
            "XINGHUO_STRATEGY_MODEL", "XINGHUO_STRONG_MODEL", "XINGHUO_FAST_MODEL",
            "DEEPSEEK_STRONG_MODEL", "DEEPSEEK_FAST_MODEL")]
        for override in overrides:
            with self.subTest(override=override):
                with self.assertRaisesRegex(ValueError, "flash4 requires"):
                    hosted_config(self.fixture_env() | override)
                with self.assertRaisesRegex(ValueError, "flash4 requires"):
                    configure_model_environment(self.fixture_env() | override, hosted_config({}))
        valid = {name: "deepseek-flash" for name in ("XINGHUO_STRATEGY_MODEL", "DEEPSEEK_STRONG_MODEL",
                                                     "DEEPSEEK_FAST_MODEL")}
        self.assertEqual(profile_contract(self.fixture_env() | valid)["models"], VERIA_FLASH4_MODELS)

    def test_config_validation_protects_direct_cli_profile(self):
        self.assertEqual(Config(veria_team_profile="flash4").validate().model, "deepseek-flash")
        bad_models = dict(VERIA_FLASH4_MODELS, reviewer="glm-5.3")
        for values in ({"model": "deepseek-v4-pro"}, {"veria_provider": "glm"},
                       {"veria_models": bad_models},
                       {"veria_providers": dict(VERIA_FLASH4_PROVIDERS, reporter="glm")}):
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, "flash4 requires"):
                Config(veria_team_profile="flash4", **values).validate()

    def test_no_flag_timeout_rejects_invalid_values(self):
        for value in (-1, True, "900", float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                Config(no_flag_timeout=value).validate()

    def test_resume_inherits_exact_flash_models_and_no_flag_clock_policy_read_only(self):
        with tempfile.TemporaryDirectory() as temp:
            root, directory = Path(temp), Path(temp) / "run"
            config = hosted_config(self.fixture_env())
            store = Store(directory)
            for name, value in {"config": config.to_dict(), "started_at": 10, "deadline": 28810,
                                "run_id": "synthetic-flash-run"}.items():
                store.set_meta(name, value)
            store.close()
            database = directory / "state.sqlite3"
            before = database.read_bytes()
            env = {"XINGHUO_RUN_MODE": "resume", "XINGHUO_RUN_DIR": str(directory),
                   "DEEPSEEK_API_KEY": "rotated-synthetic-key", "GLM_API_KEY": "unused-glm"}
            with prepare_hosted_run(env, work_root=root) as run:
                self.assertEqual(run.config.veria_team_profile, "flash4")
                self.assertEqual(run.config.no_flag_timeout, 900)
                self.assertEqual(run.store.get_meta("deadline"), 28810)
            self.assertEqual(database.read_bytes(), before)
            for override in ({"XINGHUO_TEAM_PROFILE": "mixed4"}, {"XINGHUO_PROVIDER": "glm"},
                             {"XINGHUO_NO_FLAG_TIMEOUT": "1800"}, {"XINGHUO_STRATEGY_MODEL": "glm-5.3"},
                             {"DEEPSEEK_FAST_MODEL": "deepseek-v4-pro"}):
                with self.subTest(override=override), self.assertRaises(ValueError):
                    with prepare_hosted_run(env | override, work_root=root):
                        self.fail("Resume changed the saved all-Flash or switching contract")
                self.assertEqual(database.read_bytes(), before)
            with prepare_hosted_run(env | {"XINGHUO_NO_FLAG_TIMEOUT": "900"}, work_root=root) as run:
                self.assertEqual(run.config.no_flag_timeout, 900)

    def test_old_saved_config_missing_timeout_remains_review_based(self):
        with tempfile.TemporaryDirectory() as temp:
            root, directory = Path(temp), Path(temp) / "run"
            saved = hosted_config({"XINGHUO_TEAM_PROFILE": "mixed4"}).to_dict()
            saved.pop("no_flag_timeout")
            store = Store(directory)
            for name, value in {"config": saved, "started_at": 10, "deadline": 28810,
                                "run_id": "synthetic-old-run"}.items():
                store.set_meta(name, value)
            store.close()
            env = {"XINGHUO_RUN_MODE": "resume", "XINGHUO_RUN_DIR": str(directory)}
            with prepare_hosted_run(env, work_root=root) as run:
                self.assertEqual(run.config.veria_team_profile, "mixed4")
                self.assertEqual(run.config.no_flag_timeout, 0)
            with self.assertRaisesRegex(ValueError, "XINGHUO_NO_FLAG_TIMEOUT"):
                with prepare_hosted_run(env | {"XINGHUO_NO_FLAG_TIMEOUT": "900"}, work_root=root):
                    self.fail("Old saved switching policy was silently replaced")
