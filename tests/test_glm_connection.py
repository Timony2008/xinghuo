import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from xinghuo.hosted import configure_model_environment, hosted_config, _error_detail
from xinghuo.veria.driver import client_environment

class GlmConnectionTests(unittest.TestCase):
    def test_all_roles_strategy_and_per_client_use_glm(self):
        env = {'XINGHUO_TEAM_PROFILE':'legacy6','XINGHUO_PROVIDER':'glm','GLM_API_KEY':'synthetic-glm-secret',
               'GLM_BASE_URL':'https://relay.invalid/api/anthropic'}
        c=hosted_config(env)
        self.assertEqual((c.budget,c.parallel,c.model),(28800,3,'glm-5.3'))
        self.assertEqual(len(c.veria_models),9)
        self.assertEqual([c.veria_models[f'pro-{n}'] for n in (1,2,3)],['glm-5.3']*3)
        self.assertEqual([c.veria_models[f'flash-{n}'] for n in (1,2,3)],['glm-5.3-flash']*3)
        report=configure_model_environment(env)
        self.assertEqual(report['provider'],'glm')
        with patch.dict(os.environ,{**env,'DEEPSEEK_API_KEY':'stale','DEEPSEEK_BASE_URL':'https://stale.invalid'},clear=True):
            for role,model in c.veria_models.items():
                child=client_environment(model,Path('/tmp')/role)
                self.assertEqual(child['ANTHROPIC_BASE_URL'],env['GLM_BASE_URL'])
                self.assertEqual(child['ANTHROPIC_API_KEY'],env['GLM_API_KEY'])
                self.assertEqual(child['ANTHROPIC_MODEL'],model)
                self.assertEqual(child['ANTHROPIC_SMALL_FAST_MODEL'],model)
        self.assertNotIn('synthetic-glm-secret',json.dumps(c.to_dict())+json.dumps(report))
        self.assertNotIn('synthetic-glm-secret',_error_detail(ValueError('synthetic-glm-secret'),env))

    def test_endpoint_is_explicit_and_does_not_leak_credentials(self):
        for url in ['', 'https://key:secret@relay.invalid/api', 'https://relay.invalid/api?key=secret','ftp://relay.invalid/api']:
            with self.subTest(url=url),self.assertRaises(ValueError):
                configure_model_environment({'XINGHUO_PROVIDER':'glm','GLM_API_KEY':'test','GLM_BASE_URL':url})

    def test_model_overrides_apply_to_all_nine_roles(self):
        c=hosted_config({'XINGHUO_TEAM_PROFILE':'legacy6','XINGHUO_STRONG_MODEL':'strong','XINGHUO_FAST_MODEL':'fast','XINGHUO_STRATEGY_MODEL':'strategy'})
        self.assertEqual(c.model,'strategy')
        self.assertEqual(c.veria_models['coordinator'],'strong')
        self.assertEqual(c.veria_models['reviewer'],'strong')
        self.assertEqual(c.veria_models['reporter'],'fast')
