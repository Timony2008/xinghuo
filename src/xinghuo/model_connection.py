"""Member-scoped Anthropic-compatible routing; credentials are never persisted."""
from urllib.parse import urlsplit

from .config import (VERIA_LEGACY_SOLVERS, VERIA_MIXED4_SOLVERS,
                     VERIA_MIXED4_PROVIDERS, VERIA_FLASH4_MODELS, VERIA_FLASH4_PROVIDERS)


def provider_name(env):
    provider = env.get('XINGHUO_PROVIDER') or ('glm' if env.get('GLM_API_KEY') and not env.get('DEEPSEEK_API_KEY') else 'deepseek')
    if provider not in {'glm', 'deepseek'}:
        raise ValueError('XINGHUO_PROVIDER must be glm/deepseek')
    return provider


def profile_contract(env, *, default_profile='legacy6'):
    profile = env.get('XINGHUO_TEAM_PROFILE', default_profile)
    if profile not in {'legacy6', 'mixed4', 'flash4'}:
        raise ValueError('XINGHUO_TEAM_PROFILE must be legacy6/mixed4/flash4')
    if profile == 'flash4':
        if env.get('XINGHUO_PROVIDER') not in {None, '', 'deepseek'}:
            raise ValueError('flash4 requires XINGHUO_PROVIDER=deepseek')
        for name in ('XINGHUO_STRATEGY_MODEL', 'XINGHUO_STRONG_MODEL', 'XINGHUO_FAST_MODEL',
                     'DEEPSEEK_STRONG_MODEL', 'DEEPSEEK_FAST_MODEL'):
            if name in env and env[name] != 'deepseek-flash':
                raise ValueError(f'flash4 requires {name}=deepseek-flash')
        return {'team_profile': profile, 'solver_ids': list(VERIA_MIXED4_SOLVERS),
                'models': dict(VERIA_FLASH4_MODELS), 'providers': dict(VERIA_FLASH4_PROVIDERS)}
    if profile == 'mixed4':
        strong = env.get('DEEPSEEK_STRONG_MODEL', env.get('XINGHUO_STRONG_MODEL', 'deepseek-v4-pro'))
        fast = env.get('DEEPSEEK_FAST_MODEL', env.get('XINGHUO_FAST_MODEL', 'deepseek-flash'))
        models = {'solver-1': strong, 'solver-2': fast,
                  'solver-3': env.get('GLM_STRONG_MODEL', 'glm-5.3'),
                  'solver-4': env.get('GLM_FAST_MODEL', 'glm-5.3-flash'),
                  'coordinator': env.get('GLM_STRONG_MODEL', 'glm-5.3'),
                  'reviewer': env.get('GLM_STRONG_MODEL', 'glm-5.3'), 'reporter': fast}
        return {'team_profile': profile, 'solver_ids': list(VERIA_MIXED4_SOLVERS),
                'models': models, 'providers': dict(VERIA_MIXED4_PROVIDERS)}
    provider = provider_name(env)
    strong = env.get('XINGHUO_STRONG_MODEL', 'glm-5.3' if provider == 'glm' else 'deepseek-v4-pro')
    fast = env.get('XINGHUO_FAST_MODEL', 'glm-5.3-flash' if provider == 'glm' else 'deepseek-flash')
    models = {**{f'pro-{n}': strong for n in range(1, 4)},
              **{f'flash-{n}': fast for n in range(1, 4)},
              'coordinator': strong, 'reviewer': strong, 'reporter': fast}
    return {'team_profile': profile, 'solver_ids': list(VERIA_LEGACY_SOLVERS),
            'models': models, 'providers': {member: provider for member in models}}


def model_profile(env):
    return profile_contract(env)['models']


def connection(env, *, provider=None):
    explicit = provider is not None
    provider = provider or provider_name(env)
    if provider not in {'glm', 'deepseek'}:
        raise ValueError('Member provider must be glm/deepseek')
    prefix = 'GLM' if provider == 'glm' else 'DEEPSEEK'
    # Explicit member routing never falls back to another member's aliases.
    key = env.get(prefix + '_API_KEY') or (None if explicit else (env.get('ANTHROPIC_API_KEY') or env.get('ANTHROPIC_AUTH_TOKEN')))
    base = env.get(prefix + '_BASE_URL') or (None if explicit else env.get('ANTHROPIC_BASE_URL'))
    if not key:
        raise ValueError(f'Missing {prefix}_API_KEY runtime variable')
    if not base:
        if provider == 'deepseek' and env.get('XINGHUO_DEPLOYMENT', 'local') == 'local':
            base = 'https://api.deepseek.com/anthropic'
        else:
            raise ValueError(f'Missing {prefix}_BASE_URL; supply the purchased Anthropic endpoint')
    try:
        parsed = urlsplit(base)
        valid = (parsed.scheme in {'http', 'https'} and parsed.hostname and not parsed.username
                 and not parsed.password and not parsed.query and not parsed.fragment)
        parsed.port
    except ValueError:
        valid = False
    if not valid:
        raise ValueError(f'Invalid {prefix}_BASE_URL (credentials/query/fragment forbidden)')
    if provider == 'glm' and parsed.hostname in {'console.cloudrouter.com.cn', 'console.cloudrouter.com.cn.tsecbench.gw'} and parsed.path.rstrip('/'):
        raise ValueError('GLM_BASE_URL must be the CloudRouter domain root; the SDK appends /v1/messages')
    return provider, key, base.rstrip('/')
