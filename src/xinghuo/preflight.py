"""Read-only official SDK connectivity check: VPN probe and list, never start/submit/close."""
import os
from urllib.parse import urlsplit


async def check_api(*, environ=None, client_factory=None):
    env = os.environ if environ is None else environ
    missing = [key for key in ("BENCHMARK_BASE_URL", "BENCHMARK_TOKEN") if not env.get(key)]
    if missing:
        return {"ok": False, "stage": "configuration", "missing": missing, "platform_mutations": 0}
    base = urlsplit(env["BENCHMARK_BASE_URL"])
    if base.scheme not in {"http", "https"} or not base.hostname or base.username or base.password or base.query or base.fragment:
        return {"ok": False, "stage": "configuration", "error": "BENCHMARK_BASE_URL must be an HTTP(S) base URL without credentials/query/fragment", "platform_mutations": 0}
    if client_factory is None:
        from tsec_benchmark import TSecBenchmarkAsync
        client_factory = TSecBenchmarkAsync
    client = client_factory(base_url=env["BENCHMARK_BASE_URL"], token=env["BENCHMARK_TOKEN"], timeout=15)
    try:
        vpn = await client.check_vpn()
        rows = await client.list_challenges()
        return {"ok": True, "stage": "listed", "api_host": base.hostname, "vpn_ok": vpn.ok,
                "challenge_count": len(rows), "completed_count": sum(item.is_completed for item in rows),
                "challenges": [{"code": item.unique_code, "flag_count": item.flag_count,
                    "correct_flag_count": item.correct_flag_count, "completed": item.is_completed,
                    "container_status": item.container_status} for item in rows],
                "platform_mutations": 0, "model_calls": 0}
    except Exception as error:
        # SDK detail can contain response bodies and URLs: expose only typed failure.
        return {"ok": False, "stage": "connectivity", "error_type": type(error).__name__,
                "platform_code": getattr(error, "code", None), "platform_mutations": 0, "model_calls": 0}
    finally:
        await client.aclose()
