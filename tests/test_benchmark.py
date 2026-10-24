"""Exercise the installed official SDK via HTTPX's in-memory transport."""
import json
import unittest

from xinghuo.benchmark import BenchmarkPlatform

try:
    import httpx
    from tsec_benchmark import TSecBenchmarkAsync
except ImportError:
    httpx = TSecBenchmarkAsync = None


@unittest.skipUnless(TSecBenchmarkAsync is not None, "install the benchmark optional dependency")
class BenchmarkAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.responses = {}

        def handler(request):
            self.requests.append(request)
            key = (request.method, request.url.path)
            result = self.responses[key]
            if isinstance(result, tuple):
                status, body = result
            else:
                status, body = 200, result
            return httpx.Response(status, json=body)

        self.client = TSecBenchmarkAsync(
            base_url="https://benchmark.invalid", token="synthetic-test-token",
            transport=httpx.MockTransport(handler), auto_check_vpn=False)
        await self.client.__aenter__()
        self.platform = BenchmarkPlatform(self.client)

    async def asyncTearDown(self):
        await self.client.__aexit__(None, None, None)

    async def test_full_challenge_fields_multiple_entries_and_nullable_description(self):
        self.responses[("GET", "/openapi/v1/challenges")] = [{
            "unique_code": "A", "description": None, "difficulty": "hard", "level": 3,
            "total_score": 150, "flag_count": 3, "correct_flag_count": 1,
            "is_completed": False, "container_status": "available",
            "container_addr": ["10.0.0.1:80", "10.0.0.2:8080"],
        }]
        challenge, = await self.platform.list_challenges()
        self.assertEqual(challenge.code, "A")
        self.assertEqual(challenge.description, "")
        self.assertEqual(challenge.level, 3)
        self.assertEqual(challenge.difficulty, "hard")
        self.assertEqual(challenge.total_score, 150)
        self.assertEqual(challenge.flag_count, 3)
        self.assertEqual(challenge.correct_flag_count, 1)
        self.assertFalse(challenge.completed)
        self.assertEqual(challenge.container_status, "available")
        self.assertEqual(challenge.container_addr, ("10.0.0.1:80", "10.0.0.2:8080"))
        self.assertEqual(self.requests[0].headers["BENCHMARK_TOKEN"], "synthetic-test-token")
        self.assertEqual(len(self.requests), 1)  # No VPN check or external request.

    async def test_submit_keeps_award_progress_and_matched_index(self):
        self.responses[("POST", "/openapi/v1/challenges/submit")] = {
            "correct": True, "awarded": 25, "cumulative_score": 75,
            "correct_flag_count": 2, "total_flag_count": 3, "matched_flag_index": 1,
        }
        receipt = await self.platform.submit("A", "flag{synthetic}")
        self.assertTrue(receipt.correct)
        self.assertFalse(receipt.duplicate)
        self.assertEqual(receipt.awarded, 25)
        self.assertEqual(receipt.cumulative_score, 75)
        self.assertEqual(receipt.correct_flag_count, 2)
        self.assertEqual(receipt.total_flag_count, 3)
        self.assertEqual(receipt.matched_flag_index, 1)
        self.assertEqual(json.loads(self.requests[0].content),
                         {"unique_code": "A", "flag": "flag{synthetic}"})

    async def test_duplicate_is_correct_without_fabricated_new_award_or_counts(self):
        self.responses[("POST", "/openapi/v1/challenges/submit")] = (
            409, {"code": "duplicate", "message": "already submitted", "detail": {}})
        receipt = await self.platform.submit("A", "flag{same}")
        self.assertTrue(receipt.correct and receipt.duplicate)
        self.assertEqual(receipt.awarded, 0)
        self.assertIsNone(receipt.cumulative_score)
        self.assertIsNone(receipt.correct_flag_count)
        self.assertIsNone(receipt.total_flag_count)
        self.assertIsNone(receipt.matched_flag_index)

    async def test_start_addresses_and_close_must_be_confirmed(self):
        self.responses[("POST", "/openapi/v1/challenges/start")] = {
            "unique_code": "A", "container_addr": ["10.0.0.1:80", "10.0.0.2:8080"]}
        self.assertEqual(await self.platform.start("A"), ("10.0.0.1:80", "10.0.0.2:8080"))
        self.assertEqual(self.requests[-1].url.params["unique_code"], "A")
        self.responses[("POST", "/openapi/v1/challenges/close")] = {
            "unique_code": "A", "closed": False}
        with self.assertRaises(RuntimeError):
            await self.platform.close("A")
        self.responses[("POST", "/openapi/v1/challenges/close")] = {
            "unique_code": "A", "closed": True}
        await self.platform.close("A")


if __name__ == "__main__":
    unittest.main()
