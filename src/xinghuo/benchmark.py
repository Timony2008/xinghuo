"""Thin adapter around the user-supplied official SDK contract."""
from .core import Challenge, SubmissionReceipt


class BenchmarkPlatform:
    def __init__(self, client):
        # Caller owns TSecBenchmarkAsync's async context and VPN precheck.
        self.client = client

    async def list_challenges(self):
        rows = await self.client.list_challenges()
        return [Challenge(
            code=x.unique_code, description=x.description or "", completed=x.is_completed,
            difficulty=x.difficulty, level=int(x.level), total_score=x.total_score,
            flag_count=x.flag_count, correct_flag_count=x.correct_flag_count,
            container_status=x.container_status, container_addr=tuple(x.container_addr),
        ) for x in rows]

    async def start(self, code):
        result = await self.client.start_challenge(code)
        return tuple(result.container_addr)

    async def submit(self, code, flag):
        from tsec_benchmark import DuplicateSubmit
        try:
            result = await self.client.submit_flag(code, flag)
            return SubmissionReceipt(
                correct=result.correct, awarded=result.awarded,
                cumulative_score=result.cumulative_score,
                correct_flag_count=result.correct_flag_count,
                total_flag_count=result.total_flag_count,
                matched_flag_index=result.matched_flag_index,
            )
        except DuplicateSubmit:
            return SubmissionReceipt(correct=True, duplicate=True)

    async def close(self, code):
        result = await self.client.close_challenge(code)
        if not result.closed:
            raise RuntimeError("platform did not confirm instance closure")
