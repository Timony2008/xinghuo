"""Deterministic fake lab. Never used by real run/resume."""
from __future__ import annotations

from .core import Challenge, SubmissionReceipt


class DemoPlatform:
    def __init__(self, store, count=5):
        self.store = store
        self.count = count
        self.peak = store.get_meta("demo_peak", 0)

    def _state(self):
        return self.store.get_meta("demo_platform", {f"demo-{i+1}": {"active": False, "flags": []}
                                                   for i in range(self.count)})

    async def list_challenges(self):
        return [Challenge(code, "离线合成题：两名worker上报证据并提交标记。", len(v["flags"]) == 2,
                          difficulty="synthetic", total_score=20, flag_count=2,
                          correct_flag_count=len(v["flags"]),
                          container_status="available" if v["active"] else "stopped",
                          container_addr=(f"{code}.invalid:80",) if v["active"] else ())
                for code, v in self._state().items()]

    async def start(self, code):
        state = self._state()
        if not state[code]["active"] and sum(v["active"] for v in state.values()) >= 3:
            raise RuntimeError("three active instances")
        state[code]["active"] = True
        self.peak = max(self.peak, sum(v["active"] for v in state.values()))
        self.store.set_meta("demo_platform", state)
        self.store.set_meta("demo_peak", self.peak)
        return (f"{code}.invalid:80",)

    async def submit(self, code, flag):
        state = self._state()
        row = state[code]
        expected = {f"flag{{{code}:1}}", f"flag{{{code}:2}}"}
        if flag not in expected:
            return SubmissionReceipt(correct=False, correct_flag_count=len(row["flags"]), total_flag_count=2)
        if flag in row["flags"]:
            return SubmissionReceipt(correct=True, duplicate=True)
        row["flags"].append(flag)
        self.store.set_meta("demo_platform", state)
        return SubmissionReceipt(True, awarded=10, cumulative_score=10 * len(row["flags"]),
                                 correct_flag_count=len(row["flags"]), total_flag_count=2,
                                 matched_flag_index=int(flag[-2]))

    async def close(self, code):
        state = self._state()
        state[code]["active"] = False
        self.store.set_meta("demo_platform", state)
