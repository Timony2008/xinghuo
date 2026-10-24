"""Platform contracts. Auditing is independent of instance lifetime."""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from typing import Awaitable, Callable, Protocol


def completion_confirmed(platform: dict) -> bool:
    """A platform completion bit is usable only with consistent known counts.

    A zero total means the platform did not publish a count; its explicit
    completion remains authoritative. A count by itself is never completion.
    """
    if not (platform.get("completed") or platform.get("is_completed")):
        return False
    total = platform.get("flag_count", platform.get("total_flag_count", 0))
    found = platform.get("correct_flag_count", 0)
    if (not isinstance(total, int) or isinstance(total, bool) or total < 0
            or not isinstance(found, int) or isinstance(found, bool) or found < 0):
        return False
    return total == 0 or found >= total


@dataclass(frozen=True)
class Challenge:
    code: str
    description: str = ""
    completed: bool = False
    difficulty: str = ""
    level: int = 0
    total_score: int = 0
    flag_count: int = 0
    correct_flag_count: int = 0
    container_status: str = "stopped"
    container_addr: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SubmissionReceipt:
    correct: bool = False
    duplicate: bool = False
    awarded: float = 0
    cumulative_score: float | None = None
    correct_flag_count: int | None = None
    total_flag_count: int | None = None
    matched_flag_index: int | None = None
    unknown: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


class Platform(Protocol):
    async def list_challenges(self) -> list[Challenge]: ...
    async def start(self, code: str) -> tuple[str, ...]: ...
    async def submit(self, code: str, flag: str) -> SubmissionReceipt: ...
    async def close(self, code: str) -> None: ...


class TeamPool:
    """Independent jobs: failure is a result, not sibling cancellation."""
    def __init__(self, limit: int = 2):
        if limit < 1:
            raise ValueError("worker limit must be positive")
        self.limit = limit
        self.failures: list[BaseException] = []

    async def run(self, jobs: list[Callable[[], Awaitable[None]]]) -> list:
        slots = asyncio.Semaphore(self.limit)

        async def worker(job):
            async with slots:
                return await job()

        results = await asyncio.gather(*(worker(job) for job in jobs), return_exceptions=True)
        self.failures = [r for r in results if isinstance(r, BaseException)]
        return results
