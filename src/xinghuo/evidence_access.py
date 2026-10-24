"""Read-only, versioned access to complete UTF-8 evidence files.

Offsets count Unicode characters, not UTF-8 bytes. The caller owns authorization
(which role may read which relative path); this module prevents path redirection,
validates the whole file, and never treats a page or search snippet as a full file.
"""
from __future__ import annotations

import codecs
import hashlib
import os
from contextlib import ExitStack, contextmanager
from pathlib import Path
import re
import stat


MAX_PAGE_CHARS = 24_000
MAX_QUERY_CHARS = 512
MAX_SEARCH_MATCHES = 100
SEARCH_CONTEXT_CHARS = 160
_CHUNK_BYTES = 64 * 1024


def is_private_evidence_path(path: str | Path) -> bool:
    """Shared minimum exclusion; callers may impose stricter role allowlists."""
    path = Path(path)
    name = path.name.lower()
    return (any(part.startswith(".") for part in path.parts if part not in {".", ".."})
            or "session" in name or path.suffix.lower() in {".sqlite", ".sqlite3", ".db", ".env"}
            or name.endswith(("-wal", "-shm")))


class EvidenceChangedError(ValueError):
    """Evidence changed during a read or since the previous page."""


def _integer(name: str, value: int, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _parameters(offset: int, max_chars: int, expected_sha256: str | None):
    _integer("offset", offset)
    limit = min(_integer("max_chars", max_chars, minimum=1), MAX_PAGE_CHARS)
    if expected_sha256 is not None and (
        not isinstance(expected_sha256, str)
        or re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256) is None
    ):
        raise ValueError("expected_sha256 must be a SHA-256 hex digest")
    if offset and expected_sha256 is None:
        raise ValueError("offset > 0 requires expected_sha256 from the first page")
    return limit, expected_sha256.lower() if expected_sha256 is not None else None


def _identity(handle) -> tuple:
    info = os.fstat(handle.fileno())
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


@contextmanager
def open_evidence_file(root: str | Path, relative_path: str | Path):
    """Open a regular binary file beneath a trusted root, refusing all symlinks.

    This primitive enforces filesystem containment, not role authorization or
    private-file policy. Callers must apply their allowlist and exclusions first.
    """
    path = Path(relative_path)
    if path.is_absolute() or not path.parts or any(p == ".." for p in path.parts):
        raise ValueError("evidence path must be a relative file beneath its run root")
    # Every open is relative to an already-open directory. In particular, do not
    # resolve symlinks first and then open the resulting path by its full name.
    with ExitStack() as stack:
        parent = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, parent)
        for component in path.parts[:-1]:
            parent = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                             dir_fd=parent)
            stack.callback(os.close, parent)
        fd = os.open(path.parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                     dir_fd=parent)
        handle = stack.enter_context(os.fdopen(fd, "rb"))
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError("evidence must be a regular file")
        yield handle


def _scan(handle, accept, expected_sha256: str | None) -> dict:
    before = _identity(handle)
    decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
    digest = hashlib.sha256()
    total_chars = 0
    total_bytes = 0
    while True:
        data = handle.read(_CHUNK_BYTES)
        digest.update(data)
        total_bytes += len(data)
        final = not data
        text = decoder.decode(data, final=final)
        # Preserve valid terminal logs, including ANSI/OSC controls. Match the
        # existing evidence boundary: reject NUL and malformed UTF-8 across the
        # whole file, including portions outside this returned page/snippet.
        if "\x00" in text:
            raise ValueError("evidence contains binary NUL content")
        accept(text, total_chars, final)
        total_chars += len(text)
        if final:
            break
    actual_sha256 = digest.hexdigest()
    if before != _identity(handle):
        raise EvidenceChangedError("evidence changed during reading; restart from offset 0")
    if expected_sha256 is not None and actual_sha256 != expected_sha256:
        raise EvidenceChangedError("evidence SHA-256 changed; restart from offset 0")
    return {"sha256": actual_sha256, "total_chars": total_chars, "total_bytes": total_bytes}


def read_evidence(root: str | Path, relative_path: str | Path, *, offset: int = 0,
                  max_chars: int = MAX_PAGE_CHARS,
                  expected_sha256: str | None = None) -> dict:
    """Return one bounded character page; continuation requires its file hash."""
    limit, expected = _parameters(offset, max_chars, expected_sha256)
    pieces: list[str] = []

    def accept(text, start, final):
        left = max(0, offset - start)
        right = min(len(text), offset + limit - start)
        if left < right:
            pieces.append(text[left:right])

    with open_evidence_file(root, relative_path) as handle:
        metadata = _scan(handle, accept, expected)
    if offset > metadata["total_chars"]:
        raise ValueError("offset exceeds the evidence character length")
    text = "".join(pieces)
    end = offset + len(text)
    eof = end == metadata["total_chars"]
    complete = offset == 0 and eof
    return {**metadata, "offset": offset, "text": text,
            "returned_chars": len(text), "next_offset": None if eof else end,
            "eof": eof, "complete_file": complete, "truncated": not complete,
            "note": ("完整返回引用文本。" if complete else
                     "分页返回；已截断为本页片段。请用 next_offset 和 sha256 继续，不能声称已核验全文。")}


def search_evidence(root: str | Path, relative_path: str | Path, *, query: str,
                    offset: int = 0, max_chars: int = MAX_PAGE_CHARS,
                    expected_sha256: str | None = None) -> dict:
    """Search literal case-sensitive text, with bounded snippets and pagination.

    Search offsets select the minimum matching character position. ``eof`` means
    no more matching positions remain, never that the caller has read the file.
    Full-file validation and hashing continue even after the result budget fills.
    ``max_chars`` limits returned text, not scanned bytes or hashing time; async
    callers should run this synchronous I/O in a worker thread.
    """
    limit, expected = _parameters(offset, max_chars, expected_sha256)
    if not isinstance(query, str) or not query or len(query) > MAX_QUERY_CHARS:
        raise ValueError(f"query must contain 1..{MAX_QUERY_CHARS} characters")
    if len(query) > limit:
        raise ValueError("max_chars must accommodate the complete query")
    matches = []
    buffer = ""
    base = 0
    search_from = offset
    returned = 0
    next_offset = None

    def accept(text, start, final):
        nonlocal buffer, base, search_from, returned, next_offset
        if next_offset is not None:
            return
        buffer += text
        while True:
            index = buffer.find(query, max(0, search_from - base))
            if index < 0:
                keep_from = max(0, len(buffer) - len(query) + 1 - SEARCH_CONTEXT_CHARS)
                base += keep_from
                buffer = buffer[keep_from:]
                return
            absolute = base + index
            if not final and len(buffer) < index + len(query) + SEARCH_CONTEXT_CHARS:
                keep_from = max(0, index - SEARCH_CONTEXT_CHARS)
                base += keep_from
                buffer = buffer[keep_from:]
                return
            remaining = limit - returned
            if len(matches) >= MAX_SEARCH_MATCHES or remaining < len(query):
                next_offset = absolute
                buffer = ""
                return
            extra = remaining - len(query)
            left = min(SEARCH_CONTEXT_CHARS, index, extra // 2)
            right = min(SEARCH_CONTEXT_CHARS, len(buffer) - index - len(query), extra - left)
            left = min(SEARCH_CONTEXT_CHARS, index, extra - right)
            snippet = buffer[index - left:index + len(query) + right]
            matches.append({"offset": absolute, "end_offset": absolute + len(query),
                            "snippet_offset": absolute - left,
                            "snippet_end_offset": absolute + len(query) + right,
                            "text": snippet})
            returned += len(snippet)
            # Advance one character to preserve overlapping literal matches.
            search_from = absolute + 1
            keep_from = max(0, search_from - base - SEARCH_CONTEXT_CHARS)
            base += keep_from
            buffer = buffer[keep_from:]

    with open_evidence_file(root, relative_path) as handle:
        metadata = _scan(handle, accept, expected)
    if offset > metadata["total_chars"]:
        raise ValueError("offset exceeds the evidence character length")
    return {**metadata, "offset": offset, "query": query, "matches": matches,
            "returned_chars": returned, "next_offset": next_offset,
            "scanned_through": metadata["total_chars"],
            "has_more_matches": next_offset is not None, "eof": next_offset is None,
            "complete_file": False, "literal": True, "case_sensitive": True,
            "note": "仅返回字面搜索命中片段；eof 表示没有更多匹配，不能据此声称已读取全文。"}
