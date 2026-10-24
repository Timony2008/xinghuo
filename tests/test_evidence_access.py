from __future__ import annotations

import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from xinghuo.evidence_access import (
    EvidenceChangedError, is_private_evidence_path, open_evidence_file,
    read_evidence, search_evidence,
)


class EvidenceAccessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def write(self, text, name="evidence.txt"):
        path = self.root / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_unicode_pages_reassemble_the_complete_file_across_byte_chunks(self):
        text = "甲🙂e\u0301\n" * 15_000 + "最后的突破"
        path = self.write(text)
        offset, digest, pieces = 0, None, []
        while True:
            page = read_evidence(self.root, path.name, offset=offset,
                                 max_chars=7001, expected_sha256=digest)
            self.assertEqual(page["total_chars"], len(text))
            self.assertEqual(page["total_bytes"], len(text.encode()))
            self.assertEqual(page["sha256"], hashlib.sha256(text.encode()).hexdigest())
            self.assertLessEqual(page["returned_chars"], 7001)
            pieces.append(page["text"])
            digest = page["sha256"]
            if page["eof"]:
                self.assertIsNone(page["next_offset"])
                self.assertFalse(page["complete_file"])
                break
            offset = page["next_offset"]
        self.assertEqual("".join(pieces), text)

    def test_empty_complete_file_and_exact_eof_page(self):
        path = self.write("")
        empty = read_evidence(self.root, path.name)
        self.assertTrue(empty["eof"])
        self.assertTrue(empty["complete_file"])
        self.assertFalse(empty["truncated"])
        self.assertEqual(empty["total_chars"], 0)
        self.write("abc")
        full = read_evidence(self.root, path.name)
        end = read_evidence(self.root, path.name, offset=3, expected_sha256=full["sha256"])
        self.assertEqual(end["text"], "")
        self.assertTrue(end["eof"])
        self.assertFalse(end["complete_file"])
        with self.assertRaises(ValueError):
            read_evidence(self.root, path.name, offset=4, expected_sha256=full["sha256"])

    def test_continuation_requires_exact_hash_and_rejects_file_changes(self):
        self.write("abcdef")
        first = read_evidence(self.root, "evidence.txt", max_chars=3)
        with self.assertRaises(ValueError):
            read_evidence(self.root, "evidence.txt", offset=3)
        self.write("abcXYZ")
        with self.assertRaises(EvidenceChangedError):
            read_evidence(self.root, "evidence.txt", offset=3,
                          expected_sha256=first["sha256"])
        with self.assertRaises(EvidenceChangedError):
            search_evidence(self.root, "evidence.txt", query="XYZ", offset=3,
                            expected_sha256=first["sha256"])

    def test_all_file_bytes_are_validated_even_beyond_returned_page(self):
        path = self.root / "evidence.txt"
        for tail in (b"\x00", b"\xff"):
            path.write_bytes(b"visible text" + b"x" * 80_000 + tail)
            for tool, kwargs in ((read_evidence, {}), (search_evidence, {"query": "visible"})):
                with self.subTest(tail=tail, tool=tool.__name__), self.assertRaises((ValueError, UnicodeError)):
                    tool(self.root, path.name, max_chars=20, **kwargs)

    def test_invalid_numeric_and_digest_parameters_are_rejected(self):
        self.write("abc")
        for tool, extra in ((read_evidence, {}), (search_evidence, {"query": "a"})):
            for name in ("offset", "max_chars"):
                for value in (True, False, -1, 1.5, "2"):
                    with self.subTest(tool=tool.__name__, name=name, value=value):
                        with self.assertRaises(ValueError):
                            tool(self.root, "evidence.txt", **extra, **{name: value})
            with self.assertRaises(ValueError):
                tool(self.root, "evidence.txt", max_chars=0, **extra)
            for digest in (True, "short", "g" * 64):
                with self.assertRaises(ValueError):
                    tool(self.root, "evidence.txt", expected_sha256=digest, **extra)

    def test_page_output_budget_is_capped_without_modifying_source(self):
        text = "x" * 30_000
        path = self.write(text)
        result = read_evidence(self.root, path.name, max_chars=999_999)
        self.assertEqual(result["returned_chars"], 24_000)
        self.assertFalse(result["complete_file"])
        self.assertEqual(path.read_text(), text)

    def test_ansi_terminal_evidence_is_readable_and_searchable(self):
        text = "\x1b[32mOK\x1b[0m\r\nprogress: 1\b2\n\x1b]0;工具结果\x07线索"
        self.write(text)
        page = read_evidence(self.root, "evidence.txt", max_chars=8)
        rest = read_evidence(self.root, "evidence.txt", offset=page["next_offset"],
                             expected_sha256=page["sha256"])
        self.assertEqual(page["text"] + rest["text"], text)
        result = search_evidence(self.root, "evidence.txt", query="线索")
        self.assertEqual(result["matches"][0]["offset"], text.index("线索"))

    def test_literal_search_finds_late_clue_and_accommodates_small_budget(self):
        text = "开始\n" + "无" * 30_000 + "flag[.*]关键" + "结束" * 200
        self.write(text)
        result = search_evidence(self.root, "evidence.txt", query="flag[.*]", max_chars=9)
        self.assertEqual(len(result["matches"]), 1)
        match = result["matches"][0]
        self.assertEqual(match["offset"], text.index("flag[.*]"))
        self.assertIn("flag[.*]", match["text"])
        self.assertLessEqual(result["returned_chars"], 9)
        self.assertEqual(match["text"], text[match["snippet_offset"]:match["snippet_end_offset"]])
        self.assertFalse(result["complete_file"])
        self.assertTrue(result["eof"])
        self.assertFalse(search_evidence(self.root, "evidence.txt", query="FLAG[.*]")["matches"])

    def test_search_paginates_overlapping_matches_without_loss_or_duplicates(self):
        text = "aaaa" + "x" * 40 + "aaaa"
        self.write(text)
        positions, offset, digest = [], 0, None
        while True:
            page = search_evidence(self.root, "evidence.txt", query="aaa", offset=offset,
                                   max_chars=6, expected_sha256=digest)
            self.assertLessEqual(sum(len(item["text"]) for item in page["matches"]), 6)
            positions.extend(item["offset"] for item in page["matches"])
            digest = page["sha256"]
            if not page["has_more_matches"]:
                self.assertTrue(page["eof"])
                self.assertIsNone(page["next_offset"])
                break
            self.assertGreater(page["next_offset"], offset)
            offset = page["next_offset"]
        self.assertEqual(positions, [0, 1, 44, 45])

    def test_search_matches_and_context_cross_utf8_read_boundaries(self):
        text = "左右🙂" * 500 + "边界needle🙂目标" + "尾" * 1000
        self.write(text)
        with patch("xinghuo.evidence_access._CHUNK_BYTES", 11):
            result = search_evidence(self.root, "evidence.txt", query="needle🙂目标")
        match = result["matches"][0]
        self.assertEqual(match["offset"], text.index("needle🙂目标"))
        self.assertEqual(match["text"], text[match["snippet_offset"]:match["snippet_end_offset"]])
        self.assertEqual(len(match["text"]), 320 + len("needle🙂目标"))

    def test_search_match_count_is_bounded_and_cursor_reaches_remaining_matches(self):
        self.write("x" * 130)
        first = search_evidence(self.root, "evidence.txt", query="x")
        self.assertLessEqual(len(first["matches"]), 100)
        self.assertTrue(first["has_more_matches"])
        second = search_evidence(self.root, "evidence.txt", query="x",
                                 offset=first["next_offset"], expected_sha256=first["sha256"])
        self.assertEqual([m["offset"] for m in first["matches"] + second["matches"]], list(range(130)))

    def test_empty_and_invalid_queries_and_no_hits(self):
        self.write("a\nb")
        for query in ("", None, True, "x" * 513):
            with self.assertRaises(ValueError):
                search_evidence(self.root, "evidence.txt", query=query)
        with self.assertRaises(ValueError):
            search_evidence(self.root, "evidence.txt", query="long", max_chars=2)
        result = search_evidence(self.root, "evidence.txt", query="absent")
        self.assertEqual(result["matches"], [])
        self.assertTrue(result["eof"])
        self.assertFalse(result["complete_file"])
        self.assertEqual(result["returned_chars"], 0)

    def test_traversal_symlink_directory_and_fifo_are_denied(self):
        file = self.write("safe")
        (self.root / "link.txt").symlink_to(file)
        (self.root / "directory").mkdir()
        (self.root / "parent-link").symlink_to(self.root / "directory", target_is_directory=True)
        os.mkfifo(self.root / "pipe")
        for reference in (str(file), "../escape.txt", "link.txt", "directory", "pipe",
                          "parent-link/child.txt"):
            with self.subTest(reference=reference), self.assertRaises((ValueError, OSError)):
                read_evidence(self.root, reference)

    def test_parent_symlink_swap_is_denied_by_descriptor_relative_open(self):
        (self.root / "nested").mkdir()
        (self.root / "nested" / "evidence.txt").write_text("safe")
        with tempfile.TemporaryDirectory() as other:
            outside = Path(other)
            (outside / "evidence.txt").write_text("private")
            actual_open = os.open

            def swap(path, flags, *args, **kwargs):
                if path == "nested" and "dir_fd" in kwargs:
                    (self.root / "nested").rename(self.root / "old")
                    (self.root / "nested").symlink_to(outside, target_is_directory=True)
                return actual_open(path, flags, *args, **kwargs)

            with patch("xinghuo.evidence_access.os.open", side_effect=swap):
                with self.assertRaises(OSError):
                    read_evidence(self.root, "nested/evidence.txt")

    def test_concurrent_modification_is_rejected_before_returning_text(self):
        file = self.write("original" * 100)
        actual_sha256 = hashlib.sha256

        class RacingDigest:
            def __init__(self):
                self.digest = actual_sha256()
                self.changed = False

            def update(self, data):
                self.digest.update(data)
                if not self.changed:
                    self.changed = True
                    file.write_text("changed")

            def hexdigest(self):
                return self.digest.hexdigest()

        with patch("xinghuo.evidence_access.hashlib.sha256", RacingDigest):
            with self.assertRaises(EvidenceChangedError):
                read_evidence(self.root, file.name)

    def test_binary_open_primitive_and_shared_private_file_exclusions(self):
        (self.root / "script.bin").write_bytes(b"\x00\xff")
        with open_evidence_file(self.root, "script.bin") as handle:
            self.assertEqual(handle.read(), b"\x00\xff")
        for path in (".ENV", "nested/.env.local", "STATE.SQLITE", "ROOM.DB", "sdk-session.json", "room.db-wal"):
            self.assertTrue(is_private_evidence_path(path), path)
        self.assertFalse(is_private_evidence_path("evidence/exploit.py"))


if __name__ == "__main__":
    unittest.main()
