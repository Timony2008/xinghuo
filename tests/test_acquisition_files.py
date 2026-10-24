"""Ordinary local-file safety checks for asynchronous acquisition snapshots."""
from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
import stat
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

from xinghuo.veria_runtime import VeriaRuntime


class AcquisitionFileTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.runtime = VeriaRuntime(self.root / "run", "ordinary", "attempt")
        self.addCleanup(self.runtime.room.close)
        self.source = self.runtime.directory / "shared" / "fixture.txt"
        self.source.parent.mkdir()
        self.source.write_text("ordinary offline acquisition fixture\n", encoding="utf-8")
        self.destination = self.runtime.directory / "shared" / "acquisitions" / "blobs"
        self.building = self.runtime.directory / "acquisitions" / ".building"

    async def freeze(self):
        return await self.runtime._freeze_acquisition_file(self.source, self.destination)

    def assert_no_staging_files(self):
        self.assertFalse(list(self.building.glob("copy-*.tmp")))
        self.assertFalse(list(self.destination.glob("copy-*.tmp")))

    async def test_private_temp_link_swap_never_changes_an_outside_mode(self):
        outside = self.root / "outside.txt"
        outside.write_text("ordinary file outside the attempt", encoding="utf-8")
        outside.chmod(0o600)
        actual_fchmod = os.fchmod
        swapped = False

        def swap_before_fd_chmod(fd, mode):
            nonlocal swapped
            if mode == 0o444 and not swapped:
                staged = list(self.building.glob("copy-*.tmp"))
                self.assertEqual(len(staged), 1)
                self.assertFalse(list(self.destination.glob("copy-*.tmp")))
                staged[0].unlink()
                staged[0].symlink_to(outside)
                swapped = True
            return actual_fchmod(fd, mode)

        with patch("xinghuo.veria_runtime.os.fchmod", side_effect=swap_before_fd_chmod):
            with self.assertRaisesRegex(ValueError, "staging file was exchanged"):
                await self.freeze()
        self.assertTrue(swapped)
        self.assertEqual(stat.S_IMODE(outside.stat().st_mode), 0o600)
        self.assertFalse(list(self.destination.iterdir()))
        self.assert_no_staging_files()

    async def test_published_link_swap_is_rejected_and_cleaned_without_following_it(self):
        outside = self.root / "outside.txt"
        outside.write_text("ordinary outside content", encoding="utf-8")
        outside.chmod(0o600)
        actual_replace = os.replace
        swapped = False

        def exchange_after_publish(source, destination, *args, **kwargs):
            nonlocal swapped
            result = actual_replace(source, destination, *args, **kwargs)
            if str(source).startswith("copy-") and "dst_dir_fd" in kwargs:
                os.unlink(destination, dir_fd=kwargs["dst_dir_fd"])
                os.symlink(outside, destination, dir_fd=kwargs["dst_dir_fd"])
                swapped = True
            return result

        with patch("xinghuo.veria_runtime.os.replace", side_effect=exchange_after_publish):
            with self.assertRaises(OSError):
                await self.freeze()
        self.assertTrue(swapped)
        self.assertEqual(stat.S_IMODE(outside.stat().st_mode), 0o600)
        self.assertFalse(list(self.destination.iterdir()))
        self.assert_no_staging_files()

    async def test_large_files_and_existing_blobs_are_verified_with_bounded_reads(self):
        block = b"ordinary streaming fixture\n" * 2048
        digest = hashlib.sha256()
        with self.source.open("wb") as handle:
            for _ in range(64):
                handle.write(block)
                digest.update(block)
        self.assertGreater(self.source.stat().st_size, 3 * 1024 * 1024)
        actual_fdopen = os.fdopen
        read_sizes = []

        class BoundedHandle:
            def __init__(self, handle):
                self.handle = handle

            def __enter__(self):
                self.handle.__enter__()
                return self

            def __exit__(self, *args):
                return self.handle.__exit__(*args)

            def __getattr__(self, name):
                return getattr(self.handle, name)

            def read(self, size=-1):
                if not 0 < size <= 64 * 1024:
                    raise AssertionError("acquisition verification attempted an unbounded read")
                read_sizes.append(size)
                return self.handle.read(size)

        def bounded_fdopen(*args, **kwargs):
            return BoundedHandle(actual_fdopen(*args, **kwargs))

        tracemalloc.start()
        try:
            with patch("xinghuo.veria_runtime.os.fdopen", side_effect=bounded_fdopen), \
                    patch.object(Path, "read_bytes", side_effect=AssertionError("no eager artifact reads")):
                first = await self.freeze()
                second = await self.freeze()
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(first["sha256"], digest.hexdigest())
        self.assertEqual(first, second)
        self.assertLess(peak, 2 * 1024 * 1024, "snapshot allocation must not scale with whole-file size")
        self.assertTrue(read_sizes)
        self.assertEqual(len(list(self.destination.iterdir())), 1)
        self.assert_no_staging_files()

    async def test_changed_or_symlinked_existing_blob_is_refused(self):
        saved = await self.freeze()
        frozen = Path(saved["frozen_path"])
        frozen.chmod(0o600)
        frozen.write_text("changed cached content", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "modified or exchanged"):
            await self.freeze()
        frozen.unlink()
        frozen.symlink_to(self.source)
        with self.assertRaises(OSError):
            await self.freeze()
        self.assert_no_staging_files()

    async def test_same_content_inode_exchange_of_an_existing_blob_is_refused(self):
        saved = await self.freeze()
        frozen = Path(saved["frozen_path"])
        replacement = self.destination / "replacement.txt"
        replacement.write_text(self.source.read_text(), encoding="utf-8")
        actual_open = os.open
        swapped = False

        def exchange_after_open(path, flags, *args, **kwargs):
            nonlocal swapped
            fd = actual_open(path, flags, *args, **kwargs)
            if path == frozen.name and "dir_fd" in kwargs and not swapped:
                os.replace(replacement, frozen)
                swapped = True
            return fd

        with patch("xinghuo.veria_runtime.os.open", side_effect=exchange_after_open):
            with self.assertRaisesRegex(ValueError, "modified or exchanged"):
                await self.freeze()
        self.assertTrue(swapped)
        self.assert_no_staging_files()

    async def test_cancellation_cleans_private_staging_without_publishing_partial_material(self):
        with self.source.open("wb") as handle:
            for _ in range(64):
                handle.write(b"ordinary fixture" * 4096)
        task = asyncio.create_task(self.freeze())
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.assertTrue(list(self.building.glob("copy-*.tmp")))
        self.assertFalse(list(self.destination.glob("copy-*.tmp")))
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
        self.assert_no_staging_files()
        self.assertFalse(list(self.destination.iterdir()))

    async def test_destination_symlinks_and_original_source_changes_are_rejected(self):
        outside = self.root / "outside"
        outside.mkdir()
        self.destination.parent.mkdir(parents=True)
        self.destination.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(OSError):
            await self.freeze()
        self.assertFalse(list(outside.iterdir()))
        self.destination.unlink()
        actual_sleep = asyncio.sleep
        changed = False

        async def change_after_chunk(delay):
            nonlocal changed
            if not changed:
                self.source.write_text("source changed while copying", encoding="utf-8")
                changed = True
            await actual_sleep(delay)

        with patch("xinghuo.veria_runtime.asyncio.sleep", side_effect=change_after_chunk):
            with self.assertRaisesRegex(ValueError, "source changed"):
                await self.freeze()
        self.assert_no_staging_files()
        self.assertFalse(list(self.destination.iterdir()))


if __name__ == "__main__":
    unittest.main()
