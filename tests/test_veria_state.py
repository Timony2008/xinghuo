import json
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from xinghuo.room import Room
from xinghuo.veria.state import SCHEMA_VERSION, SOLVER_IDS, StaleAcquisitionJob, VeriaRoom


class VeriaStateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.room = VeriaRoom(self.directory, "challenge", "attempt-1")
        self.add_members()

    def tearDown(self):
        self.room.close()
        self.temporary.cleanup()

    def add_members(self):
        for member in (*SOLVER_IDS, "coordinator", "reporter", "reviewer"):
            self.room.register_member(member, "solver" if member in SOLVER_IDS else member,
                                      "deepseek-flash" if member.startswith("flash") or member == "reporter" else "deepseek-v4-pro",
                                      run_id="run-1")

    def reopen(self, attempt="attempt-1"):
        self.room.close()
        self.room = VeriaRoom(self.directory, "challenge", attempt)

    def publish_batch(self):
        batch = self.room.reporter_batch()
        self.assertIsNotNone(batch)
        return self.room.publish_report({"overview": "evidence reviewed"},
                                        seen_through=batch["through_event"], batch_id=batch["batch_id"])

    def test_shared_room_lock_and_member_identity_survive_restart(self):
        with self.assertRaisesRegex(RuntimeError, "another runtime"):
            Room(self.directory, "challenge", "attempt-1")
        for index in range(1, 4):
            self.room.set_session(f"pro-{index}", f"session-{index}", generation=f"generation-{index}",
                                  session_path=f"sessions/pro-{index}")
        self.reopen()
        members = [self.room.member(f"pro-{index}") for index in range(1, 4)]
        self.assertEqual(len({member["session_id"] for member in members}), 3)
        self.assertEqual(len({member["workspace"] for member in members}), 3)
        self.assertEqual(len({member["model_id"] for member in members}), 1)
        with self.assertRaisesRegex(ValueError, "cannot change"):
            self.room.register_member("pro-1", "solver", "deepseek-flash")
        self.assertEqual(self.room.db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 0)

    def test_session_replacement_records_lineage_and_original_files(self):
        self.room.set_session("pro-1", "first", generation="g-1", session_path="old/path")
        self.room.set_session("pro-1", "first", generation="g-2")
        self.room.set_session("pro-1", "second", generation="g-3", reason="session_files_missing", session_path="new/path")
        lineage = self.room.session_lineage("pro-1")
        self.assertEqual(len(lineage), 2)
        self.assertEqual(lineage[0]["session_path"], "old/path")
        self.assertEqual(lineage[1]["parent_session_id"], "first")
        self.assertEqual(lineage[1]["reason"], "session_files_missing")

    def test_reset_session_and_local_recovery_do_not_touch_other_members(self):
        self.room.set_session("pro-1", "old-session")
        broadcast = self.room.broadcast("coordinator", "next experiment")
        for agent in ("pro-1", "pro-2"):
            self.room.mark_injected(agent, [broadcast["id"]], "turn-" + agent)
            bump = self.room.queue_bump(agent, "guidance")
            self.room.inject_bumps(agent, [bump["id"]], "turn-" + agent)
        self.room.reset_session("pro-1", "session_files_missing")
        self.assertIsNone(self.room.member("pro-1")["session_id"])
        self.assertEqual(self.room.mailbox("pro-1")[0]["state"], "uncertain")
        self.assertEqual(self.room.mailbox("pro-2"), [])
        self.assertEqual(self.room.pending_bumps("pro-2"), [])
        self.assertEqual(self.room.pending_bumps("pro-1")[0]["state"], "uncertain")
        self.reopen()
        self.room.set_session("pro-1", "replacement-session")
        self.assertEqual(self.room.session_lineage("pro-1")[-1]["parent_session_id"], "old-session")
        self.assertEqual(self.room.session_lineage("pro-1")[-1]["reason"], "session_files_missing")

    def test_notifications_are_not_broadcast_and_cursors_are_independent(self):
        notice = self.room.notice("pro-1", "found a reusable primitive", message_id="notice-1")
        self.assertEqual([item["id"] for item in self.room.mailbox("coordinator")], [notice["id"]])
        self.assertEqual(self.room.mailbox("pro-2"), [])
        self.assertEqual(self.room.mailbox("reporter"), [])
        self.assertTrue(self.room.reporter_batch())
        broadcast = self.room.broadcast("coordinator", "try this primitive", reply_to=notice["id"])
        self.room.mark_injected("pro-1", [broadcast["id"]], "turn-1")
        self.assertEqual(self.room.mailbox("pro-1"), [])
        self.assertEqual(self.room.mailbox("pro-2")[0]["id"], broadcast["id"])
        self.assertEqual(self.room.mailbox("pro-1", include_injected=True)[0]["state"], "injected")
        with self.assertRaisesRegex(ValueError, "belongs to coordinator"):
            self.room.broadcast("pro-1", "bypass coordinator")

    def test_injected_messages_replay_same_id_after_crash_until_explicit_ack(self):
        message = self.room.notice("pro-1", "durable", message_id="message-1")
        self.room.mark_injected("coordinator", [message["id"]], "turn-before-crash")
        self.reopen()
        pending = self.room.mailbox("coordinator")
        self.assertEqual(pending[0]["id"], "message-1")
        self.assertEqual(pending[0]["state"], "uncertain")
        self.room.mark_injected("coordinator", ["message-1"], "new-turn")
        self.room.acknowledge_messages("coordinator", ["message-1"], "explicit reply")
        self.reopen()
        self.assertEqual(self.room.mailbox("coordinator", include_injected=True), [])
        self.assertEqual(self.room.veria_message("message-1")["deliveries"][0]["injections"], 2)
        self.assertEqual(self.room.notice("pro-1", "durable", message_id="message-1")["id"], "message-1")
        with self.assertRaisesRegex(ValueError, "different content"):
            self.room.notice("pro-1", "changed", message_id="message-1")

    def test_broadcast_preserves_all_selected_source_messages_across_restart(self):
        first = self.room.notice("pro-1", "first primitive", message_id="origin-1")
        second = self.room.notice("reviewer", "shared reset prerequisite", message_id="origin-2")
        sources = [first["id"], second["id"]]
        broadcast = self.room.broadcast("coordinator", "coordinate these observations", message_id="combined-broadcast",
            recipients=["pro-1", "reviewer"], source_message_ids=sources)
        self.assertEqual(broadcast["source_message_ids"], sources)
        self.assertEqual(self.room.mailbox("reviewer")[0]["source_message_ids"], sources)
        event = self.room.evidence_record(broadcast["event_seq"])
        self.assertEqual(event["data"]["source_message_ids"], sources)
        self.room.mark_injected("reviewer", [broadcast["id"]], "review-before-restart")
        self.reopen()
        replay = self.room.mailbox("reviewer")[0]
        self.assertEqual(replay["id"], broadcast["id"])
        self.assertEqual(replay["state"], "uncertain")
        self.assertEqual(replay["source_message_ids"], sources)
        self.assertEqual(self.room.broadcast("coordinator", "coordinate these observations", message_id="combined-broadcast",
            recipients=["reviewer", "pro-1"], source_message_ids=sources)["id"], broadcast["id"])
        with self.assertRaisesRegex(ValueError, "different content"):
            self.room.broadcast("coordinator", "coordinate these observations", message_id="combined-broadcast",
                recipients=["pro-1", "reviewer"], source_message_ids=[first["id"]])
        before = len(self.room.events())
        for invalid in [["missing-message"], "origin-1", [1], [""]]:
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "source_message_ids"):
                self.room.broadcast("coordinator", "invalid source", source_message_ids=invalid)
        self.assertEqual(len(self.room.events()), before)

    def test_message_ack_validation_rolls_back_whole_batch(self):
        first = self.room.notice("pro-1", "first")
        second = self.room.notice("pro-2", "second")
        self.room.mark_injected("coordinator", [first["id"]], "turn")
        with self.assertRaises(ValueError):
            self.room.acknowledge_messages("coordinator", [first["id"], second["id"]])
        self.assertEqual(self.room.veria_message(first["id"])["deliveries"][0]["state"], "injected")

    def test_bumps_fifo_bounded_batch_and_uncertain_replay(self):
        self.room.queue_bump("pro-1", "first", bump_id="one")
        self.room.queue_bump("pro-1", "second", bump_id="two")
        auto = self.room.queue_bump("pro-1", "automatic", source_key="sibling-watermark-7")
        again = self.room.queue_bump("pro-1", "a different rendering of that watermark", source_key="sibling-watermark-7")
        self.assertEqual(auto["id"], again["id"])
        self.assertEqual([b["id"] for b in self.room.pending_bumps("pro-1", max_chars=5)], ["one"])
        with self.assertRaisesRegex(ValueError, "FIFO"):
            self.room.inject_bumps("pro-1", ["two"], "turn")
        self.room.inject_bumps("pro-1", ["one", "two"], "crashed-turn")
        self.reopen()
        pending = self.room.pending_bumps("pro-1")
        self.assertEqual([b["id"] for b in pending], ["one", "two", auto["id"]])
        self.assertEqual(pending[0]["state"], "uncertain")
        self.room.inject_bumps("pro-1", ["one", "two"], "next-turn")
        self.room.complete_turn("pro-1", "unrelated-turn")
        self.room.complete_turn("pro-1", "next-turn")
        self.assertEqual([b["id"] for b in self.room.pending_bumps("pro-1")], [auto["id"]])

    def test_candidate_dedup_preserves_case_and_all_sources(self):
        first = self.room.save_candidate("pro-1", "  FLAG{Mixed Case} \n", evidence_refs=["a.py"])
        second = self.room.save_candidate("flash-1", "FLAG{Mixed Case}", evidence_refs=["b.py"])
        different = self.room.save_candidate("pro-2", "flag{Mixed Case}")
        self.assertEqual(first["request_id"], second["request_id"])
        self.assertNotEqual(first["request_id"], different["request_id"])
        self.assertEqual(second["evidence_refs"], ["a.py", "b.py"])
        self.assertEqual([s["sender"] for s in second["sources"]], ["pro-1", "flash-1"])
        self.assertEqual(self.room.candidate_for_flag(" FLAG{Mixed Case} ")["request_id"], first["request_id"])
        with self.assertRaises(ValueError):
            self.room.save_candidate("pro-1", "different", request_id=first["request_id"])

    def test_unknown_receipt_can_resolve_and_duplicate_replay_does_not_regress(self):
        candidate = self.room.save_candidate("pro-1", "flag{one}")
        request_id = candidate["request_id"]
        self.assertTrue(self.room.apply_receipt(request_id, {"unknown": True, "correct": False}))
        self.assertEqual(self.room.candidate(request_id)["status"], "unknown")
        self.reopen()
        self.assertEqual(self.room.pending_candidates()[0]["request_id"], request_id)
        receipt = {"correct": True, "duplicate": False, "awarded": 10, "cumulative_score": 20,
                   "correct_flag_count": 2, "total_flag_count": 4, "matched_flag_index": 3, "unknown": False,
                   "raw_additional_field": {"preserved": True}}
        self.assertTrue(self.room.apply_receipt(request_id, receipt))
        watermark = self.room.evidence_watermark
        self.assertFalse(self.room.apply_receipt(request_id, receipt))
        self.assertFalse(self.room.apply_receipt(request_id, {"unknown": True, "message": "late timeout"}))
        self.assertEqual(self.room.evidence_watermark, watermark)
        self.assertEqual(self.room.confirmed_submissions()[0]["receipt"], receipt)
        self.assertEqual(len(self.room.receipt_history(request_id)), 3)
        self.assertEqual(self.room.pending_candidates(), [])
        # Accepting one candidate never prevents recording another flag.
        self.assertEqual(self.room.save_candidate("pro-1", "flag{two}")["status"], "pending")
        with self.assertRaisesRegex(ValueError, "boolean"):
            self.room.apply_receipt(request_id, {"correct": "false"})

    def test_reporter_batches_do_not_skip_backlog_or_self_trigger(self):
        for index in range(5):
            self.room.record_evidence("pro-1", "visible_text", f"finding {index}")
        first = self.room.reporter_batch(limit=2)
        self.assertEqual(len(first["events"]), 2)
        self.room.record_evidence("pro-2", "visible_text", "concurrent sixth finding")
        self.assertEqual(self.room.reporter_batch()["batch_id"], first["batch_id"])
        first_report = self.room.publish_report({"overview": "first two"}, seen_through=first["through_event"], batch_id=first["batch_id"])
        repeated = self.room.publish_report({"overview": "retry"}, seen_through=first["through_event"], batch_id=first["batch_id"])
        self.assertEqual(first_report, repeated)
        second = self.room.reporter_batch(limit=10)
        self.assertEqual(len(second["events"]), 4)
        self.room.publish_report({"overview": "remaining"}, seen_through=second["through_event"], batch_id=second["batch_id"])
        for kind in ("sdk_event", "tool_call", "tool_result", "visible_text"):
            self.room.record_evidence("reporter", kind, "reporter work")
        self.room.record_evidence("coordinator", "tool_call", "get status", metadata={"name": "get_team_status"})
        self.room.record_evidence("coordinator", "tool_result", "status result", metadata={"reportable": False})
        self.room.event("heartbeat", {})
        self.room.request_audit_report("request", 1)
        self.assertIsNone(self.room.reporter_batch())

    def test_audit_cutoff_and_bound_version_survive_new_events_and_restart(self):
        self.room.notice("pro-1", "before audit")
        request = self.room.request_audit_report("request-1", 1)
        cutoff = request["required_through_event"]
        self.assertFalse(request["ready"])
        batch = self.room.reporter_batch()
        self.room.notice("pro-2", "after cutoff")
        self.assertEqual(self.room.request_audit_report("request-1", 1)["required_through_event"], cutoff)
        first = self.room.publish_report({"overview": "first"}, seen_through=batch["through_event"], batch_id=batch["batch_id"])
        self.publish_batch()
        self.reopen()
        status = self.room.audit_report_status("request-1")
        self.assertTrue(status["ready"])
        self.assertEqual(status["report_version"], first["report_version"])
        self.assertEqual(status["pending_after_cutoff"], 1)
        self.room.set_meta("context", {"platform": {"correct_flag_count": 2}})
        snapshot = self.room.audit_snapshot("request-1")
        replay = self.room.audit_snapshot("request-1")
        self.assertEqual(snapshot["report"]["overview"], "first")
        self.assertEqual(snapshot["platform"]["correct_flag_count"], 2)
        self.assertNotIn("correct_flag_count", snapshot["report"]["platform"])
        self.assertEqual(snapshot["through_event"], cutoff)
        self.assertGreater(snapshot["evidence_watermark"], cutoff)
        self.assertEqual(len(replay["windows"]), 1)
        self.assertEqual(snapshot["window_id"], replay["window_id"])
        self.assertEqual(replay["report_mode"], "reporter_snapshot")
        self.assertEqual(self.room.mailbox("coordinator")[0]["body"], "before audit")
        with self.assertRaises(ValueError):
            self.room.request_audit_report("request-1", 2)

    def test_reporter_checkpoint_catches_up_500_full_records_with_bounded_previews(self):
        original = "tool output " * 300
        for index in range(500):
            self.room.record_evidence("pro-1", "tool_result", original, metadata={
                "generation": "g" * 32, "turn_id": "t" * 32, "review_id": None,
                "model_id": "deepseek-v4-pro", "name": "Bash"})
        checkpoint = self.room.evidence_watermark
        request = self.room.request_audit_report("backlog-audit", 1)
        self.room.record_evidence("pro-2", "tool_result", "after checkpoint")
        processed = []
        for iteration in range(8):
            batch = self.room.reporter_batch(through=checkpoint)
            if batch is None:
                break
            self.assertLessEqual(len(batch["events"]), 100)
            self.assertLess(sum(len(json.dumps(event, ensure_ascii=False)) for event in batch["events"]), 60000)
            for event in batch["events"]:
                self.assertLessEqual(len(event["data"]["preview"]), 300)
                self.assertTrue(event["data"]["preview_truncated"])
                self.assertEqual(event["data"]["original_length"], len(original))
                self.assertEqual(Path(event["data"]["reference"]).read_text(), original)
            processed += [event["seq"] for event in batch["events"]]
            self.room.publish_report({"overview": "bounded preview with full evidence references"},
                seen_through=batch["through_event"], batch_id=batch["batch_id"])
        self.assertEqual(len(processed), 500)
        self.assertEqual(len(set(processed)), 500)
        self.assertIsNone(self.room.reporter_batch(through=checkpoint))
        self.assertTrue(self.room.audit_report_status(request["request_id"])["ready"])
        self.assertEqual(len(self.room.reporter_batch()["events"]), 1)
        original_event = self.room.events(after=processed[0] - 1, limit=1)[0]
        self.assertEqual(len(original_event["data"]["preview"]), 2000)

    def test_long_notice_preview_has_full_reference_without_new_reportable_event(self):
        body = "important source detail " * 1000
        notice = self.room.notice("pro-1", body)
        original_watermark = self.room.evidence_watermark
        batch = self.room.reporter_batch(through=original_watermark)
        view = batch["events"][0]["data"]
        self.assertEqual(len(view["body"]), 300)
        self.assertTrue(view["preview_truncated"])
        self.assertEqual(view["original_length"], len(body))
        self.assertEqual(json.loads(Path(view["reference"]).read_text())["data"]["body"], body)
        self.assertEqual(self.room.veria_message(notice["id"])["body"], body)
        self.assertEqual(self.room.reporter_batch()["events"], batch["events"])
        self.assertEqual(self.room.evidence_watermark, original_watermark)
        backlog = {"checkpoint": original_watermark, "covered_through": 0, "unprocessed_at_checkpoint": True,
                   "evidence_index": str(self.directory / "evidence")}
        self.room.set_meta("reporter_backlog", backlog)
        self.assertEqual(self.room.snapshot()["reporter_backlog"], backlog)

    def test_evidence_preserves_full_content_and_report_is_program_owned(self):
        text = "完整输出" * 10000
        evidence = self.room.record_evidence("pro-1", "tool_result", text)
        self.assertEqual(Path(evidence["path"]).read_text(), text)
        self.assertTrue(evidence["truncated"])
        self.assertLess(len(json.dumps(self.room.events()[-1])), 20000)
        self.room.set_meta("context", {"platform": {"correct_flag_count": 1}})
        batch = self.room.reporter_batch()
        report = self.room.publish_report({"overview": "one known flag", "platform": {"correct_flag_count": 999}},
                                          seen_through=batch["through_event"], batch_id=batch["batch_id"])
        self.assertEqual(report["platform"]["correct_flag_count"], 1)

    def test_evidence_lookup_has_real_sender_review_and_tool_provenance(self):
        evidence = self.room.record_evidence("reviewer", "tool_result", {"stdout": "freshly reproduced flag"},
                                            metadata={"review_id": "review-current", "name": "Bash", "turn_id": "review-turn"})
        for reference in (evidence["event_seq"], str(evidence["event_seq"]), evidence["id"], evidence["path"]):
            record = self.room.evidence_record(reference)
            self.assertEqual(record["seq"], evidence["event_seq"])
            self.assertEqual(record["sender"], "reviewer")
            self.assertEqual(record["data"]["kind"], "tool_result")
            self.assertEqual(record["data"]["metadata"]["review_id"], "review-current")
        raw_event = self.room.event("progress", {"result": "new primitive"}, "pro-1")
        self.assertEqual(self.room.evidence_record(raw_event)["type"], "progress")
        reporter = self.room.record_evidence("reporter", "visible_text", "a summary is not a new reproduction")
        self.assertEqual(self.room.evidence_record(reporter["id"])["sender"], "reporter")
        self.assertFalse(self.room.evidence_record(reporter["path"])["data"]["reportable"])
        for reference in ("invented", "/tmp/unregistered-evidence.txt", True, 0, -1, 2**65, None):
            self.assertIsNone(self.room.evidence_record(reference))
        self.reopen(attempt="attempt-2")
        self.assertIsNone(self.room.evidence_record(evidence["id"]))
        self.assertIsNone(self.room.evidence_record(evidence["path"]))

    def test_audit_delta_is_complete_fixed_and_never_promotes_reporter_cursor(self):
        self.room.notice("pro-1", "baseline evidence")
        baseline = self.publish_batch()
        evidence = self.room.record_evidence("pro-1", "tool_execution_result", "full result " * 1000)
        request = self.room.request_audit_report("delta-round", 7)
        self.assertFalse(request["ready"])
        ready = self.room.audit_report_status("delta-round", allow_delta=True)
        self.assertTrue(ready["ready"])
        self.assertEqual(ready["report_mode"], "report_plus_delta")
        snapshot = self.room.audit_snapshot("delta-round", allow_delta=True)
        self.assertEqual(snapshot["through_event"], request["required_through_event"])
        self.assertEqual(snapshot["report"]["through_event"], baseline["through_event"])
        self.assertEqual(self.room.latest_report()["through_event"], baseline["through_event"])
        self.assertEqual(snapshot["delta_events"][0]["data"]["sha256"], evidence["sha256"])
        self.assertEqual(snapshot["delta_evidence_refs"][0]["bytes"], evidence["bytes"])
        self.assertEqual(snapshot["delta_evidence_refs"][0]["content"], "full result " * 1000)
        self.publish_batch()  # A late covering report must not rewrite the applied material.
        replay = self.room.audit_snapshot("delta-round", allow_delta=True)
        self.assertEqual(snapshot["delta_events"], replay["delta_events"])
        self.assertEqual(replay["report_version"], baseline["report_version"])
        self.assertEqual(replay["window_id"], snapshot["window_id"])

    def test_unapplied_delta_can_explicitly_wait_for_and_promote_late_report(self):
        self.room.notice("pro-1", "baseline")
        self.publish_batch()
        self.room.notice("pro-1", "complete later delta")
        self.room.request_audit_report("promote", 9)
        initial = self.room.audit_snapshot("promote", allow_delta=True)
        self.assertEqual(initial["report_mode"], "report_plus_delta")
        wait = self.room.audit_report_status("promote", allow_delta=False)
        self.assertFalse(wait["ready"])
        self.assertTrue(wait["delta_error"]["awaiting_reporter"])
        late = self.publish_batch()
        promoted = self.room.audit_snapshot("promote", allow_delta=False)
        self.assertEqual(promoted["window_id"], initial["window_id"])
        self.assertEqual(promoted["report_mode"], "reporter_snapshot")
        self.assertEqual(promoted["report_version"], late["report_version"])
        self.assertNotIn("delta_events", promoted)
        self.assertEqual(self.room.audit_snapshot("promote", allow_delta=True)["report_mode"], "reporter_snapshot")

    def test_audit_delta_rejects_missing_or_changed_evidence_and_no_baseline(self):
        evidence = self.room.record_evidence("pro-1", "tool_execution_result", "not a baseline")
        self.room.request_audit_report("no-baseline", 1)
        self.assertFalse(self.room.audit_report_status("no-baseline", allow_delta=True)["ready"])
        self.publish_batch()
        evidence = self.room.record_evidence("pro-1", "tool_execution_result", "must remain immutable")
        self.room.request_audit_report("missing", 2)
        Path(evidence["path"]).write_text("changed", encoding="utf-8")
        status = self.room.audit_report_status("missing", allow_delta=True)
        self.assertFalse(status["coverage_complete"])
        self.assertFalse(status["ready"])
        with self.assertRaisesRegex(ValueError, "fixed request"):
            self.room.audit_snapshot("missing", allow_delta=True)

    def test_oversized_delta_is_explicit_and_never_marked_complete(self):
        self.room.notice("pro-1", "baseline")
        self.publish_batch()
        self.room.record_evidence("pro-1", "tool_execution_result", "x" * (768 * 1024))
        self.room.request_audit_report("oversized-delta", 8)
        status = self.room.audit_report_status("oversized-delta", allow_delta=True)
        self.assertFalse(status["ready"])
        self.assertEqual(status["delta_error"]["error_type"], "material_too_large")
        self.publish_batch()
        late = self.room.audit_report_status("oversized-delta", allow_delta=True)
        self.assertTrue(late["ready"])
        self.assertEqual(late["report_mode"], "reporter_snapshot")

    def test_controlled_recovery_is_atomic_and_preserves_session_after_reopen(self):
        self.room.set_session("pro-1", "original-session")
        self.room.update_agent("pro-1", status="failed", restarts=8, last_error="402")
        signature = {"provider": "deepseek", "reason": "replenished", "members": ["pro-1"]}
        receipt = self.room.authorize_recovery("once", signature, ["pro-1"], {}, 10000)
        self.reopen()
        self.assertEqual(self.room.agent("pro-1")["status"], "queued")
        self.assertEqual(self.room.agent("pro-1")["session_id"], "original-session")
        self.assertEqual(self.room.agent("pro-1")["restarts"], 0)
        self.assertEqual(self.room.authorize_recovery("once", signature, ["pro-1"], {}, 10000), receipt)

    def test_mixed4_roster_survives_restart_and_cannot_become_legacy(self):
        other = self.directory / "mixed"
        roster = tuple(f"solver-{index}" for index in range(1, 5))
        room = VeriaRoom(other, "mixed", "attempt", solver_ids=roster)
        try:
            for member in (*roster, "coordinator"):
                room.register_member(member, "solver" if member in roster else member, "fixture-model")
            self.assertEqual(set(room.broadcast("coordinator", "all four")["recipients"]), set(roster))
            with self.assertRaisesRegex(ValueError, "unknown"):
                room.register_member("pro-1", "solver", "fixture-model")
        finally:
            room.close()
        room = VeriaRoom(other, "mixed", "attempt")
        self.assertEqual(room.solver_ids, roster)
        room.close()
        with self.assertRaisesRegex(ValueError, "roster cannot change"):
            VeriaRoom(other, "mixed", "attempt", solver_ids=SOLVER_IDS)

    def test_only_new_review_identity_atomically_gets_a_fresh_retry_budget(self):
        self.room.start_review("failed-review", {"deadline": 1800})
        self.room.update_agent("reviewer", restarts=3, status="failed", last_error="retry budget exhausted")
        self.room.finish_review("failed-review", "failed", "retry budget exhausted")
        self.room.start_review("failed-review", {"deadline": 1800})
        self.assertEqual(self.room.agent("reviewer")["restarts"], 3)
        self.room.start_review("new-review", {"deadline": 3600})
        self.reopen()
        self.assertEqual(self.room.agent("reviewer")["restarts"], 0)
        self.assertEqual(self.room.agent("reviewer")["status"], "queued")
        self.assertEqual(self.room.review("failed-review")["request"]["deadline"], 1800)
        self.assertEqual(self.room.review("new-review")["request"]["deadline"], 3600)
        # A runtime restart of this same active identity keeps its consumed retries.
        self.room.update_agent("reviewer", restarts=1)
        self.room.start_review("new-review", {"deadline": 3600})
        self.assertEqual(self.room.agent("reviewer")["restarts"], 1)

    def test_partial_review_keeps_phase_evidence_without_complete_marker(self):
        self.room.start_review("partial", {"started_at": 1, "deadline": 1801})
        self.room.set_meta("review_reproductions:partial", {"event_seq": 1, "reproductions": []})
        self.room.review_progress("partial", phase="coverage", sections={"coverage": "checked"}, evidence_refs=[3])
        review = self.room.finish_review("partial", "cancelled", "supervisor selected another challenge")
        self.assertTrue(review["phase_evidence_complete"])
        self.assertTrue(review["result"]["partial"])
        self.assertIsNone(self.room.meta("last_review"))
        self.assertEqual(review["request"]["deadline"], 1801)
        with self.assertRaisesRegex(ValueError, "partial review"):
            self.room.complete_review("partial", {"summary": "cannot claim complete"})

    def test_reviews_are_independent_idempotent_and_do_not_reenter(self):
        first = self.room.start_review("review-1", {"reason": "no new flag"})
        self.assertEqual(self.room.start_review("review-1", {"reason": "no new flag"}), first)
        with self.assertRaisesRegex(ValueError, "already running"):
            self.room.start_review("review-2", {})
        self.reopen()
        self.assertEqual(self.room.recover_review("review-1")["recoveries"], 1)
        result = {"reproductions": [{"status": "blocked", "reason": "one-shot prerequisite"}], "next_steps": ["coordinate with coordinator"]}
        completed = self.room.complete_review("review-1", result)
        self.assertEqual(self.room.complete_review("review-1", result), completed)
        self.assertEqual(self.room.meta("last_review")["review_id"], "review-1")
        self.assertEqual(self.room.start_review("review-2", {})["status"], "running")

    def test_new_attempt_archives_extension_tables_and_full_evidence(self):
        evidence = self.room.record_evidence("pro-1", "tool_result", "historical evidence")
        self.room.notice("pro-1", "historical notice", message_id="old-message")
        self.room.queue_bump("pro-1", "old guidance")
        self.room.save_candidate("pro-1", "old-flag")
        self.room.start_review("old-review", {})
        (self.directory / "environment.json").write_text('{"attempt_id":"attempt-1"}')
        (self.directory / "command-history.jsonl").write_text('{"old_command":"owned"}\n')
        for name in ("members", "inputs", "control", "shared"):
            (self.directory / name).mkdir()
            (self.directory / name / "old.txt").write_text("old attempt material")
        self.publish_batch()
        self.reopen(attempt="attempt-2")
        archive = Path(self.room.archived_attempt)
        with closing(sqlite3.connect(archive / "room.sqlite3")) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM veria_candidates").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM veria_messages").fetchone()[0], 1)
        self.assertTrue((archive / "evidence" / Path(evidence["path"]).name).exists())
        self.assertTrue((archive / "environment.json").exists())
        self.assertFalse((self.directory / "environment.json").exists())
        self.assertTrue((archive / "command-history.jsonl").exists())
        self.assertFalse((self.directory / "command-history.jsonl").exists())
        for name in ("members", "inputs", "control", "shared"):
            self.assertTrue((archive / name / "old.txt").exists())
            self.assertFalse((self.directory / name).exists())
        self.assertEqual(self.room.members(), [])
        self.assertEqual(self.room.pending_candidates(), [])
        self.assertEqual(self.room.pending_bumps("pro-1"), [])
        self.assertEqual(self.room.mailbox("coordinator"), [])
        self.assertIsNone(self.room.review("old-review"))
        self.assertIsNone(self.room.reporter_batch())
        self.assertEqual(self.room.db.execute("SELECT MAX(version) FROM veria_schema").fetchone()[0], SCHEMA_VERSION)

    def test_cannot_convert_active_native_attempt_to_veria(self):
        native_directory = self.directory / "native"
        native = Room(native_directory, "other", "attempt-native")
        native.add_agent("leader", "leader")
        native.close()
        with self.assertRaisesRegex(ValueError, "native members"):
            VeriaRoom(native_directory, "other", "attempt-native")
        native = Room(native_directory, "other", "attempt-native")
        self.assertIsNotNone(native.agent("leader"))
        native.close()

    def test_acquisition_jobs_require_a_real_local_correct_receipt(self):
        for status, receipt in (("pending", {"pending": True}), ("unknown", {"unknown": True}),
                ("incorrect", {"correct": False}), ("duplicate", {"correct": True, "duplicate": True})):
            candidate = self.room.save_candidate("pro-1", "flag{" + status + "}")
            self.room.apply_receipt(candidate["request_id"], receipt)
            self.assertIsNone(self.room.acquisition_status(candidate["request_id"])["job"])
        self.assertEqual(self.room.recover_acquisitions(), 0)
        candidate = self.room.save_candidate("pro-1", "flag{correct}")
        request_id = candidate["request_id"]
        self.room.record_acquisition("pro-1", request_id, {"prerequisites": []})
        receipt = {"correct": True, "duplicate": False}
        self.room.apply_receipt(request_id, receipt)
        job = self.room.acquisition_jobs()[0]
        self.assertEqual(job["request_id"], request_id)
        self.assertEqual(job["draft_revision"], 1)
        self.assertFalse(self.room.apply_receipt(request_id, receipt))
        self.room.apply_receipt(request_id, {"correct": True, "duplicate": True})
        self.assertEqual(self.room.acquisition_jobs(), [job])
        self.reopen()
        self.assertEqual(self.room.recover_acquisitions(), 0)
        self.assertEqual(self.room.acquisition_jobs(), [job])

    def test_acquisition_versions_and_late_supplements_are_traceable(self):
        request_id = self.room.save_candidate("pro-1", "flag{versions}")["request_id"]
        self.room.apply_receipt(request_id, {"correct": True})
        first_job = self.room.acquisition_jobs()[0]
        first = self.room.save_acquisition(first_job, {"request_id": request_id, "pending_fields": ["replay_commands"]})
        self.assertEqual(first["version"], 1)
        self.assertEqual(self.room.acquisition_jobs(), [])
        self.room.record_acquisition("pro-1", request_id, {"replay_commands": ["python acquire.py"]})
        second_job = self.room.acquisition_jobs()[0]
        self.room.record_acquisition("pro-2", request_id, {"prerequisites": ["session cookie"]})
        third_job = self.room.acquisition_jobs()[0]
        with self.assertRaises(StaleAcquisitionJob):
            self.room.save_acquisition(second_job, {"request_id": request_id, "pending_fields": ["prerequisites"]})
        self.assertEqual(self.room.acquisition_jobs(), [third_job], "a late old worker cannot mark a newer draft ready")
        last = self.room.save_acquisition(third_job, {"request_id": request_id, "pending_fields": []})
        self.assertEqual(last["version"], 2)
        self.assertEqual(self.room.save_acquisition(third_job, {"request_id": request_id, "pending_fields": []}), last)
        self.assertEqual(self.room.acquisition_status(request_id, version=1)["manifest"], first)
        self.assertEqual(json.loads(Path(first["manifest_path"]).read_text()), first)
        self.room.record_acquisition("pro-2", request_id, {"prerequisites": ["session cookie"]})
        self.assertEqual(self.room.acquisition_jobs(), [])
        self.reopen()
        self.assertEqual(self.room.acquisition_status(request_id)["manifest"], last)
        self.assertEqual(self.room.acquisition_status(request_id)["draft"]["revision"], 2)
        updates = [e for e in self.room.events(limit=1000) if e["type"] == "acquisition_updated"]
        self.assertEqual(updates[1]["data"]["previous_fields"], {"replay_commands": ["python acquire.py"]})

    def test_acquisition_draft_validation_migration_and_attempt_archive(self):
        request_id = self.room.save_candidate("pro-1", "flag{migrate}")["request_id"]
        self.room.apply_receipt(request_id, {"correct": True})
        with self.assertRaisesRegex(ValueError, "persisted candidate"):
            self.room.record_acquisition("pro-1", "missing", {"notes": "not a candidate"})
        for fields in ({"replay_commands": "python solve.py"}, {"entrypoints": [""]},
                {"evidence_refs": [True]}, {"unknown": []}, {"notes": []}):
            with self.assertRaises(ValueError):
                self.room.record_acquisition("pro-1", request_id, fields)
        # A genuine schema-1 database must migrate and backfill existing receipts.
        for table in ("veria_acquisition_drafts", "veria_acquisition_jobs", "veria_acquisition_versions"):
            self.room.db.execute("DROP TABLE " + table)
        self.room.db.execute("DELETE FROM veria_schema WHERE version=2")
        self.room.db.commit()
        self.reopen()
        self.assertEqual(self.room.recover_acquisitions(), 1)
        manifest = self.room.save_acquisition(self.room.acquisition_jobs()[0],
            {"request_id": request_id, "pending_fields": ["tool_evidence"]})
        self.reopen("attempt-2")
        self.assertEqual(self.room.acquisition_jobs(), [])
        self.assertEqual(self.room.acquisition_summaries(), [])
        self.assertFalse(Path(manifest["manifest_path"]).exists())
        self.assertTrue(list((self.directory / "attempts").glob("*/acquisitions/*/manifest-v1.json")))

    def test_acquisition_material_rejects_a_job_selected_before_a_late_draft(self):
        request_id = self.room.save_candidate("pro-1", "flag{stale-material}")["request_id"]
        self.room.apply_receipt(request_id, {"correct": True})
        selected = self.room.acquisition_jobs()[0]
        self.room.record_acquisition("pro-1", request_id, {"replay_commands": ["python new.py"]})
        fresh = self.room.acquisition_jobs()[0]
        with self.assertRaises(StaleAcquisitionJob):
            self.room.acquisition_material(selected)
        material = self.room.acquisition_material(fresh)
        self.assertEqual(material["draft"]["revision"], fresh["draft_revision"])
        self.assertEqual(material["draft"]["payload"]["replay_commands"], ["python new.py"])
        # A corrupted/stale job cannot borrow a newer draft even if its saved
        # job row accidentally retained the older revision.
        with self.room.db:
            self.room.db.execute("UPDATE veria_acquisition_jobs SET draft_revision=0 WHERE request_id=?",
                                 (request_id,))
        inconsistent = self.room.acquisition_jobs()[0]
        with self.assertRaises(StaleAcquisitionJob):
            self.room.acquisition_material(inconsistent)

    def test_old_acquisition_failure_does_not_poison_a_new_pending_job(self):
        request_id = self.room.save_candidate("pro-1", "flag{stale-failure}")["request_id"]
        self.room.apply_receipt(request_id, {"correct": True})
        old = self.room.acquisition_jobs()[0]
        self.room.record_acquisition("pro-1", request_id, {"notes": "new supplement"})
        latest = self.room.acquisition_jobs()[0]
        self.assertFalse(self.room.fail_acquisition(old, "old copy cancelled"))
        self.assertEqual(self.room.acquisition_jobs(), [latest])
        self.assertFalse([event for event in self.room.events(limit=1000)
                          if event["type"] == "acquisition_failed"])
        self.assertTrue(self.room.fail_acquisition(latest, "actual latest copy failed"))
        self.assertEqual(self.room.acquisition_status(request_id)["job"]["state"], "error")

    def test_acquisition_fields_keep_their_original_member_when_notes_are_supplemented(self):
        request_id = self.room.save_candidate("pro-1", "flag{field-owner}")["request_id"]
        self.room.record_acquisition("pro-1", request_id, {"script_paths": ["solve.py"],
            "replay_commands": ["python solve.py"], "prerequisites": []})
        self.room.record_acquisition("coordinator", request_id, {"notes": "coordinated replay",
            "script_paths": ["solve.py"]})
        draft = self.room.acquisition_draft(request_id)
        self.assertEqual(draft["sender"], "coordinator")
        self.assertEqual(draft["field_senders"]["script_paths"], "pro-1")
        self.assertEqual(draft["field_senders"]["replay_commands"], "pro-1")
        self.assertEqual(draft["field_senders"]["notes"], "coordinator")
        self.room.record_acquisition("pro-2", request_id, {"script_paths": ["better.py"]})
        self.assertEqual(self.room.acquisition_draft(request_id)["field_senders"]["script_paths"], "pro-2")
        self.reopen()
        self.assertEqual(self.room.acquisition_draft(request_id)["field_senders"]["replay_commands"], "pro-1")

    def test_legacy_acquisition_draft_field_owners_are_rebuilt_from_journal_before_images(self):
        request_id = self.room.save_candidate("pro-1", "flag{legacy-owner}")["request_id"]
        self.room.record_acquisition("pro-1", request_id, {"script_paths": ["solve.py"]})
        self.room.record_acquisition("coordinator", request_id, {"notes": "supplement",
            "script_paths": ["solve.py"]})
        # Schema-2 drafts written before field provenance was added have the
        # same table but no explicit provenance in acquisition_updated events.
        with self.room.db:
            rows = self.room.db.execute("SELECT seq,data FROM events WHERE type='acquisition_updated'").fetchall()
            for row in rows:
                data = json.loads(row["data"])
                data.pop("field_senders", None)
                data.pop("changed_fields", None)
                self.room.db.execute("UPDATE events SET data=? WHERE seq=?", (json.dumps(data), row["seq"]))
        self.reopen()
        draft = self.room.acquisition_draft(request_id)
        self.assertEqual(draft["field_senders"], {"script_paths": "pro-1", "notes": "coordinator"})
        self.room.record_acquisition("reviewer", request_id, {"prerequisites": ["fresh session"]})
        self.assertEqual(self.room.acquisition_draft(request_id)["field_senders"],
            {"script_paths": "pro-1", "notes": "coordinator", "prerequisites": "reviewer"})


if __name__ == "__main__":
    unittest.main()
