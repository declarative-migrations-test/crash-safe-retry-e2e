import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from crash_safe_retry import Crash, MigrationPlan, SimulatedMigrator  # noqa: E402

SOURCE = json.loads((ROOT / "source.json").read_text(encoding="utf-8"))
SOURCE_SHA = SOURCE["productionSources"][0]["commit"]
EMAIL_PLAN = MigrationPlan(
    version=1,
    sql="ALTER TABLE app_accounts ADD COLUMN email TEXT",
    column="email",
)
EDITED_PLAN = MigrationPlan(
    version=1,
    sql="ALTER TABLE app_accounts ADD COLUMN email TEXT DEFAULT 'edited'",
    column="email",
)


class CrashSafeRetryE2E(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="crash-safe-retry-e2e-")
        self.db_path = Path(self.tmp.name) / "app.sqlite3"
        self.engine = SimulatedMigrator(
            self.db_path,
            source_sha=SOURCE_SHA,
            now=1_700_000_000,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _assert_evidence_contract(self, evidence) -> None:
        payload = evidence.to_json()
        self.assertEqual(payload["schemaVersion"], 1)
        self.assertEqual(payload["engine"], "sqlite-simulated-migrator")
        self.assertEqual(payload["sourceSha"], SOURCE_SHA)
        self.assertRegex(payload["planDigest"], r"^[0-9a-f]{64}$")
        self.assertRegex(payload["finalSchemaDigest"], r"^[0-9a-f]{64}$")
        self.assertIsInstance(payload["lockTransitions"], list)
        self.assertIn(payload["recoveryGuidance"]["writes"], {
            "applied",
            "skipped",
            "history-only",
            "refused",
            "not-acknowledged",
        })

    def test_crash_before_txn_is_retryable_without_schema_change(self) -> None:
        with self.assertRaises(Crash) as raised:
            self.engine.apply(
                EMAIL_PLAN, owner="migrator-a", token="tok-a", crash_at="before_txn"
            )
        self.assertEqual(raised.exception.point, "before_txn")
        state = self.engine.inspect()
        self.assertEqual(state.columns, ["id", "name"])
        self.assertEqual(state.history, [])
        self.assertIsNotNone(state.lock)

        evidence = self.engine.retry(EMAIL_PLAN, owner="migrator-a", token="tok-a")
        self._assert_evidence_contract(evidence)
        self.assertEqual(evidence.decision, "applied")
        self.assertEqual(self.engine.inspect().columns.count("email"), 1)
        self.assertEqual(evidence.history_versions, [1])

    def test_crash_during_ddl_rolls_back_then_retry_applies_once(self) -> None:
        with self.assertRaises(Crash):
            self.engine.apply(
                EMAIL_PLAN, owner="migrator-a", token="tok-a", crash_at="during_ddl"
            )
        self.assertNotIn("email", self.engine.inspect().columns)
        self.assertEqual(self.engine.inspect().history, [])

        evidence = self.engine.retry(EMAIL_PLAN, owner="migrator-a", token="tok-a")
        self._assert_evidence_contract(evidence)
        self.assertEqual(evidence.decision, "applied")
        self.assertEqual(self.engine.inspect().columns.count("email"), 1)
        replay = self.engine.retry(EMAIL_PLAN, owner="migrator-a", token="tok-a")
        self.assertEqual(replay.decision, "replayed")
        self.assertEqual(self.engine.inspect().columns.count("email"), 1)

    def test_crash_after_ddl_before_history_recovers_without_double_apply(self) -> None:
        with self.assertRaises(Crash):
            self.engine.apply(
                EMAIL_PLAN,
                owner="migrator-a",
                token="tok-a",
                crash_at="after_ddl_before_history",
            )
        state = self.engine.inspect()
        self.assertIn("email", state.columns)
        self.assertEqual(state.history, [])

        evidence = self.engine.retry(EMAIL_PLAN, owner="migrator-a", token="tok-a")
        self._assert_evidence_contract(evidence)
        self.assertEqual(evidence.decision, "recovered-history")
        self.assertEqual(evidence.recovery_guidance["writes"], "history-only")
        self.assertEqual(self.engine.inspect().columns.count("email"), 1)
        self.assertEqual(self.engine.inspect().history[0]["checksum"], EMAIL_PLAN.digest())

    def test_crash_after_history_before_ack_is_idempotent(self) -> None:
        with self.assertRaises(Crash):
            self.engine.apply(
                EMAIL_PLAN,
                owner="migrator-a",
                token="tok-a",
                crash_at="after_history_before_ack",
            )
        state = self.engine.inspect()
        self.assertIn("email", state.columns)
        self.assertEqual(len(state.history), 1)

        evidence = self.engine.retry(EMAIL_PLAN, owner="migrator-a", token="tok-a")
        self._assert_evidence_contract(evidence)
        self.assertEqual(evidence.decision, "replayed")
        self.assertEqual(evidence.recovery_guidance["writes"], "skipped")
        self.assertEqual(self.engine.inspect().columns.count("email"), 1)
        self.assertEqual(len(self.engine.inspect().history), 1)

    def test_stolen_lock_refuses_writes(self) -> None:
        with self.assertRaises(Crash):
            self.engine.apply(
                EMAIL_PLAN, owner="migrator-a", token="tok-a", crash_at="before_txn"
            )
        self.engine.steal_lock("migrator-b", "tok-b")
        before = self.engine.inspect()
        evidence = self.engine.retry(EMAIL_PLAN, owner="migrator-a", token="tok-a")
        self._assert_evidence_contract(evidence)
        self.assertEqual(evidence.decision, "refused")
        self.assertEqual(evidence.recovery_guidance["reason"], "stolen-lock")
        self.assertEqual(evidence.recovery_guidance["writes"], "refused")
        after = self.engine.inspect()
        self.assertEqual(after.columns, before.columns)
        self.assertEqual(after.history, before.history)
        self.assertEqual(after.lock["owner"], "migrator-b")

    def test_stale_lock_refuses_writes(self) -> None:
        with self.assertRaises(Crash):
            self.engine.apply(
                EMAIL_PLAN, owner="migrator-a", token="tok-old", crash_at="before_txn"
            )
        before = self.engine.inspect()
        evidence = self.engine.retry(EMAIL_PLAN, owner="migrator-a", token="tok-new")
        self._assert_evidence_contract(evidence)
        self.assertEqual(evidence.decision, "refused")
        self.assertEqual(evidence.recovery_guidance["reason"], "stale-lock")
        after = self.engine.inspect()
        self.assertEqual(after.columns, before.columns)
        self.assertEqual(after.history, [])
        self.assertEqual(after.lock["token"], "tok-old")

    def test_checksum_mismatch_refuses_writes(self) -> None:
        applied = self.engine.apply(EMAIL_PLAN, owner="migrator-a", token="tok-a")
        self.assertEqual(applied.decision, "applied")
        before = self.engine.inspect()
        evidence = self.engine.apply(EDITED_PLAN, owner="migrator-a", token="tok-a")
        self._assert_evidence_contract(evidence)
        self.assertEqual(evidence.decision, "refused")
        self.assertEqual(evidence.recovery_guidance["reason"], "checksum-mismatch")
        after = self.engine.inspect()
        self.assertEqual(after.columns, before.columns)
        self.assertEqual(after.history[0]["checksum"], EMAIL_PLAN.digest())
        self.assertNotEqual(EMAIL_PLAN.digest(), EDITED_PLAN.digest())

    def test_expired_lock_takeover_then_identical_retry_is_a_noop(self) -> None:
        with self.assertRaises(Crash):
            self.engine.apply(
                EMAIL_PLAN, owner="migrator-a", token="tok-a", crash_at="before_txn"
            )
        self.engine.expire_lock()
        recovered = self.engine.retry(EMAIL_PLAN, owner="migrator-b", token="tok-b")
        self.assertEqual(recovered.decision, "applied")
        replay = self.engine.retry(EMAIL_PLAN, owner="migrator-a", token="tok-a")
        self.assertEqual(replay.decision, "replayed")
        self.assertEqual(self.engine.inspect().columns.count("email"), 1)


if __name__ == "__main__":
    unittest.main()
