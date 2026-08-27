"""Disposable SQLite migrator with explicit crash points, lock fencing, and checksums.

The engine is a simulated migration executor, not a production credential path.
Every durable transition is inspectable so retry, stolen/stale lock, and
checksum mismatch have one correct outcome or a generated manual-intervention
state.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Literal

SCHEMA_VERSION = 1
ENGINE_NAME = "sqlite-simulated-migrator"
FAULT_POINTS = (
    "before_txn",
    "during_ddl",
    "after_ddl_before_history",
    "after_history_before_ack",
)
ApplyDecision = Literal[
    "applied",
    "replayed",
    "recovered-history",
    "refused",
    "crashed",
]


class Crash(Exception):
    """Injected process crash after a named durable boundary."""

    def __init__(self, point: str) -> None:
        super().__init__(point)
        self.point = point


class Refusal(Exception):
    """Fail-closed write refusal with machine-readable recovery guidance."""

    def __init__(self, reason: str, guidance: dict[str, Any]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.guidance = guidance


@dataclass(frozen=True)
class MigrationPlan:
    version: int
    sql: str
    column: str

    def digest(self) -> str:
        return plan_checksum(self.version, self.sql)


@dataclass
class DurableState:
    history: list[dict[str, Any]]
    lock: dict[str, Any] | None
    columns: list[str]
    schema_digest: str


@dataclass
class Evidence:
    schema_version: int
    engine: str
    engine_version: str
    source_sha: str
    plan_digest: str
    fault_point: str | None
    lock_transitions: list[dict[str, Any]]
    final_schema_digest: str
    decision: ApplyDecision
    recovery_guidance: dict[str, Any]
    history_versions: list[int]

    def to_json(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "engine": self.engine,
            "engineVersion": self.engine_version,
            "sourceSha": self.source_sha,
            "planDigest": self.plan_digest,
            "faultPoint": self.fault_point,
            "lockTransitions": self.lock_transitions,
            "finalSchemaDigest": self.final_schema_digest,
            "decision": self.decision,
            "recoveryGuidance": self.recovery_guidance,
            "historyVersions": self.history_versions,
        }


def plan_checksum(version: int, sql: str) -> str:
    payload = json.dumps(
        {"version": version, "sql": sql.strip()},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def schema_digest(columns: list[str]) -> str:
    payload = json.dumps(columns, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def recovery_guidance(
    *,
    decision: ApplyDecision,
    reason: str | None,
    fault_point: str | None,
    lock_owner: str | None,
) -> dict[str, Any]:
    """Generate operator guidance from durable machine state only."""
    if decision == "refused" and reason == "checksum-mismatch":
        return {
            "action": "manual-intervention",
            "writes": "refused",
            "reason": "checksum-mismatch",
            "next": "restore the reviewed plan bytes or repair history; do not apply",
        }
    if decision == "refused" and reason in {"stolen-lock", "stale-lock"}:
        return {
            "action": "manual-intervention",
            "writes": "refused",
            "reason": reason,
            "lockOwner": lock_owner,
            "next": "identify the live lock owner before retrying; do not steal the fence",
        }
    if decision == "crashed":
        return {
            "action": "retry",
            "writes": "not-acknowledged",
            "reason": f"crash:{fault_point}",
            "next": "retry the same reviewed plan; the engine inspects history and schema",
        }
    if decision == "replayed":
        return {
            "action": "none",
            "writes": "skipped",
            "reason": "identical-history",
            "next": "already applied; further retries are no-ops",
        }
    if decision == "recovered-history":
        return {
            "action": "none",
            "writes": "history-only",
            "reason": "ddl-durable-history-missing",
            "next": "schema was already changed; history was completed without re-applying DDL",
        }
    return {
        "action": "none",
        "writes": "applied",
        "reason": "converged",
        "next": "migration acknowledged",
    }


class SimulatedMigrator:
    def __init__(
        self,
        db_path: Path,
        *,
        source_sha: str,
        now: int | None = None,
        lock_ttl_seconds: int = 30,
    ) -> None:
        self.db_path = Path(db_path)
        self.source_sha = source_sha
        self.now = now if now is not None else int(time.time())
        self.lock_ttl_seconds = lock_ttl_seconds
        self.lock_transitions: list[dict[str, Any]] = []
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._session() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS app_accounts (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS migration_history (
                    version INTEGER PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    sql TEXT NOT NULL,
                    applied_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS migration_lock (
                    slot INTEGER PRIMARY KEY CHECK (slot = 1),
                    owner TEXT NOT NULL,
                    token TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO app_accounts(id, name) VALUES (1, 'baseline');
                """
            )
            conn.commit()

    def inspect(self) -> DurableState:
        with self._session() as conn:
            history = [
                {key: row[key] for key in row.keys()}
                for row in conn.execute(
                    "SELECT version, checksum, sql, applied_at FROM migration_history ORDER BY version"
                )
            ]
            lock_row = conn.execute(
                "SELECT owner, token, epoch, expires_at FROM migration_lock WHERE slot = 1"
            ).fetchone()
            columns = [row[1] for row in conn.execute("PRAGMA table_info(app_accounts)")]
        return DurableState(
            history=history,
            lock={key: lock_row[key] for key in lock_row.keys()} if lock_row is not None else None,
            columns=columns,
            schema_digest=schema_digest(columns),
        )

    def steal_lock(self, owner: str, token: str) -> None:
        """Adversarial overwrite used to prove stolen-lock refusal."""
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO migration_lock(slot, owner, token, epoch, expires_at)
                VALUES (1, ?, ?, 1, ?)
                ON CONFLICT(slot) DO UPDATE SET
                    owner = excluded.owner,
                    token = excluded.token,
                    epoch = migration_lock.epoch + 1,
                    expires_at = excluded.expires_at
                """,
                (owner, token, self.now + self.lock_ttl_seconds),
            )
            conn.commit()
        self._record_lock("stolen-by-peer", owner, token)

    def expire_lock(self) -> None:
        with self._session() as conn:
            conn.execute(
                "UPDATE migration_lock SET expires_at = ? WHERE slot = 1",
                (self.now - 1,),
            )
            conn.commit()
        self._record_lock("expired", None, None)

    def apply(
        self,
        plan: MigrationPlan,
        *,
        owner: str,
        token: str,
        crash_at: str | None = None,
    ) -> Evidence:
        if crash_at is not None and crash_at not in FAULT_POINTS:
            raise ValueError(f"unknown fault point {crash_at}")
        self.lock_transitions = []
        try:
            self._acquire_lock(owner, token)
            self._refuse_checksum_mismatch(plan)
            state = self.inspect()
            if self._already_applied(plan, state):
                self._release_lock(owner, token)
                return self._evidence("replayed", plan, crash_at)
            if crash_at == "before_txn":
                raise Crash("before_txn")
            ddl_applied = plan.column in state.columns
            if not ddl_applied:
                self._run_ddl(plan, crash_at)
            if crash_at == "after_ddl_before_history":
                raise Crash("after_ddl_before_history")
            if not self._history_matches(plan):
                self._write_history(plan)
            if crash_at == "after_history_before_ack":
                raise Crash("after_history_before_ack")
            self._release_lock(owner, token)
            decision: ApplyDecision = "recovered-history" if ddl_applied else "applied"
            return self._evidence(decision, plan, crash_at)
        except Crash:
            raise
        except Refusal as refused:
            try:
                self._release_lock(owner, token)
            except Refusal:
                pass
            return self._evidence("refused", plan, crash_at, refused=refused)

    def retry(
        self,
        plan: MigrationPlan,
        *,
        owner: str,
        token: str,
    ) -> Evidence:
        return self.apply(plan, owner=owner, token=token, crash_at=None)

    def _acquire_lock(self, owner: str, token: str) -> None:
        with self._session() as conn:
            row = conn.execute(
                "SELECT owner, token, epoch, expires_at FROM migration_lock WHERE slot = 1"
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO migration_lock(slot, owner, token, epoch, expires_at)
                    VALUES (1, ?, ?, 1, ?)
                    """,
                    (owner, token, self.now + self.lock_ttl_seconds),
                )
                conn.commit()
                self._record_lock("acquired", owner, token)
                return
            expired = int(row["expires_at"]) < self.now
            if row["owner"] == owner and row["token"] == token:
                conn.execute(
                    "UPDATE migration_lock SET expires_at = ? WHERE slot = 1",
                    (self.now + self.lock_ttl_seconds,),
                )
                conn.commit()
                self._record_lock("reclaimed", owner, token)
                return
            if row["owner"] == owner and row["token"] != token:
                guidance = recovery_guidance(
                    decision="refused",
                    reason="stale-lock",
                    fault_point=None,
                    lock_owner=row["owner"],
                )
                raise Refusal("stale-lock", guidance)
            if not expired:
                guidance = recovery_guidance(
                    decision="refused",
                    reason="stolen-lock",
                    fault_point=None,
                    lock_owner=row["owner"],
                )
                raise Refusal("stolen-lock", guidance)
            conn.execute(
                """
                UPDATE migration_lock
                SET owner = ?, token = ?, epoch = epoch + 1, expires_at = ?
                WHERE slot = 1
                """,
                (owner, token, self.now + self.lock_ttl_seconds),
            )
            conn.commit()
            self._record_lock("expired-takeover", owner, token)

    def _release_lock(self, owner: str, token: str) -> None:
        with self._session() as conn:
            deleted = conn.execute(
                "DELETE FROM migration_lock WHERE slot = 1 AND owner = ? AND token = ?",
                (owner, token),
            ).rowcount
            conn.commit()
        if deleted != 1:
            guidance = recovery_guidance(
                decision="refused",
                reason="stolen-lock",
                fault_point=None,
                lock_owner=None,
            )
            raise Refusal("stolen-lock", guidance)
        self._record_lock("released", owner, token)

    def _refuse_checksum_mismatch(self, plan: MigrationPlan) -> None:
        expected = plan.digest()
        with self._session() as conn:
            row = conn.execute(
                "SELECT checksum, sql FROM migration_history WHERE version = ?",
                (plan.version,),
            ).fetchone()
        if row is None:
            return
        if row["checksum"] != expected or row["sql"] != plan.sql:
            guidance = recovery_guidance(
                decision="refused",
                reason="checksum-mismatch",
                fault_point=None,
                lock_owner=None,
            )
            raise Refusal("checksum-mismatch", guidance)

    def _already_applied(self, plan: MigrationPlan, state: DurableState) -> bool:
        for row in state.history:
            if int(row["version"]) == plan.version and row["checksum"] == plan.digest():
                return plan.column in state.columns
        return False

    def _history_matches(self, plan: MigrationPlan) -> bool:
        with self._session() as conn:
            row = conn.execute(
                "SELECT checksum FROM migration_history WHERE version = ?",
                (plan.version,),
            ).fetchone()
        return row is not None and row["checksum"] == plan.digest()

    def _run_ddl(self, plan: MigrationPlan, crash_at: str | None) -> None:
        conn = sqlite3.connect(self.db_path)
        # Disable implicit commits so a crash during DDL can roll back.
        conn.isolation_level = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(plan.sql)
            if crash_at == "during_ddl":
                conn.execute("ROLLBACK")
                raise Crash("during_ddl")
            conn.execute("COMMIT")
        except Crash:
            raise
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _write_history(self, plan: MigrationPlan) -> None:
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO migration_history(version, checksum, sql, applied_at)
                VALUES (?, ?, ?, ?)
                """,
                (plan.version, plan.digest(), plan.sql, self.now),
            )
            conn.commit()

    def _record_lock(self, event: str, owner: str | None, token: str | None) -> None:
        self.lock_transitions.append(
            {
                "event": event,
                "owner": owner,
                "token": token,
                "at": self.now,
            }
        )

    def _evidence(
        self,
        decision: ApplyDecision,
        plan: MigrationPlan,
        crash_at: str | None,
        refused: Refusal | None = None,
    ) -> Evidence:
        state = self.inspect()
        if refused is not None:
            guidance = refused.guidance
            decision = "refused"
        else:
            guidance = recovery_guidance(
                decision=decision,
                reason=None,
                fault_point=crash_at,
                lock_owner=None,
            )
        return Evidence(
            schema_version=SCHEMA_VERSION,
            engine=ENGINE_NAME,
            engine_version=sqlite3.sqlite_version,
            source_sha=self.source_sha,
            plan_digest=plan.digest(),
            fault_point=crash_at,
            lock_transitions=list(self.lock_transitions),
            final_schema_digest=state.schema_digest,
            decision=decision,
            recovery_guidance=guidance,
            history_versions=[int(row["version"]) for row in state.history],
        )
