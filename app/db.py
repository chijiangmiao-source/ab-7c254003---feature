"""SQLite persistence: sessions (with received bitmap), per-chunk digests and
the resumable finalization protocol (lease, fence generation, checkpoints).

The finalization table is created and backfilled in place: existing databases
keep every session, chunk and published artifact; sessions the old one-shot
finalizer had already completed simply appear as completed finalizations.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id    TEXT PRIMARY KEY,
    file_size     INTEGER NOT NULL,
    chunk_size    INTEGER NOT NULL,
    total_chunks  INTEGER NOT NULL,
    file_sha256   TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',
    bitmap        BLOB NOT NULL,
    expires_at    TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    completed_at  TEXT,
    final_sha256  TEXT,
    artifact_path TEXT
);
CREATE TABLE IF NOT EXISTS chunks (
    session_id  TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    path        TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (session_id, chunk_index),
    FOREIGN KEY (session_id) REFERENCES sessions (session_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS finalization (
    session_id         TEXT PRIMARY KEY,
    generation         INTEGER NOT NULL DEFAULT 0,
    state              TEXT NOT NULL DEFAULT 'idle',
    confirmed_bytes    INTEGER NOT NULL DEFAULT 0,
    total_bytes        INTEGER NOT NULL DEFAULT 0,
    checkpoint_version INTEGER NOT NULL DEFAULT 1,
    hasher_state       BLOB,
    lease_owner        TEXT,
    lease_expires_at   REAL,
    publish_intent     INTEGER NOT NULL DEFAULT 0,
    final_sha256       TEXT,
    last_error         TEXT,
    updated_at         TEXT NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions (session_id) ON DELETE CASCADE
);
"""

# The database clock is the single time source for lease decisions, so
# competing processes never depend on their local clocks.  The julianday
# expression works on every SQLite version (unixepoch 'subsec' needs 3.42).
DB_NOW_SQL = "((julianday('now') - 2440587.5) * 86400.0)"

# Current on-disk checkpoint format; anything else is a recovery error.
CHECKPOINT_VERSION = 1


class Database:
    """Single-connection store guarded by an RLock; every write commits immediately."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(SCHEMA)
            self._conn.commit()
            self._migrate()

    def _migrate(self) -> None:
        """In-place upgrade of pre-finalization databases.

        Sessions completed before the finalization table existed get a
        completed row (generation 0: no lease was ever issued for them).
        Nothing is deleted and no session has to be re-uploaded.
        """
        with self.lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO finalization"
                " (session_id, generation, state, confirmed_bytes, total_bytes,"
                "  checkpoint_version, updated_at)"
                " SELECT session_id, 0, 'completed', file_size, file_size, ?,"
                "        COALESCE(completed_at, created_at)"
                " FROM sessions WHERE status = 'completed'",
                (CHECKPOINT_VERSION,),
            )

    # ---- sessions / chunks (unchanged) ----

    def create_session(self, rec: dict) -> None:
        with self.lock, self._conn:
            self._conn.execute(
                "INSERT INTO sessions (session_id, file_size, chunk_size, total_chunks,"
                " file_sha256, status, bitmap, expires_at, created_at)"
                " VALUES (:session_id, :file_size, :chunk_size, :total_chunks,"
                " :file_sha256, :status, :bitmap, :expires_at, :created_at)",
                rec,
            )

    def get_session(self, session_id: str) -> dict | None:
        with self.lock:
            row = self._conn.execute(
                "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_sessions(self) -> list[dict]:
        with self.lock:
            rows = self._conn.execute("SELECT * FROM sessions ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    def get_chunk(self, session_id: str, index: int) -> dict | None:
        with self.lock:
            row = self._conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?",
                (session_id, index),
            ).fetchone()
        return dict(row) if row else None

    def list_chunks(self, session_id: str) -> list[dict]:
        with self.lock:
            rows = self._conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
                (session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def insert_chunk_with_bitmap(self, rec: dict, bitmap: bytes) -> None:
        """Record a confirmed chunk and flip its bitmap bit in one transaction."""
        with self.lock, self._conn:
            self._conn.execute(
                "INSERT INTO chunks (session_id, chunk_index, size, sha256, path, received_at)"
                " VALUES (:session_id, :chunk_index, :size, :sha256, :path, :received_at)",
                rec,
            )
            self._conn.execute(
                "UPDATE sessions SET bitmap = ? WHERE session_id = ?",
                (bitmap, rec["session_id"]),
            )

    def delete_chunk(self, session_id: str, index: int) -> None:
        with self.lock, self._conn:
            self._conn.execute(
                "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                (session_id, index),
            )

    def update_bitmap(self, session_id: str, bitmap: bytes) -> None:
        with self.lock, self._conn:
            self._conn.execute(
                "UPDATE sessions SET bitmap = ? WHERE session_id = ?", (bitmap, session_id)
            )

    # ---- finalization protocol ----

    def db_now(self) -> float:
        """Current time according to the database clock (unix seconds)."""
        with self.lock:
            row = self._conn.execute(f"SELECT {DB_NOW_SQL} AS t").fetchone()
        return float(row["t"])

    def get_finalization(self, session_id: str) -> dict | None:
        with self.lock:
            row = self._conn.execute(
                "SELECT * FROM finalization WHERE session_id = ?", (session_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_finalizations(self) -> list[dict]:
        with self.lock:
            rows = self._conn.execute("SELECT * FROM finalization ORDER BY session_id").fetchall()
        return [dict(r) for r in rows]

    def ensure_finalization(self, session_id: str, total_bytes: int, now_iso: str) -> dict:
        """Create the idle row on first use; never touches an existing one."""
        with self.lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO finalization"
                " (session_id, generation, state, confirmed_bytes, total_bytes,"
                "  checkpoint_version, updated_at)"
                " VALUES (?, 0, 'idle', 0, ?, ?, ?)",
                (session_id, total_bytes, CHECKPOINT_VERSION, now_iso),
            )
        return self.get_finalization(session_id)

    def try_acquire_lease(
        self, session_id: str, owner: str, ttl_seconds: float, total_bytes: int, now_iso: str
    ) -> dict | None:
        """Take the lease and bump the fence generation, atomically.

        Succeeds only if no live lease is held; the generation is strictly
        increasing and never reused, so a stale holder can never masquerade as
        the current one.  Returns the fresh row, or None if someone else holds
        a live lease.
        """
        with self.lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE finalization SET"
                f"   generation = generation + 1,"
                f"   lease_owner = :owner,"
                f"   lease_expires_at = {DB_NOW_SQL} + :ttl,"
                f"   state = CASE WHEN state = 'publishing' THEN 'publishing' ELSE 'assembling' END,"
                f"   total_bytes = :total,"
                f"   updated_at = :now"
                f" WHERE session_id = :sid"
                f"   AND state NOT IN ('completed', 'failed')"
                f"   AND (lease_owner IS NULL OR lease_owner = :owner OR lease_expires_at IS NULL"
                f"        OR lease_expires_at <= {DB_NOW_SQL})",
                {"owner": owner, "ttl": ttl_seconds, "total": total_bytes, "now": now_iso, "sid": session_id},
            )
        if cur.rowcount != 1:
            return None
        return self.get_finalization(session_id)

    def advance_checkpoint(
        self,
        session_id: str,
        generation: int,
        owner: str,
        confirmed_bytes: int,
        hasher_state: bytes,
        ttl_seconds: float,
        now_iso: str,
    ) -> bool:
        """Fenced checkpoint commit; also renews the lease by the db clock.

        Only the current generation/owner may advance the checkpoint, and only
        while assembling.  Returns False when the lease was lost.
        """
        with self.lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE finalization SET"
                f"   confirmed_bytes = ?, hasher_state = ?,"
                f"   lease_expires_at = {DB_NOW_SQL} + ?, updated_at = ?"
                f" WHERE session_id = ? AND generation = ? AND lease_owner = ?"
                f"   AND state = 'assembling'",
                (confirmed_bytes, hasher_state, ttl_seconds, now_iso, session_id, generation, owner),
            )
        return cur.rowcount == 1

    def declare_publish_intent(
        self, session_id: str, generation: int, owner: str, final_sha256: str, ttl_seconds: float, now_iso: str
    ) -> bool:
        """Fenced transition assembling -> publishing; recorded before the rename."""
        with self.lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE finalization SET"
                f"   state = 'publishing', publish_intent = 1, final_sha256 = ?,"
                f"   lease_expires_at = {DB_NOW_SQL} + ?, updated_at = ?"
                f" WHERE session_id = ? AND generation = ? AND lease_owner = ?"
                f"   AND state = 'assembling'",
                (final_sha256, ttl_seconds, now_iso, session_id, generation, owner),
            )
        return cur.rowcount == 1

    def complete_finalization(
        self,
        session_id: str,
        generation: int,
        owner: str,
        completed_at: str,
        final_sha256: str,
        artifact_path: str,
        now_iso: str,
    ) -> bool:
        """Mark the session and the finalization completed in one transaction.

        Fenced on the current generation/owner and the publishing state; a
        failed fence rolls the whole transaction back, so the two tables can
        never diverge.
        """
        with self.lock, self._conn:
            cur = self._conn.execute(
                "UPDATE finalization SET"
                "   state = 'completed', confirmed_bytes = total_bytes,"
                "   lease_owner = NULL, lease_expires_at = NULL, last_error = NULL,"
                "   updated_at = ?"
                " WHERE session_id = ? AND generation = ? AND lease_owner = ?"
                "   AND state = 'publishing'",
                (now_iso, session_id, generation, owner),
            )
            if cur.rowcount != 1:
                return False
            self._conn.execute(
                "UPDATE sessions SET status = 'completed', completed_at = ?,"
                " final_sha256 = ?, artifact_path = ? WHERE session_id = ?",
                (completed_at, final_sha256, artifact_path, session_id),
            )
        return True

    def fail_finalization(
        self, session_id: str, generation: int, owner: str, error_json: str, now_iso: str
    ) -> bool:
        """Fenced transition to failed with a replayable structured error."""
        with self.lock, self._conn:
            cur = self._conn.execute(
                "UPDATE finalization SET"
                "   state = 'failed', last_error = ?,"
                "   lease_owner = NULL, lease_expires_at = NULL, updated_at = ?"
                " WHERE session_id = ? AND generation = ? AND lease_owner = ?"
                "   AND state IN ('assembling', 'publishing')",
                (error_json, now_iso, session_id, generation, owner),
            )
        return cur.rowcount == 1

    def force_fail_finalization(self, session_id: str, error_json: str, now_iso: str) -> None:
        """Persist unrecoverable corruption found during crash recovery.

        Terminal states are never overridden; an actively leased worker is
        stopped indirectly because its next fenced write requires the
        assembling/publishing state.
        """
        with self.lock, self._conn:
            self._conn.execute(
                "UPDATE finalization SET"
                "   state = 'failed', last_error = ?,"
                "   lease_owner = NULL, lease_expires_at = NULL, updated_at = ?"
                " WHERE session_id = ? AND state NOT IN ('completed', 'failed')",
                (error_json, now_iso, session_id),
            )

    def close(self) -> None:
        with self.lock:
            self._conn.close()
