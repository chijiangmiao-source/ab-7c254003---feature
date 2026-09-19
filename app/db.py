"""SQLite persistence: sessions (with received bitmap), per-chunk digests and
the crash-safe finalization protocol state (checkpoints, fence generations,
persistent leases)."""

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
"""

# Bumped to 2: finalization protocol metadata.
FINALIZATIONS_DDL = """
CREATE TABLE IF NOT EXISTS finalizations (
    session_id       TEXT PRIMARY KEY,
    schema_version   INTEGER NOT NULL,
    fence_generation INTEGER NOT NULL,
    phase            TEXT NOT NULL,
    confirmed_bytes  INTEGER NOT NULL,
    total_bytes      INTEGER NOT NULL,
    declared_sha256  TEXT NOT NULL,
    hasher_state     TEXT,
    final_digest     TEXT,
    tmp_path         TEXT,
    publish_intent   INTEGER NOT NULL DEFAULT 0,
    lease_owner      TEXT,
    lease_until      INTEGER,
    last_error_code  TEXT,
    last_error       TEXT,
    updated_at       TEXT NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions (session_id) ON DELETE CASCADE
);
"""

CURRENT_USER_VERSION = 2


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
            # Multiple processes may open the same database concurrently
            # (the API plus a taking-over process); wait rather than fail a
            # writer that briefly meets another commit.
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._migrate()
            self._conn.executescript(SCHEMA)
            self._conn.executescript(FINALIZATIONS_DDL)
            self._conn.commit()

    def _migrate(self) -> None:
        """In-place migration; existing sessions/chunks/artifacts are untouched."""
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version < 1:
            self._conn.executescript(SCHEMA)
        if version < CURRENT_USER_VERSION:
            self._conn.executescript(FINALIZATIONS_DDL)
            self._conn.execute(f"PRAGMA user_version = {CURRENT_USER_VERSION}")
        self._conn.commit()

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

    def get_finalization(self, session_id: str) -> dict | None:
        with self.lock:
            row = self._conn.execute(
                "SELECT * FROM finalizations WHERE session_id = ?", (session_id,)
            ).fetchone()
        return dict(row) if row else None

    def create_finalization(self, rec: dict) -> None:
        """Insert the first protocol generation. Idempotent no-op if one exists."""
        cols = (
            "session_id, schema_version, fence_generation, phase, confirmed_bytes,"
            " total_bytes, declared_sha256, tmp_path, updated_at"
        )
        with self.lock, self._conn:
            self._conn.execute(
                f"INSERT OR IGNORE INTO finalizations ({cols})"
                " VALUES (:session_id, :schema_version, :fence_generation, :phase,"
                " :confirmed_bytes, :total_bytes, :declared_sha256, :tmp_path, :updated_at)",
                rec,
            )

    def acquire_finalization(self, session_id: str, owner: str, now_iso: str, lease_ms: int) -> dict | None:
        """Take over a finalization under a new strictly higher fence generation.

        Succeeds when no lease is held (``lease_until`` on the *database* clock
        is in the past, including NULL) and atomically bumps fence_generation,
        stamping the new deadline from the database clock. Returns the new row,
        or ``None`` if another owner still holds a valid lease or the row is
        already terminal.
        """
        with self.lock, self._conn:
            cur = self._conn.execute(
                "UPDATE finalizations SET fence_generation = fence_generation + 1,"
                " lease_owner = ?,"
                " lease_until = CAST(strftime('%s', 'now') AS INTEGER) * 1000 + ?,"
                " phase ="
                " CASE WHEN phase = 'completed' OR phase = 'failed' THEN phase ELSE 'assembling' END,"
                " last_error_code = NULL, last_error = NULL, updated_at = ?"
                " WHERE session_id = ?"
                " AND (lease_until IS NULL"
                "      OR lease_until <= CAST(strftime('%s', 'now') AS INTEGER) * 1000)"
                " AND phase != 'completed' AND phase != 'failed'",
                (owner, lease_ms, now_iso, session_id),
            )
            if cur.rowcount != 1:
                return None
        return self.get_finalization(session_id)

    def renew_lease(self, session_id: str, generation: int, owner: str, lease_ms: int) -> bool:
        """Extend the lease on the database clock.

        Only the current owner/generation whose lease has not yet expired can
        renew; a superseded owner gets False and must stop all work.
        """
        with self.lock, self._conn:
            cur = self._conn.execute(
                "UPDATE finalizations"
                " SET lease_until = CAST(strftime('%s', 'now') AS INTEGER) * 1000 + ?"
                " WHERE session_id = ? AND fence_generation = ? AND lease_owner = ?"
                " AND lease_until > CAST(strftime('%s', 'now') AS INTEGER) * 1000",
                (lease_ms, session_id, generation, owner),
            )
            return cur.rowcount == 1

    def release_lease(self, session_id: str, generation: int, owner: str) -> None:
        with self.lock, self._conn:
            self._conn.execute(
                "UPDATE finalizations SET lease_until = NULL, lease_owner = NULL"
                " WHERE session_id = ? AND fence_generation = ? AND lease_owner = ?",
                (session_id, generation, owner),
            )

    def clear_lease(self, session_id: str) -> None:
        """Drop a dead lease without changing the generation (recovery only)."""
        with self.lock, self._conn:
            self._conn.execute(
                "UPDATE finalizations SET lease_owner = NULL, lease_until = NULL"
                " WHERE session_id = ?",
                (session_id,),
            )

    def save_checkpoint(
        self,
        session_id: str,
        generation: int,
        owner: str,
        confirmed_bytes: int,
        hasher_state: str,
        updated_at: str,
    ) -> bool:
        """Advance the confirmed prefix. Fenced: a stale generation is a no-op."""
        with self.lock, self._conn:
            cur = self._conn.execute(
                "UPDATE finalizations SET confirmed_bytes = ?, hasher_state = ?,"
                " updated_at = ?"
                " WHERE session_id = ? AND fence_generation = ? AND lease_owner = ?"
                " AND phase = 'assembling'",
                (confirmed_bytes, hasher_state, updated_at, session_id, generation, owner),
            )
            return cur.rowcount == 1

    def set_phase(
        self,
        session_id: str,
        generation: int,
        owner: str,
        phase: str,
        updated_at: str,
        *,
        final_digest: str | None = None,
        publish_intent: bool | None = None,
    ) -> bool:
        sets = ["phase = ?", "updated_at = ?"]
        params: list = [phase, updated_at]
        if final_digest is not None:
            sets.append("final_digest = ?")
            params.append(final_digest)
        if publish_intent is not None:
            sets.append("publish_intent = ?")
            params.append(1 if publish_intent else 0)
        params.extend([session_id, generation, owner])
        with self.lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE finalizations SET {', '.join(sets)}"
                " WHERE session_id = ? AND fence_generation = ? AND lease_owner = ?",
                params,
            )
            return cur.rowcount == 1

    def mark_finalization_failed(
        self, session_id: str, generation: int, owner: str, code: str, message: str, updated_at: str
    ) -> bool:
        with self.lock, self._conn:
            cur = self._conn.execute(
                "UPDATE finalizations SET phase = 'failed', last_error_code = ?,"
                " last_error = ?, publish_intent = 0, lease_until = NULL, lease_owner = NULL,"
                " updated_at = ?"
                " WHERE session_id = ? AND fence_generation = ? AND lease_owner = ?",
                (code, message, updated_at, session_id, generation, owner),
            )
            return cur.rowcount == 1

    def mark_finalization_completed(
        self, session_id: str, generation: int, owner: str, digest: str, updated_at: str
    ) -> bool:
        with self.lock, self._conn:
            cur = self._conn.execute(
                "UPDATE finalizations SET phase = 'completed', final_digest = ?,"
                " publish_intent = 0, lease_until = NULL, lease_owner = NULL, updated_at = ?"
                " WHERE session_id = ? AND fence_generation = ? AND lease_owner = ?",
                (digest, updated_at, session_id, generation),
            )
            return cur.rowcount == 1

    def mark_both_completed(
        self,
        session_id: str,
        generation: int,
        owner: str,
        completed_at: str,
        final_sha256: str,
        artifact_path: str,
    ) -> bool:
        """Mark the session and its finalization completed in ONE transaction.

        Collapsing the two updates removes the crash window "artifact renamed
        but the completion transaction was not committed".  Fenced: a stale
        generation/owner cannot flip the state.
        """
        with self.lock, self._conn:
            cur = self._conn.execute(
                "UPDATE finalizations SET phase = 'completed', final_digest = ?,"
                " publish_intent = 0, lease_until = NULL, lease_owner = NULL, updated_at = ?"
                " WHERE session_id = ? AND fence_generation = ? AND lease_owner = ?",
                (final_sha256, completed_at, session_id, generation, owner),
            )
            if cur.rowcount != 1:
                return False
            self._conn.execute(
                "UPDATE sessions SET status = 'completed', completed_at = COALESCE(completed_at, ?),"
                " final_sha256 = ?, artifact_path = ? WHERE session_id = ?",
                (completed_at, final_sha256, artifact_path, session_id),
            )
            return True

    def list_finalizations(self) -> list[dict]:
        with self.lock:
            rows = self._conn.execute(
                "SELECT * FROM finalizations ORDER BY session_id"
            ).fetchall()
        return [dict(r) for r in rows]

    def converge_completed(
        self, session_id: str, completed_at: str, digest: str, artifact_path: str
    ) -> None:
        """Startup recovery: a verified artifact already exists — converge to
        completed without re-assembling.  Session + finalization commit in one
        transaction; any stale lease/intent is discarded."""
        with self.lock, self._conn:
            self._conn.execute(
                "UPDATE finalizations SET phase = 'completed', final_digest = ?,"
                " publish_intent = 0, lease_owner = NULL, lease_until = NULL,"
                " last_error_code = NULL, last_error = NULL, updated_at = ?"
                " WHERE session_id = ?",
                (digest, completed_at, session_id),
            )
            self._conn.execute(
                "UPDATE sessions SET status = 'completed', completed_at = COALESCE(completed_at, ?),"
                " final_sha256 = ?, artifact_path = ? WHERE session_id = ?",
                (completed_at, digest, artifact_path, session_id),
            )

    def converge_failed(self, session_id: str, code: str, message: str, updated_at: str) -> None:
        """Startup recovery: persist an unrecoverable/integrity failure."""
        with self.lock, self._conn:
            self._conn.execute(
                "UPDATE finalizations SET phase = 'failed', last_error_code = ?,"
                " last_error = ?, publish_intent = 0, lease_owner = NULL, lease_until = NULL,"
                " updated_at = ? WHERE session_id = ?",
                (code, message, updated_at, session_id),
            )

    def ensure_completed_finalization(self, rec: dict) -> None:
        """Backfill a protocol row for a session completed before the migration."""
        with self.lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO finalizations (session_id, schema_version,"
                " fence_generation, phase, confirmed_bytes, total_bytes, declared_sha256,"
                " final_digest, publish_intent, updated_at)"
                " VALUES (:session_id, :schema_version, :fence_generation, 'completed',"
                " :confirmed_bytes, :total_bytes, :declared_sha256, :final_digest, 0,"
                " :updated_at)",
                rec,
            )

    def reset_to_assembling(self, session_id: str, updated_at: str) -> None:
        """Discard a stale publishing attempt so assembly can resume (recovery)."""
        with self.lock, self._conn:
            self._conn.execute(
                "UPDATE finalizations SET phase = 'assembling', publish_intent = 0,"
                " lease_owner = NULL, lease_until = NULL, updated_at = ?"
                " WHERE session_id = ? AND phase = 'publishing'",
                (updated_at, session_id),
            )

    def close(self) -> None:
        with self.lock:
            self._conn.close()
