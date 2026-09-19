"""Core upload/resume/finalize logic shared by the HTTP routes.

Finalization is coordinated by a durable, per-session protocol row
(``finalizations`` table): a strictly increasing fence generation, a
database-clock lease and a confirmed-byte checkpoint together make the
operation safe across process crashes and across concurrent takeover.
"""

from __future__ import annotations

import json
import re
import typing
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterable

from . import clock, finalization
from .bitmap import count_set, missing_indices, new_bitmap, set_bit
from .db import Database
from .errors import ApiError
from .finalization import (
    FAULT_KILL_AFTER_INTENT,
    FAULT_KILL_AFTER_LINK,
    FAULT_KILL_AFTER_RENAME,
    FenceLost,
    ProtocolSettings,
    RecoveryBroken,
)
from .schemas import CreateSessionRequest
from .sha256state import ResumableSHA256
from .storage import CheckpointFileError, ChunkStore

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CHECKPOINT_SCHEMA_VERSION = 1
RECOVERY_ERROR_CODE = "FINALIZATION_RECOVERY_ERROR"


class UploadService:
    def __init__(self, db: Database, store: ChunkStore, data_dir: Path):
        self.db = db
        self.store = store
        self.data_dir = data_dir
        self.protocol = ProtocolSettings()

    # ---- sessions ----

    def create_session(self, req: CreateSessionRequest) -> dict:
        now = clock.utcnow()
        if req.expires_at <= now:
            raise ApiError(
                422,
                "SESSION_EXPIRES_IN_PAST",
                "expires_at must be in the future",
                {"expires_at": req.expires_at.isoformat()},
            )
        total = -(-req.file_size // req.chunk_size)  # ceil division
        session_id = uuid.uuid4().hex
        self.db.create_session(
            {
                "session_id": session_id,
                "file_size": req.file_size,
                "chunk_size": req.chunk_size,
                "total_chunks": total,
                "file_sha256": req.file_sha256,
                "status": "active",
                "bitmap": bytes(new_bitmap(total)),
                "expires_at": req.expires_at.isoformat(),
                "created_at": now.isoformat(),
            }
        )
        return self.public_session(self.get_session_or_404(session_id))

    def status(self, session_id: str) -> dict:
        return self.public_session(self.get_session_or_404(session_id))

    # ---- chunks ----

    async def upload_chunk(
        self,
        session_id: str,
        raw_index: str,
        declared_digest: str,
        stream: AsyncIterable[bytes],
    ) -> tuple[dict, int]:
        session = self.get_session_or_404(session_id)
        total = session["total_chunks"]
        try:
            index = int(raw_index)
        except ValueError:
            index = -1
        if index < 0 or index >= total:
            raise ApiError(
                400,
                "CHUNK_INDEX_OUT_OF_RANGE",
                f"chunk index {raw_index!r} is out of range; valid indices are 0..{total - 1}",
                {"chunk_index": raw_index, "total_chunks": total},
            )
        digest = declared_digest.strip().lower()
        if not _SHA256_RE.fullmatch(digest):
            raise ApiError(
                400,
                "INVALID_CHUNK_DIGEST",
                "X-Chunk-SHA256 must be 64 hexadecimal characters",
                {"received": declared_digest},
            )

        tmp, size, actual = await self.store.write_chunk_tmp(session_id, stream)
        committed = False
        try:
            expected = self.expected_chunk_size(session, index)
            if size != expected:
                raise ApiError(
                    400,
                    "CHUNK_SIZE_MISMATCH",
                    f"chunk {index} must be exactly {expected} bytes, got {size}",
                    {"chunk_index": index, "expected_size": expected, "actual_size": size},
                )
            if actual != digest:
                raise ApiError(
                    400,
                    "CHUNK_DIGEST_MISMATCH",
                    "chunk body SHA-256 does not match X-Chunk-SHA256; chunk was discarded",
                    {"chunk_index": index, "declared_sha256": digest, "actual_sha256": actual},
                )
            with self.db.lock:
                existing = self.db.get_chunk(session_id, index)
                if existing is not None:
                    if existing["sha256"] == digest:
                        return self._chunk_receipt(session, existing, duplicate=True), 200
                    raise ApiError(
                        409,
                        "CHUNK_CONFLICT",
                        "chunk index already holds different content; the stored chunk is unchanged",
                        {
                            "chunk_index": index,
                            "stored_sha256": existing["sha256"],
                            "rejected_sha256": digest,
                        },
                    )
                if self.is_expired(session):
                    raise ApiError(
                        410,
                        "SESSION_EXPIRED",
                        "session has expired; new chunks are rejected",
                        {"expires_at": session["expires_at"]},
                    )
                if session["status"] != "active":
                    raise ApiError(
                        409,
                        "SESSION_ALREADY_COMPLETED",
                        "session is already completed and immutable",
                    )
                final_path = self.store.chunk_path(session_id, index)
                self.store.commit_tmp(tmp, final_path)
                committed = True
                record = {
                    "session_id": session_id,
                    "chunk_index": index,
                    "size": size,
                    "sha256": digest,
                    "path": str(final_path),
                    "received_at": clock.utcnow().isoformat(),
                }
                bitmap = bytearray(session["bitmap"])
                set_bit(bitmap, index)
                session["bitmap"] = bytes(bitmap)
                self.db.insert_chunk_with_bitmap(record, session["bitmap"])
                return self._chunk_receipt(session, record, duplicate=False), 201
        finally:
            if not committed:
                self.store.discard(tmp)

    # ---- finalize ----

    def finalize(self, session_id: str) -> dict:
        session = self.get_session_or_404(session_id)

        row = self.db.get_finalization(session_id)
        if row is not None:
            if row["phase"] == "completed":
                return self._completed_receipt_or_raise(session, row)
            if row["phase"] == "failed":
                self._raise_stored_failure(row)

        if session["status"] == "completed" and self.store.artifact_path(session_id).exists():
            return self._finalize_receipt(session)

        total = session["total_chunks"]
        missing = missing_indices(session["bitmap"], total)
        if missing:
            details = {
                "missing_chunks": missing,
                "received_count": total - len(missing),
                "total_chunks": total,
            }
            if self.is_expired(session):
                raise ApiError(
                    410,
                    "SESSION_EXPIRED",
                    "session expired with chunks still missing",
                    {**details, "expires_at": session["expires_at"]},
                )
            raise ApiError(409, "CHUNKS_INCOMPLETE", "cannot finalize; chunks are missing", details)

        paths, lost = [], []
        for i in range(total):
            path = self.store.chunk_path(session_id, i)
            if path.exists():
                paths.append(path)
            else:
                lost.append(i)
        if lost:
            raise ApiError(
                409,
                "CHUNKS_INCOMPLETE",
                "chunk files are missing on disk",
                {"missing_chunks": lost, "total_chunks": total},
            )

        # First call: create the protocol row at generation 0 with no lease.
        if row is None:
            self.db.create_finalization(
                {
                    "session_id": session_id,
                    "schema_version": CHECKPOINT_SCHEMA_VERSION,
                    "fence_generation": 0,
                    "phase": "assembling",
                    "confirmed_bytes": 0,
                    "total_bytes": session["file_size"],
                    "declared_sha256": session["file_sha256"],
                    "tmp_path": str(self.store.finalize_tmp_path(session_id)),
                    "updated_at": clock.utcnow().isoformat(),
                }
            )

        owner = uuid.uuid4().hex
        lease_ms = int(self.protocol.lease_seconds * 1000)
        acquired = self.db.acquire_finalization(
            session_id,
            owner,
            clock.utcnow().isoformat(),
            lease_ms,
        )
        if acquired is None:
            raise self._in_progress_error(self.db.get_finalization(session_id))

        generation = acquired["fence_generation"]
        try:
            return self._run_finalization(session, acquired, owner, paths)
        except FenceLost:
            current = self.db.get_finalization(session_id)
            if current is not None and current["phase"] == "completed":
                return self._completed_receipt_or_raise(session, current)
            if current is not None and current["phase"] == "failed":
                self._raise_stored_failure(current)
            raise self._in_progress_error(current)
        except ApiError:
            # Structured outcomes (422, recovery failure) already settled the
            # row and released/cleared the lease inside _run_finalization.
            raise
        except BaseException:
            # An in-process failure (not a kill) must not pin the lease until
            # it times out; a crash obviously cannot run this.
            self.db.release_lease(session_id, generation, owner)
            raise

    def _run_finalization(self, session: dict, row: dict, owner: str, paths: list[Path]) -> dict:
        session_id = session["session_id"]
        generation = row["fence_generation"]

        def renew() -> None:
            if not self.db.renew_lease(
                session_id,
                generation,
                owner,
                int(self.protocol.lease_seconds * 1000),
            ):
                raise FenceLost()

        def save_checkpoint(confirmed: int, state: str) -> bool:
            return self.db.save_checkpoint(
                session_id,
                generation,
                owner,
                confirmed,
                state,
                clock.utcnow().isoformat(),
            )

        # Crash/takeover window "artifact renamed, completion txn not yet
        # committed": a verified artifact at the final path converges
        # directly — never re-assembled.
        artifact = self.store.artifact_path(session_id)
        if artifact.exists() and self._artifact_matches(session, artifact):
            completed_at = clock.utcnow().isoformat()
            if self.db.mark_both_completed(
                session_id, generation, owner, completed_at, session["file_sha256"], str(artifact)
            ):
                self._post_publish_cleanup(session_id, generation)
                return self._finalize_receipt(self.get_session_or_404(session_id))
            raise FenceLost()

        gen_tmp: Path | None = None
        size = digest = 0
        try:
            confirmed = row["confirmed_bytes"]
            previous_tmp, previous_gen = self._latest_existing_tmp(session_id, generation)
            if confirmed and previous_tmp is None:
                raise RecoveryBroken(
                    RECOVERY_ERROR_CODE,
                    "finalization temp file is missing but the checkpoint confirms bytes",
                    {"reason": "tmp_missing", "confirmed_bytes": confirmed},
                )
            if previous_tmp is not None:
                self._validate_tmp_prefix(previous_tmp, confirmed)
            if previous_tmp is None:
                source_tmp = self.store.finalize_tmp_path(session_id)
            else:
                source_tmp = previous_tmp
            if previous_gen == generation:
                # This same generation already owns a temp (e.g. resuming
                # within one call after a dead publishing attempt): reuse it;
                # open_generation_tmp drops any unconfirmed tail.
                gen_tmp = source_tmp
            else:
                # Takeover or restart: clone only the confirmed prefix into a
                # fresh generation-owned inode so stale owners can't interleave.
                gen_tmp = finalization.clone_confirmed_prefix(
                    self.store, session_id, source_tmp, generation, confirmed
                )
            self._validate_hasher_state(row["hasher_state"], confirmed)

            size, digest = finalization.stream_assemble(
                data_dir=self.data_dir,
                session=session,
                source_paths=paths,
                tmp_path=gen_tmp,
                confirmed_bytes=confirmed,
                hasher_state=row["hasher_state"],
                settings=self.protocol,
                renew=renew,
                save_checkpoint=save_checkpoint,
                fault_session_id=session_id,
                gate=finalization.gate_for(self.data_dir),
            )
        except RecoveryBroken as exc:
            self._record_recovery_failure(session_id, generation, owner, exc)
        except CheckpointFileError as exc:
            self._record_recovery_failure(
                session_id,
                generation,
                owner,
                RecoveryBroken(
                    RECOVERY_ERROR_CODE,
                    "finalization temp file does not match the persisted checkpoint",
                    {
                        "reason": exc.reason,
                        "confirmed_bytes": exc.expected,
                        "tmp_bytes": exc.actual,
                    },
                ),
            )

        if size != session["file_size"] or digest != session["file_sha256"]:
            details = {
                "declared_sha256": session["file_sha256"],
                "assembled_sha256": digest,
                "declared_size": session["file_size"],
                "assembled_size": size,
            }
            self._fail_finalization(
                session_id,
                generation,
                owner,
                "INTEGRITY_MISMATCH",
                "assembled file does not match the declared SHA-256; uploaded chunks are kept",
                details,
            )
            self.store.discard(gen_tmp)
            self.store.remove_candidate(self.store.candidate_path(session_id, generation))
            finalization.cleanup_old_generations(self.store, session_id, generation)
            raise ApiError(422, "INTEGRITY_MISMATCH",
                           "assembled file does not match the declared SHA-256; uploaded chunks are kept",
                           details)

        # ---- publishing: every window below is crash-recoverable ----
        if not self.db.set_phase(
            session_id, generation, owner, "publishing", clock.utcnow().isoformat()
        ):
            raise FenceLost()

        candidate = self.store.candidate_path(session_id, generation)
        self.store.link_candidate(gen_tmp, candidate)
        finalization.fire_kill(self.data_dir, FAULT_KILL_AFTER_LINK, session_id)

        if not self.db.set_phase(
            session_id,
            generation,
            owner,
            "publishing",
            clock.utcnow().isoformat(),
            publish_intent=True,
        ):
            raise FenceLost()
        finalization.fire_kill(self.data_dir, FAULT_KILL_AFTER_INTENT, session_id)

        final = self.store.commit_candidate(candidate, session_id)
        finalization.fire_kill(self.data_dir, FAULT_KILL_AFTER_RENAME, session_id)

        completed_at = clock.utcnow().isoformat()
        if not self.db.mark_both_completed(
            session_id, generation, owner, completed_at, digest, str(final)
        ):
            # Renamed but our transaction was fenced out: the new owner is
            # responsible for converging; the artifact bytes are identical.
            raise FenceLost()

        self.store.remove_finalize_tmp(session_id)
        self.store.discard(gen_tmp)
        finalization.cleanup_old_generations(self.store, session_id, generation)
        return self._finalize_receipt(self.get_session_or_404(session_id))

    # ---- read-only finalization progress ----

    def finalization_status(self, session_id: str) -> dict:
        self.get_session_or_404(session_id)
        row = self.db.get_finalization(session_id)
        if row is None:
            return {
                "session_id": session_id,
                "state": "idle",
                "confirmed_bytes": 0,
                "total_bytes": 0,
                "generation": 0,
                "last_error": None,
            }
        last_error = None
        if row["last_error_code"]:
            try:
                payload = json.loads(row["last_error"] or "{}")
            except (ValueError, TypeError):
                payload = {}
            last_error = {
                "code": row["last_error_code"],
                "message": payload.get("message", row["last_error"]),
                "details": payload.get("details", {}),
            }
        return {
            "session_id": session_id,
            "state": row["phase"],
            "confirmed_bytes": row["confirmed_bytes"],
            "total_bytes": row["total_bytes"],
            "generation": row["fence_generation"],
            "last_error": last_error,
        }

    # ---- artifact ----

    def artifact_file(self, session_id: str) -> tuple[Path, str]:
        session = self.get_session_or_404(session_id)
        path = self.store.artifact_path(session_id)
        if session["status"] != "completed" or not path.exists():
            raise ApiError(
                409,
                "ARTIFACT_NOT_READY",
                "no published artifact for this session",
                {"status": self._derived_status(session)},
            )
        return path, session["final_sha256"]

    # ---- helpers ----

    def get_session_or_404(self, session_id: str) -> dict:
        session = self.db.get_session(session_id)
        if session is None:
            raise ApiError(
                404,
                "SESSION_NOT_FOUND",
                f"no such session: {session_id}",
                {"session_id": session_id},
            )
        return session

    @staticmethod
    def expected_chunk_size(session: dict, index: int) -> int:
        if index == session["total_chunks"] - 1:
            return session["file_size"] - session["chunk_size"] * (session["total_chunks"] - 1)
        return session["chunk_size"]

    @staticmethod
    def is_expired(session: dict) -> bool:
        return clock.utcnow() >= datetime.fromisoformat(session["expires_at"])

    def _derived_status(self, session: dict) -> str:
        if session["status"] == "completed":
            return "completed"
        return "expired" if self.is_expired(session) else "active"

    def public_session(self, session: dict) -> dict:
        total = session["total_chunks"]
        missing = missing_indices(session["bitmap"], total)
        status = self._derived_status(session)
        return {
            "session_id": session["session_id"],
            "status": status,
            "file_size": session["file_size"],
            "chunk_size": session["chunk_size"],
            "total_chunks": total,
            "file_sha256": session["file_sha256"],
            "received_count": total - len(missing),
            "missing_chunks": missing,
            "expires_at": session["expires_at"],
            "created_at": session["created_at"],
            "completed_at": session["completed_at"],
            "final_sha256": session["final_sha256"],
            "artifact_url": f"/sessions/{session['session_id']}/artifact" if status == "completed" else None,
        }

    def _chunk_receipt(self, session: dict, record: dict, duplicate: bool) -> dict:
        total = session["total_chunks"]
        return {
            "session_id": session["session_id"],
            "chunk_index": record["chunk_index"],
            "size": record["size"],
            "sha256": record["sha256"],
            "duplicate": duplicate,
            "received_count": count_set(session["bitmap"], total),
            "total_chunks": total,
        }

    @staticmethod
    def _finalize_receipt(session: dict) -> dict:
        session_id = session["session_id"]
        return {
            "session_id": session_id,
            "status": "completed",
            "file_size": session["file_size"],
            "final_sha256": session["final_sha256"],
            "artifact_size": session["file_size"],
            "artifact_url": f"/sessions/{session_id}/artifact",
            "completed_at": session["completed_at"],
        }

    # ---- finalization internals ----

    def _latest_existing_tmp(self, session_id: str, below_generation: int) -> tuple[Path | None, int]:
        best: Path | None = None
        best_gen = -1
        for entry in self.store.finalize_dir.glob(f"{session_id}.g*.tmp"):
            try:
                gen = int(entry.name.split(".g", 1)[1].split(".", 1)[0])
            except (IndexError, ValueError):
                continue
            if gen < below_generation and gen > best_gen and entry.exists():
                best, best_gen = entry, gen
        if best is not None:
            return best, best_gen
        stable = self.store.finalize_tmp_path(session_id)
        return (stable, -1) if stable.exists() else (None, -1)

    @staticmethod
    def _validate_tmp_prefix(tmp: Path, confirmed: int) -> None:
        actual = tmp.stat().st_size
        if actual < confirmed:
            raise RecoveryBroken(
                RECOVERY_ERROR_CODE,
                "finalization temp file is shorter than the confirmed checkpoint",
                {
                    "reason": "tmp_shorter_than_checkpoint",
                    "confirmed_bytes": confirmed,
                    "tmp_bytes": actual,
                },
            )

    @staticmethod
    def _validate_hasher_state(state: str | None, confirmed: int) -> None:
        if not state:
            if confirmed:
                raise RecoveryBroken(
                    RECOVERY_ERROR_CODE,
                    "checkpoint confirms bytes but records no SHA-256 state",
                    {"reason": "missing_hasher_state", "confirmed_bytes": confirmed},
                )
            return
        try:
            hasher = ResumableSHA256.from_state(state)
        except ValueError as exc:
            raise RecoveryBroken(
                RECOVERY_ERROR_CODE,
                "persisted SHA-256 checkpoint is unreadable or of an unknown version",
                {"reason": "bad_hasher_state", "detail": str(exc)},
            ) from exc
        if hasher.length != confirmed:
            raise RecoveryBroken(
                RECOVERY_ERROR_CODE,
                "checkpoint byte count disagrees with the persisted SHA-256 state",
                {
                    "reason": "hasher_length_mismatch",
                    "confirmed_bytes": confirmed,
                    "hasher_bytes": hasher.length,
                },
            )

    def _record_recovery_failure(
        self, session_id: str, generation: int, owner: str, exc: RecoveryBroken
    ) -> typing.NoReturn:
        ok = self.db.mark_finalization_failed(
            session_id,
            generation,
            owner,
            exc.code,
            json.dumps({"message": exc.message, "details": exc.details}),
            clock.utcnow().isoformat(),
        )
        if not ok:
            self.db.converge_failed(
                session_id,
                exc.code,
                json.dumps({"message": exc.message, "details": exc.details}),
                clock.utcnow().isoformat(),
            )
        # Never leave a downloadable artifact behind a failed finalization.
        self.store.discard(self.store.artifact_path(session_id))
        raise ApiError(500, exc.code, exc.message, exc.details)

    def _fail_finalization(
        self, session_id: str, generation: int, owner: str, code: str, message: str, details: dict
    ) -> None:
        self.db.mark_finalization_failed(
            session_id,
            generation,
            owner,
            code,
            json.dumps({"message": message, "details": details}),
            clock.utcnow().isoformat(),
        )

    def _raise_stored_failure(self, row: dict) -> typing.NoReturn:
        code = row["last_error_code"] or "FINALIZATION_FAILED"
        try:
            payload = json.loads(row["last_error"] or "{}")
        except (ValueError, TypeError):
            payload = {}
        message = payload.get("message", "finalization previously failed")
        details = payload.get("details", {})
        status = 422 if code == "INTEGRITY_MISMATCH" else 500
        raise ApiError(status, code, message, details)

    def _completed_receipt_or_raise(self, session: dict, row: dict) -> dict:
        final = self.store.artifact_path(session["session_id"])
        if not final.exists():
            raise ApiError(
                500,
                RECOVERY_ERROR_CODE,
                "finalization is recorded as completed but the artifact is missing",
                {"reason": "artifact_missing"},
            )
        refreshed = self.get_session_or_404(session["session_id"])
        if refreshed["status"] != "completed":
            # Defensive: converge the session row to the verified final state.
            self.db.converge_completed(
                session["session_id"], clock.utcnow().isoformat(),
                row["final_digest"], str(final),
            )
            refreshed = self.get_session_or_404(session["session_id"])
        return self._finalize_receipt(refreshed)

    def _in_progress_error(self, row: dict | None) -> ApiError:
        details: dict = {"generation": row["fence_generation"] if row else 0}
        if row is not None:
            details["phase"] = row["phase"]
            lease_until = row["lease_until"]
            if lease_until is not None:
                now_ms = clock.utcnow().timestamp() * 1000
                wait_seconds = max(0.0, (lease_until - now_ms) / 1000.0)
                retry_at = datetime.fromtimestamp(lease_until / 1000.0, tz=timezone.utc)
                details["retry_at"] = retry_at.isoformat()
                details["retry_after_seconds"] = round(wait_seconds, 3)
        return ApiError(
            409,
            "FINALIZATION_IN_PROGRESS",
            "another finalization attempt holds the lease for this session",
            details,
        )

    def _post_publish_cleanup(self, session_id: str, generation: int) -> None:
        self.store.remove_finalize_tmp(session_id)
        for tmp in self.store.finalize_dir.glob(f"{session_id}.g*.tmp"):
            self.store.discard(tmp)
        finalization.cleanup_old_generations(self.store, session_id, generation)

    def _artifact_matches(self, session: dict, path: Path) -> bool:
        try:
            size, digest = self.store.hash_file(path)
        except OSError:
            return False
        return size == session["file_size"] and digest == session["file_sha256"]


def reconcile(db: Database, store: ChunkStore, data_dir: Path) -> None:
    """Rebuild durable state after a (possibly unclean) restart.

    - chunk rows whose files vanished or have a wrong size are dropped;
    - chunk files without a matching row (crashed before commit) are removed;
    - the persisted bitmap is rebuilt from the surviving rows;
    - leftover chunk temp files are removed;
    - finalization protocol rows are converged across every crash window
      (assembly checkpoint, publish intent, atomic rename, completion txn).
    """
    for session in db.list_sessions():
        session_id = session["session_id"]
        confirmed: set[int] = set()
        for row in db.list_chunks(session_id):
            path = Path(row["path"])
            if path.exists() and path.stat().st_size == row["size"]:
                confirmed.add(row["chunk_index"])
            else:
                db.delete_chunk(session_id, row["chunk_index"])
        chunk_dir = store.chunk_dir(session_id)
        if chunk_dir.exists():
            for entry in chunk_dir.iterdir():
                if entry.suffix == ".tmp":
                    entry.unlink()
                elif entry.suffix == ".chunk":
                    try:
                        index = int(entry.stem)
                    except ValueError:
                        entry.unlink()
                        continue
                    if index not in confirmed:
                        entry.unlink()
        bitmap = new_bitmap(session["total_chunks"])
        for index in confirmed:
            set_bit(bitmap, index)
        db.update_bitmap(session_id, bytes(bitmap))
    store.purge_orphan_temps()
    recover_finalizations(db, store)


def recover_finalizations(db: Database, store: ChunkStore) -> None:
    """Crash-window convergence for every persisted finalization row.

    Only a file whose length AND sha-256 match the archived values can be
    converged to ``completed``; nothing half-written is ever exposed for
    download and a completed rename never triggers re-assembly.
    """
    existing = {row["session_id"] for row in db.list_finalizations()}
    for session in db.list_sessions():
        # Backfill metadata for sessions completed before the in-place
        # migration: historical artifacts stay queryable and downloadable, no
        # re-upload and no re-assembly.
        if session["session_id"] not in existing and session["status"] == "completed":
            artifact = store.artifact_path(session["session_id"])
            if artifact.exists() and _matches(session, artifact):
                db.ensure_completed_finalization(
                    {
                        "session_id": session["session_id"],
                        "schema_version": CHECKPOINT_SCHEMA_VERSION,
                        "fence_generation": 0,
                        "confirmed_bytes": session["file_size"],
                        "total_bytes": session["file_size"],
                        "declared_sha256": session["file_sha256"],
                        "final_digest": session["final_sha256"] or session["file_sha256"],
                        "updated_at": clock.utcnow().isoformat(),
                    }
                )

    for row in db.list_finalizations():
        session = db.get_session(row["session_id"])
        if session is None:
            continue
        sid = row["session_id"]
        phase = row["phase"]
        now = clock.utcnow().isoformat()
        artifact = store.artifact_path(sid)

        if phase == "completed":
            if artifact.exists() and _matches(session, artifact):
                if session["status"] != "completed":
                    db.converge_completed(sid, now, row["final_digest"], str(artifact))
                # Crash between the completion commit and file cleanup.
                store.discard_finalization_temps(sid)
                for cand in store.artifacts_dir.glob(f".{sid}.g*.cand"):
                    store.remove_candidate(cand)
                continue
            # Recorded completed without a matching artifact: do not guess.
            db.converge_failed(
                sid,
                RECOVERY_ERROR_CODE,
                json.dumps(
                    {
                        "message": "completed artifact is missing or corrupt",
                        "details": {"reason": "artifact_missing_or_corrupt"},
                    }
                ),
                now,
            )
            continue

        if phase == "failed":
            # A failed finalization must never have a downloadable artifact,
            # and stale assembly/publish files must not be mistaken for state.
            if artifact.exists():
                store.discard(artifact)
            candidate = store.candidate_path(sid, row["fence_generation"])
            store.remove_candidate(candidate)
            continue

        if phase == "publishing":
            _recover_publishing(db, store, session, row, now)
            continue

        if phase == "assembling":
            _recover_assembling(db, store, row, now)


def _matches(session: dict, path: Path) -> bool:
    try:
        size, digest = ChunkStore.hash_file(path)
    except OSError:
        return False
    return size == session["file_size"] and digest == session["file_sha256"]


def _recover_publishing(db: Database, store: ChunkStore, session: dict, row: dict, now: str) -> None:
    sid = row["session_id"]
    gen = row["fence_generation"]
    candidate = store.candidate_path(sid, gen)
    artifact = store.artifact_path(sid)

    if artifact.exists():
        if _matches(session, artifact):
            # Renamed (intent or not) and bytes verified: converge, no rebuild.
            db.converge_completed(sid, now, session["file_sha256"], str(artifact))
            store.remove_candidate(candidate)
            store.remove_finalize_tmp(sid)
            return
        # A file at the download path that does not match the archive values
        # must never be served; move it aside and resume from the checkpoint.
        store.discard(artifact)

    if candidate.exists() and _matches(session, candidate):
        final = store.commit_candidate(candidate, sid)
        db.converge_completed(sid, now, session["file_sha256"], str(final))
        store.remove_finalize_tmp(sid)
        return

    # No verified publishable file: resume assembly from the last checkpoint;
    # the assembled temp and resumable digest state are still in place.
    store.remove_candidate(candidate)
    _recover_assembling(db, store, row, now, allow_reset=True)


def _recover_assembling(
    db: Database, store: ChunkStore, row: dict, now: str, *, allow_reset: bool = False
) -> None:
    sid = row["session_id"]
    confirmed = row["confirmed_bytes"]
    tmp = _latest_existing_tmp(store, sid)

    try:
        if row["schema_version"] != CHECKPOINT_SCHEMA_VERSION:
            raise RecoveryBroken(
                RECOVERY_ERROR_CODE,
                f"unknown checkpoint schema version: {row['schema_version']}",
                {"reason": "unknown_checkpoint_version", "schema_version": row["schema_version"]},
            )
        if confirmed:
            if tmp is None:
                raise RecoveryBroken(
                    RECOVERY_ERROR_CODE,
                    "finalization temp file is missing but the checkpoint confirms bytes",
                    {"reason": "tmp_missing", "confirmed_bytes": confirmed},
                )
            actual = tmp.stat().st_size
            if actual < confirmed:
                raise RecoveryBroken(
                    RECOVERY_ERROR_CODE,
                    "finalization temp file is shorter than the confirmed checkpoint",
                    {
                        "reason": "tmp_shorter_than_checkpoint",
                        "confirmed_bytes": confirmed,
                        "tmp_bytes": actual,
                    },
                )
        state = row["hasher_state"]
        if state:
            hasher = ResumableSHA256.from_state(state)  # raises ValueError if corrupt
            if hasher.length != confirmed:
                raise RecoveryBroken(
                    RECOVERY_ERROR_CODE,
                    "checkpoint byte count disagrees with the persisted SHA-256 state",
                    {
                        "reason": "hasher_length_mismatch",
                        "confirmed_bytes": confirmed,
                        "hasher_bytes": hasher.length,
                    },
                )
        elif confirmed:
            raise RecoveryBroken(
                RECOVERY_ERROR_CODE,
                "checkpoint confirms bytes but records no SHA-256 state",
                {"reason": "missing_hasher_state", "confirmed_bytes": confirmed},
            )
    except RecoveryBroken as exc:
        db.converge_failed(sid, exc.code,
                           json.dumps({"message": exc.message, "details": exc.details}), now)
        return
    except ValueError as exc:
        db.converge_failed(
            sid,
            RECOVERY_ERROR_CODE,
            json.dumps(
                {
                    "message": "persisted SHA-256 checkpoint is unreadable or of an unknown version",
                    "details": {"reason": "bad_hasher_state", "detail": str(exc)},
                }
            ),
            now,
        )
        return

    # State is consistent: any process death left a dead lease behind; clear it
    # so the next request acquires immediately, and keep assembling (or reset a
    # stale publishing attempt that was passed in from _recover_publishing).
    if allow_reset:
        db.reset_to_assembling(sid, now)
    else:
        db.clear_lease(sid)


def _latest_existing_tmp(store: ChunkStore, session_id: str) -> Path | None:
    best: Path | None = None
    best_gen = -1
    for entry in store.finalize_dir.glob(f"{session_id}.g*.tmp"):
        try:
            gen = int(entry.name.split(".g", 1)[1].split(".", 1)[0])
        except (IndexError, ValueError):
            continue
        if gen > best_gen and entry.exists():
            best, best_gen = entry, gen
    if best is not None:
        return best
    stable = store.finalize_tmp_path(session_id)
    return stable if stable.exists() else None
