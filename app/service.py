"""Core upload/resume/finalize logic shared by the HTTP routes.

Finalization is a crash-resumable, lease-fenced protocol:

- assembly streams chunk files onto a deterministic temp file, persisting a
  checkpoint (confirmed byte count + serializable SHA-256 state) only after
  the corresponding output bytes are fsynced;
- a persistent lease (renewed by the database clock) and a strictly
  increasing fence generation coordinate concurrent finalizers — a stale
  generation can no longer advance checkpoints, publish, or complete;
- the publish phase records its intent before the atomic rename, and the
  completion transaction covers both tables, so every crash window converges
  on the next call without re-assembling or exposing half-finished output.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterable, Callable

from . import clock
from .bitmap import count_set, missing_indices, new_bitmap, set_bit
from .db import CHECKPOINT_VERSION, Database
from .errors import ApiError
from .resumable_hash import ResumableSha256
from .schemas import CreateSessionRequest
from .storage import ChunkStore

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# finalization states (persisted)
F_IDLE = "idle"
F_ASSEMBLING = "assembling"
F_PUBLISHING = "publishing"
F_COMPLETED = "completed"
F_FAILED = "failed"


class LeaseLost(Exception):
    """A fenced write matched no row: another generation took over."""


class UploadService:
    def __init__(
        self,
        db: Database,
        store: ChunkStore,
        *,
        checkpoint_bytes: int = 1 << 20,
        lease_ttl_seconds: float = 10.0,
        checkpoint_hook: Callable[[str, int], None] | None = None,
    ):
        self.db = db
        self.store = store
        self.checkpoint_bytes = checkpoint_bytes
        self.lease_ttl_seconds = lease_ttl_seconds
        # test/observability hook invoked after every committed checkpoint
        self._checkpoint_hook = checkpoint_hook

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

    # ---- finalize / artifact ----

    def finalize(self, session_id: str) -> dict:
        session = self.get_session_or_404(session_id)
        if session["status"] == "completed" and self.store.artifact_path(session_id).exists():
            return self._finalize_receipt(session)

        fin = self.db.ensure_finalization(session_id, session["file_size"], self._now_iso())

        if fin["state"] == F_FAILED:
            # integrity failures and recovery errors are deterministic: replay
            raise self._stored_error(fin)
        if fin["state"] == F_COMPLETED:
            # The completion transaction covers both tables, so reaching here
            # means the artifact or the session row was lost out of band.
            raise self._recovery_error(
                session, fin, "finalization is completed but the session row or artifact is missing"
            )

        self._validate_checkpoint(session, fin)

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

        lost = [i for i in range(total) if not self.store.chunk_path(session_id, i).exists()]
        if lost:
            raise ApiError(
                409,
                "CHUNKS_INCOMPLETE",
                "chunk files are missing on disk",
                {"missing_chunks": lost, "total_chunks": total},
            )

        owner = uuid.uuid4().hex
        fin = self.db.try_acquire_lease(
            session_id, owner, self.lease_ttl_seconds, session["file_size"], self._now_iso()
        )
        if fin is None:
            raise self._in_progress(session_id)
        try:
            if fin["state"] == F_PUBLISHING and fin["publish_intent"]:
                self._converge_publish(session, fin, owner)
            else:
                digest = self._assemble(session, fin, owner)
                self._publish(session, fin, owner, digest)
        except LeaseLost:
            raise self._in_progress(session_id)
        return self._finalize_receipt(self.get_session_or_404(session_id))

    def finalization_status(self, session_id: str) -> dict:
        """Read-only view of the durable finalization state (never regresses)."""
        session = self.get_session_or_404(session_id)
        fin = self.db.get_finalization(session_id)
        if fin is None:
            completed = session["status"] == "completed"
            return {
                "session_id": session_id,
                "state": F_COMPLETED if completed else F_IDLE,
                "confirmed_bytes": session["file_size"] if completed else 0,
                "total_bytes": session["file_size"],
                "generation": 0,
                "last_error": None,
                "lease_expires_at": None,
                "updated_at": None,
            }
        last_error = None
        if fin["last_error"]:
            try:
                payload = json.loads(fin["last_error"])
                last_error = {
                    "code": payload["code"],
                    "message": payload["message"],
                    "details": payload.get("details") or {},
                }
            except (ValueError, KeyError, TypeError):
                last_error = {"code": "UNREADABLE_ERROR", "message": str(fin["last_error"]), "details": {}}
        lease_expires_at = None
        if fin["lease_expires_at"] is not None:
            lease_expires_at = datetime.fromtimestamp(fin["lease_expires_at"], tz=timezone.utc).isoformat()
        return {
            "session_id": session_id,
            "state": fin["state"],
            "confirmed_bytes": fin["confirmed_bytes"],
            "total_bytes": fin["total_bytes"],
            "generation": fin["generation"],
            "last_error": last_error,
            "lease_expires_at": lease_expires_at,
            "updated_at": fin["updated_at"],
        }

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

    # ---- finalize internals ----

    def _validate_checkpoint(self, session: dict, fin: dict) -> None:
        """Read-only validation of the recoverable state.

        Anything unknown or inconsistent becomes a persisted ``failed`` state
        with a structured recovery error; progress is never guessed.
        """
        state = fin["state"]
        if state == F_IDLE:
            return
        if fin["checkpoint_version"] != CHECKPOINT_VERSION:
            raise self._recovery_error(
                session, fin, f"unknown checkpoint version {fin['checkpoint_version']}"
            )
        confirmed = fin["confirmed_bytes"]
        total = session["file_size"]
        if confirmed < 0 or confirmed > total:
            raise self._recovery_error(
                session, fin, f"confirmed_bytes {confirmed} is outside 0..{total}"
            )
        if confirmed > 0:
            try:
                ResumableSha256.from_state(fin["hasher_state"] or b"")
            except ValueError as exc:
                raise self._recovery_error(session, fin, f"hasher checkpoint is unreadable: {exc}")
        tmp = self.store.finalize_tmp_path(session["session_id"])
        if state == F_ASSEMBLING:
            if confirmed > 0:
                if not tmp.exists():
                    raise self._recovery_error(session, fin, "finalization temp file is missing")
                if tmp.stat().st_size < confirmed:
                    raise self._recovery_error(
                        session,
                        fin,
                        "finalization temp file is shorter than the committed checkpoint",
                    )
        elif state == F_PUBLISHING:
            if not fin["publish_intent"] or not fin["final_sha256"]:
                raise self._recovery_error(
                    session, fin, "publishing state without a recorded publish intent"
                )
            if tmp.exists():
                if tmp.stat().st_size != total:
                    raise self._recovery_error(
                        session, fin, "finalization temp file size does not match the declared file size"
                    )
            elif not self.store.artifact_path(session["session_id"]).exists():
                raise self._recovery_error(
                    session, fin, "publish intent recorded but both temp file and artifact are missing"
                )

    def _assemble(self, session: dict, fin: dict, owner: str) -> str:
        """Stream the remaining source bytes onto the checkpointed temp file.

        Resumes exactly at ``confirmed_bytes``: the confirmed prefix is never
        truncated or re-read; only the unconfirmed tail (written but not yet
        checkpointed) is discarded before appending.
        """
        session_id = session["session_id"]
        total = session["file_size"]
        confirmed = fin["confirmed_bytes"]
        hasher = ResumableSha256.from_state(fin["hasher_state"]) if confirmed else ResumableSha256()
        tmp = self.store.finalize_tmp_path(session_id)
        generation = fin["generation"]
        offset = confirmed
        with open(tmp, "r+b" if tmp.exists() else "wb") as out:
            out.truncate(confirmed)  # drop only the unconfirmed tail
            out.seek(confirmed)
            since_checkpoint = 0
            for block in self.store.read_chunks_from(session_id, session["chunk_size"], total, offset):
                out.write(block)
                hasher.update(block)
                offset += len(block)
                since_checkpoint += len(block)
                if since_checkpoint >= self.checkpoint_bytes:
                    self._commit_checkpoint(session_id, generation, owner, out, offset, hasher)
                    since_checkpoint = 0
            # final checkpoint: every output byte is durable before this commit
            self._commit_checkpoint(session_id, generation, owner, out, offset, hasher)
        digest = hasher.hexdigest()
        if offset != total or digest != session["file_sha256"]:
            error = {
                "status_code": 422,
                "code": "INTEGRITY_MISMATCH",
                "message": "assembled file does not match the declared SHA-256; uploaded chunks are kept",
                "details": {
                    "declared_sha256": session["file_sha256"],
                    "assembled_sha256": digest,
                    "declared_size": total,
                    "assembled_size": offset,
                },
            }
            if not self.db.fail_finalization(session_id, generation, owner, json.dumps(error), self._now_iso()):
                raise LeaseLost
            # the failed state is terminal, so the rejected output can go;
            # confirmed chunks are of course kept
            self.store.discard(tmp)
            raise ApiError(422, error["code"], error["message"], error["details"])
        return digest

    def _commit_checkpoint(
        self, session_id: str, generation: int, owner: str, out, offset: int, hasher: ResumableSha256
    ) -> None:
        # the checkpoint may only advance after the output bytes are durable
        out.flush()
        os.fsync(out.fileno())
        ok = self.db.advance_checkpoint(
            session_id, generation, owner, offset, hasher.state(), self.lease_ttl_seconds, self._now_iso()
        )
        if not ok:
            raise LeaseLost
        if self._checkpoint_hook is not None:
            self._checkpoint_hook(session_id, offset)

    def _publish(self, session: dict, fin: dict, owner: str, digest: str) -> None:
        session_id = session["session_id"]
        generation = fin["generation"]
        # crash window 1: the intent is durable before the rename; a restart
        # finds the temp file and re-does the rename.
        if not self.db.declare_publish_intent(
            session_id, generation, owner, digest, self.lease_ttl_seconds, self._now_iso()
        ):
            raise LeaseLost
        final = self.store.publish(self.store.finalize_tmp_path(session_id), session_id)
        # crash window 2: rename done, completion not committed; a restart
        # verifies the artifact and completes without re-assembling.
        if not self.db.complete_finalization(
            session_id, generation, owner, self._now_iso(), digest, str(final), self._now_iso()
        ):
            raise LeaseLost

    def _converge_publish(self, session: dict, fin: dict, owner: str) -> None:
        """Converge a publish-phase crash window without re-assembling."""
        session_id = session["session_id"]
        tmp = self.store.finalize_tmp_path(session_id)
        artifact = self.store.artifact_path(session_id)
        if tmp.exists():
            # intent was recorded but the rename never happened: the temp file
            # holds the fully assembled, checkpointed bytes -> publish them
            final = self.store.publish(tmp, session_id)
        else:
            # rename happened but the completion transaction did not commit:
            # only a byte-identical artifact may be recovered into completed
            size, digest = self.store.sha256_of(artifact)
            if size != session["file_size"] or digest != session["file_sha256"]:
                raise self._recovery_error(
                    session, fin, "published artifact does not match the declared size and SHA-256"
                )
            final = artifact
        if not self.db.complete_finalization(
            session_id, fin["generation"], owner, self._now_iso(), session["file_sha256"], str(final), self._now_iso()
        ):
            raise LeaseLost

    def _recovery_error(self, session: dict, fin: dict, reason: str) -> ApiError:
        error = {
            "status_code": 409,
            "code": "FINALIZATION_RECOVERY_ERROR",
            "message": f"finalization state is not recoverable: {reason}",
            "details": {
                "session_id": session["session_id"],
                "state": fin["state"],
                "generation": fin["generation"],
                "confirmed_bytes": fin["confirmed_bytes"],
                "reason": reason,
            },
        }
        self.db.force_fail_finalization(session["session_id"], json.dumps(error), self._now_iso())
        return ApiError(409, error["code"], error["message"], error["details"])

    @staticmethod
    def _stored_error(fin: dict) -> ApiError:
        try:
            payload = json.loads(fin["last_error"] or "")
            return ApiError(
                payload["status_code"], payload["code"], payload["message"], payload.get("details") or {}
            )
        except (ValueError, KeyError, TypeError):
            return ApiError(
                409,
                "FINALIZATION_RECOVERY_ERROR",
                "finalization failed and its recorded error is unreadable",
                {"state": fin["state"]},
            )

    def _in_progress(self, session_id: str) -> ApiError:
        fin = self.db.get_finalization(session_id) or {}
        lease_expires_at = fin.get("lease_expires_at")
        now = self.db.db_now()
        retry_after = max(0.0, (lease_expires_at or now) - now)
        return ApiError(
            409,
            "FINALIZATION_IN_PROGRESS",
            "finalization is already running under a different lease; retry after it expires",
            {
                "session_id": session_id,
                "state": fin.get("state"),
                "generation": fin.get("generation"),
                "retry_after": round(retry_after, 3),
            },
        )

    @staticmethod
    def _now_iso() -> str:
        return clock.utcnow().isoformat()

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


def reconcile(db: Database, store: ChunkStore) -> None:
    """Rebuild durable state after a (possibly unclean) restart.

    - chunk rows whose files vanished or have a wrong size are dropped;
    - chunk files without a matching row (crashed before commit) are removed;
    - the persisted bitmap is rebuilt from the surviving rows;
    - leftover temp files are removed.

    Finalization rows are *not* rewritten here: their crash windows converge
    lazily (and deterministically) on the next finalize call, so confirmed
    progress never regresses across a restart.

    Net effect: confirmed chunks are never reported missing, and unconfirmed
    bytes are never reported as received.
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
    store.purge_tmp()
