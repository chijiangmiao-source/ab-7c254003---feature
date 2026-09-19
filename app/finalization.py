"""Crash-safe finalization protocol mechanics.

This module is deliberately free of HTTP/SQL wiring: it implements the
streaming assembly loop with durable checkpoints, database-clock lease
renewal and strict fence-generation enforcement, plus the small set of
fault-injection hooks used by the real-process recovery tests (control files
under ``DATA_DIR/.faults``; absent by default, so production behaviour is
unchanged).
"""

from __future__ import annotations

import os
import signal
import time
from contextlib import contextmanager
from pathlib import Path

from .sha256state import ResumableSHA256
from .storage import CheckpointFileError, FinalizeWriter, _fsync_dir

FAULTS_DIRNAME = ".faults"

# One-shot fault control files.  Each file contains the target session_id, or
# "*" for the next finalization to reach the window.  A trigger is consumed
# (atomically renamed aside) before it fires, so a taking-over generation
# never inherits it.
FAULT_KILL_PRE_CHECKPOINT = "kill_pre_checkpoint"
FAULT_KILL_POST_CHECKPOINT = "kill_post_checkpoint"
FAULT_KILL_AFTER_LINK = "kill_after_link"
FAULT_KILL_AFTER_INTENT = "kill_after_intent"
FAULT_KILL_AFTER_RENAME = "kill_after_rename"
FAULT_FREEZE_RENEWALS = "freeze_renewals"


class FenceLost(Exception):
    """The caller's fence generation is no longer current; all work must stop."""


class RecoveryBroken(Exception):
    """Persisted finalization state cannot be reconciled; no guessing allowed."""

    def __init__(self, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


class ProtocolSettings:
    """Lease/checkpoint timings, overridable via env for the acceptance harness."""

    def __init__(self) -> None:
        self.lease_seconds = float(os.environ.get("FINALIZE_LEASE_SECONDS", "30"))
        self.renew_seconds = float(os.environ.get("FINALIZE_LEASE_RENEW_SECONDS", "10"))
        self.checkpoint_bytes = int(
            os.environ.get("FINALIZE_CHECKPOINT_BYTES", str(8 * 1024 * 1024))
        )
        self.copy_buffer = int(os.environ.get("FINALIZE_COPY_BUFFER", str(1024 * 1024)))


# ---- fault injection ----


def _faults_dir(data_dir: Path) -> Path:
    return data_dir / FAULTS_DIRNAME


def claim_fault(data_dir: Path, name: str, session_id: str, position: int | None = None) -> bool:
    """Consume a one-shot fault trigger exactly once.

    The trigger file content is either ``*``, a session id, or
    ``"<session_id>:<min_position>"`` — the latter only fires once assembly has
    reached ``min_position`` bytes, letting tests kill at a later checkpoint
    while an earlier one is already committed.  The trigger is unlinked *before*
    it fires, so the effect (including a SIGKILL) cannot be inherited by the
    generation that takes over.
    """
    path = _faults_dir(data_dir) / name
    try:
        target = path.read_text().strip()
    except (FileNotFoundError, OSError):
        return False
    min_position = 0
    if ":" in target:
        target_s, _, position_s = target.partition(":")
        try:
            min_position = int(position_s)
        except ValueError:
            return False
        target = target_s
    if target != "*" and target != session_id:
        return False
    if position is not None and position < min_position:
        return False
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    return True


def fire_kill(data_dir: Path, name: str, session_id: str, position: int | None = None) -> None:
    if claim_fault(data_dir, name, session_id, position):
        # SIGKILL: no finally blocks, no fsync, indistinguishable from real
        # kernel-level process death.
        os.kill(os.getpid(), signal.SIGKILL)


class FileGate:
    """Deterministic assembly barrier used by the real-process acceptance tests.

    After each *intermediate* checkpoint the holder appends its position to
    ``.faults/progress`` and, while ``.faults/gate`` exists, blocks there
    (stopping lease renewal) until ``.faults/gate_released`` appears.  This is
    a pure scheduling aid: the checkpoints themselves are real fsync + SQLite
    commits, and blocking without renewing is exactly what lets the database
    lease expire and be taken over.
    """

    def __init__(self, data_dir: Path, timeout: float = 60.0):
        self.dir = data_dir / FAULTS_DIRNAME
        self.gate = self.dir / "gate"
        self.release = self.dir / "gate_released"
        self.progress = self.dir / "progress"
        self.timeout = timeout

    def after_checkpoint(self, position: int) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        with open(self.progress, "a") as fh:
            fh.write(f"{position}\n")
            fh.flush()
            os.fsync(fh.fileno())
        if not self.gate.exists():
            return
        deadline = time.monotonic() + self.timeout
        while not self.release.exists():
            if time.monotonic() > deadline:
                return
            time.sleep(0.02)


def gate_for(data_dir: Path):
    # Present only under the acceptance harness; production never creates it.
    if (data_dir / FAULTS_DIRNAME).is_dir():
        return FileGate(data_dir)
    return None


# ---- generation-tagged temp files ----


def generation_tmp_path(store, session_id: str, generation: int) -> Path:
    return store.finalize_dir / f"{session_id}.g{generation}.tmp"


def clone_confirmed_prefix(
    store, session_id: str, previous_tmp: Path, generation: int, confirmed_bytes: int
) -> Path:
    """Create a fresh, generation-owned temp containing only the confirmed prefix.

    A new inode on takeover means a superseded owner that still has the old
    file open can only orphan its own writes — it can never interleave bytes
    into the new owner's file.  Bytes beyond the checkpoint are never copied.
    """
    new_path = generation_tmp_path(store, session_id, generation)
    new_path.parent.mkdir(parents=True, exist_ok=True)
    if confirmed_bytes == 0:
        with open(new_path, "wb"):
            pass
        _fsync_dir(new_path.parent)
        return new_path
    if not previous_tmp.exists():
        raise RecoveryBroken(
            "FINALIZATION_RECOVERY_ERROR",
            "finalization temp file is missing but the checkpoint confirms bytes",
            {"reason": "tmp_missing", "confirmed_bytes": confirmed_bytes},
        )
    actual = previous_tmp.stat().st_size
    if actual < confirmed_bytes:
        raise RecoveryBroken(
            "FINALIZATION_RECOVERY_ERROR",
            "finalization temp file is shorter than the confirmed checkpoint",
            {
                "reason": "tmp_shorter_than_checkpoint",
                "confirmed_bytes": confirmed_bytes,
                "tmp_bytes": actual,
            },
        )
    with open(previous_tmp, "rb") as src, open(new_path, "wb") as dst:
        remaining = confirmed_bytes
        while remaining:
            block = src.read(min(1024 * 1024, remaining))
            if not block:
                raise RecoveryBroken(
                    "FINALIZATION_RECOVERY_ERROR",
                    "finalization temp file ended before the confirmed checkpoint",
                    {"reason": "tmp_truncated", "confirmed_bytes": confirmed_bytes},
                )
            dst.write(block)
            remaining -= len(block)
        dst.flush()
        os.fsync(dst.fileno())
    _fsync_dir(new_path.parent)
    return new_path


@contextmanager
def open_generation_tmp(tmp_path: Path, confirmed_bytes: int):
    """Open a generation temp at the confirmed boundary, dropping any tail.

    - longer than the checkpoint: the unconfirmed tail is truncated + fsynced
      before any new byte is appended;
    - shorter than the checkpoint, or missing with confirmed bytes:
      ``CheckpointFileError`` — the caller must fail recovery rather than
      guess progress.
    """
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    if tmp_path.exists():
        fh = open(tmp_path, "r+b")
        try:
            size = fh.seek(0, os.SEEK_END)
            if size < confirmed_bytes:
                raise CheckpointFileError(
                    "tmp_shorter_than_checkpoint",
                    expected=confirmed_bytes,
                    actual=size,
                )
            if size > confirmed_bytes:
                fh.truncate(confirmed_bytes)
                fh.flush()
                os.fsync(fh.fileno())
            fh.seek(0, os.SEEK_END)
            yield FinalizeWriter(fh, tmp_path, confirmed_bytes)
        finally:
            fh.close()
    else:
        if confirmed_bytes:
            raise CheckpointFileError("tmp_missing", expected=confirmed_bytes, actual=0)
        fh = open(tmp_path, "w+b")
        try:
            yield FinalizeWriter(fh, tmp_path, 0)
        finally:
            fh.close()


# ---- streaming assembly ----


def stream_assemble(
    *,
    data_dir: Path,
    session: dict,
    source_paths: list[Path],
    tmp_path: Path,
    confirmed_bytes: int,
    hasher_state: str | None,
    settings: ProtocolSettings,
    renew,
    save_checkpoint,
    fault_session_id: str,
    gate=None,
) -> tuple[int, str]:
    """Append source bytes from the confirmed boundary into ``tmp_path``.

    The source prefix of ``confirmed_bytes`` is skipped (never re-read, never
    re-hashed); SHA-256 continues from ``hasher_state``.  Everything is
    streamed in bounded buffers — never a whole chunk or file in memory.  The
    persisted checkpoint advances only after the appended bytes are fsynced.

    ``renew()`` and ``save_checkpoint(bytes, state)`` raise/return-false when
    the caller's fence generation has been superseded; assembly then stops and
    the superseded owner never touches the file or the database again.

    Returns ``(total_bytes, final_digest)`` after the *final* checkpoint has
    been durably committed at the full file length.
    """
    total = session["file_size"]
    if confirmed_bytes > total:
        raise RecoveryBroken(
            "FINALIZATION_RECOVERY_ERROR",
            "checkpoint is beyond the declared file size",
            {
                "reason": "checkpoint_overflow",
                "confirmed_bytes": confirmed_bytes,
                "total_bytes": total,
            },
        )

    try:
        hasher = (
            ResumableSHA256.from_state(hasher_state)
            if hasher_state
            else ResumableSHA256()
        )
    except ValueError as exc:
        raise RecoveryBroken(
            "FINALIZATION_RECOVERY_ERROR",
            "persisted SHA-256 checkpoint is unreadable or of an unknown version",
            {"reason": "bad_hasher_state", "detail": str(exc)},
        ) from exc

    frozen = claim_fault(data_dir, FAULT_FREEZE_RENEWALS, fault_session_id)

    with open_generation_tmp(tmp_path, confirmed_bytes) as writer:
        position = confirmed_bytes
        since_checkpoint = 0
        skip = confirmed_bytes
        buffer_size = settings.copy_buffer
        checkpoint_every = max(settings.checkpoint_bytes, buffer_size)
        next_renew = time.monotonic() + settings.renew_seconds

        for path in source_paths:
            file_len = path.stat().st_size
            if skip >= file_len:
                skip -= file_len
                continue
            with open(path, "rb") as src:
                if skip:
                    src.seek(skip)
                    skip = 0
                while True:
                    block = src.read(buffer_size)
                    if not block:
                        break
                    if frozen:
                        # Lease holder has stopped renewing; keep producing
                        # (unconfirmed) bytes slowly so takeover happens
                        # mid-assembly rather than between requests.
                        time.sleep(0.05)
                    writer.write(block)
                    position += len(block)
                    since_checkpoint += len(block)
                    hasher.update(block)

                    if not frozen and time.monotonic() >= next_renew:
                        renew()
                        next_renew = time.monotonic() + settings.renew_seconds

                    if since_checkpoint >= checkpoint_every:
                        writer.sync()
                        # PRE threshold is measured against bytes already
                        # committed before *this* checkpoint; POST against the
                        # newly confirmed position.
                        fire_kill(
                            data_dir, FAULT_KILL_PRE_CHECKPOINT, fault_session_id,
                            position - since_checkpoint,
                        )
                        if not save_checkpoint(position, hasher.to_state()):
                            raise FenceLost()
                        fire_kill(
                            data_dir, FAULT_KILL_POST_CHECKPOINT, fault_session_id, position
                        )
                        since_checkpoint = 0
                        if not frozen and time.monotonic() >= next_renew:
                            renew()
                            next_renew = time.monotonic() + settings.renew_seconds
                        # Blocking happens *after* the renewal point: while the
                        # holder waits here it sends no heartbeats, so the
                        # database lease expires and another generation can
                        # take over.
                        if gate is not None:
                            gate.after_checkpoint(position)

        # Commit the final (possibly partial) checkpoint at the full length so
        # a crash during publishing never forces re-assembly.
        writer.sync()
        if not save_checkpoint(position, hasher.to_state()):
            raise FenceLost()
        return position, hasher.hexdigest()


def cleanup_old_generations(store, session_id: str, generation: int) -> None:
    """Remove temp/candidate files belonging to strictly older generations."""
    for directory, pattern in (
        (store.finalize_dir, f"{session_id}.g*.tmp"),
        (store.artifacts_dir, f".{session_id}.g*.cand"),
    ):
        for entry in directory.glob(pattern):
            try:
                gen = int(entry.name.split(".g", 1)[1].split(".", 1)[0])
            except (IndexError, ValueError):
                continue
            if gen < generation:
                store.discard(entry)
    _fsync_dir(store.finalize_dir)
    _fsync_dir(store.artifacts_dir)
