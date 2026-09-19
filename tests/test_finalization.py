"""Crash-resumable, lease-fenced finalization protocol coverage.

The recovery tests genuinely kill a worker process with SIGKILL or abandon a
worker mid-checkpoint without any cleanup, then resume from a brand-new
service instance over the same data directory — no in-memory carry-over, no
recomputation from byte 0.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import Database
from app.errors import ApiError
from app.main import create_app
from app.resumable_hash import ResumableSha256
from app.schemas import CreateSessionRequest
from app.service import UploadService
from app.storage import ChunkStore

WORKSPACE = Path(__file__).resolve().parent.parent


# ---------- helpers ----------

def make_bytes(size: int, seed: str = "payload") -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"{seed}:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def future_expiry(hours: float = 1.0) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def make_service(data_dir: Path, **kwargs) -> UploadService:
    kwargs.setdefault("checkpoint_bytes", 1024)
    kwargs.setdefault("lease_ttl_seconds", 30.0)
    return UploadService(Database(data_dir / "db.sqlite3"), ChunkStore(data_dir), **kwargs)


def seed_session(service: UploadService, payload: bytes, chunk_size: int, *, file_sha256: str | None = None) -> str:
    req = CreateSessionRequest(
        file_size=len(payload),
        chunk_size=chunk_size,
        file_sha256=file_sha256 or sha256(payload),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    return service.create_session(req)["session_id"]


async def _one_shot(data: bytes):
    yield data


def upload_all(service: UploadService, sid: str, payload: bytes, chunk_size: int) -> None:
    for i in range(-(-len(payload) // chunk_size)):
        body = payload[i * chunk_size : (i + 1) * chunk_size]
        _, status = asyncio.run(service.upload_chunk(sid, str(i), sha256(body), _one_shot(body)))
        assert status == 201


class CountingStore(ChunkStore):
    """Records every source-read offset/length to prove resume never re-reads."""

    def __init__(self, root: Path):
        super().__init__(root)
        self.reads: list[tuple[int, int]] = []  # (offset, length)

    def read_chunks_from(self, session_id, chunk_size, total_bytes, offset):
        for block in super().read_chunks_from(session_id, chunk_size, total_bytes, offset):
            self.reads.append((offset, len(block)))
            offset += len(block)
            yield block


class Crash(Exception):
    """Raised by a checkpoint hook to abandon a worker mid-flight."""


def crash_after(service: UploadService, checkpoints: int, box: dict) -> None:
    """Install a hook that 'kills' the worker after N committed checkpoints."""
    def hook(_sid: str, confirmed: int) -> None:
        box.setdefault("offsets", []).append(confirmed)
        if len(box["offsets"]) == checkpoints:
            raise Crash(f"simulated SIGKILL at confirmed={confirmed}")

    service._checkpoint_hook = hook


def force_expire_lease(db: Database, sid: str) -> None:
    with db.lock, db._conn:
        db._conn.execute("UPDATE finalization SET lease_expires_at = 0 WHERE session_id = ?", (sid,))


def get_fin(db: Database, sid: str) -> dict:
    fin = db.get_finalization(sid)
    assert fin is not None
    return fin


# ---------- ResumableSha256 ----------

def test_resumable_sha256_matches_hashlib():
    for size in (0, 1, 55, 56, 63, 64, 65, 127, 128, 129, 1000, 4099):
        data = make_bytes(size, seed=f"vec-{size}")
        assert ResumableSha256().hexdigest() == hashlib.sha256(b"").hexdigest() if size == 0 else True
        hasher = ResumableSha256()
        hasher.update(data)
        assert hasher.hexdigest() == sha256(data), size


def test_resumable_sha256_state_roundtrip():
    data = make_bytes(5000)
    hasher = ResumableSha256()
    hasher.update(data[:1234])
    restored = ResumableSha256.from_state(hasher.state())
    restored.update(data[1234:])
    assert restored.hexdigest() == sha256(data)
    # digest() must not disturb the running state
    assert ResumableSha256.from_state(hasher.state()).hexdigest() == hasher.hexdigest()


def test_resumable_sha256_rejects_corrupt_state():
    good = ResumableSha256().state()
    for bad in (b"", b"short", b"BADMAGIC!" + b"\x00" * 60, good[:-1], good + b"x" * 64):
        with pytest.raises(ValueError):
            ResumableSha256.from_state(bad)


# ---------- read-only progress endpoint ----------

def test_finalization_endpoint_idle_and_404(tmp_path):
    app = create_app(Settings(data_dir=tmp_path / "data"))
    with TestClient(app) as client:
        assert client.get("/sessions/nope/finalization").status_code == 404
        payload = make_bytes(100)
        resp = client.post(
            "/sessions",
            json={
                "file_size": len(payload),
                "chunk_size": 64,
                "file_sha256": sha256(payload),
                "expires_at": future_expiry(),
            },
        )
        sid = resp.json()["session_id"]
        fin = client.get(f"/sessions/{sid}/finalization").json()
        assert fin["state"] == "idle"
        assert fin["confirmed_bytes"] == 0
        assert fin["total_bytes"] == len(payload)
        assert fin["generation"] == 0
        assert fin["last_error"] is None


def test_finalize_then_endpoint_reports_completed(tmp_path):
    app = create_app(Settings(data_dir=tmp_path / "data", finalize_checkpoint_bytes=64))
    with TestClient(app) as client:
        payload = make_bytes(200)
        resp = client.post(
            "/sessions",
            json={
                "file_size": len(payload),
                "chunk_size": 64,
                "file_sha256": sha256(payload),
                "expires_at": future_expiry(),
            },
        )
        sid = resp.json()["session_id"]
        for i in range(4):
            body = payload[i * 64 : (i + 1) * 64]
            assert client.put(
                f"/sessions/{sid}/chunks/{i}", content=body, headers={"X-Chunk-SHA256": sha256(body)}
            ).status_code == 201
        assert client.post(f"/sessions/{sid}/finalize").status_code == 200
        fin = client.get(f"/sessions/{sid}/finalization").json()
        assert fin["state"] == "completed"
        assert fin["confirmed_bytes"] == len(payload)
        assert fin["total_bytes"] == len(payload)
        assert fin["generation"] == 1
        assert fin["last_error"] is None
        # idempotent re-finalize keeps the same generation
        assert client.post(f"/sessions/{sid}/finalize").status_code == 200
        assert client.get(f"/sessions/{sid}/finalization").json()["generation"] == 1


# ---------- crash / resume ----------

def test_resume_continues_from_checkpoint_without_reread(tmp_path):
    data_dir = tmp_path / "data"
    payload = make_bytes(1024 * 5 + 100)
    service = make_service(data_dir)
    sid = seed_session(service, payload, 1024)
    upload_all(service, sid, payload, 1024)

    box: dict = {}
    crash_after(service, 2, box)
    with pytest.raises(Crash):
        service.finalize(sid)
    force_expire_lease(service.db, sid)  # the dead worker's lease is now stale

    fin = get_fin(service.db, sid)
    assert fin["state"] == "assembling"
    assert fin["confirmed_bytes"] == 2048
    assert fin["generation"] == 1
    tmp = service.store.finalize_tmp_path(sid)
    assert tmp.stat().st_size == 2048  # checkpoint committed only durable bytes

    # restart: brand-new service over the same data dir, instrumented reads
    store = CountingStore(data_dir)
    resumed = UploadService(Database(data_dir / "db.sqlite3"), store, checkpoint_bytes=1024)
    receipt = resumed.finalize(sid)
    assert receipt["final_sha256"] == sha256(payload)
    assert receipt["status"] == "completed"

    # not a single already-confirmed source byte was re-read
    assert store.reads, "resume must stream the remaining source bytes"
    assert store.reads[0][0] == 2048
    assert sum(n for _, n in store.reads) == len(payload) - 2048

    assert (data_dir / "artifacts" / f"{sid}.bin").read_bytes() == payload
    fin = get_fin(resumed.db, sid)
    assert fin["state"] == "completed"
    assert fin["confirmed_bytes"] == len(payload)
    assert fin["generation"] == 2  # takeover bumped the fence exactly once


def test_unconfirmed_tail_is_discarded_on_resume(tmp_path):
    data_dir = tmp_path / "data"
    payload = make_bytes(1024 * 4)
    service = make_service(data_dir)
    sid = seed_session(service, payload, 1024)
    upload_all(service, sid, payload, 1024)

    box: dict = {}
    crash_after(service, 2, box)
    with pytest.raises(Crash):
        service.finalize(sid)
    force_expire_lease(service.db, sid)

    # bytes written but never checkpointed may exist past the confirmed offset
    tmp = service.store.finalize_tmp_path(sid)
    with open(tmp, "ab") as fh:
        fh.write(os.urandom(777))
    assert tmp.stat().st_size == 2048 + 777

    resumed = make_service(data_dir)
    receipt = resumed.finalize(sid)
    assert receipt["final_sha256"] == sha256(payload)
    assert (data_dir / "artifacts" / f"{sid}.bin").read_bytes() == payload


def test_sigkill_crash_recovery_across_processes(tmp_path):
    """A worker is really SIGKILLed mid-assembly; a new process resumes."""
    data_dir = tmp_path / "data"
    payload = make_bytes(1024 * 5 + 100)
    service = make_service(data_dir)
    sid = seed_session(service, payload, 1024)
    upload_all(service, sid, payload, 1024)
    service.db.close()

    child = WORKSPACE / "tests" / "finalize_worker.py"
    env = {**os.environ, "PYTHONPATH": str(WORKSPACE)}

    crash = subprocess.run(
        [sys.executable, str(child), str(data_dir), sid, "crash"],
        capture_output=True, text=True, env=env,
    )
    assert crash.returncode == -signal.SIGKILL, crash.stderr

    # durable state after the kill: checkpoint committed, progress persisted
    db = Database(data_dir / "db.sqlite3")
    fin = get_fin(db, sid)
    assert fin["state"] == "assembling"
    assert fin["confirmed_bytes"] == 2048
    assert (data_dir / "artifacts" / f".{sid}.finalizing").stat().st_size == 2048
    db.close()

    resumed = subprocess.run(
        [sys.executable, str(child), str(data_dir), sid, "resume"],
        capture_output=True, text=True, env=env,
    )
    assert resumed.returncode == 0, resumed.stderr
    receipt = json.loads(resumed.stdout.strip().splitlines()[-1])
    assert receipt["final_sha256"] == sha256(payload)
    assert (data_dir / "artifacts" / f"{sid}.bin").read_bytes() == payload

    db = Database(data_dir / "db.sqlite3")
    fin = get_fin(db, sid)
    assert fin["state"] == "completed"
    assert fin["confirmed_bytes"] == len(payload)
    assert fin["generation"] == 2
    db.close()


def test_progress_does_not_regress_across_restart(tmp_path):
    data_dir = tmp_path / "data"
    payload = make_bytes(1024 * 3)
    service = make_service(data_dir)
    sid = seed_session(service, payload, 1024)
    upload_all(service, sid, payload, 1024)

    box: dict = {}
    crash_after(service, 2, box)
    with pytest.raises(Crash):
        service.finalize(sid)
    force_expire_lease(service.db, sid)

    restarted = make_service(data_dir)
    view = restarted.finalization_status(sid)
    assert view["state"] == "assembling"
    assert view["confirmed_bytes"] == 2048  # survived the 'restart'
    assert view["generation"] == 1

    restarted.finalize(sid)
    view = restarted.finalization_status(sid)
    assert view["state"] == "completed"
    assert view["confirmed_bytes"] == len(payload)


# ---------- lease / fence ----------

def test_concurrent_finalize_gets_structured_409(tmp_path):
    app = create_app(Settings(data_dir=tmp_path / "data", finalize_lease_ttl_seconds=30.0))
    with TestClient(app) as client:
        service = app.state.service
        payload = make_bytes(300)
        sid = seed_session(service, payload, 64)
        upload_all(service, sid, payload, 64)

        # another worker holds a live lease (generation 1)
        service.db.ensure_finalization(sid, len(payload), "now")
        held = service.db.try_acquire_lease(sid, "other-worker", 30.0, len(payload), "now")
        assert held is not None and held["generation"] == 1

        resp = client.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 409
        error = resp.json()["error"]
        assert error["code"] == "FINALIZATION_IN_PROGRESS"
        assert error["details"]["generation"] == 1
        assert error["details"]["retry_after"] > 0

        fin = client.get(f"/sessions/{sid}/finalization").json()
        assert fin["state"] == "assembling"
        assert fin["generation"] == 1
        assert fin["lease_expires_at"] is not None


def test_takeover_after_lease_expiry(tmp_path):
    data_dir = tmp_path / "data"
    service = make_service(data_dir)
    payload = make_bytes(300)
    sid = seed_session(service, payload, 64)
    upload_all(service, sid, payload, 64)

    service.db.ensure_finalization(sid, len(payload), "now")
    held = service.db.try_acquire_lease(sid, "stalled-worker", 30.0, len(payload), "now")
    assert held is not None
    force_expire_lease(service.db, sid)  # lease is now stale by the db clock

    receipt = service.finalize(sid)
    assert receipt["final_sha256"] == sha256(payload)
    fin = get_fin(service.db, sid)
    assert fin["state"] == "completed"
    assert fin["generation"] == 2  # strictly increasing, never reused


def test_paused_worker_cannot_advance_after_takeover(tmp_path):
    """A stale generation that resumes must not checkpoint, publish or complete."""
    data_dir = tmp_path / "data"
    payload = make_bytes(1024 * 4)
    service_a = make_service(data_dir)
    sid = seed_session(service_a, payload, 1024)
    upload_all(service_a, sid, payload, 1024)

    takeover = {"done": False}

    def hook(sid_: str, _confirmed: int) -> None:
        if takeover["done"]:
            return
        takeover["done"] = True
        # A stalls past its lease TTL; B takes over and finishes the job.
        force_expire_lease(service_a.db, sid_)
        service_b = make_service(data_dir)  # independent db connection
        assert service_b.finalize(sid_)["final_sha256"] == sha256(payload)

    service_a._checkpoint_hook = hook
    with pytest.raises(ApiError) as excinfo:
        service_a.finalize(sid)
    assert excinfo.value.status_code == 409
    assert excinfo.value.code == "FINALIZATION_IN_PROGRESS"

    # B's result stands; A's fenced writes changed nothing
    db = Database(data_dir / "db.sqlite3")
    fin = get_fin(db, sid)
    assert fin["state"] == "completed"
    assert fin["generation"] == 2
    assert fin["confirmed_bytes"] == len(payload)
    assert (data_dir / "artifacts" / f"{sid}.bin").read_bytes() == payload
    db.close()


def test_fenced_writes_reject_old_generation(tmp_path):
    data_dir = tmp_path / "data"
    service = make_service(data_dir)
    payload = make_bytes(300)
    sid = seed_session(service, payload, 64)
    upload_all(service, sid, payload, 64)
    service.db.ensure_finalization(sid, len(payload), "now")

    gen1 = service.db.try_acquire_lease(sid, "worker-a", 30.0, len(payload), "now")
    assert gen1["generation"] == 1
    force_expire_lease(service.db, sid)
    gen2 = service.db.try_acquire_lease(sid, "worker-b", 30.0, len(payload), "now")
    assert gen2["generation"] == 2

    # every fenced write of the old generation is a no-op
    assert not service.db.advance_checkpoint(sid, 1, "worker-a", 100, b"state", 30.0, "now")
    assert not service.db.declare_publish_intent(sid, 1, "worker-a", "x" * 64, 30.0, "now")
    assert not service.db.complete_finalization(sid, 1, "worker-a", "now", "x" * 64, "/tmp/x", "now")
    assert not service.db.fail_finalization(sid, 1, "worker-a", "{}", "now")
    fin = get_fin(service.db, sid)
    assert fin["generation"] == 2
    assert fin["confirmed_bytes"] == 0
    assert fin["state"] == "assembling"


# ---------- integrity failure is deterministic and sticky ----------

def test_integrity_mismatch_is_replayed_and_keeps_chunks(tmp_path):
    app = create_app(Settings(data_dir=tmp_path / "data", finalize_checkpoint_bytes=64))
    with TestClient(app) as client:
        service = app.state.service
        payload = make_bytes(200)
        wrong_sha = sha256(b"something-else")
        sid = seed_session(service, payload, 64, file_sha256=wrong_sha)
        upload_all(service, sid, payload, 64)

        resp = client.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "INTEGRITY_MISMATCH"

        fin = client.get(f"/sessions/{sid}/finalization").json()
        assert fin["state"] == "failed"
        assert fin["last_error"]["code"] == "INTEGRITY_MISMATCH"

        # deterministic replay: same status, same code, same details
        again = client.post(f"/sessions/{sid}/finalize")
        assert again.status_code == 422
        assert again.json() == resp.json()

        # all confirmed chunks are kept; nothing downloadable was left behind
        status = client.get(f"/sessions/{sid}").json()
        assert status["received_count"] == status["total_chunks"]
        assert client.get(f"/sessions/{sid}/artifact").status_code == 409


# ---------- recovery errors ----------

def _crashed_mid_assembly(tmp_path: Path, payload: bytes):
    data_dir = tmp_path / "data"
    service = make_service(data_dir)
    sid = seed_session(service, payload, 1024)
    upload_all(service, sid, payload, 1024)
    box: dict = {}
    crash_after(service, 2, box)
    with pytest.raises(Crash):
        service.finalize(sid)
    force_expire_lease(service.db, sid)  # a restarted process may take over
    return data_dir, service, sid


def _assert_recovery_error(service: UploadService, sid: str, payload: bytes, *, expect_artifact: bool = False) -> None:
    with pytest.raises(ApiError) as excinfo:
        service.finalize(sid)
    assert excinfo.value.status_code == 409
    assert excinfo.value.code == "FINALIZATION_RECOVERY_ERROR"
    fin = get_fin(service.db, sid)
    assert fin["state"] == "failed"
    assert json.loads(fin["last_error"])["code"] == "FINALIZATION_RECOVERY_ERROR"
    # original chunks are untouched and nothing downloadable was published
    session = service.get_session_or_404(sid)
    assert session["status"] == "active"
    assert service.public_session(session)["received_count"] == session["total_chunks"]
    assert service.store.artifact_path(sid).exists() is expect_artifact
    # the failure replays deterministically
    with pytest.raises(ApiError) as excinfo2:
        service.finalize(sid)
    assert excinfo2.value.code == "FINALIZATION_RECOVERY_ERROR"


def test_recovery_error_on_unknown_checkpoint_version(tmp_path):
    payload = make_bytes(1024 * 3)
    data_dir, service, sid = _crashed_mid_assembly(tmp_path, payload)
    with service.db.lock, service.db._conn:
        service.db._conn.execute(
            "UPDATE finalization SET checkpoint_version = 999 WHERE session_id = ?", (sid,)
        )
    _assert_recovery_error(make_service(data_dir), sid, payload)


def test_recovery_error_on_missing_temp_file(tmp_path):
    payload = make_bytes(1024 * 3)
    data_dir, service, sid = _crashed_mid_assembly(tmp_path, payload)
    service.store.finalize_tmp_path(sid).unlink()
    _assert_recovery_error(make_service(data_dir), sid, payload)


def test_recovery_error_on_temp_shorter_than_checkpoint(tmp_path):
    payload = make_bytes(1024 * 3)
    data_dir, service, sid = _crashed_mid_assembly(tmp_path, payload)
    tmp = service.store.finalize_tmp_path(sid)
    with open(tmp, "r+b") as fh:
        fh.truncate(1024)  # checkpoint says 2048: the confirmed prefix is gone
    _assert_recovery_error(make_service(data_dir), sid, payload)


def test_recovery_error_on_corrupt_hasher_state(tmp_path):
    payload = make_bytes(1024 * 3)
    data_dir, service, sid = _crashed_mid_assembly(tmp_path, payload)
    with service.db.lock, service.db._conn:
        service.db._conn.execute(
            "UPDATE finalization SET hasher_state = ? WHERE session_id = ?", (b"garbage", sid)
        )
    _assert_recovery_error(make_service(data_dir), sid, payload)


# ---------- publish-phase crash windows ----------

def _craft_publish_state(db: Database, sid: str, final_sha256: str) -> None:
    """Persist a publish intent as if the worker died right after committing it."""
    with db.lock, db._conn:
        db._conn.execute(
            "UPDATE finalization SET state = 'publishing', publish_intent = 1,"
            " final_sha256 = ?, lease_expires_at = 0 WHERE session_id = ?",
            (final_sha256, sid),
        )


def test_publish_window_intent_recorded_rename_not_done(tmp_path):
    data_dir = tmp_path / "data"
    payload = make_bytes(1024 * 3)
    service = make_service(data_dir)
    sid = seed_session(service, payload, 1024)
    upload_all(service, sid, payload, 1024)

    # die after the final checkpoint, before the publish intent
    box: dict = {}
    crash_after(service, 3, box)  # 3 checkpoints cover the whole 3072-byte file
    with pytest.raises(Crash):
        service.finalize(sid)
    assert get_fin(service.db, sid)["confirmed_bytes"] == len(payload)

    _craft_publish_state(service.db, sid, sha256(payload))
    resumed = make_service(data_dir)
    receipt = resumed.finalize(sid)
    assert receipt["final_sha256"] == sha256(payload)
    assert (data_dir / "artifacts" / f"{sid}.bin").read_bytes() == payload
    assert get_fin(resumed.db, sid)["state"] == "completed"


def test_publish_window_rename_done_commit_not_done(tmp_path):
    data_dir = tmp_path / "data"
    payload = make_bytes(1024 * 3)
    service = make_service(data_dir)
    sid = seed_session(service, payload, 1024)
    upload_all(service, sid, payload, 1024)

    box: dict = {}
    crash_after(service, 3, box)
    with pytest.raises(Crash):
        service.finalize(sid)

    # rename happened, completion transaction did not
    tmp = service.store.finalize_tmp_path(sid)
    artifact = service.store.artifact_path(sid)
    os.replace(tmp, artifact)
    _craft_publish_state(service.db, sid, sha256(payload))

    store = CountingStore(data_dir)
    resumed = UploadService(Database(data_dir / "db.sqlite3"), store, checkpoint_bytes=1024)
    receipt = resumed.finalize(sid)
    assert receipt["final_sha256"] == sha256(payload)
    assert store.reads == []  # no re-assembly: the artifact was verified, not rebuilt
    assert artifact.read_bytes() == payload
    assert get_fin(resumed.db, sid)["state"] == "completed"


def test_publish_window_artifact_mismatch_is_recovery_error(tmp_path):
    data_dir = tmp_path / "data"
    payload = make_bytes(1024 * 3)
    service = make_service(data_dir)
    sid = seed_session(service, payload, 1024)
    upload_all(service, sid, payload, 1024)

    box: dict = {}
    crash_after(service, 3, box)
    with pytest.raises(Crash):
        service.finalize(sid)

    # a wrong file sits at the artifact path and the temp is gone
    garbage = make_bytes(len(payload), seed="garbage")
    artifact = service.store.artifact_path(sid)
    artifact.write_bytes(garbage)
    service.store.finalize_tmp_path(sid).unlink()
    _craft_publish_state(service.db, sid, sha256(payload))

    resumed = make_service(data_dir)
    _assert_recovery_error(resumed, sid, payload, expect_artifact=True)
    # the suspicious artifact is left untouched, session not completed
    assert artifact.read_bytes() == garbage
    assert resumed.get_session_or_404(sid)["status"] == "active"


# ---------- in-place migration of legacy databases ----------

OLD_SCHEMA = """
CREATE TABLE sessions (
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
CREATE TABLE chunks (
    session_id  TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    path        TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (session_id, chunk_index)
);
"""


def _bitmap(total: int, received: list[int]) -> bytes:
    from app.bitmap import new_bitmap, set_bit

    bitmap = new_bitmap(total)
    for i in received:
        set_bit(bitmap, i)
    return bytes(bitmap)


def test_migration_of_legacy_database(tmp_path):
    data_dir = tmp_path / "data"
    chunks_root = data_dir / "chunks"
    artifacts = data_dir / "artifacts"
    artifacts.mkdir(parents=True)

    active_payload = make_bytes(200)
    done_payload = make_bytes(150, seed="done")
    expired_payload = make_bytes(64, seed="expired")
    now = datetime.now(timezone.utc)
    sessions = [
        # (sid, payload, chunk_size, received, status, expires_in_hours)
        ("legacy-active", active_payload, 64, [0, 2], "active", 1.0),
        ("legacy-done", done_payload, 64, [0, 1, 2], "completed", 1.0),
        ("legacy-expired", expired_payload, 64, [], "active", -1.0),
    ]
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    conn.executescript(OLD_SCHEMA)
    for sid, payload, chunk_size, received, status, hours in sessions:
        total = -(-len(payload) // chunk_size)
        expires = (now + timedelta(hours=hours)).isoformat()
        conn.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                sid, len(payload), chunk_size, total, sha256(payload), status,
                _bitmap(total, received), expires, now.isoformat(),
                now.isoformat() if status == "completed" else None,
                sha256(payload) if status == "completed" else None,
                str(artifacts / f"{sid}.bin") if status == "completed" else None,
            ),
        )
        chunk_dir = chunks_root / sid
        chunk_dir.mkdir(parents=True)
        for i in received:
            body = payload[i * chunk_size : (i + 1) * chunk_size]
            path = chunk_dir / f"{i:08d}.chunk"
            path.write_bytes(body)
            conn.execute(
                "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
                (sid, i, len(body), sha256(body), str(path), now.isoformat()),
            )
        if status == "completed":
            (artifacts / f"{sid}.bin").write_bytes(payload)
    conn.commit()
    conn.close()

    # open the app on the legacy database: migration runs in place
    app = create_app(Settings(data_dir=data_dir, finalize_checkpoint_bytes=64))
    with TestClient(app) as client:
        # completed session: still queryable and downloadable, no re-upload
        status = client.get("/sessions/legacy-done").json()
        assert status["status"] == "completed"
        assert status["received_count"] == 3
        resp = client.get("/sessions/legacy-done/artifact")
        assert resp.status_code == 200 and resp.content == done_payload
        fin = client.get("/sessions/legacy-done/finalization").json()
        assert fin["state"] == "completed"
        assert fin["confirmed_bytes"] == len(done_payload)
        assert fin["generation"] == 0

        # expired session: still reported expired, new chunks rejected
        assert client.get("/sessions/legacy-expired").json()["status"] == "expired"
        body = expired_payload[:64]
        resp = client.put(
            "/sessions/legacy-expired/chunks/0", content=body, headers={"X-Chunk-SHA256": sha256(body)}
        )
        assert resp.status_code == 410

        # active session: previously confirmed chunks survive; resume + finalize
        status = client.get("/sessions/legacy-active").json()
        assert status["status"] == "active"
        assert status["missing_chunks"] == [1, 3]
        for i in (1, 3):
            body = active_payload[i * 64 : (i + 1) * 64]
            resp = client.put(
                f"/sessions/legacy-active/chunks/{i}", content=body, headers={"X-Chunk-SHA256": sha256(body)}
            )
            assert resp.status_code == 201
        assert client.post("/sessions/legacy-active/finalize").status_code == 200
        assert client.get("/sessions/legacy-active/artifact").content == active_payload
        fin = client.get("/sessions/legacy-active/finalization").json()
        assert fin["state"] == "completed"
        assert fin["generation"] == 1

        # the legacy rows are still intact in the same database file
        service = app.state.service
        assert service.db.get_session("legacy-active") is not None
        assert service.db.get_finalization("legacy-done")["state"] == "completed"
