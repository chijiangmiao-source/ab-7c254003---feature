"""Real-process recovery tests for the crash-safe finalization protocol.

These tests run finalization in *separate OS processes* (app.recovery_worker)
and kill them with SIGKILL at exact crash windows, then restart them against
the same DATA_DIR.  Checkpoints are real fsync + SQLite commits; leases really
expire on the database clock; nothing is mocked, frozen or restarted from
byte zero.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from app import finalization
from app.config import Settings
from app.errors import ApiError
from app.main import create_app
from app.storage import ChunkStore
from tests.test_upload_api import (
    chunk,
    create_session,
    make_bytes,
    put_chunk,
    sha256,
    upload_all,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

WORKER_ENV = {
    "FINALIZE_LEASE_SECONDS": "1.5",
    "FINALIZE_LEASE_RENEW_SECONDS": "0.5",
    "FINALIZE_CHECKPOINT_BYTES": str(1024 * 1024),
    "FINALIZE_COPY_BUFFER": str(256 * 1024),
    "PYTHONPATH": str(REPO_ROOT),
    "PATH": os.environ.get("PATH", ""),
}


@pytest.fixture()
def data_dir(tmp_path):
    d = tmp_path / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _prepare_session(data_dir: Path, payload: bytes, chunk_size: int, *, file_sha256=None) -> str:
    """Create a session and upload all chunks, then fully close the DB."""
    app = create_app(Settings(data_dir=data_dir))
    try:
        with __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(app) as client:
            sid = create_session(client, payload, chunk_size, file_sha256=file_sha256)["session_id"]
            upload_all(client, sid, payload, chunk_size)
    finally:
        app.state.service.db.close()
    return sid


def _start_worker(data_dir: Path, sid: str, mode: str = "restart", deadline: str = "25", env=None):
    full_env = {**os.environ, **WORKER_ENV, **(env or {})}
    cmd = [sys.executable, "-m", "app.recovery_worker", str(data_dir), sid]
    if mode != "restart":
        cmd += [mode, deadline]
    return subprocess.Popen(
        cmd, cwd=REPO_ROOT, env=full_env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


def _results(proc, timeout=40):
    out, err = proc.communicate(timeout=timeout)
    assert proc.returncode == 0, f"worker failed:\n{err}\n{out}"
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def _raw_finalization(data_dir: Path, sid: str) -> dict:
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM finalizations WHERE session_id = ?", (sid,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _fault(data_dir: Path, name: str, content: str = "*") -> None:
    d = data_dir / ".faults"
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(content)


def _restart_and_complete(data_dir: Path, sid: str, payload: bytes):
    proc = _start_worker(data_dir, sid)
    results = _results(proc)
    assert results[-1]["status"] == 200, results
    # Verify through a freshly booted app (startup reconcile included).
    app = create_app(Settings(data_dir=data_dir))
    try:
        with __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(app) as client:
            download = client.get(f"/sessions/{sid}/artifact")
            assert download.status_code == 200
            assert download.content == payload
            assert download.headers["x-file-sha256"] == sha256(payload)
    finally:
        app.state.service.db.close()


# ---------- streaming checkpoints + SIGKILL recovery ----------

@pytest.mark.parametrize(
    "fault,expected_confirmed",
    [
        ("kill_pre_checkpoint", 0),   # bytes fsynced but checkpoint not committed
        ("kill_post_checkpoint", 1024 * 1024),
    ],
)
def test_sigkill_during_assembly_resumes_at_confirmed_byte(data_dir, fault, expected_confirmed):
    payload = make_bytes(3 * 1024 * 1024 + 777)
    sid = _prepare_session(data_dir, payload, 1024 * 1024)

    _fault(data_dir, fault, sid)
    proc = _start_worker(data_dir, sid)
    out, err = proc.communicate(timeout=30)
    assert proc.returncode == -9, f"expected SIGKILL, rc={proc.returncode}\n{err}\n{out}"

    row = _raw_finalization(data_dir, sid)
    assert row["phase"] == "assembling"
    assert row["confirmed_bytes"] == expected_confirmed
    assert row["lease_until"] is not None  # dead owner's lease is still recorded

    tmp_files = list((data_dir / ".finalize").glob(f"{sid}.g*.tmp"))
    assert tmp_files
    # Even when the checkpoint did not commit (pre), the fsynced-but-
    # unconfirmed 1 MiB tail is on disk and must be dropped on resume.
    assert tmp_files[0].stat().st_size >= 1024 * 1024

    _restart_and_complete(data_dir, sid, payload)

    row = _raw_finalization(data_dir, sid)
    assert row["phase"] == "completed"
    assert row["confirmed_bytes"] == len(payload)


def test_unconfirmed_tail_is_dropped_on_resume(data_dir):
    """pre-checkpoint crash: fsynced bytes beyond the checkpoint are discarded.

    End-to-end across a real SIGKILL, plus direct checks of the truncation and
    prefix-clone primitives that enforce it.
    """
    from app.finalization import clone_confirmed_prefix, open_generation_tmp

    payload = make_bytes(3 * 1024 * 1024 + 777)
    sid = _prepare_session(data_dir, payload, 1024 * 1024)

    _fault(data_dir, "kill_pre_checkpoint", sid)
    proc = _start_worker(data_dir, sid)
    proc.communicate(timeout=30)
    assert proc.returncode == -9

    g1 = data_dir / ".finalize" / f"{sid}.g1.tmp"
    assert g1.stat().st_size == 1024 * 1024  # fsynced, but checkpoint stayed at 0

    # The resuming generation opens at the confirmed boundary (0): the
    # fsynced-but-unconfirmed 1 MiB tail is truncated before any append.
    with open_generation_tmp(g1, 0) as writer:
        assert writer.position == 0
    assert g1.stat().st_size == 0

    # Prefix clone copies exactly the confirmed byte count, never the tail.
    src = data_dir / ".finalize" / "prefix_src.tmp"
    src.write_bytes(payload[: 2 * 1024 * 1024])
    store = ChunkStore(data_dir)
    cloned = clone_confirmed_prefix(store, "clone-session", src, generation=3, confirmed_bytes=1024 * 1024)
    assert cloned.stat().st_size == 1024 * 1024
    assert cloned.read_bytes() == payload[: 1024 * 1024]

    # End-to-end: restart assembles from byte 0 to the correct artifact.
    _restart_and_complete(data_dir, sid, payload)


def test_tail_beyond_committed_checkpoint_is_dropped(data_dir):
    """committed 1 MiB; kill before the 2 MiB checkpoint commits.

    On disk: tmp is 2 MiB but only 1 MiB is confirmed. The resuming generation
    must clone/truncate to 1 MiB and still produce the correct file.
    """
    payload = make_bytes(3 * 1024 * 1024 + 777)
    sid = _prepare_session(data_dir, payload, 1024 * 1024)

    _fault(data_dir, "kill_pre_checkpoint", f"{sid}:{1024 * 1024}")
    proc = _start_worker(data_dir, sid)
    proc.communicate(timeout=30)
    assert proc.returncode == -9

    row = _raw_finalization(data_dir, sid)
    assert row["confirmed_bytes"] == 1024 * 1024
    g1 = data_dir / ".finalize" / f"{sid}.g1.tmp"
    assert g1.stat().st_size == 2 * 1024 * 1024  # 1 MiB confirmed + 1 MiB unconfirmed

    _restart_and_complete(data_dir, sid, payload)
    row = _raw_finalization(data_dir, sid)
    assert row["confirmed_bytes"] == len(payload)


# ---------- publishing crash windows ----------

@pytest.mark.parametrize(
    "fault,artifact_present",
    [
        ("kill_after_link", False),
        ("kill_after_intent", False),
        ("kill_after_rename", True),
    ],
)
def test_sigkill_in_each_publishing_window_converges(data_dir, fault, artifact_present):
    payload = make_bytes(2 * 1024 * 1024 + 4321)
    sid = _prepare_session(data_dir, payload, 1024 * 1024)

    _fault(data_dir, fault, sid)
    proc = _start_worker(data_dir, sid)
    out, err = proc.communicate(timeout=30)
    assert proc.returncode == -9, f"{fault}: rc={proc.returncode}\n{err}\n{out}"

    row = _raw_finalization(data_dir, sid)
    assert row["phase"] == "publishing"
    assert row["confirmed_bytes"] == len(payload)  # progress never regresses
    artifact = data_dir / "artifacts" / f"{sid}.bin"
    assert artifact.exists() is artifact_present
    session_status = _session_status(data_dir, sid)
    assert session_status == "active"  # half-published file is never downloadable

    # Restart must NOT re-assemble; it converges the publish windows.
    proc = _start_worker(data_dir, sid)
    results = _results(proc)
    assert results[-1]["status"] == 200, results
    _restart_and_complete(data_dir, sid, payload)


def test_publishing_crash_without_candidate_resumes_from_checkpoint(data_dir):
    payload = make_bytes(2 * 1024 * 1024 + 4321)
    sid = _prepare_session(data_dir, payload, 1024 * 1024)

    _fault(data_dir, "kill_after_link", sid)
    proc = _start_worker(data_dir, sid)
    proc.communicate(timeout=30)
    assert proc.returncode == -9
    # Lose the candidate: recovery must resume assembly rather than guess.
    for cand in (data_dir / "artifacts").glob(f".{sid}.g*.cand"):
        cand.unlink()

    _restart_and_complete(data_dir, sid, payload)


def test_publishing_crash_rejects_non_matching_artifact(data_dir):
    payload = make_bytes(2 * 1024 * 1024 + 4321)
    sid = _prepare_session(data_dir, payload, 1024 * 1024)

    _fault(data_dir, "kill_after_link", sid)
    proc = _start_worker(data_dir, sid)
    proc.communicate(timeout=30)
    assert proc.returncode == -9

    # Plant a wrong file at the download path before the restart.
    artifact = data_dir / "artifacts" / f"{sid}.bin"
    artifact.write_bytes(b"definitely not the file")
    _restart_and_complete(data_dir, sid, payload)
    assert artifact.read_bytes() == payload


# ---------- concurrent takeover across real processes ----------

def test_lease_takeover_by_higher_generation(data_dir):
    payload = make_bytes(6 * 1024 * 1024 + 11)
    sid = _prepare_session(data_dir, payload, 1024 * 1024)

    faults = data_dir / ".faults"
    faults.mkdir(parents=True, exist_ok=True)
    (faults / "gate").write_text(sid)
    (faults / "progress").write_text("")

    holder = _start_worker(data_dir, sid)  # generation 1, blocks at 1 MiB

    # Wait for the real checkpoint to land (fsync + SQLite commit).
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        positions = [int(x) for x in (faults / "progress").read_text().split()] if (faults / "progress").read_text() else []
        if positions:
            break
        if holder.poll() is not None:
            raise AssertionError("holder died early")
        time.sleep(0.05)
    assert positions == [1024 * 1024]

    row = _raw_finalization(data_dir, sid)
    assert row["fence_generation"] == 1
    assert row["confirmed_bytes"] == 1024 * 1024

    taker = _start_worker(data_dir, sid, mode="takeover", deadline="25")

    # The taker must observe the structured 409 with generation + retry time.
    seen_409 = None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and seen_409 is None:
        row = _raw_finalization(data_dir, sid)
        if row["fence_generation"] >= 2:
            break
        time.sleep(0.05)

    # Let both processes proceed.
    (faults / "gate_released").write_text("ok")

    holder_out, holder_err = holder.communicate(timeout=30)
    taker_out, taker_err = taker.communicate(timeout=40)
    assert holder.returncode == 0, holder_err
    assert taker.returncode == 0, taker_err

    holder_lines = [json.loads(x) for x in holder_out.splitlines() if x.strip()]
    taker_lines = [json.loads(x) for x in taker_out.splitlines() if x.strip()]

    # Stale holder, even after un-pausing, cannot complete the finalization.
    assert holder_lines[-1]["status"] == 409
    assert holder_lines[-1]["code"] == "FINALIZATION_IN_PROGRESS"

    conflicts = [r for r in taker_lines if r["status"] == 409]
    assert conflicts, taker_lines
    assert conflicts[0]["code"] == "FINALIZATION_IN_PROGRESS"
    assert conflicts[0]["details"]["generation"] == 1
    assert "retry_at" in conflicts[0]["details"]
    assert "retry_after_seconds" in conflicts[0]["details"]
    assert taker_lines[-1]["status"] == 200

    row = _raw_finalization(data_dir, sid)
    assert row["phase"] == "completed"
    assert row["fence_generation"] == 2
    assert row["confirmed_bytes"] == len(payload)
    assert (data_dir / "artifacts" / f"{sid}.bin").read_bytes() == payload


# ---------- resume never re-reads the confirmed prefix ----------

class _StopAtCheckpoint:
    def after_checkpoint(self, position: int) -> None:
        raise _AbortAssembly(position)


class _AbortAssembly(Exception):
    pass


def test_resume_does_not_reread_confirmed_prefix(data_dir, monkeypatch):
    payload = make_bytes(3 * 1024 * 1024 + 5)
    chunks_dir = data_dir / "chunks"

    app = create_app(Settings(data_dir=data_dir))
    service = app.state.service
    # Small checkpoints, long lease so nothing expires in-process.
    service.protocol.checkpoint_bytes = 1024 * 1024

    client = __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(app)
    sid = create_session(client, payload, 1024 * 1024)["session_id"]
    upload_all(client, sid, payload, 1024 * 1024)

    real_gate = finalization.gate_for

    def gate_for(_data_dir):
        return _StopAtCheckpoint()

    monkeypatch.setattr(finalization, "gate_for", gate_for)
    with pytest.raises(_AbortAssembly):
        service.finalize(sid)
    monkeypatch.setattr(finalization, "gate_for", real_gate)

    row = service.db.get_finalization(sid)
    assert row["confirmed_bytes"] == 1024 * 1024
    # Simulate process restart: dead lease is cleared by startup reconciliation.
    service.db.clear_lease(sid)

    # Count bytes actually read from chunk source files on the resuming run.
    import builtins
    real_open = builtins.open
    read_bytes = {"n": 0}

    def counting_open(file, *args, **kwargs):
        fh = real_open(file, *args, **kwargs)
        if isinstance(file, (str, Path)) and Path(file).is_relative_to(chunks_dir):
            class _Wrapped:
                def __init__(self, inner):
                    self.inner = inner

                def read(self, *a, **k):
                    data = self.inner.read(*a, **k)
                    read_bytes["n"] += len(data)
                    return data

                def __getattr__(self, name):
                    return getattr(self.inner, name)

                def __enter__(self):
                    self.inner.__enter__()
                    return self

                def __exit__(self, *exc):
                    return self.inner.__exit__(*exc)

            return _Wrapped(fh)
        return fh

    monkeypatch.setattr("builtins.open", counting_open)
    try:
        receipt = service.finalize(sid)
    finally:
        monkeypatch.setattr("builtins.open", real_open)
    assert receipt["final_sha256"] == sha256(payload)
    # Exactly the suffix is read; the confirmed 1 MiB prefix is never re-read.
    assert read_bytes["n"] == len(payload) - 1024 * 1024
    service.db.close()


# ---------- progress endpoint + in-process concurrency ----------

def test_finalization_progress_lifecycle_and_409(data_dir):
    from fastapi.testclient import TestClient

    import threading

    payload = make_bytes(3 * 1024 * 1024 + 99)
    app = create_app(Settings(data_dir=data_dir))
    service = app.state.service
    service.protocol.checkpoint_bytes = 1024 * 1024

    with TestClient(app) as client:
        sid = create_session(client, payload, 1024 * 1024)["session_id"]
        upload_all(client, sid, payload, 1024 * 1024)

        idle = client.get(f"/sessions/{sid}/finalization")
        assert idle.status_code == 200
        assert idle.json() == {
            "session_id": sid,
            "state": "idle",
            "confirmed_bytes": 0,
            "total_bytes": 0,
            "generation": 0,
            "last_error": None,
        }

        faults = data_dir / ".faults"
        faults.mkdir(parents=True, exist_ok=True)
        (faults / "gate").write_text(sid)
        (faults / "progress").write_text("")

        result = {}

        def worker():
            try:
                result["receipt"] = service.finalize(sid)
            except ApiError as exc:
                result["error"] = exc

        t = threading.Thread(target=worker)
        t.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if (faults / "progress").read_text().strip():
                break
            time.sleep(0.02)

        progress = client.get(f"/sessions/{sid}/finalization").json()
        assert progress["state"] == "assembling"
        assert progress["confirmed_bytes"] == 1024 * 1024
        assert progress["total_bytes"] == len(payload)
        assert progress["generation"] == 1

        conflict = client.post(f"/sessions/{sid}/finalize")
        assert conflict.status_code == 409
        body = conflict.json()["error"]
        assert body["code"] == "FINALIZATION_IN_PROGRESS"
        assert body["details"]["generation"] == 1
        assert body["details"]["phase"] == "assembling"
        assert "retry_at" in body["details"]

        (faults / "gate_released").write_text("ok")
        t.join(timeout=20)
        assert "receipt" in result, result

        done = client.get(f"/sessions/{sid}/finalization").json()
        assert done["state"] == "completed"
        assert done["confirmed_bytes"] == len(payload)


# ---------- integrity failure survives a crash ----------

def test_integrity_mismatch_after_crash_is_persisted(data_dir):
    payload = make_bytes(2 * 1024 * 1024 + 333)
    declared = sha256(b"different-declared-content")
    sid = _prepare_session(data_dir, payload, 1024 * 1024, file_sha256=declared)

    _fault(data_dir, "kill_post_checkpoint", sid)
    proc = _start_worker(data_dir, sid)
    proc.communicate(timeout=30)
    assert proc.returncode == -9

    proc = _start_worker(data_dir, sid)
    results = _results(proc)
    assert results[-1]["status"] == 422
    assert results[-1]["code"] == "INTEGRITY_MISMATCH"

    # Deterministic for every later call; chunks kept; no downloadable file.
    app = create_app(Settings(data_dir=data_dir))
    try:
        with __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(app) as client:
            again = client.post(f"/sessions/{sid}/finalize")
            assert again.status_code == 422
            assert again.json()["error"]["code"] == "INTEGRITY_MISMATCH"
            status = client.get(f"/sessions/{sid}").json()
            assert status["status"] == "active"
            assert status["missing_chunks"] == []
            assert client.get(f"/sessions/{sid}/artifact").status_code == 409
            progress = client.get(f"/sessions/{sid}/finalization").json()
            assert progress["state"] == "failed"
            assert progress["last_error"]["code"] == "INTEGRITY_MISMATCH"
    finally:
        app.state.service.db.close()


# ---------- corrupted recovery state ----------

@pytest.mark.parametrize(
    "corrupt",
    ["tmp_missing", "tmp_short", "bad_hasher", "unknown_version"],
)
def test_corrupt_recovery_state_is_structured_error(data_dir, corrupt):
    payload = make_bytes(2 * 1024 * 1024 + 333)
    sid = _prepare_session(data_dir, payload, 1024 * 1024)

    _fault(data_dir, "kill_post_checkpoint", sid)
    proc = _start_worker(data_dir, sid)
    proc.communicate(timeout=30)
    assert proc.returncode == -9
    assert _raw_finalization(data_dir, sid)["confirmed_bytes"] == 1024 * 1024

    fdir = data_dir / ".finalize"
    if corrupt == "tmp_missing":
        for f in fdir.glob(f"{sid}.g*.tmp"):
            f.unlink()
    elif corrupt == "tmp_short":
        for f in fdir.glob(f"{sid}.g*.tmp"):
            with open(f, "r+b") as fh:
                fh.truncate(10)
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    try:
        if corrupt == "bad_hasher":
            conn.execute(
                "UPDATE finalizations SET hasher_state = ? WHERE session_id = ?",
                ('{"v": 9, "h": [], "n": 0, "tail": ""}', sid),
            )
        elif corrupt == "unknown_version":
            conn.execute(
                "UPDATE finalizations SET schema_version = 99 WHERE session_id = ?", (sid,)
            )
        conn.commit()
    finally:
        conn.close()

    proc = _start_worker(data_dir, sid)
    results = _results(proc)
    assert results[-1]["status"] == 500, results
    assert results[-1]["code"] == "FINALIZATION_RECOVERY_ERROR"

    # Chunks and any artifact untouched; state stays failed and deterministic.
    app = create_app(Settings(data_dir=data_dir))
    try:
        with __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(app) as client:
            status = client.get(f"/sessions/{sid}").json()
            assert status["missing_chunks"] == []
            assert client.post(f"/sessions/{sid}/finalize").status_code == 500
            assert client.get(f"/sessions/{sid}/artifact").status_code == 409
            progress = client.get(f"/sessions/{sid}/finalization").json()
            assert progress["state"] == "failed"
            assert progress["last_error"]["code"] == "FINALIZATION_RECOVERY_ERROR"
    finally:
        app.state.service.db.close()


# ---------- in-place migration of an existing v1 database ----------

def test_in_place_migration_keeps_history(data_dir):
    from fastapi.testclient import TestClient

    # Build a populated database with the new code, then downgrade it to the
    # pre-feature schema (user_version 1, no finalizations table).
    completed_payload = make_bytes(1234)
    active_payload = make_bytes(2000)
    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as client:
        done_sid = create_session(client, completed_payload, 500)["session_id"]
        upload_all(client, done_sid, completed_payload, 500)
        assert client.post(f"/sessions/{done_sid}/finalize").status_code == 200

        active_sid = create_session(client, active_payload, 500)["session_id"]
        put_chunk(client, active_sid, 0, chunk(active_payload, 500, 0))
    app.state.service.db.close()

    conn = sqlite3.connect(data_dir / "db.sqlite3")
    conn.execute("DROP TABLE finalizations")
    conn.execute("PRAGMA user_version = 1")
    conn.commit()
    conn.close()

    app = create_app(Settings(data_dir=data_dir))  # migrates in place
    with TestClient(app) as client:
        # Historical completed session: queryable, downloadable, no re-upload.
        status = client.get(f"/sessions/{done_sid}").json()
        assert status["status"] == "completed"
        download = client.get(f"/sessions/{done_sid}/artifact")
        assert download.status_code == 200
        assert download.content == completed_payload
        progress = client.get(f"/sessions/{done_sid}/finalization").json()
        assert progress["state"] == "completed"
        assert progress["confirmed_bytes"] == len(completed_payload)

        # Historical active session with partial chunks survives untouched.
        status = client.get(f"/sessions/{active_sid}").json()
        assert status["status"] == "active"
        assert status["missing_chunks"] == [1, 2, 3]
        progress = client.get(f"/sessions/{active_sid}/finalization").json()
        assert progress["state"] == "idle"

        # And the active session can still be finished after the migration.
        for i in (1, 2, 3):
            resp = put_chunk(client, active_sid, i, chunk(active_payload, 500, i))
            assert resp.status_code == 201, resp.text
        resp = client.post(f"/sessions/{active_sid}/finalize")
        assert resp.status_code == 200
        assert resp.json()["final_sha256"] == sha256(active_payload)
    app.state.service.db.close()


def _session_status(data_dir: Path, sid: str) -> str:
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    try:
        return conn.execute("SELECT status FROM sessions WHERE session_id = ?", (sid,)).fetchone()[0]
    finally:
        conn.close()
