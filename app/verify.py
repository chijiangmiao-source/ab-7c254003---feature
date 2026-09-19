"""One-shot acceptance client: exercises the full resume flow against a live API.

Run with:  API_BASE_URL=http://api:8000 python -m app.verify
Exit code 0 means every check passed.

Besides the upload/resume/finalize flow it drives *real cross-process
recovery*: through the shared DATA_DIR volume it arms a one-shot fault that
makes the API SIGKILL itself at an exact finalization crash window, waits for
the container to restart and reconcile, then asserts the protocol resumes at
the last confirmed byte (or converges the publishing windows) — no fixed
delays, no in-memory shortcuts.
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
# Real cross-process crash/takeover scenarios need the verify client to share
# the API's DATA_DIR volume (the docker-compose acceptance setup mounts it).
# They are opt-in locally via RECOVERY_SCENARIOS=1 DATA_DIR=...
RECOVERY_SCENARIOS_ENABLED = os.environ.get("RECOVERY_SCENARIOS", "") in ("1", "true", "yes")
CHUNK_SIZE = 1024 * 1024
FILE_SIZE = CHUNK_SIZE * 4 + 12345  # 5 chunks, last one short


class VerifyFailure(Exception):
    pass


def check(condition: bool, message: str) -> None:
    if not condition:
        raise VerifyFailure(message)


def payload(size: int) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"verify-payload:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def wait_for_api(client: httpx.Client, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if client.get("/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise VerifyFailure(f"API at {BASE_URL} did not become healthy within {timeout:.0f}s")


# ---- real cross-process crash / takeover acceptance ----


def faults_dir() -> Path:
    path = DATA_DIR / ".faults"
    path.mkdir(parents=True, exist_ok=True)
    return path


def arm_fault(name: str, session_id: str) -> None:
    (faults_dir() / name).write_text(session_id)


def finalization_state(client: httpx.Client, sid: str) -> dict:
    resp = client.get(f"/sessions/{sid}/finalization")
    check(resp.status_code == 200, f"finalization status failed: {resp.text}")
    return resp.json()


def upload_full_session(client: httpx.Client, data: bytes, chunks: list[bytes], digests: list[str]) -> str:
    resp = client.post(
        "/sessions",
        json={
            "file_size": len(data),
            "chunk_size": CHUNK_SIZE,
            "file_sha256": sha256(data),
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(),
        },
    )
    check(resp.status_code == 201, f"create session failed: {resp.status_code} {resp.text}")
    sid = resp.json()["session_id"]
    for i, part in enumerate(chunks):
        resp = put_chunk(client, sid, i, part, digests[i])
        check(resp.status_code == 201, f"chunk {i} upload failed: {resp.text}")
    return sid


def crash_api_during_finalize(client: httpx.Client, sid: str) -> None:
    """POST finalize; the armed fault makes the API SIGKILL itself."""
    try:
        client.post(f"/sessions/{sid}/finalize", timeout=httpx.Timeout(10.0))
    except httpx.RequestError:
        return  # connection died with the killed process — expected
    raise VerifyFailure("expected the API process to be killed during finalize")


def check_artifact(client: httpx.Client, sid: str, data: bytes) -> None:
    resp = client.get(f"/sessions/{sid}/artifact")
    check(resp.status_code == 200, f"artifact download failed: {resp.text}")
    check(resp.content == data, "artifact bytes differ from the original file")
    check(resp.headers.get("x-file-sha256") == sha256(data), "artifact digest header mismatch")


def recovery_scenarios(client: httpx.Client) -> None:
    # Scenario 1: SIGKILL right after a durable assembly checkpoint. The fresh
    # process resumes at the confirmed byte and never recomputes the prefix.
    data = payload(FILE_SIZE)
    chunks = [data[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE] for i in range(-(-FILE_SIZE // CHUNK_SIZE))]
    digests = [sha256(c) for c in chunks]
    sid = upload_full_session(client, data, chunks, digests)

    arm_fault("kill_post_checkpoint", sid)
    crash_api_during_finalize(client, sid)
    wait_for_api(client)

    state = finalization_state(client, sid)
    check(state["state"] == "assembling", f"expected assembling after crash, got {state}")
    check(
        state["confirmed_bytes"] == CHUNK_SIZE,
        f"checkpoint must survive the kill at the confirmed byte, got {state['confirmed_bytes']}",
    )
    print(f"[verify] assembly crash resumed: checkpoint holds {state['confirmed_bytes']} bytes")

    resp = client.post(f"/sessions/{sid}/finalize")
    check(resp.status_code == 200, f"resumed finalize failed: {resp.text}")
    check(resp.json()["final_sha256"] == sha256(data), "resumed finalize produced a wrong digest")
    check_artifact(client, sid, data)
    print("[verify] SIGKILL after checkpoint -> fresh process continued SHA-256 and published")

    # Scenario 2: every publishing crash window converges after a restart and
    # never exposes a half-published artifact or re-assembles the file.
    for fault in ("kill_after_link", "kill_after_intent", "kill_after_rename"):
        sid = upload_full_session(client, data, chunks, digests)
        arm_fault(fault, sid)
        crash_api_during_finalize(client, sid)
        wait_for_api(client)

        state = finalization_state(client, sid)
        check(
            state["confirmed_bytes"] == FILE_SIZE,
            f"{fault}: progress must not regress after restart, got {state}",
        )
        resp = client.post(f"/sessions/{sid}/finalize")
        check(resp.status_code == 200, f"{fault}: converging finalize failed: {resp.text}")
        check(resp.json()["final_sha256"] == sha256(data), f"{fault}: wrong digest")
        check_artifact(client, sid, data)
        print(f"[verify] publishing window {fault} converged after restart without re-assembly")

    # Scenario 3: a concurrent finalize that does not hold the lease gets the
    # structured 409 carrying the current generation and a retry time.
    sid = upload_full_session(client, data, chunks, digests)
    d = faults_dir()
    (d / "progress").write_text("")
    (d / "gate").write_text(sid)
    try:
        import threading

        holder = {}

        def run_holder() -> None:
            try:
                holder["resp"] = client.post(f"/sessions/{sid}/finalize", timeout=httpx.Timeout(30.0))
            except Exception as exc:  # pragma: no cover - surfaced via check below
                holder["error"] = exc

        t = threading.Thread(target=run_holder)
        t.start()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and not (d / "progress").read_text().strip():
            time.sleep(0.05)

        conflict = client.post(f"/sessions/{sid}/finalize")
        check(conflict.status_code == 409, f"expected 409 for concurrent finalize: {conflict.text}")
        body = conflict.json()["error"]
        check(body["code"] == "FINALIZATION_IN_PROGRESS", body)
        details = body["details"]
        check(details["generation"] == 1, details)
        check("retry_at" in details and "retry_after_seconds" in details, details)

        (d / "gate_released").write_text("ok")
        t.join(timeout=30)
        check("resp" in holder, holder)
        check(holder["resp"].status_code == 200, holder["resp"].text)
        check_artifact(client, sid, data)
        print("[verify] concurrent finalize got structured 409 with generation + retry time")
    finally:
        for name in ("gate", "gate_released", "progress"):
            (d / name).unlink(missing_ok=True)



def expect_error(resp: httpx.Response, status: int, code: str) -> dict:
    check(resp.status_code == status, f"expected HTTP {status}, got {resp.status_code}: {resp.text}")
    body = resp.json()
    check(isinstance(body.get("error"), dict), f"error envelope missing: {body}")
    check(
        body["error"].get("code") == code,
        f"expected error code {code}, got {body['error'].get('code')}",
    )
    return body


def put_chunk(client: httpx.Client, sid: str, index: int, body: bytes, digest: str) -> httpx.Response:
    return client.put(f"/sessions/{sid}/chunks/{index}", content=body, headers={"X-Chunk-SHA256": digest})


def main() -> int:
    started = time.monotonic()
    with httpx.Client(base_url=BASE_URL, timeout=30.0) as client:
        wait_for_api(client)
        print(f"[verify] API healthy at {BASE_URL}")

        data = payload(FILE_SIZE)
        file_sha = sha256(data)
        chunks = [data[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE] for i in range(-(-FILE_SIZE // CHUNK_SIZE))]
        digests = [sha256(c) for c in chunks]

        resp = client.post(
            "/sessions",
            json={
                "file_size": FILE_SIZE,
                "chunk_size": CHUNK_SIZE,
                "file_sha256": file_sha,
                "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(),
            },
        )
        check(resp.status_code == 201, f"create session failed: {resp.status_code} {resp.text}")
        session = resp.json()
        sid = session["session_id"]
        check(session["total_chunks"] == len(chunks), "total_chunks mismatch")
        check(session["missing_chunks"] == list(range(len(chunks))), "fresh session must miss every chunk")
        print(f"[verify] session {sid} created ({len(chunks)} chunks, {FILE_SIZE} bytes)")

        # Invalid chunks must be rejected and never recorded.
        expect_error(put_chunk(client, sid, 0, chunks[0], digests[1]), 400, "CHUNK_DIGEST_MISMATCH")
        expect_error(put_chunk(client, sid, len(chunks), chunks[0], digests[0]), 400, "CHUNK_INDEX_OUT_OF_RANGE")
        truncated = chunks[0][:-1]
        expect_error(put_chunk(client, sid, 0, truncated, sha256(truncated)), 400, "CHUNK_SIZE_MISMATCH")
        status = client.get(f"/sessions/{sid}").json()
        check(status["received_count"] == 0, "rejected chunks must not be recorded")
        print("[verify] digest/size/index violations rejected and not recorded")

        # Simulate a dropped connection: only the first 3 chunks go out.
        for i in range(3):
            resp = put_chunk(client, sid, i, chunks[i], digests[i])
            check(resp.status_code == 201, f"chunk {i} upload failed: {resp.text}")
        status = client.get(f"/sessions/{sid}").json()
        check(status["missing_chunks"] == [3, 4], f"expected missing [3, 4], got {status['missing_chunks']}")
        print("[verify] partial upload visible after 'interruption': missing [3, 4]")

        # Idempotent replay of the same bytes; conflicting bytes must get 409.
        resp = put_chunk(client, sid, 1, chunks[1], digests[1])
        check(resp.status_code == 200 and resp.json()["duplicate"] is True,
              f"idempotent replay failed: {resp.status_code} {resp.text}")
        other = bytes(len(chunks[1]))  # same length, different content
        expect_error(put_chunk(client, sid, 1, other, sha256(other)), 409, "CHUNK_CONFLICT")
        print("[verify] idempotent replay accepted, conflicting content rejected with 409")

        # Finalize too early must fail and list the missing chunks.
        body = expect_error(client.post(f"/sessions/{sid}/finalize"), 409, "CHUNKS_INCOMPLETE")
        check(body["error"]["details"]["missing_chunks"] == [3, 4], "finalize error must list missing chunks")

        # Resume: upload the remaining chunks.
        for i in (3, 4):
            resp = put_chunk(client, sid, i, chunks[i], digests[i])
            check(resp.status_code == 201, f"resume chunk {i} failed: {resp.text}")
        status = client.get(f"/sessions/{sid}").json()
        check(status["missing_chunks"] == [], "all chunks should be received now")
        print("[verify] resumed upload completed the bitmap")

        # Finalize, re-finalize (idempotent), download and verify the artifact.
        resp = client.post(f"/sessions/{sid}/finalize")
        check(resp.status_code == 200, f"finalize failed: {resp.text}")
        check(resp.json()["final_sha256"] == file_sha, "final SHA-256 mismatch")
        again = client.post(f"/sessions/{sid}/finalize")
        check(again.status_code == 200 and again.json()["final_sha256"] == file_sha,
              "finalize must be idempotent")
        resp = client.get(f"/sessions/{sid}/artifact")
        check(resp.status_code == 200, f"artifact download failed: {resp.text}")
        check(resp.content == data, "artifact bytes differ from the original file")
        check(resp.headers.get("x-file-sha256") == file_sha, "artifact digest header mismatch")
        status = client.get(f"/sessions/{sid}").json()
        check(status["status"] == "completed", "session must be completed")
        print(f"[verify] artifact published and verified (sha256={file_sha[:16]}...)")

        if RECOVERY_SCENARIOS_ENABLED:
            recovery_scenarios(client)
        else:
            print("[verify] crash/takeover scenarios skipped (set RECOVERY_SCENARIOS=1 with"
                  " a shared DATA_DIR to enable)")

    print(f"[verify] ALL CHECKS PASSED in {time.monotonic() - started:.1f}s")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except VerifyFailure as exc:
        print(f"[verify] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPError as exc:
        print(f"[verify] FAILED: HTTP error: {exc}", file=sys.stderr)
        sys.exit(1)
