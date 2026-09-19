"""One-shot acceptance client: exercises the full resume flow against a live API.

Covers the classic resume chain plus the finalization protocol: the read-only
progress endpoint, lease-fenced concurrent finalizes, and — when the docker
socket is mounted (it is in docker-compose.yml) — a genuine SIGKILL of the
API container mid-assembly followed by a checkpoint resume after restart.

Run with:  API_BASE_URL=http://api:8000 python -m app.verify
Exit code 0 means every check passed.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
DOCKER_SOCKET = os.environ.get("VERIFY_DOCKER_SOCKET", "/var/run/docker.sock")
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


def create_session(client: httpx.Client, file_sha: str) -> str:
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
    return resp.json()["session_id"]


def upload_all(client: httpx.Client, sid: str, chunks: list[bytes], digests: list[str]) -> None:
    for i, (body, digest) in enumerate(zip(chunks, digests)):
        resp = put_chunk(client, sid, i, body, digest)
        check(resp.status_code == 201, f"chunk {i} upload failed: {resp.text}")


def finalize_until_done(client: httpx.Client, sid: str, timeout: float = 120.0) -> dict:
    """POST /finalize, transparently retrying 409 FINALIZATION_IN_PROGRESS."""
    deadline = time.monotonic() + timeout
    while True:
        resp = client.post(f"/sessions/{sid}/finalize", timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        body = expect_error(resp, 409, "FINALIZATION_IN_PROGRESS")
        details = body["error"]["details"]
        check(isinstance(details.get("generation"), int), "409 must carry the current generation")
        check(details.get("retry_after") is not None, "409 must carry a retry hint")
        check(time.monotonic() < deadline, "finalize did not converge in time")
        time.sleep(min(float(details["retry_after"]), 2.0) + 0.1)


def docker_client() -> httpx.Client | None:
    if not os.path.exists(DOCKER_SOCKET):
        return None
    return httpx.Client(
        transport=httpx.HTTPTransport(uds=DOCKER_SOCKET), base_url="http://docker", timeout=15.0
    )


def find_api_container(docker: httpx.Client) -> str:
    filters = json.dumps({"label": ["com.docker.compose.service=api"]})
    resp = docker.get("/containers/json", params={"filters": filters})
    resp.raise_for_status()
    containers = resp.json()
    check(containers, "no running container with label com.docker.compose.service=api")
    return containers[0]["Id"]


def verify_finalization_endpoint(client: httpx.Client, sid: str, state: str, confirmed: int) -> dict:
    resp = client.get(f"/sessions/{sid}/finalization")
    check(resp.status_code == 200, f"finalization status failed: {resp.text}")
    fin = resp.json()
    check(fin["state"] == state, f"expected state {state}, got {fin['state']}")
    check(fin["confirmed_bytes"] == confirmed, f"expected {confirmed} confirmed bytes, got {fin['confirmed_bytes']}")
    check(fin["total_bytes"] == FILE_SIZE, "total_bytes must equal the file size")
    check(isinstance(fin["generation"], int), "generation must be an integer")
    return fin


def verify_concurrent_finalize(client: httpx.Client, data: bytes, file_sha: str, chunks, digests) -> None:
    sid = create_session(client, file_sha)
    upload_all(client, sid, chunks, digests)

    results: dict[int, httpx.Response] = {}

    def fire(i: int) -> None:
        results[i] = client.post(f"/sessions/{sid}/finalize", timeout=120.0)

    threads = [threading.Thread(target=fire, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok, busy = [], []
    for resp in results.values():
        if resp.status_code == 200:
            ok.append(resp)
        else:
            busy.append(expect_error(resp, 409, "FINALIZATION_IN_PROGRESS"))
    check(len(ok) >= 1, "at least one concurrent finalize must succeed")
    check(len(busy) >= 1, "concurrent finalizes must be fenced with 409 FINALIZATION_IN_PROGRESS")
    for body in busy:
        details = body["error"]["details"]
        check(isinstance(details.get("generation"), int), "409 must carry the current generation")
        check(details.get("retry_after") is not None, "409 must carry a retry hint")
    for resp in ok:
        check(resp.json()["final_sha256"] == file_sha, "winner returned a wrong digest")

    # every later call converges to the same deterministic receipt
    receipt = finalize_until_done(client, sid)
    check(receipt["final_sha256"] == file_sha, "post-conflict finalize mismatch")
    fin = verify_finalization_endpoint(client, sid, "completed", FILE_SIZE)
    check(fin["generation"] >= 1, "generation must be at least 1 after completion")
    resp = client.get(f"/sessions/{sid}/artifact")
    check(resp.status_code == 200 and resp.content == data, "artifact after concurrent finalize is wrong")
    print(f"[verify] concurrent finalize fenced (409 with generation+retry_after), outcome deterministic")


def verify_sigkill_recovery(client: httpx.Client, data: bytes, file_sha: str, chunks, digests) -> None:
    docker = docker_client()
    if docker is None:
        print("[verify] docker socket unavailable; skipping live SIGKILL recovery (covered by pytest)")
        return
    api_id = find_api_container(docker)

    sid = create_session(client, file_sha)
    upload_all(client, sid, chunks, digests)
    fin = verify_finalization_endpoint(client, sid, "idle", 0)

    outcome: dict[str, object] = {}

    def background_finalize() -> None:
        try:
            outcome["resp"] = client.post(f"/sessions/{sid}/finalize", timeout=180.0)
        except httpx.HTTPError as exc:  # the kill severs the in-flight request
            outcome["error"] = exc

    thread = threading.Thread(target=background_finalize)
    thread.start()

    # wait for a *durable* checkpoint — real progress, no fixed delay
    confirmed_before = 0
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        fin = client.get(f"/sessions/{sid}/finalization").json()
        if fin["confirmed_bytes"] > 0:
            confirmed_before = fin["confirmed_bytes"]
            break
        time.sleep(0.1)
    check(confirmed_before > 0, "no checkpoint was committed before the crash")
    check(fin["state"] == "assembling", f"expected assembling, got {fin['state']}")

    resp = docker.post(f"/containers/{api_id}/kill", params={"signal": "SIGKILL"})
    check(resp.status_code in (204, 304), f"docker kill failed: {resp.status_code} {resp.text}")
    thread.join(timeout=30)
    resp = docker.post(f"/containers/{api_id}/start")
    check(resp.status_code in (204, 304), f"docker start failed: {resp.status_code} {resp.text}")
    wait_for_api(client)
    print(f"[verify] API SIGKILLed at {confirmed_before} confirmed bytes and restarted")

    # progress must not regress across the restart
    fin = client.get(f"/sessions/{sid}/finalization").json()
    check(
        fin["confirmed_bytes"] >= confirmed_before,
        f"progress regressed: {fin['confirmed_bytes']} < {confirmed_before}",
    )
    if fin["state"] != "completed":
        # half-finished output must not be exposed as a download
        expect_error(client.get(f"/sessions/{sid}/artifact"), 409, "ARTIFACT_NOT_READY")
        # the retried call takes over once the dead holder's lease expires and
        # resumes from the last confirmed byte
        receipt = finalize_until_done(client, sid)
        check(receipt["final_sha256"] == file_sha, "resumed finalize returned a wrong digest")

    resp = client.get(f"/sessions/{sid}/artifact")
    check(resp.status_code == 200, f"artifact download failed after recovery: {resp.text}")
    check(resp.content == data, "recovered artifact bytes differ from the original file")
    fin = verify_finalization_endpoint(client, sid, "completed", FILE_SIZE)
    check(fin["generation"] >= 2, "takeover must have bumped the fence generation")
    print("[verify] SIGKILL recovery: resumed from checkpoint, lease takeover, artifact byte-identical")


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

        # The finalization protocol: progress endpoint, concurrency fencing,
        # and genuine crash recovery.
        fin = verify_finalization_endpoint(client, sid, "completed", FILE_SIZE)
        check(fin["last_error"] is None, "completed finalization must not carry an error")
        print(f"[verify] finalization endpoint reports completed (generation {fin['generation']})")

        verify_concurrent_finalize(client, data, file_sha, chunks, digests)
        verify_sigkill_recovery(client, data, file_sha, chunks, digests)

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
