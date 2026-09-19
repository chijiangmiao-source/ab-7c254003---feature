"""Standalone finalization worker for real-process crash/takeover acceptance.

Runs in a fresh process against the on-disk DATA_DIR so a SIGKILL tears down
everything (locks, memory, open fds) exactly like a real crash.  Prints JSON
lines describing attempts:

    {"status": 200, "body": {...}}
    {"status": 409, "code": "FINALIZATION_IN_PROGRESS", "details": {...}}

Modes:
    python -m app.recovery_worker <data_dir> <sid>
        Full (re)start: build the app, which runs startup reconciliation.

    python -m app.recovery_worker <data_dir> <sid> takeover [seconds]
        A second, already-running process: no startup reconciliation (that
        only runs at boot).  It keeps issuing finalize calls until it either
        wins and completes or the deadline passes, printing one line per
        attempt, so callers observe the structured 409 *and* the eventual
        lease-based takeover.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from .config import Settings
from .db import Database
from .errors import ApiError
from .main import create_app
from .service import UploadService
from .storage import ChunkStore


def _emit(payload: dict) -> None:
    print(json.dumps(payload), flush=True)


def _attempt(service: UploadService, sid: str) -> dict:
    try:
        body = service.finalize(sid)
        return {"status": 200, "body": body}
    except ApiError as exc:
        return {
            "status": exc.status_code,
            "code": exc.code,
            "message": exc.message,
            "details": exc.details,
        }


def main() -> int:
    data_dir = Path(sys.argv[1])
    sid = sys.argv[2]
    mode = sys.argv[3] if len(sys.argv) > 3 else "restart"

    if mode == "restart":
        app = create_app(Settings(data_dir=data_dir))
        service = app.state.service
        try:
            _emit(_attempt(service, sid))
        finally:
            service.db.close()
        return 0

    # takeover: a concurrently running process — no reconcile on purpose.
    deadline = time.monotonic() + float(sys.argv[4] if len(sys.argv) > 4 else "20")
    db = Database(data_dir / "db.sqlite3")
    store = ChunkStore(data_dir)
    service = UploadService(db, store, data_dir)
    try:
        while True:
            result = _attempt(service, sid)
            _emit(result)
            if result["status"] == 200:
                return 0
            if time.monotonic() >= deadline:
                return 1
            time.sleep(0.2)
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
