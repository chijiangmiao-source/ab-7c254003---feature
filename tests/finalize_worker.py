"""Child-process driver for the genuine SIGKILL recovery test.

Usage: python tests/finalize_worker.py <data_dir> <session_id> <crash|resume>

In "crash" mode the worker kills itself with SIGKILL once the second
checkpoint has been committed, leaving exactly the durable state a crashed
process would leave.  In "resume" mode it runs finalize to completion —
taking the lease over once the dead holder's short lease expires — and prints
the receipt as JSON on the last stdout line.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path

from app.db import Database
from app.errors import ApiError
from app.service import UploadService
from app.storage import ChunkStore

LEASE_TTL = 0.2  # short so the dead holder's lease expires quickly


def main() -> int:
    data_dir, session_id, mode = sys.argv[1], sys.argv[2], sys.argv[3]
    hook = None
    if mode == "crash":
        checkpoints: list[int] = []

        def hook(_sid: str, confirmed: int) -> None:  # noqa: F811
            checkpoints.append(confirmed)
            if len(checkpoints) == 2:
                os.kill(os.getpid(), signal.SIGKILL)

    service = UploadService(
        Database(Path(data_dir) / "db.sqlite3"),
        ChunkStore(Path(data_dir)),
        checkpoint_bytes=1024,
        lease_ttl_seconds=LEASE_TTL,
        checkpoint_hook=hook,
    )
    deadline = time.monotonic() + 30.0
    while True:
        try:
            receipt = service.finalize(session_id)
            break
        except ApiError as exc:
            # the crashed holder's lease is still valid for a short while
            if exc.code != "FINALIZATION_IN_PROGRESS" or time.monotonic() > deadline:
                raise
            time.sleep(0.1)
    print(json.dumps(receipt))
    return 0


if __name__ == "__main__":
    sys.exit(main())
