from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    # finalization checkpoint cadence: the temp file is fsynced and the
    # checkpoint committed at least every this-many assembled bytes
    finalize_checkpoint_bytes: int = 1 << 20
    # lease time-to-live (database clock); a stalled holder can be taken over
    # once this elapses without a renewal
    finalize_lease_ttl_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            data_dir=Path(os.environ.get("DATA_DIR", "./data")).resolve(),
            finalize_checkpoint_bytes=int(os.environ.get("FINALIZE_CHECKPOINT_BYTES", str(1 << 20))),
            finalize_lease_ttl_seconds=float(os.environ.get("FINALIZE_LEASE_TTL_SECONDS", "10")),
        )
