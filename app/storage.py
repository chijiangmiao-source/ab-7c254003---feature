"""On-disk layout for chunk bodies and published artifacts.

Chunk bodies land via a temp file that is fsynced and then ``os.replace``d into
place.  Finalization keeps its own stable, per-session temp file under
``.finalize/`` so that a restart can continue appending at the confirmed byte
boundary; publishing goes through a generation-tagged hard link (candidate)
that is atomically renamed over the final path.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import uuid
from pathlib import Path
from typing import AsyncIterable

_COPY_BUFFER = 1024 * 1024
FINALIZE_DIRNAME = ".finalize"


class CheckpointFileError(Exception):
    """The finalization temp file contradicts the persisted checkpoint."""

    def __init__(self, reason: str, *, expected: int = 0, actual: int = 0):
        super().__init__(reason)
        self.reason = reason
        self.expected = expected
        self.actual = actual


class FinalizeWriter:
    """Append handle on the stable finalization temp file."""

    def __init__(self, fh, path: Path, position: int):
        self._fh = fh
        self.path = path
        self.position = position

    def write(self, data: bytes) -> int:
        self._fh.write(data)
        self.position += len(data)
        return self.position

    def sync(self) -> None:
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def fileno(self) -> int:
        return self._fh.fileno()


class ChunkStore:
    def __init__(self, root: Path):
        self.root = root
        self.chunks_root = root / "chunks"
        self.artifacts_dir = root / "artifacts"
        self.finalize_dir = root / FINALIZE_DIRNAME
        self.chunks_root.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def chunk_dir(self, session_id: str) -> Path:
        return self.chunks_root / session_id

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.chunk_dir(session_id) / f"{index:08d}.chunk"

    def artifact_path(self, session_id: str) -> Path:
        return self.artifacts_dir / f"{session_id}.bin"

    def finalize_tmp_path(self, session_id: str) -> Path:
        return self.finalize_dir / f"{session_id}.tmp"

    def candidate_path(self, session_id: str, generation: int) -> Path:
        return self.artifacts_dir / f".{session_id}.g{generation}.cand"

    async def write_chunk_tmp(self, session_id: str, stream: AsyncIterable[bytes]) -> tuple[Path, int, str]:
        """Stream a request body to a temp file; returns (tmp_path, size, sha256).

        The caller validates size/digest before committing the temp file with
        commit_tmp(); nothing is visible at the final path until then.
        """
        target_dir = self.chunk_dir(session_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        tmp = target_dir / f".{uuid.uuid4().hex}.tmp"
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp, "wb") as fh:
                async for part in stream:
                    if not part:
                        continue
                    hasher.update(part)
                    fh.write(part)
                    size += len(part)
                fh.flush()
                os.fsync(fh.fileno())
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return tmp, size, hasher.hexdigest()

    def commit_tmp(self, tmp: Path, final: Path) -> None:
        os.replace(tmp, final)
        _fsync_dir(final.parent)

    # ---- finalization temp file ----

    # ---- publishing ----

    def link_candidate(self, tmp: Path, candidate: Path) -> None:
        """Hard-link the assembled temp as a generation-tagged candidate.

        The temp keeps its own link, so a crash around the rename never
        destroys the only copy of the bytes.
        """
        candidate.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(FileNotFoundError):
            candidate.unlink()
        os.link(tmp, candidate)
        _fsync_dir(candidate.parent)

    def commit_candidate(self, candidate: Path, session_id: str) -> Path:
        final = self.artifact_path(session_id)
        os.replace(candidate, final)
        _fsync_dir(final.parent)
        return final

    @staticmethod
    def hash_file(path: Path) -> tuple[int, str]:
        """Stream a file in bounded buffers; returns (size, sha256)."""
        hasher = hashlib.sha256()
        size = 0
        with open(path, "rb") as fh:
            while True:
                block = fh.read(_COPY_BUFFER)
                if not block:
                    break
                hasher.update(block)
                size += len(block)
        return size, hasher.hexdigest()

    def remove_finalize_tmp(self, session_id: str) -> None:
        with contextlib.suppress(OSError):
            path = self.finalize_tmp_path(session_id)
            path.unlink(missing_ok=True)
            _fsync_dir(path.parent)

    def discard_finalization_temps(self, session_id: str) -> None:
        """Remove every assembly temp (stable + all generations) for a session."""
        for pattern in (f"{session_id}.tmp", f"{session_id}.g*.tmp"):
            for entry in self.finalize_dir.glob(pattern):
                with contextlib.suppress(OSError):
                    entry.unlink(missing_ok=True)
        with contextlib.suppress(OSError):
            _fsync_dir(self.finalize_dir)

    def remove_candidate(self, candidate: Path) -> None:
        with contextlib.suppress(OSError):
            candidate.unlink(missing_ok=True)
            _fsync_dir(candidate.parent)

    @staticmethod
    def discard(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

    def purge_orphan_temps(self) -> None:
        """Remove legacy assembly temps from the artifacts dir.

        The current protocol never writes temp files here (they live under
        ``.finalize/``); generation candidates carry a ``.cand`` suffix and are
        converged per session, not swept.
        """
        for entry in self.artifacts_dir.glob("*.tmp"):
            entry.unlink(missing_ok=True)


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
