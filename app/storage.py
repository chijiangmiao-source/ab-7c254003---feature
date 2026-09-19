"""On-disk layout for chunk bodies and published artifacts.

Every write goes to a temp file, is fsynced, and is then moved into place with
os.replace so a crash never leaves a half-written file at a final path.

The finalization temp file is the single exception to the random-temp rule:
its path is derived from the session id so that a restarted process keeps
appending to the *same* file its checkpoint refers to.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from typing import AsyncIterable, Iterator

_COPY_BUFFER = 1024 * 1024


class ChunkStore:
    def __init__(self, root: Path):
        self.root = root
        self.chunks_root = root / "chunks"
        self.artifacts_dir = root / "artifacts"
        self.chunks_root.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def chunk_dir(self, session_id: str) -> Path:
        return self.chunks_root / session_id

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.chunk_dir(session_id) / f"{index:08d}.chunk"

    def artifact_path(self, session_id: str) -> Path:
        return self.artifacts_dir / f"{session_id}.bin"

    def finalize_tmp_path(self, session_id: str) -> Path:
        """Deterministic path of the resumable finalization output."""
        return self.artifacts_dir / f".{session_id}.finalizing"

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

    def read_chunks_from(
        self, session_id: str, chunk_size: int, total_bytes: int, offset: int
    ) -> Iterator[bytes]:
        """Stream assembled bytes starting at absolute ``offset``.

        Used to resume a finalization: source bytes before ``offset`` were
        already checked and are never re-read.  Yields at most _COPY_BUFFER
        bytes at a time; stops short if a chunk file shrank unexpectedly (the
        caller's digest/length check then fails).
        """
        while offset < total_bytes:
            index, inner = divmod(offset, chunk_size)
            size = min(_COPY_BUFFER, chunk_size - inner, total_bytes - offset)
            with open(self.chunk_path(session_id, index), "rb") as src:
                src.seek(inner)
                block = src.read(size)
            if not block:
                return
            yield block
            offset += len(block)

    def publish(self, tmp: Path, session_id: str) -> Path:
        final = self.artifact_path(session_id)
        os.replace(tmp, final)
        _fsync_dir(final.parent)
        return final

    @staticmethod
    def sha256_of(path: Path) -> tuple[int, str]:
        """Stream a file through SHA-256; returns (size, hexdigest)."""
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

    @staticmethod
    def discard(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

    def purge_tmp(self) -> None:
        for entry in self.artifacts_dir.glob("*.tmp"):
            entry.unlink(missing_ok=True)


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
