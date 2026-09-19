"""Standard SHA-256 with a serializable state.

``hashlib`` objects cannot be checkpointed to disk, so the finalizer uses
this interoperable implementation instead: the persisted state is enough to
continue the *standard* SHA-256 of a byte stream from an exact offset without
re-reading any earlier bytes.  Digests are identical to ``hashlib.sha256``.
"""

from __future__ import annotations

import struct

_MASK = 0xFFFFFFFF

_K = (
    0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5, 0x3956C25B, 0x59F111F1, 0x923F82A4, 0xAB1C5ED5,
    0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3, 0x72BE5D74, 0x80DEB1FE, 0x9BDC06A7, 0xC19BF174,
    0xE49B69C1, 0xEFBE4786, 0x0FC19DC6, 0x240CA1CC, 0x2DE92C6F, 0x4A7484AA, 0x5CB0A9DC, 0x76F988DA,
    0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7, 0xC6E00BF3, 0xD5A79147, 0x06CA6351, 0x14292967,
    0x27B70A85, 0x2E1B2138, 0x4D2C6DFC, 0x53380D13, 0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85,
    0xA2BFE8A1, 0xA81A664B, 0xC24B8B70, 0xC76C51A3, 0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070,
    0x19A4C116, 0x1E376C08, 0x2748774C, 0x34B0BCB5, 0x391C0CB3, 0x4ED8AA4A, 0x5B9CCA4F, 0x682E6FF3,
    0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208, 0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7, 0xC67178F2,
)

_H0 = (0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19)

_STATE_MAGIC = b"SHA256CHK"
_STATE_VERSION = 1
_HEADER_LEN = len(_STATE_MAGIC) + 1 + 8 * 4 + 8  # magic | version | h[8] | total length


def _rotr(x: int, n: int) -> int:
    return ((x >> n) | (x << (32 - n))) & _MASK


class ResumableSha256:
    """SHA-256 whose compression state round-trips through ``state()``."""

    def __init__(self) -> None:
        self._h = list(_H0)
        self._length = 0  # total bytes fed so far, including the pending tail
        self._buf = bytearray()  # < 64 unprocessed bytes

    # -- hashing ---------------------------------------------------------

    def update(self, data) -> None:
        self._length += len(data)
        buf = self._buf + data
        full = len(buf) // 64 * 64
        for off in range(0, full, 64):
            self._compress(buf[off : off + 64])
        self._buf = buf[full:]

    def _compress(self, block) -> None:
        w = list(struct.unpack(">16I", block))
        for i in range(16, 64):
            x = w[i - 15]
            s0 = _rotr(x, 7) ^ _rotr(x, 18) ^ (x >> 3)
            x = w[i - 2]
            s1 = _rotr(x, 17) ^ _rotr(x, 19) ^ (x >> 10)
            w.append((w[i - 16] + s0 + w[i - 7] + s1) & _MASK)
        a, b, c, d, e, f, g, h = self._h
        k = _K
        for i in range(64):
            s1 = _rotr(e, 6) ^ _rotr(e, 11) ^ _rotr(e, 25)
            ch = (e & f) ^ (~e & g)
            t1 = (h + s1 + ch + k[i] + w[i]) & _MASK
            s0 = _rotr(a, 2) ^ _rotr(a, 13) ^ _rotr(a, 22)
            maj = (a & b) ^ (a & c) ^ (b & c)
            t2 = (s0 + maj) & _MASK
            h, g, f, e, d, c, b, a = g, f, e, (d + t1) & _MASK, c, b, a, (t1 + t2) & _MASK
        self._h = [(x + y) & _MASK for x, y in zip(self._h, (a, b, c, d, e, f, g, h))]

    def digest(self) -> bytes:
        clone = self._copy()
        bit_len = self._length * 8
        clone.update(b"\x80")
        while len(clone._buf) != 56:
            clone.update(b"\x00")
        clone.update(struct.pack(">Q", bit_len))
        return struct.pack(">8I", *clone._h)

    def hexdigest(self) -> str:
        return self.digest().hex()

    # -- checkpointing ---------------------------------------------------

    def state(self) -> bytes:
        """Freeze the exact compression state (versioned, self-validating)."""
        return (
            _STATE_MAGIC
            + bytes([_STATE_VERSION])
            + struct.pack(">8IQ", *self._h, self._length)
            + bytes(self._buf)
        )

    @classmethod
    def from_state(cls, blob) -> "ResumableSha256":
        """Restore a hasher from ``state()``; raises ValueError if corrupt."""
        if not isinstance(blob, (bytes, bytearray)) or len(blob) < _HEADER_LEN:
            raise ValueError("checkpoint state is truncated")
        if not blob.startswith(_STATE_MAGIC):
            raise ValueError("checkpoint state has a bad magic")
        version = blob[len(_STATE_MAGIC)]
        if version != _STATE_VERSION:
            raise ValueError(f"unknown checkpoint state version: {version}")
        fields = struct.unpack(">8IQ", blob[len(_STATE_MAGIC) + 1 : _HEADER_LEN])
        buf = bytes(blob[_HEADER_LEN:])
        length = fields[8]
        if len(buf) >= 64 or len(buf) != length % 64:
            raise ValueError("checkpoint state is inconsistent with its length")
        hasher = cls.__new__(cls)
        hasher._h = list(fields[:8])
        hasher._length = length
        hasher._buf = bytearray(buf)
        return hasher

    def _copy(self) -> "ResumableSha256":
        clone = self.__class__.__new__(self.__class__)
        clone._h = list(self._h)
        clone._length = self._length
        clone._buf = bytearray(self._buf)
        return clone
