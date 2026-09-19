"""Resumable standard SHA-256.

The only state needed to resume a SHA-256 computation is the eight 32-bit
chaining words, the number of message bytes already processed and the (at most
63 byte) incomplete block.  Persisting that state at a checkpoint lets a
restarted process continue *standard* SHA-256 without re-reading the confirmed
prefix.  The produced digest is bit-identical to :func:`hashlib.sha256`.
"""

from __future__ import annotations

import json

# FIPS 180-4 round constants and initial hash value.
_K = (
    0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5, 0x3956C25B, 0x59F111F1,
    0x923F82A4, 0xAB1C5ED5, 0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3,
    0x72BE5D74, 0x80DEB1FE, 0x9BDC06A7, 0xC19BF174, 0xE49B69C1, 0xEFBE4786,
    0x0FC19DC6, 0x240CA1CC, 0x2DE92C6F, 0x4A7484AA, 0x5CB0A9DC, 0x76F988DA,
    0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7, 0xC6E00BF3, 0xD5A79147,
    0x06CA6351, 0x14292967, 0x27B70A85, 0x2E1B2138, 0x4D2C6DFC, 0x53380D13,
    0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85, 0xA2BFE8A1, 0xA81A664B,
    0xC24B8B70, 0xC76C51A3, 0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070,
    0x19A4C116, 0x1E376C08, 0x2748774C, 0x34B0BCB5, 0x391C0CB3, 0x4ED8AA4A,
    0x5B9CCA4F, 0x682E6FF3, 0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208,
    0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7, 0xC67178F2,
)
_H0 = (
    0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
    0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19,
)
_MASK = 0xFFFFFFFF
BLOCK_SIZE = 64
STATE_VERSION = 1


def _rotr(x: int, n: int) -> int:
    return ((x >> n) | (x << (32 - n))) & _MASK


def _compress(h: list[int], block: bytes) -> None:
    w = list(int.from_bytes(block[i * 4 : i * 4 + 4], "big") for i in range(16))
    for i in range(16, 64):
        s0 = _rotr(w[i - 15], 7) ^ _rotr(w[i - 15], 18) ^ (w[i - 15] >> 3)
        s1 = _rotr(w[i - 2], 17) ^ _rotr(w[i - 2], 19) ^ (w[i - 2] >> 10)
        w.append((w[i - 16] + s0 + w[i - 7] + s1) & _MASK)

    a, b, c, d, e, f, g, hh = h
    for i in range(64):
        s1 = _rotr(e, 6) ^ _rotr(e, 11) ^ _rotr(e, 25)
        ch = (e & f) ^ ((~e) & g)
        t1 = (hh + s1 + ch + _K[i] + w[i]) & _MASK
        s0 = _rotr(a, 2) ^ _rotr(a, 13) ^ _rotr(a, 22)
        maj = (a & b) ^ (a & c) ^ (b & c)
        t2 = (s0 + maj) & _MASK
        hh, g, f, e, d, c, b, a = g, f, e, (d + t1) & _MASK, c, b, a, (t1 + t2) & _MASK

    h[0] = (h[0] + a) & _MASK
    h[1] = (h[1] + b) & _MASK
    h[2] = (h[2] + c) & _MASK
    h[3] = (h[3] + d) & _MASK
    h[4] = (h[4] + e) & _MASK
    h[5] = (h[5] + f) & _MASK
    h[6] = (h[6] + g) & _MASK
    h[7] = (h[7] + hh) & _MASK


class ResumableSHA256:
    """SHA-256 with a JSON-serialisable state at any byte boundary."""

    def __init__(self) -> None:
        self.h = list(_H0)
        self.length = 0
        self._buf = bytearray()

    def copy(self) -> "ResumableSHA256":
        clone = ResumableSHA256()
        clone.h = list(self.h)
        clone.length = self.length
        clone._buf = bytearray(self._buf)
        return clone

    def update(self, data: bytes) -> None:
        self._buf.extend(data)
        self.length += len(data)
        index = 0
        while len(self._buf) - index >= BLOCK_SIZE:
            _compress(self.h, bytes(self._buf[index : index + BLOCK_SIZE]))
            index += BLOCK_SIZE
        if index:
            del self._buf[:index]

    def to_state(self) -> str:
        return json.dumps(
            {
                "v": STATE_VERSION,
                "h": self.h,
                "n": self.length,
                "tail": bytes(self._buf).hex(),
            },
            separators=(",", ":"),
        )

    @classmethod
    def from_state(cls, state: str) -> "ResumableSHA256":
        try:
            payload = json.loads(state)
            version = payload["v"]
            if version != STATE_VERSION:
                raise ValueError(f"unsupported checkpoint version: {version}")
            h = payload["h"]
            length = payload["n"]
            tail = bytes.fromhex(payload["tail"])
            if (
                not isinstance(h, list)
                or len(h) != 8
                or not all(isinstance(v, int) and 0 <= v <= _MASK for v in h)
                or not isinstance(length, int)
                or length < 0
                or len(tail) >= BLOCK_SIZE
                or len(tail) != length % BLOCK_SIZE
            ):
                raise ValueError("malformed checkpoint state")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid SHA-256 checkpoint: {exc}") from exc
        clone = cls()
        clone.h = list(h)
        clone.length = length
        clone._buf = bytearray(tail)
        return clone

    def hexdigest(self) -> str:
        clone = self.copy()
        bit_length = (clone.length << 3) & 0xFFFFFFFFFFFFFFFF
        clone._buf.append(0x80)
        if len(clone._buf) > 56:
            clone._buf.extend(b"\x00" * (BLOCK_SIZE - len(clone._buf)))
            _compress(clone.h, bytes(clone._buf))
            clone._buf = bytearray()
        clone._buf.extend(b"\x00" * (56 - len(clone._buf)))
        clone._buf.extend(bit_length.to_bytes(8, "big"))
        _compress(clone.h, bytes(clone._buf))
        return b"".join(word.to_bytes(4, "big") for word in clone.h).hex()
