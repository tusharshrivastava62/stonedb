import math
import hashlib
import struct


class BloomFilter:
    """Probabilistic set membership test.
    Can tell you 'definitely not in set' or 'maybe in set'.
    False positives possible, false negatives impossible."""

    def __init__(self, expected_count, fpr=0.01):
        # optimal bit array size: -n * ln(p) / (ln2)^2
        self.size = max(64, int(-expected_count * math.log(fpr) / (math.log(2) ** 2)))
        # optimal hash count: (m/n) * ln2
        self.n_hashes = max(1, int((self.size / max(expected_count, 1)) * math.log(2)))
        self.bits = bytearray(self.size)

    def add(self, key):
        for i in range(self.n_hashes):
            idx = self._hash(key, i) % self.size
            self.bits[idx] = 1

    def might_contain(self, key):
        for i in range(self.n_hashes):
            idx = self._hash(key, i) % self.size
            if self.bits[idx] == 0:
                return False
        return True

    def _hash(self, key, seed):
        # double hashing: h1(x) + i * h2(x)
        data = key.encode() if isinstance(key, str) else key
        h1 = int.from_bytes(hashlib.md5(data).digest()[:4], "big")
        h2 = int.from_bytes(hashlib.sha1(data).digest()[:4], "big")
        return h1 + seed * h2

    def serialize(self):
        """Pack bloom filter state into bytes for disk storage."""
        header = struct.pack(">III", self.size, self.n_hashes, len(self.bits))
        return header + bytes(self.bits)

    @classmethod
    def deserialize(cls, data):
        """Reconstruct bloom filter from serialized bytes."""
        size, n_hashes, bits_len = struct.unpack(">III", data[:12])
        bf = cls.__new__(cls)
        bf.size = size
        bf.n_hashes = n_hashes
        bf.bits = bytearray(data[12:12 + bits_len])
        return bf
