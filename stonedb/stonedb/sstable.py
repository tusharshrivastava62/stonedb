import os
import struct

# SSTable file layout:
#   [data section] entries sorted by key, each: [klen:4][vlen:4][key][value]
#   [index section] [num_entries:4] then per entry: [klen:4][key][offset:8]
#   [footer] [index_offset:8][magic:4]
#
# reads load the index into memory, then seek to offset for key lookup

ENTRY_HEADER = ">II"
ENTRY_HEADER_SIZE = struct.calcsize(ENTRY_HEADER)
MAGIC = b"\xdb\xdb\xdb\xdb"


class SSTableWriter:
    @staticmethod
    def write(path, items):
        """Write sorted (key, value) pairs to an SSTable file.
        Items must already be sorted by key."""
        index = []  # (key, offset) pairs
        with open(path, "wb") as f:
            for k, v in items:
                offset = f.tell()
                kb, vb = k.encode(), v.encode()
                f.write(struct.pack(ENTRY_HEADER, len(kb), len(vb)))
                f.write(kb)
                f.write(vb)
                index.append((kb, offset))

            # write index section
            index_offset = f.tell()
            f.write(struct.pack(">I", len(index)))
            for kb, off in index:
                f.write(struct.pack(">I", len(kb)))
                f.write(kb)
                f.write(struct.pack(">Q", off))

            # footer
            f.write(struct.pack(">Q", index_offset))
            f.write(MAGIC)


class SSTableReader:
    def __init__(self, path):
        self.path = path
        self._index = {}  # key -> byte offset into data section
        self._load_index()

    def _load_index(self):
        with open(self.path, "rb") as f:
            # read footer to find index offset
            f.seek(-12, 2)  # 8 bytes offset + 4 bytes magic
            footer = f.read(12)
            index_offset = struct.unpack(">Q", footer[:8])[0]
            magic = footer[8:]
            if magic != MAGIC:
                raise ValueError(f"bad sstable magic in {self.path}")

            # read index entries
            f.seek(index_offset)
            count = struct.unpack(">I", f.read(4))[0]
            for _ in range(count):
                klen = struct.unpack(">I", f.read(4))[0]
                key = f.read(klen).decode()
                offset = struct.unpack(">Q", f.read(8))[0]
                self._index[key] = offset

    def get(self, key: str):
        if key not in self._index:
            return None

        offset = self._index[key]
        with open(self.path, "rb") as f:
            f.seek(offset)
            header = f.read(ENTRY_HEADER_SIZE)
            klen, vlen = struct.unpack(ENTRY_HEADER, header)
            f.read(klen)  # skip key, we already know it
            val = f.read(vlen).decode()
        return val

    def keys(self):
        return set(self._index.keys())

    def items(self):
        """Read all entries in sorted order. Used during compaction."""
        result = []
        with open(self.path, "rb") as f:
            for key in sorted(self._index.keys()):
                f.seek(self._index[key])
                header = f.read(ENTRY_HEADER_SIZE)
                klen, vlen = struct.unpack(ENTRY_HEADER, header)
                f.read(klen)
                val = f.read(vlen).decode()
                result.append((key, val))
        return result
