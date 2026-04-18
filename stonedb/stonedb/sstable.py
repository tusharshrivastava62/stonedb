import os
import struct

# SSTable file layout:
#   [data section] entries sorted by key, each: [klen:4][vlen:4][key][value]
#   [index section] [num_entries:4] then per entry: [klen:4][key][offset:8]
#   [footer] [index_offset:8][magic:4]

ENTRY_HEADER = ">II"
ENTRY_HEADER_SIZE = struct.calcsize(ENTRY_HEADER)
MAGIC = b"\xdb\xdb\xdb\xdb"


class SSTableWriter:
    @staticmethod
    def write(path, items):
        """Write sorted (key, value) pairs to an SSTable file.
        Items must already be sorted by key."""
        index = []
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
        self._disk_reads = 0

    def _read_footer(self, f):
        f.seek(-12, 2)
        footer = f.read(12)
        index_offset = struct.unpack(">Q", footer[:8])[0]
        if footer[8:] != MAGIC:
            raise ValueError(f"bad sstable magic in {self.path}")
        return index_offset

    def get(self, key):
        """Look up a key by reading index from disk each time.
        More realistic than caching — in production you can't hold
        every SSTable's index in memory."""
        self._disk_reads += 1
        target = key.encode()

        # TODO: binary search on index block instead of linear scan.
        # index is sorted so we could cut lookup from O(n) to O(log n).
        # not worth it at current scale but would matter with large SSTables.
        with open(self.path, "rb") as f:
            index_offset = self._read_footer(f)

            f.seek(index_offset)
            count = struct.unpack(">I", f.read(4))[0]

            data_offset = None
            for _ in range(count):
                klen = struct.unpack(">I", f.read(4))[0]
                k = f.read(klen)
                off = struct.unpack(">Q", f.read(8))[0]
                if k == target:
                    data_offset = off
                    break

            if data_offset is None:
                return None

            f.seek(data_offset)
            header = f.read(ENTRY_HEADER_SIZE)
            klen, vlen = struct.unpack(ENTRY_HEADER, header)
            f.read(klen)  # skip key
            val = f.read(vlen).decode()

        return val

    def keys(self):
        result = set()
        with open(self.path, "rb") as f:
            index_offset = self._read_footer(f)
            f.seek(index_offset)
            count = struct.unpack(">I", f.read(4))[0]
            for _ in range(count):
                klen = struct.unpack(">I", f.read(4))[0]
                key = f.read(klen).decode()
                f.read(8)
                result.add(key)
        return result

    def items(self):
        """Read all entries in sorted order. Used during compaction."""
        entries = []
        with open(self.path, "rb") as f:
            index_offset = self._read_footer(f)
            f.seek(index_offset)
            count = struct.unpack(">I", f.read(4))[0]
            index = []
            for _ in range(count):
                klen = struct.unpack(">I", f.read(4))[0]
                key = f.read(klen).decode()
                off = struct.unpack(">Q", f.read(8))[0]
                index.append((key, off))

            for key, off in sorted(index, key=lambda x: x[0]):
                f.seek(off)
                header = f.read(ENTRY_HEADER_SIZE)
                klen, vlen = struct.unpack(ENTRY_HEADER, header)
                f.read(klen)
                val = f.read(vlen).decode()
                entries.append((key, val))

        return entries
