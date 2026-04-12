import os
import glob
from stonedb.wal import WAL
from stonedb.memtable import Memtable
from stonedb.sstable import SSTableWriter, SSTableReader
from stonedb.bloom import BloomFilter
from stonedb.compaction import compact


TOMBSTONE = "__STONEDB_TOMBSTONE__"


class DB:
    """LSM-Tree key-value storage engine."""

    def __init__(self, path, memtable_threshold=4 * 1024 * 1024):
        self.path = path
        os.makedirs(path, exist_ok=True)

        self._wal = WAL(os.path.join(path, "wal.log"))
        self._memtable = Memtable(flush_threshold=memtable_threshold)
        self._sstables = []  # (SSTableReader, BloomFilter) pairs, newest first
        self._bloom_checks_skipped = 0

        self._sst_counter = 0
        self._recover()

    def _recover(self):
        sst_files = sorted(glob.glob(os.path.join(self.path, "*.sst")))
        for f in sst_files:
            reader = SSTableReader(f)
            bloom = self._load_bloom(f)
            self._sstables.append((reader, bloom))

            num = int(os.path.basename(f).split(".")[0])
            if num >= self._sst_counter:
                self._sst_counter = num + 1

        self._sstables.reverse()

        wal_path = os.path.join(self.path, "wal.log")
        entries = WAL.recover(wal_path)
        for key, value in entries:
            self._memtable.put(key, value)

        if self._memtable.should_flush():
            self._flush()

    def _load_bloom(self, sst_path):
        bloom_path = sst_path.replace(".sst", ".bloom")
        if not os.path.exists(bloom_path):
            return None
        with open(bloom_path, "rb") as f:
            return BloomFilter.deserialize(f.read())

    def put(self, key, value):
        self._wal.append(key, value)
        self._memtable.put(key, value)

        if self._memtable.should_flush():
            self._flush()

    def get(self, key):
        val = self._memtable.get(key)
        if val is not None:
            if val == TOMBSTONE:
                return None
            return val

        for reader, bloom in self._sstables:
            if bloom is not None and not bloom.might_contain(key):
                self._bloom_checks_skipped += 1
                continue

            val = reader.get(key)
            if val is not None:
                if val == TOMBSTONE:
                    return None
                return val

        return None

    def delete(self, key):
        self.put(key, TOMBSTONE)

    def run_compaction(self, num_tables=None):
        """Merge oldest SSTables into one. Defaults to compacting all."""
        if len(self._sstables) < 2:
            return

        if num_tables is None:
            num_tables = len(self._sstables)
        num_tables = min(num_tables, len(self._sstables))

        # take the oldest N sstables (they're at the end since list is newest-first)
        to_compact = self._sstables[-num_tables:]
        remaining = self._sstables[:-num_tables]

        sst_paths = [reader.path for reader, _ in to_compact]

        output_path = os.path.join(self.path, f"{self._sst_counter:06d}.sst")
        compact(sst_paths, output_path)

        new_reader = SSTableReader(output_path)
        new_bloom = self._load_bloom(output_path)
        self._sst_counter += 1

        # rebuild sstable list: remaining (newest first) + new compacted at end
        self._sstables = remaining + [(new_reader, new_bloom)]

    def _flush(self):
        if len(self._memtable) == 0:
            return

        items = self._memtable.items()

        sst_path = os.path.join(self.path, f"{self._sst_counter:06d}.sst")
        SSTableWriter.write(sst_path, items)

        bloom = BloomFilter(len(items))
        for k, _ in items:
            bloom.add(k)
        bloom_path = sst_path.replace(".sst", ".bloom")
        with open(bloom_path, "wb") as f:
            f.write(bloom.serialize())

        reader = SSTableReader(sst_path)
        self._sstables.insert(0, (reader, bloom))
        self._sst_counter += 1

        self._memtable.clear()
        self._wal.truncate()

    def close(self):
        if len(self._memtable) > 0:
            self._flush()
        self._wal.close()
