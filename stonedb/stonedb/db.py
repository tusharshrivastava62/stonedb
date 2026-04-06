import os
import glob
from stonedb.wal import WAL
from stonedb.memtable import Memtable
from stonedb.sstable import SSTableWriter, SSTableReader
from stonedb.bloom import BloomFilter


class DB:
    """LSM-Tree key-value storage engine."""

    def __init__(self, path, memtable_threshold=4 * 1024 * 1024):
        self.path = path
        os.makedirs(path, exist_ok=True)

        self._wal = WAL(os.path.join(path, "wal.log"))
        self._memtable = Memtable(flush_threshold=memtable_threshold)
        self._sstables = []  # (SSTableReader, BloomFilter) pairs, newest first
        self._bloom_checks_skipped = 0  # for benchmarking

        self._sst_counter = 0
        self._recover()

    def _recover(self):
        """Rebuild state from WAL and existing SSTables on startup."""
        sst_files = sorted(glob.glob(os.path.join(self.path, "*.sst")))
        for f in sst_files:
            reader = SSTableReader(f)
            bloom = self._load_bloom(f)
            self._sstables.append((reader, bloom))

            num = int(os.path.basename(f).split(".")[0])
            if num >= self._sst_counter:
                self._sst_counter = num + 1

        # newest first for read path
        self._sstables.reverse()

        # replay WAL into memtable
        wal_path = os.path.join(self.path, "wal.log")
        entries = WAL.recover(wal_path)
        for key, value in entries:
            self._memtable.put(key, value)

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
        # check memtable first — most recent writes live here
        val = self._memtable.get(key)
        if val is not None:
            return val

        # search sstables newest to oldest
        for reader, bloom in self._sstables:
            # bloom filter says 'definitely not here' — skip this sstable
            if bloom is not None and not bloom.might_contain(key):
                self._bloom_checks_skipped += 1
                continue

            val = reader.get(key)
            if val is not None:
                return val

        return None

    def _flush(self):
        if len(self._memtable) == 0:
            return

        items = self._memtable.items()

        # write sstable
        sst_path = os.path.join(self.path, f"{self._sst_counter:06d}.sst")
        SSTableWriter.write(sst_path, items)

        # build and save bloom filter
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
