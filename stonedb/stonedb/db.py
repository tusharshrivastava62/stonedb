import os
import glob
from stonedb.wal import WAL
from stonedb.memtable import Memtable
from stonedb.sstable import SSTableWriter, SSTableReader


class DB:
    """LSM-Tree key-value storage engine."""

    def __init__(self, path, memtable_threshold=4 * 1024 * 1024):
        self.path = path
        os.makedirs(path, exist_ok=True)

        self._wal = WAL(os.path.join(path, "wal.log"))
        self._memtable = Memtable(flush_threshold=memtable_threshold)
        self._sstables = []  # newest first

        self._sst_counter = 0
        self._recover()

    def _recover(self):
        """Rebuild state from WAL and existing SSTables on startup."""
        # load existing sstables, sorted by creation order
        sst_files = sorted(glob.glob(os.path.join(self.path, "*.sst")))
        for f in sst_files:
            self._sstables.append(SSTableReader(f))
            num = int(os.path.basename(f).split(".")[0])
            if num >= self._sst_counter:
                self._sst_counter = num + 1

        # newest should be first for read path
        self._sstables.reverse()

        # replay WAL into memtable
        entries = WAL.recover(os.path.join(self.path, "wal.log"))
        for key, value in entries:
            self._memtable.put(key, value)

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
        for sst in self._sstables:
            val = sst.get(key)
            if val is not None:
                return val

        return None

    def _flush(self):
        if len(self._memtable) == 0:
            return

        # write memtable to new sstable
        sst_path = os.path.join(self.path, f"{self._sst_counter:06d}.sst")
        SSTableWriter.write(sst_path, self._memtable.items())

        reader = SSTableReader(sst_path)
        self._sstables.insert(0, reader)  # newest first
        self._sst_counter += 1

        # clear memtable and WAL
        self._memtable.clear()
        self._wal.truncate()

    def close(self):
        # flush any remaining data in memtable
        if len(self._memtable) > 0:
            self._flush()
        self._wal.close()
