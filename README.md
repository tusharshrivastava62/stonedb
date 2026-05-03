# stonedb

LSM-Tree key-value storage engine built from scratch in Python. implements the same core architecture as LevelDB and RocksDB — write-ahead log, sorted memtable, SSTable files, bloom filters, and compaction.

## benchmark results

| metric | value |
|---|---|
| durable writes/sec | 38,000 (fsync per entry) |
| bloom filter disk read elimination | 98.5% |
| p99 read latency before bloom filters | 61ms |
| p99 read latency after bloom filters | 6ms |
| SSTables before compaction | 1,191 |
| SSTables after compaction | 1 |
| space amplification before compaction | 1.58x |
| space amplification after compaction | 1.06x |
| crash recovery | 100% across random WAL truncation points |

## architecture

```
write path:
  put(key, value)
    → WAL (fsync to disk)
    → memtable (sorted in memory)
    → when memtable full: flush to SSTable on disk

read path:
  get(key)
    → check memtable first
    → for each SSTable (newest first):
        → check bloom filter (skip if key definitely absent)
        → binary search index
        → read data block

compaction:
  merge multiple SSTables into one
  drop overwritten values and expired tombstones
  reduce space amplification
```

## how it works

**write-ahead log (WAL)**

every write is appended to the WAL and fsynced before being applied to the memtable. if the process crashes mid-write, recovery replays the WAL from the last valid entry. tested by truncating the WAL at random byte offsets and verifying full recovery.

**memtable**

writes accumulate in a sorted in-memory structure. when the memtable reaches its size threshold it is flushed to a new SSTable file on disk and cleared.

**SSTable files**

immutable sorted files on disk. each file has a data section, an index section for binary search, and a footer with a magic byte sequence for validation. bloom filter is serialized into the file header.

**bloom filters**

before reading an SSTable, the bloom filter answers "does this key definitely not exist here?" if yes, the entire file is skipped. sized using the optimal formula `m = -n*ln(p) / ln(2)^2` where n is expected key count and p is false positive rate target. this eliminated 98.5% of disk reads, cutting p99 from 61ms to 6ms across 100K keys.

**compaction**

merges all SSTables into one using a k-way merge. tombstones (delete markers) must outlive the records they mark — dropping a tombstone before the original key is compacted away causes deleted data to reappear on the next read. compaction reduced space amplification from 1.58x to 1.06x by eliminating redundant versions and applying tombstones.

## design decisions

**WAL before memtable, not after**

writing to the WAL first means a crash between WAL write and memtable write loses nothing — recovery replays the WAL entry. the reverse order would silently drop writes that never reached the WAL.

**bloom filter sized with optimal bits-per-key formula**

a fixed bits-per-key value wastes space at small key counts and degrades precision at large ones. the optimal formula sizes the filter exactly for the false positive rate you want. under 1% false positive rate measured at 100K keys.

**tombstones must outlive their targets**

deletes write a tombstone marker, not an immediate removal. during compaction, if the tombstone is dropped before the original key has been seen in all SSTables, the original key resurfaces. the fix: tombstones are always preserved until the compaction pass that also processes the original key.

**sequential WAL writes convert random I/O to sequential**

random writes to arbitrary SSTable positions are expensive on disk. WAL appends are sequential — the OS can buffer and flush them efficiently. this is the core reason LSM-trees outperform B-trees on write-heavy workloads.

## running it

```bash
pip install sortedcontainers

# run all benchmarks
python benchmarks/run_all.py

# run tests
pytest tests/
```

```python
from stonedb.db import DB

db = DB("/tmp/mydata")
db.put("user:1", '{"name": "tushar"}')
print(db.get("user:1"))
db.delete("user:1")
db.close()
```

## limitations

- **single-threaded** — no concurrent read/write support. production engines like RocksDB use fine-grained locking and lock-free structures for concurrent access.
- **no block cache** — frequently accessed SSTable blocks are not cached in memory. every read that misses the bloom filter goes to disk.
- **no compression** — SSTable data blocks are stored uncompressed. RocksDB uses Snappy/Zstd per block.
- **compaction is full merge** — merges all SSTables in one pass. production engines use leveled or tiered compaction strategies to bound write amplification.
- **Python performance ceiling** — bloom filter and binary search are implemented in pure Python. equivalent C++ implementation would be 10-50x faster on the same hardware.

## what i'd do next

- **block cache** — LRU cache for hot SSTable blocks to reduce disk reads on repeated access patterns
- **leveled compaction** — separate SSTables into levels with size budgets to bound space and write amplification independently
- **concurrent reads** — reader-writer lock on the memtable so reads don't block on flushes
- **compression** — Snappy compression per data block to reduce SSTable size on disk

## lessons learned

**bloom filter optimization only helps when disk reads are the bottleneck**

before adding bloom filters, profiling showed 94% of time was spent in SSTable reads. after adding bloom filters, 98.5% of those reads were eliminated. the lesson: measure before optimizing. if disk reads hadn't been the bottleneck, the bloom filter would have added complexity for no gain.

**tombstone resurrection is subtle and non-obvious**

the tombstone bug only manifests when compaction runs across SSTable boundaries in a specific order. the fix requires understanding the invariant: a tombstone must always be written to the output of any compaction that processes the key it marks, even if the original key isn't in the current input set. same invariant Cassandra enforces with gc_grace_seconds.

**WAL truncation testing found a real bug**

testing crash recovery by cleanly closing the database would never expose a mid-write failure. truncating the WAL file at random byte offsets — simulating a crash at any point in a write — found a bug where partial header writes were being replayed as valid entries. the fix added a checksum to the WAL record header.
