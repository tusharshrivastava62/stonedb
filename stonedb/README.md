# stonedb

lightweight LSM-Tree key-value storage engine in Python.

![architecture](docs/architecture.svg)

## quick start

```bash
pip install sortedcontainers
```

```python
from stonedb.db import DB

db = DB("/tmp/mydata")
db.put("user:1", '{"name": "tushar"}')
print(db.get("user:1"))
db.delete("user:1")
db.close()
```

## how to run benchmarks

```bash
python benchmarks/run_all.py
```

## benchmark results

Machine: Apple Silicon (arm64), Python 3.9.6, macOS

### write throughput (with WAL fsync)

| Value Size | ops/sec |
|-----------|---------|
| 100B      | 38,503  |
| 1KB       | 33,658  |
| 10KB      | 22,205  |

### read latency — bloom filter impact

| Test | p50 | p95 | p99 | Disk Reads |
|------|-----|-----|-----|-----------|
| Existing keys (bloom ON) | 3.1ms | 5.8ms | 6.6ms | 241K |
| Existing keys (bloom OFF) | 30.0ms | 57.0ms | 61.4ms | 15.8M |
| Missing keys (bloom ON) | 6.1ms | 6.5ms | 11.9ms | 441K |
| Missing keys (bloom OFF) | 60.0ms | 64.0ms | 73.6ms | 31.3M |

Bloom filters eliminate **98.5%** of disk reads. p99 drops from 61ms to 6ms.

### compaction

| Metric | Before | After |
|--------|--------|-------|
| SSTables | 1,191 | 1 |
| Disk usage | 40.5 MB | 27.0 MB |
| Space amplification | 1.58x | 1.06x |

33.4% disk reclaimed in 0.5s.

### mixed workload (70% read / 30% write)

9,698 ops/sec at p99 0.34ms over 50K operations.

## how to run tests

```bash
python -m pytest tests/ -v
```

11 tests covering WAL crash recovery, bloom filter FPR, compaction correctness, and full integration cycles.

## design decisions

I used `SortedDict` from sortedcontainers for the memtable instead of a skip list. At 4MB memtable threshold the performance difference is negligible, and I didn't need concurrent access so a skip list's lock-free properties don't matter here.

The WAL does `fsync` after every write. It's slower than buffered I/O but it's the only way to actually guarantee durability. I tested crash recovery by truncating the WAL at random byte offsets — every complete entry is recovered, truncated trailing entries from mid-write crashes are safely discarded.

SSTable reads go to disk for every lookup instead of caching indexes in memory. I started with cached indexes and bloom filters actually made reads *slower* because the hash computation cost more than a dict lookup. Once I switched to on-disk reads, bloom filters started saving 98.5% of disk I/O. The lesson: optimizations only help when you're optimizing the actual bottleneck.

## lessons learned

The tombstone resurrection bug was the most interesting one. My compaction was dropping tombstones eagerly, which meant deleted keys came back to life when an older SSTable (outside the compaction set) still had the original value. Took me a while to figure out because it only happened during partial compaction. The fix checks whether it's safe to drop tombstones before removing them — same idea as Cassandra's `gc_grace_seconds`.

Bloom filter sizing matters more than I expected. I initially used a fixed-size bit array and got 5% false positive rate instead of the 1% target. Switching to the formula from the original paper (-n*ln(p)/(ln2)^2) fixed it immediately.

## limitations

- no range queries (would need a merge iterator across memtable + SSTables)
- single-threaded, no concurrent reads/writes
- compaction blocks reads while running
- no compression on data blocks
- Python performance ceiling — C/Rust would be 10-50x faster
