import os
from stonedb.sstable import SSTableWriter, SSTableReader
from stonedb.bloom import BloomFilter


def compact(sst_paths, output_path):
    """Merge multiple SSTables into one. Newer entries win for duplicate keys.
    sst_paths should be ordered oldest to newest."""

    # read all entries from all sstables
    merged = {}
    for path in sst_paths:
        reader = SSTableReader(path)
        for key, val in reader.items():
            merged[key] = val  # newer sstable overwrites older

    # BUG: dropping tombstones here — this causes deleted keys to
    # "resurrect" if an older sstable (not in this compaction set)
    # still has the live value. Will be fixed in next commit.
    items = sorted(
        [(k, v) for k, v in merged.items() if v != "__STONEDB_TOMBSTONE__"],
        key=lambda x: x[0]
    )

    # write merged sstable
    SSTableWriter.write(output_path, items)

    # build bloom filter
    bloom = BloomFilter(len(items))
    for k, _ in items:
        bloom.add(k)
    bloom_path = output_path.replace(".sst", ".bloom")
    with open(bloom_path, "wb") as f:
        f.write(bloom.serialize())

    # clean up old files
    for path in sst_paths:
        os.remove(path)
        bp = path.replace(".sst", ".bloom")
        if os.path.exists(bp):
            os.remove(bp)

    return output_path, bloom
