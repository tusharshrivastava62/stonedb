import os
from stonedb.sstable import SSTableWriter, SSTableReader
from stonedb.bloom import BloomFilter

# TODO: add size-tiered compaction as alternative strategy.
# current approach always merges the oldest N tables, but
# size-tiered would group tables by similar size and merge
# those first, which can reduce write amplification.


def compact(sst_paths, output_path, drop_tombstones=False):
    """Merge multiple SSTables into one. Newer entries win for duplicate keys.
    sst_paths should be ordered oldest to newest.

    drop_tombstones: only safe when ALL sstables are included in compaction.
    If older sstables outside this set might have the key, keeping tombstones
    prevents deleted keys from resurrecting."""

    merged = {}
    for path in sst_paths:
        reader = SSTableReader(path)
        for key, val in reader.items():
            merged[key] = val

    if drop_tombstones:
        items = sorted(
            [(k, v) for k, v in merged.items() if v != "__STONEDB_TOMBSTONE__"],
            key=lambda x: x[0]
        )
    else:
        items = sorted(merged.items(), key=lambda x: x[0])

    SSTableWriter.write(output_path, items)

    bloom = BloomFilter(len(items))
    for k, _ in items:
        bloom.add(k)
    bloom_path = output_path.replace(".sst", ".bloom")
    with open(bloom_path, "wb") as f:
        f.write(bloom.serialize())

    for path in sst_paths:
        os.remove(path)
        bp = path.replace(".sst", ".bloom")
        if os.path.exists(bp):
            os.remove(bp)

    return output_path, bloom
