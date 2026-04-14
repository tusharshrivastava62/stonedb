#!/usr/bin/env python3
"""Benchmark suite for stonedb. Run: python benchmarks/run_all.py"""

import os
import sys
import time
import shutil
import random
import platform
import statistics
import glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from stonedb.db import DB


BENCH_DIR = "/tmp/stonedb_bench"
NUM_KEYS = 100000
READ_SAMPLE = 10000
RUNS = 3


def print_header():
    print("=" * 70)
    print("stonedb benchmark suite")
    print("=" * 70)
    print(f"Machine: {platform.machine()}, Python {platform.python_version()}")
    print(f"OS: {platform.system()} {platform.release()}")
    print(f"Keys: {NUM_KEYS}, Read sample: {READ_SAMPLE}, Runs: {RUNS}")
    print("=" * 70)
    print()


def clean():
    if os.path.exists(BENCH_DIR):
        shutil.rmtree(BENCH_DIR)


def percentile(data, p):
    k = (len(data) - 1) * (p / 100)
    f = int(k)
    c = f + 1
    if c >= len(data):
        return data[f]
    return data[f] + (k - f) * (data[c] - data[f])


def disk_usage(path):
    total = 0
    for ext in ["*.sst", "*.bloom"]:
        for f in glob.glob(os.path.join(path, ext)):
            total += os.path.getsize(f)
    return total


def bench_sequential_writes(value_size):
    value = "x" * value_size
    results = []

    for run in range(RUNS):
        clean()
        db = DB(BENCH_DIR)

        start = time.perf_counter()
        for i in range(NUM_KEYS):
            db.put(f"key_{i:08d}", value)
        elapsed = time.perf_counter() - start

        ops_per_sec = NUM_KEYS / elapsed
        results.append(ops_per_sec)
        db.close()

    clean()
    return statistics.median(results)


def setup_read_db(num_keys=NUM_KEYS, value_size=1024):
    clean()
    db = DB(BENCH_DIR, memtable_threshold=32 * 1024)
    value = "x" * value_size
    for i in range(num_keys):
        db.put(f"key_{i:08d}", value)
    db.close()
    return BENCH_DIR


def bench_read_latency(db, keys):
    latencies = []
    for key in keys:
        start = time.perf_counter()
        db.get(key)
        elapsed = (time.perf_counter() - start) * 1000
        latencies.append(elapsed)
    latencies.sort()
    return latencies


def count_disk_reads(db):
    total = 0
    for reader, _ in db._sstables:
        total += reader._disk_reads
    return total


def reset_disk_reads(db):
    for reader, _ in db._sstables:
        reader._disk_reads = 0


def run_write_benchmarks():
    print("WRITE THROUGHPUT (sequential, with WAL fsync)")
    print("-" * 50)
    print(f"{'Value Size':<15} {'ops/sec':<15}")
    print("-" * 50)

    for vsize in [100, 1024, 10240]:
        label = f"{vsize}B" if vsize < 1024 else f"{vsize // 1024}KB"
        ops = bench_sequential_writes(vsize)
        print(f"{label:<15} {ops:>12,.0f}")
    print()


def run_read_benchmarks():
    print("READ LATENCY (on-disk index reads)")
    print("-" * 75)
    print(f"{'Test':<30} {'p50':>8} {'p95':>8} {'p99':>8} {'disk reads':>11} {'bloom skip':>11}")
    print("-" * 75)

    db_path = setup_read_db()

    existing_keys = random.sample(
        [f"key_{i:08d}" for i in range(NUM_KEYS)], READ_SAMPLE
    )
    missing_keys = [f"miss_{i:08d}" for i in range(READ_SAMPLE)]

    # with bloom
    db = DB(db_path, memtable_threshold=32 * 1024)
    db._bloom_checks_skipped = 0
    reset_disk_reads(db)

    lats = bench_read_latency(db, existing_keys)
    skips = db._bloom_checks_skipped
    reads = count_disk_reads(db)
    print(f"{'Existing keys (bloom ON)':<30} {percentile(lats, 50):>7.3f}ms {percentile(lats, 95):>7.3f}ms {percentile(lats, 99):>7.3f}ms {reads:>11,} {skips:>11,}")

    db._bloom_checks_skipped = 0
    reset_disk_reads(db)
    lats = bench_read_latency(db, missing_keys)
    skips = db._bloom_checks_skipped
    reads = count_disk_reads(db)
    print(f"{'Missing keys (bloom ON)':<30} {percentile(lats, 50):>7.3f}ms {percentile(lats, 95):>7.3f}ms {percentile(lats, 99):>7.3f}ms {reads:>11,} {skips:>11,}")

    db.close()

    # without bloom
    db2 = DB(db_path, memtable_threshold=32 * 1024)
    db2._sstables = [(reader, None) for reader, _ in db2._sstables]
    reset_disk_reads(db2)

    lats = bench_read_latency(db2, existing_keys)
    reads_no_bloom = count_disk_reads(db2)
    print(f"{'Existing keys (bloom OFF)':<30} {percentile(lats, 50):>7.3f}ms {percentile(lats, 95):>7.3f}ms {percentile(lats, 99):>7.3f}ms {reads_no_bloom:>11,} {'—':>11}")

    reset_disk_reads(db2)
    lats = bench_read_latency(db2, missing_keys)
    reads_no_bloom_miss = count_disk_reads(db2)
    print(f"{'Missing keys (bloom OFF)':<30} {percentile(lats, 50):>7.3f}ms {percentile(lats, 95):>7.3f}ms {percentile(lats, 99):>7.3f}ms {reads_no_bloom_miss:>11,} {'—':>11}")

    db2.close()
    clean()
    print()


def run_compaction_benchmarks():
    print("COMPACTION (space amplification)")
    print("-" * 55)

    clean()
    db = DB(BENCH_DIR, memtable_threshold=32 * 1024)

    value = "x" * 512
    for i in range(50000):
        db.put(f"key_{i:08d}", value)
    for i in range(0, 50000, 2):
        db.put(f"key_{i:08d}", "y" * 512)

    db.close()

    sst_count_before = len(glob.glob(os.path.join(BENCH_DIR, "*.sst")))
    disk_before = disk_usage(BENCH_DIR)
    logical_size = 50000 * (8 + 512 + 16)
    amp_before = disk_before / logical_size

    print(f"Before compaction:")
    print(f"  SSTables:           {sst_count_before}")
    print(f"  Disk usage:         {disk_before / 1024 / 1024:.2f} MB")
    print(f"  Space amplification: {amp_before:.2f}x")

    db2 = DB(BENCH_DIR, memtable_threshold=32 * 1024)

    start = time.perf_counter()
    db2.run_compaction()
    compact_time = time.perf_counter() - start

    db2.close()

    sst_count_after = len(glob.glob(os.path.join(BENCH_DIR, "*.sst")))
    disk_after = disk_usage(BENCH_DIR)
    amp_after = disk_after / logical_size

    print(f"\nAfter compaction:")
    print(f"  SSTables:           {sst_count_after}")
    print(f"  Disk usage:         {disk_after / 1024 / 1024:.2f} MB")
    print(f"  Space amplification: {amp_after:.2f}x")
    print(f"  Compaction time:    {compact_time:.2f}s")
    print(f"  Space reclaimed:    {(disk_before - disk_after) / 1024 / 1024:.2f} MB ({(1 - disk_after / disk_before) * 100:.1f}%)")

    clean()
    print()


def run_mixed_workload():
    print("MIXED WORKLOAD (70% read / 30% write)")
    print("-" * 55)

    clean()
    db = DB(BENCH_DIR, memtable_threshold=64 * 1024)

    # seed with 10k keys
    value = "x" * 256
    for i in range(10000):
        db.put(f"key_{i:08d}", value)

    ops = 50000
    read_count = 0
    write_count = 0
    latencies = []

    for i in range(ops):
        if random.random() < 0.7:
            # read
            k = f"key_{random.randint(0, 9999):08d}"
            start = time.perf_counter()
            db.get(k)
            elapsed = (time.perf_counter() - start) * 1000
            latencies.append(elapsed)
            read_count += 1
        else:
            # write
            k = f"key_{random.randint(0, 19999):08d}"
            start = time.perf_counter()
            db.put(k, value)
            elapsed = (time.perf_counter() - start) * 1000
            latencies.append(elapsed)
            write_count += 1

    latencies.sort()
    total_time = sum(latencies) / 1000  # back to seconds

    print(f"  Operations:    {ops} ({read_count} reads, {write_count} writes)")
    print(f"  Throughput:    {ops / total_time:,.0f} ops/sec")
    print(f"  Latency p50:   {percentile(latencies, 50):.3f}ms")
    print(f"  Latency p95:   {percentile(latencies, 95):.3f}ms")
    print(f"  Latency p99:   {percentile(latencies, 99):.3f}ms")

    db.close()
    clean()
    print()


if __name__ == "__main__":
    print_header()
    run_write_benchmarks()
    run_read_benchmarks()
    run_compaction_benchmarks()
    run_mixed_workload()
