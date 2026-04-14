#!/usr/bin/env python3
"""Benchmark suite for stonedb. Run: python benchmarks/run_all.py"""

import os
import sys
import time
import shutil
import random
import platform
import statistics

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
    """Create a DB with data flushed to SSTables for read benchmarks."""
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
    """Sum disk reads across all sstable readers."""
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

    # --- with bloom (normal) ---
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

    # --- without bloom ---
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


if __name__ == "__main__":
    print_header()
    run_write_benchmarks()
    run_read_benchmarks()
