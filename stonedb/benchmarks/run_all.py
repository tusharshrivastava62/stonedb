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
    # small threshold to force lots of sstables
    db = DB(BENCH_DIR, memtable_threshold=32 * 1024)
    value = "x" * value_size
    for i in range(num_keys):
        db.put(f"key_{i:08d}", value)
    db.close()
    return BENCH_DIR


def bench_read_latency(db, keys):
    """Measure per-read latency for a list of keys. Returns list of latencies in ms."""
    latencies = []
    for key in keys:
        start = time.perf_counter()
        db.get(key)
        elapsed = (time.perf_counter() - start) * 1000  # ms
        latencies.append(elapsed)
    latencies.sort()
    return latencies


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
    print("READ LATENCY")
    print("-" * 70)
    print(f"{'Test':<30} {'p50':>8} {'p95':>8} {'p99':>8} {'bloom skips':>12}")
    print("-" * 70)

    # setup
    db_path = setup_read_db()

    # existing keys
    existing_keys = random.sample(
        [f"key_{i:08d}" for i in range(NUM_KEYS)], READ_SAMPLE
    )
    # keys that don't exist
    missing_keys = [f"miss_{i:08d}" for i in range(READ_SAMPLE)]

    # --- with bloom filters (normal) ---
    db = DB(db_path, memtable_threshold=32 * 1024)
    db._bloom_checks_skipped = 0

    lats = bench_read_latency(db, existing_keys)
    skips = db._bloom_checks_skipped
    print(f"{'Existing keys (bloom ON)':<30} {percentile(lats, 50):>7.3f}ms {percentile(lats, 95):>7.3f}ms {percentile(lats, 99):>7.3f}ms {skips:>12}")

    db._bloom_checks_skipped = 0
    lats = bench_read_latency(db, missing_keys)
    skips = db._bloom_checks_skipped
    print(f"{'Missing keys (bloom ON)':<30} {percentile(lats, 50):>7.3f}ms {percentile(lats, 95):>7.3f}ms {percentile(lats, 99):>7.3f}ms {skips:>12}")

    db.close()

    # --- without bloom filters ---
    db2 = DB(db_path, memtable_threshold=32 * 1024)
    # disable bloom by setting all blooms to None
    db2._sstables = [(reader, None) for reader, _ in db2._sstables]
    db2._bloom_checks_skipped = 0

    lats = bench_read_latency(db2, existing_keys)
    print(f"{'Existing keys (bloom OFF)':<30} {percentile(lats, 50):>7.3f}ms {percentile(lats, 95):>7.3f}ms {percentile(lats, 99):>7.3f}ms {'—':>12}")

    lats = bench_read_latency(db2, missing_keys)
    print(f"{'Missing keys (bloom OFF)':<30} {percentile(lats, 50):>7.3f}ms {percentile(lats, 95):>7.3f}ms {percentile(lats, 99):>7.3f}ms {'—':>12}")

    db2.close()

    clean()
    print()


if __name__ == "__main__":
    print_header()
    run_write_benchmarks()
    run_read_benchmarks()
