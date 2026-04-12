#!/usr/bin/env python3
"""Benchmark suite for stonedb. Run: python benchmarks/run_all.py"""

import os
import sys
import time
import shutil
import platform
import statistics

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from stonedb.db import DB


BENCH_DIR = "/tmp/stonedb_bench"
NUM_KEYS = 100000
RUNS = 3


def print_header():
    print("=" * 70)
    print("stonedb benchmark suite")
    print("=" * 70)
    print(f"Machine: {platform.machine()}, Python {platform.python_version()}")
    print(f"OS: {platform.system()} {platform.release()}")
    print(f"Keys: {NUM_KEYS}, Runs per test: {RUNS}")
    print("=" * 70)
    print()


def clean():
    if os.path.exists(BENCH_DIR):
        shutil.rmtree(BENCH_DIR)


def bench_sequential_writes(value_size):
    """Measure write throughput for a given value size."""
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
    median = statistics.median(results)
    return median


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


if __name__ == "__main__":
    print_header()
    run_write_benchmarks()
