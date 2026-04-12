import os
import shutil
import pytest
from stonedb.db import DB


TEST_DIR = "/tmp/stonedb_test_integration"


def setup_function():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)


def teardown_function():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)


def test_write_read_10k_keys():
    db = DB(TEST_DIR, memtable_threshold=4096)

    for i in range(10000):
        db.put(f"key_{i:06d}", f"value_{i}")

    for i in range(10000):
        assert db.get(f"key_{i:06d}") == f"value_{i}"

    db.close()


def test_delete_then_compact():
    db = DB(TEST_DIR, memtable_threshold=2048)

    # write 10k
    for i in range(10000):
        db.put(f"key_{i:06d}", f"value_{i}")

    # delete 5k
    for i in range(0, 10000, 2):  # delete even-numbered keys
        db.delete(f"key_{i:06d}")

    # compact everything
    db.run_compaction()

    # verify: even keys should be gone, odd keys should remain
    for i in range(10000):
        val = db.get(f"key_{i:06d}")
        if i % 2 == 0:
            assert val is None, f"key_{i:06d} should be deleted but got {val}"
        else:
            assert val == f"value_{i}", f"key_{i:06d} = {val}, expected value_{i}"

    db.close()


def test_overwrite_then_compact():
    """Overwrite same key many times, compact, verify only latest survives."""
    db = DB(TEST_DIR, memtable_threshold=512)

    for version in range(20):
        db.put("counter", str(version))

    db.run_compaction()
    assert db.get("counter") == "19"
    db.close()


def test_recovery_after_compaction():
    db = DB(TEST_DIR, memtable_threshold=2048)
    for i in range(1000):
        db.put(f"k{i}", f"v{i}")

    db.run_compaction()
    db.close()

    # reopen
    db2 = DB(TEST_DIR, memtable_threshold=2048)
    assert db2.get("k500") == "v500"
    assert db2.get("k999") == "v999"
    db2.close()
