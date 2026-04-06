import os
import shutil
import struct
import pytest
from stonedb.db import DB
from stonedb.wal import WAL, HEADER_FMT, HEADER_SIZE


TEST_DIR = "/tmp/stonedb_test_wal"


def setup_function():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)


def teardown_function():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)


def test_recovery_after_clean_shutdown():
    db = DB(TEST_DIR, memtable_threshold=1024)
    for i in range(10):
        db.put(f"key_{i:04d}", f"value_{i}")
    db.close()

    # reopen and verify
    db2 = DB(TEST_DIR, memtable_threshold=1024)
    for i in range(10):
        assert db2.get(f"key_{i:04d}") == f"value_{i}"
    db2.close()


def test_recovery_with_truncated_wal():
    """Simulate crash mid-write by truncating WAL at random point."""
    os.makedirs(TEST_DIR, exist_ok=True)
    wal_path = os.path.join(TEST_DIR, "wal.log")

    # write 5 complete entries directly to WAL
    w = WAL(wal_path)
    for i in range(5):
        w.append(f"key_{i}", f"value_{i}")

    # append a partial entry (just the header, no payload)
    kb = b"incomplete_key"
    vb = b"incomplete_value"
    header = struct.pack(HEADER_FMT, len(kb), len(vb))
    w._f.write(header[:3])  # write only 3 of 8 header bytes
    w._f.flush()
    w.close()

    # recovery should get the 5 complete entries, skip the partial one
    entries = WAL.recover(wal_path)
    assert len(entries) == 5
    for i in range(5):
        assert entries[i] == (f"key_{i}", f"value_{i}")


def test_recovery_preserves_most_recent_value():
    """If same key written multiple times, recovery should keep last value."""
    db = DB(TEST_DIR, memtable_threshold=1024 * 1024)  # big threshold, no flush
    db.put("color", "red")
    db.put("color", "blue")
    db.put("color", "green")
    # don't call close() — simulate crash by just abandoning the object
    db._wal.close()

    db2 = DB(TEST_DIR, memtable_threshold=1024 * 1024)
    assert db2.get("color") == "green"
    db2.close()
