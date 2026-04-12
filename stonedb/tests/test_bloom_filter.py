import pytest
from stonedb.bloom import BloomFilter


def test_no_false_negatives():
    """Every key that was added must be found."""
    bf = BloomFilter(1000, fpr=0.01)
    keys = [f"key_{i:06d}" for i in range(1000)]
    for k in keys:
        bf.add(k)

    for k in keys:
        assert bf.might_contain(k), f"false negative on {k}"


def test_fpr_within_bounds():
    """False positive rate should be roughly within 2x of target."""
    bf = BloomFilter(10000, fpr=0.01)
    for i in range(10000):
        bf.add(f"exists_{i}")

    false_positives = 0
    test_count = 10000
    for i in range(test_count):
        if bf.might_contain(f"absent_{i}"):
            false_positives += 1

    actual_fpr = false_positives / test_count
    # should be roughly 1%, allow up to 3% since it's probabilistic
    assert actual_fpr < 0.03, f"FPR too high: {actual_fpr:.3f} (target 0.01)"


def test_serialize_roundtrip():
    bf = BloomFilter(500, fpr=0.01)
    for i in range(500):
        bf.add(f"item_{i}")

    data = bf.serialize()
    bf2 = BloomFilter.deserialize(data)

    for i in range(500):
        assert bf2.might_contain(f"item_{i}")


def test_empty_bloom_filter():
    bf = BloomFilter(100)
    # nothing added, everything should be absent
    assert not bf.might_contain("anything")
