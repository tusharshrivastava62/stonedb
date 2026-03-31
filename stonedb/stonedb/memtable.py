from sortedcontainers import SortedDict

# default 4MB flush threshold
DEFAULT_THRESHOLD = 4 * 1024 * 1024


class Memtable:
    def __init__(self, flush_threshold=DEFAULT_THRESHOLD):
        self._data = SortedDict()
        self._size = 0
        self.flush_threshold = flush_threshold

    def put(self, key: str, value: str):
        old = self._data.get(key)
        if old is not None:
            self._size -= len(key) + len(old)
        self._data[key] = value
        self._size += len(key) + len(value)

    def get(self, key: str):
        return self._data.get(key)

    def should_flush(self) -> bool:
        return self._size >= self.flush_threshold

    def items(self):
        """Sorted key-value pairs for flushing to SSTable."""
        return list(self._data.items())

    def clear(self):
        self._data.clear()
        self._size = 0

    def __len__(self):
        return len(self._data)

    def __contains__(self, key):
        return key in self._data
