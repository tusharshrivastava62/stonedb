import os


class DB:
    """LSM-Tree key-value storage engine."""

    def __init__(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)

    def put(self, key: str, value: str):
        raise NotImplementedError

    def get(self, key: str):
        raise NotImplementedError

    def delete(self, key: str):
        raise NotImplementedError

    def close(self):
        pass
