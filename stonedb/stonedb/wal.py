import os
import struct

# entry format: [key_len:4][val_len:4][key][value]
HEADER_FMT = ">II"
HEADER_SIZE = struct.calcsize(HEADER_FMT)


class WAL:
    def __init__(self, path):
        self.path = path
        self._f = open(path, "ab")

    def append(self, key: str, value: str):
        kb = key.encode()
        vb = value.encode()
        header = struct.pack(HEADER_FMT, len(kb), len(vb))
        self._f.write(header + kb + vb)
        self._f.flush()
        os.fsync(self._f.fileno())

    @staticmethod
    def recover(path):
        """Replay WAL entries. Tolerates truncated trailing entry from crash."""
        entries = []
        if not os.path.exists(path):
            return entries

        with open(path, "rb") as f:
            while True:
                header = f.read(HEADER_SIZE)
                if len(header) < HEADER_SIZE:
                    break  # incomplete header, probably crashed mid-write

                klen, vlen = struct.unpack(HEADER_FMT, header)
                payload = f.read(klen + vlen)
                if len(payload) < klen + vlen:
                    break  # truncated entry

                key = payload[:klen].decode()
                val = payload[klen:].decode()
                entries.append((key, val))

        return entries

    def truncate(self):
        """Clear the WAL after a successful flush."""
        self._f.close()
        self._f = open(self.path, "wb")  # overwrite
        self._f.close()
        self._f = open(self.path, "ab")

    def close(self):
        self._f.close()
