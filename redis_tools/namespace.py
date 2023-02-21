_IN_USED = set()
SEPARATOR = b'\x00'


def register(prefix: str):
    assert prefix not in _IN_USED
    _IN_USED.add(prefix)
    return Namespace(prefix)


class Namespace:
    def __init__(self, prefix: str):
        self._prefix = prefix
        self._full_prefix = prefix.encode('utf-8') + SEPARATOR

    @property
    def prefix(self):
        return self._prefix

    def make(self, suffix: str) -> bytes:
        return self._full_prefix + suffix.encode('utf-8')

    @staticmethod
    def split(key: bytes):
        pairs = key.rsplit(SEPARATOR, maxsplit=1)
        prefix = pairs[0].decode('utf-8')
        suffix = pairs[1].decode('utf-8')
        return prefix, suffix

    def contains(self, key: bytes):
        prefix, _ = self.split(key)
        return prefix == self._prefix
