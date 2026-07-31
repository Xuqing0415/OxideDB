import pytest
from oxidedb.storage.mvcc import MVCCStorage


class TestMVCCStorage:
    def setup_method(self):
        self.storage = MVCCStorage()

    def test_get_nonexistent_key(self):
        assert self.storage.get(b"key", 1) is None

    def test_set_and_get(self):
        self.storage.set(b"key", b"value", 1)
        assert self.storage.get(b"key", 2) == b"value"

    def test_multiple_versions(self):
        self.storage.set(b"key", b"v1", 1)
        self.storage.set(b"key", b"v2", 2)
        self.storage.set(b"key", b"v3", 3)

        assert self.storage.get(b"key", 0) is None
        assert self.storage.get(b"key", 1) == b"v1"
        assert self.storage.get(b"key", 2) == b"v2"
        assert self.storage.get(b"key", 3) == b"v3"
        assert self.storage.get(b"key", 10) == b"v3"

    def test_delete(self):
        self.storage.set(b"key", b"value", 1)
        assert self.storage.get(b"key", 2) == b"value"

        self.storage.delete(b"key", 3)
        assert self.storage.get(b"key", 2) == b"value"
        assert self.storage.get(b"key", 3) is None
        assert self.storage.get(b"key", 10) is None

    def test_scan(self):
        self.storage.set(b"a", b"1", 1)
        self.storage.set(b"b", b"2", 1)
        self.storage.set(b"c", b"3", 1)
        self.storage.set(b"d", b"4", 1)

        result = self.storage.scan(b"a", b"c", 2)
        assert len(result) == 2
        assert result == [(b"a", b"1"), (b"b", b"2")]

    def test_get_latest_version(self):
        self.storage.set(b"key", b"v1", 1)
        self.storage.set(b"key", b"v2", 2)

        record = self.storage.get_latest_version(b"key")
        assert record is not None
        assert record.value == b"v2"
        assert record.timestamp == 2

        record = self.storage.get_latest_version(b"nonexistent")
        assert record is None
