import pytest
from oxidedb.database import Database


class TestDatabase:
    def setup_method(self):
        self.db = Database()

    def test_get_set_delete(self):
        self.db.set(b"key", b"value")
        assert self.db.get(b"key") == b"value"

        self.db.delete(b"key")
        assert self.db.get(b"key") is None

    def test_scan(self):
        self.db.set(b"a", b"1")
        self.db.set(b"b", b"2")
        self.db.set(b"c", b"3")

        result = self.db.scan(b"a", b"c")
        assert len(result) == 2
        assert result == [(b"a", b"1"), (b"b", b"2")]

    def test_transaction(self):
        txn = self.db.begin()
        txn.set(b"key1", b"value1")
        txn.set(b"key2", b"value2")
        txn.commit()

        assert self.db.get(b"key1") == b"value1"
        assert self.db.get(b"key2") == b"value2"

    def test_transaction_with_db_methods(self):
        txn = self.db.begin()
        self.db.set(b"key", b"value", txn)
        assert self.db.get(b"key", txn) == b"value"
        txn.commit()

        assert self.db.get(b"key") == b"value"

    def test_rollback(self):
        txn = self.db.begin()
        self.db.set(b"key", b"value", txn)
        txn.rollback()

        assert self.db.get(b"key") is None

    def test_clear(self):
        self.db.set(b"key1", b"value1")
        self.db.set(b"key2", b"value2")
        assert self.db.get(b"key1") == b"value1"

        self.db.clear()
        assert self.db.get(b"key1") is None
        assert self.db.get(b"key2") is None
