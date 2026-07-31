import pytest
from oxidedb.storage.mvcc import MVCCStorage
from oxidedb.transaction.local import TransactionManager, TransactionState


class TestTransaction:
    def setup_method(self):
        self.storage = MVCCStorage()
        self.txn_manager = TransactionManager(self.storage)

    def test_begin_commit(self):
        txn = self.txn_manager.begin()
        txn.set(b"key", b"value")
        assert txn.commit()
        assert txn.state == TransactionState.COMMITTED

        assert self.storage.get(b"key", txn.commit_timestamp) == b"value"

    def test_begin_rollback(self):
        txn = self.txn_manager.begin()
        txn.set(b"key", b"value")
        txn.rollback()
        assert txn.state == TransactionState.ABORTED

        assert self.storage.get(b"key", 100) is None

    def test_transaction_get(self):
        self.storage.set(b"key", b"old", 1)

        txn = self.txn_manager.begin()
        assert txn.get(b"key") == b"old"

    def test_transaction_set_then_get(self):
        txn = self.txn_manager.begin()
        txn.set(b"key", b"value")
        assert txn.get(b"key") == b"value"

    def test_transaction_delete(self):
        self.storage.set(b"key", b"value", 1)

        txn = self.txn_manager.begin()
        txn.delete(b"key")
        assert txn.get(b"key") is None
        txn.commit()

        assert self.storage.get(b"key", txn.commit_timestamp) is None

    def test_transaction_isolation(self):
        txn1 = self.txn_manager.begin()
        txn1.set(b"key", b"v1")

        txn2 = self.txn_manager.begin()
        assert txn2.get(b"key") is None

        txn1.commit()

        txn3 = self.txn_manager.begin()
        assert txn3.get(b"key") == b"v1"

    def test_multiple_transactions(self):
        txn1 = self.txn_manager.begin()
        txn1.set(b"a", b"1")
        txn1.set(b"b", b"2")
        txn1.commit()

        txn2 = self.txn_manager.begin()
        txn2.set(b"a", b"updated")
        txn2.delete(b"b")
        txn2.commit()

        assert self.storage.get(b"a", txn2.commit_timestamp) == b"updated"
        assert self.storage.get(b"b", txn2.commit_timestamp) is None
        assert self.storage.get(b"a", txn1.commit_timestamp) == b"1"
        assert self.storage.get(b"b", txn1.commit_timestamp) == b"2"

    def test_commit_after_abort(self):
        txn = self.txn_manager.begin()
        txn.set(b"key", b"value")
        txn.rollback()

        with pytest.raises(ValueError):
            txn.commit()

    def test_operations_after_commit(self):
        txn = self.txn_manager.begin()
        txn.set(b"key", b"value")
        txn.commit()

        with pytest.raises(ValueError):
            txn.set(b"key", b"another")

        with pytest.raises(ValueError):
            txn.get(b"key")

        with pytest.raises(ValueError):
            txn.delete(b"key")
