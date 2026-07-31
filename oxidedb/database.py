from typing import Optional, List, Tuple
from oxidedb.storage.mvcc import MVCCStorage
from oxidedb.transaction.local import Transaction, TransactionManager


class Database:
    def __init__(self):
        self._storage = MVCCStorage()
        self._txn_manager = TransactionManager(self._storage)

    def begin(self) -> Transaction:
        return self._txn_manager.begin()

    def get(self, key: bytes, txn: Optional[Transaction] = None) -> Optional[bytes]:
        if txn is not None:
            return txn.get(key)
        return self._storage.get(key, self._txn_manager.get_current_timestamp())

    def set(self, key: bytes, value: bytes, txn: Optional[Transaction] = None) -> None:
        if txn is not None:
            txn.set(key, value)
        else:
            txn = self.begin()
            txn.set(key, value)
            txn.commit()

    def delete(self, key: bytes, txn: Optional[Transaction] = None) -> None:
        if txn is not None:
            txn.delete(key)
        else:
            txn = self.begin()
            txn.delete(key)
            txn.commit()

    def scan(self, start_key: bytes, end_key: bytes, txn: Optional[Transaction] = None) -> List[Tuple[bytes, bytes]]:
        timestamp = txn.start_timestamp if txn else self._txn_manager.get_current_timestamp()
        return self._storage.scan(start_key, end_key, timestamp)

    def clear(self) -> None:
        self._storage.clear()
