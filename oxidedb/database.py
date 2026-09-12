from typing import Optional, List, Tuple
from oxidedb.storage.engine import create_engine
from oxidedb.storage.mvcc import MVCCStorage
from oxidedb.transaction.local import Transaction, TransactionManager


class Database:
    """Embedded single-process database over the MVCC storage layer.

    ``data_dir`` picks the engine: ``None`` keeps everything in memory and throws
    it away when the process exits, while a directory puts the keyspace in
    ``<data_dir>/data.sqlite3`` behind the durable ``SQLiteEngine``.
    """

    def __init__(self, data_dir: Optional[str] = None):
        # ``name="data"`` matches the convention the clusters use, where the MVCC
        # keyspace lives in ``<data_dir>/data.sqlite3`` next to the Raft one.
        self._storage = MVCCStorage(engine=create_engine(data_dir, name="data"))
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

    def close(self) -> None:
        self._storage.close()
