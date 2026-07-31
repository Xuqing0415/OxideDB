from enum import Enum
from typing import Dict, Optional
from oxidedb.storage.mvcc import MVCCStorage
from oxidedb.storage.timestamp import get_timestamp, get_current_timestamp


class TransactionState(Enum):
    PENDING = "pending"
    COMMITTED = "committed"
    ABORTED = "aborted"


class Transaction:
    def __init__(self, storage: MVCCStorage, txn_id: int):
        self._storage = storage
        self._txn_id = txn_id
        self._start_timestamp = get_timestamp()
        self._commit_timestamp: Optional[int] = None
        self._state = TransactionState.PENDING
        self._writes: Dict[bytes, bytes] = {}
        self._deletes: set = set()

    @property
    def txn_id(self) -> int:
        return self._txn_id

    @property
    def start_timestamp(self) -> int:
        return self._start_timestamp

    @property
    def commit_timestamp(self) -> Optional[int]:
        return self._commit_timestamp

    @property
    def state(self) -> TransactionState:
        return self._state

    def get(self, key: bytes) -> Optional[bytes]:
        if self._state != TransactionState.PENDING:
            raise ValueError(f"Transaction is {self._state.value}")

        if key in self._writes:
            return self._writes[key]

        if key in self._deletes:
            return None

        return self._storage.get(key, self._start_timestamp)

    def set(self, key: bytes, value: bytes) -> None:
        if self._state != TransactionState.PENDING:
            raise ValueError(f"Transaction is {self._state.value}")

        self._writes[key] = value
        self._deletes.discard(key)

    def delete(self, key: bytes) -> None:
        if self._state != TransactionState.PENDING:
            raise ValueError(f"Transaction is {self._state.value}")

        self._deletes.add(key)
        self._writes.pop(key, None)

    def commit(self) -> bool:
        if self._state != TransactionState.PENDING:
            raise ValueError(f"Transaction is {self._state.value}")

        self._commit_timestamp = get_timestamp()

        try:
            for key, value in self._writes.items():
                self._storage.set(key, value, self._commit_timestamp)

            for key in self._deletes:
                self._storage.delete(key, self._commit_timestamp)

            self._state = TransactionState.COMMITTED
            return True
        except Exception:
            self._state = TransactionState.ABORTED
            return False

    def rollback(self) -> None:
        self._state = TransactionState.ABORTED
        self._writes.clear()
        self._deletes.clear()


class TransactionManager:
    def __init__(self, storage: MVCCStorage):
        self._storage = storage
        self._txn_counter = 0

    def begin(self) -> Transaction:
        self._txn_counter += 1
        return Transaction(self._storage, self._txn_counter)

    def get_current_timestamp(self) -> int:
        return get_current_timestamp()
