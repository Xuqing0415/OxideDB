"""Pluggable local storage engines (plan E).

Instead of hand-rolling durability, OxideDB delegates it to a proven embedded
engine and keeps only the *encoding* of the logical keyspace in this project.
Every engine exposes the same contract:

* keys and values are ``bytes``;
* keys are ordered by plain byte comparison (``memcmp``), shorter prefix first;
* ``scan`` and ``delete_range`` take a half-open ``[start, end)`` range;
* writes through ``put``/``put_batch``/``delete``/``delete_range`` are durable
  once ``flush`` returns.

``MemoryEngine`` reproduces the original in-process behaviour used by the
embedded clusters in the test-suite.  ``SQLiteEngine`` is the durable default:
it ships with CPython, runs in WAL mode and stores keys as SQLite BLOBs, whose
collation is exactly the ``memcmp`` order the callers rely on.  A RocksDB or
LMDB backend can be added later by implementing the same interface, without
touching any caller.

That byte ordering is what makes a ``prefix || body || timestamp`` encoding (see
:mod:`oxidedb.storage.mvcc`) a valid MVCC index with no extra sorting work.
"""

import os
import sqlite3
import threading
from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Optional, Tuple


class Engine(ABC):
    """Minimal ordered key/value store."""

    @abstractmethod
    def put(self, key: bytes, value: bytes) -> None:
        pass

    @abstractmethod
    def get(self, key: bytes) -> Optional[bytes]:
        pass

    @abstractmethod
    def delete(self, key: bytes) -> None:
        pass

    def put_batch(self, items: Iterable[Tuple[bytes, bytes]]) -> None:
        for key, value in items:
            self.put(key, value)

    def delete_batch(self, keys: Iterable[bytes]) -> None:
        for key in keys:
            self.delete(key)

    @abstractmethod
    def delete_range(self, start: bytes, end: bytes) -> None:
        pass

    @abstractmethod
    def scan(self, start: bytes, end: bytes) -> List[Tuple[bytes, bytes]]:
        pass

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self.flush()


def _as_bytes(value, what: str = "key") -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError(f"{what} must be bytes, got {type(value).__name__}")
    return bytes(value)


class MemoryEngine(Engine):
    """Non-durable engine backed by a dict.

    Used for embedded/in-process clusters and for tests that do not care about
    surviving a restart.  Scans sort on demand; that is deliberate, because
    keeping a second sorted index here would only duplicate what SQLite already
    does correctly.
    """

    def __init__(self) -> None:
        self._data: Dict[bytes, bytes] = {}
        self._lock = threading.RLock()

    def put(self, key: bytes, value: bytes) -> None:
        with self._lock:
            self._data[_as_bytes(key)] = _as_bytes(value, "value")

    def get(self, key: bytes) -> Optional[bytes]:
        with self._lock:
            return self._data.get(_as_bytes(key))

    def delete(self, key: bytes) -> None:
        with self._lock:
            self._data.pop(_as_bytes(key), None)

    def put_batch(self, items: Iterable[Tuple[bytes, bytes]]) -> None:
        with self._lock:
            for key, value in items:
                self._data[_as_bytes(key)] = _as_bytes(value, "value")

    def delete_range(self, start: bytes, end: bytes) -> None:
        start, end = _as_bytes(start, "start"), _as_bytes(end, "end")
        with self._lock:
            for key in [k for k in self._data if start <= k < end]:
                del self._data[key]

    def scan(self, start: bytes, end: bytes) -> List[Tuple[bytes, bytes]]:
        start, end = _as_bytes(start, "start"), _as_bytes(end, "end")
        with self._lock:
            return [(k, self._data[k]) for k in sorted(self._data) if start <= k < end]

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


class SQLiteEngine(Engine):
    """Durable engine built on the ``sqlite3`` module from the standard library.

    The schema is a single ``WITHOUT ROWID`` table, so the primary key *is* the
    clustered index: range scans walk the B-tree in byte order with no second
    index to maintain.  WAL mode keeps readers from blocking the Raft thread
    that appends log entries.
    """

    _DDL = "CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID"
    _UPSERT = "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v"

    def __init__(self, path: str, synchronous: str = "NORMAL") -> None:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._path = path
        # Raft drives this engine from several threads and a sqlite3 connection
        # is not safe for concurrent use, so all access is serialised here.
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(f"PRAGMA synchronous={synchronous}")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(self._DDL)

    def put(self, key: bytes, value: bytes) -> None:
        with self._lock:
            self._conn.execute(self._UPSERT, (_as_bytes(key), _as_bytes(value, "value")))

    def get(self, key: bytes) -> Optional[bytes]:
        with self._lock:
            row = self._conn.execute("SELECT v FROM kv WHERE k = ?", (_as_bytes(key),)).fetchone()
        return None if row is None else bytes(row[0])

    def delete(self, key: bytes) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM kv WHERE k = ?", (_as_bytes(key),))

    def put_batch(self, items: Iterable[Tuple[bytes, bytes]]) -> None:
        rows = [(_as_bytes(k), _as_bytes(v, "value")) for k, v in items]
        if not rows:
            return
        with self._lock:
            # One transaction: a batch is either fully durable or not at all.
            self._conn.execute("BEGIN")
            try:
                self._conn.executemany(self._UPSERT, rows)
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")

    def delete_range(self, start: bytes, end: bytes) -> None:
        start, end = _as_bytes(start, "start"), _as_bytes(end, "end")
        with self._lock:
            self._conn.execute("DELETE FROM kv WHERE k >= ? AND k < ?", (start, end))

    def scan(self, start: bytes, end: bytes) -> List[Tuple[bytes, bytes]]:
        start, end = _as_bytes(start, "start"), _as_bytes(end, "end")
        with self._lock:
            rows = self._conn.execute(
                "SELECT k, v FROM kv WHERE k >= ? AND k < ? ORDER BY k", (start, end)
            ).fetchall()
        return [(bytes(k), bytes(v)) for k, v in rows]

    def flush(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    @property
    def path(self) -> str:
        return self._path


def next_key(key: bytes) -> bytes:
    """Smallest key that sorts strictly after ``key``.

    Keys are plain byte strings, so the successor of ``b"users:1"`` is
    ``b"users:2"``: increment the last byte that is not ``0xFF`` and drop the
    trailing ``0xFF`` bytes.  A key made only of ``0xFF`` has no successor and
    ``b"\xff"`` (the conventional end of the keyspace) is returned instead.

    This replaces the ``key + b"\x00"`` idiom: ``0x00`` is the field separator
    inside the MVCC keyspace and therefore not a legal user key byte.
    """
    for index in range(len(key) - 1, -1, -1):
        if key[index] != 0xFF:
            return key[:index] + bytes([key[index] + 1])
    return b"\xff"


def create_engine(data_dir: Optional[str] = None, name: str = "oxidedb", **kwargs) -> Engine:
    """Build the engine appropriate for ``data_dir``.

    ``None`` yields the volatile :class:`MemoryEngine`; a directory yields a
    durable :class:`SQLiteEngine` at ``<data_dir>/<name>.sqlite3``.
    """
    if data_dir is None:
        return MemoryEngine()
    return SQLiteEngine(os.path.join(data_dir, f"{name}.sqlite3"), **kwargs)
