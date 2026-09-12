"""MVCC storage encoded onto a byte-ordered :class:`~oxidedb.storage.engine.Engine`.

The version chain lives in the keyspace rather than in a Python ``dict`` of
lists, which is what makes the data durable and bounded-memory instead of
growing until the process dies:

* ``\\x01 || key || \\x00 || ts(8B big-endian)``        -> ``flag(1B) || value``
* ``\\x03 || key || \\x00 || commit_ts(8B big-endian)`` -> ``start_ts(8B)``
* ``\\x04 || key || \\x00 || start_ts(8B big-endian)``  -> ``msgpack lock record``

``\\x02`` used to hold a bare write intent.  A lock record (``\\x04``) carries the
same value plus the primary key and the TTL, so the intent namespace was retired
and ``set_write_intent`` is now a thin wrapper over ``put_lock``.

Because the engine orders keys with plain byte comparison, appending the
timestamp as a big-endian suffix turns "all versions of a key, oldest first"
into a contiguous forward range scan - no sorting, no second index.  A read at
timestamp ``t`` is then just that range truncated at ``t``, taking the last
entry.

``\\x00`` is the field separator inside the version keyspace, so user keys must
not contain it (the same constraint TiKV-style encoders place on keys).  It is
validated on the way in rather than silently producing a corrupt index.

The default engine is :class:`~oxidedb.storage.engine.MemoryEngine`, which keeps
the original in-process behaviour.  Pass a ``SQLiteEngine`` (or any other
engine) to get durability.
"""

import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import msgpack

from .engine import Engine, MemoryEngine


_VERSION = b"\x01"
_WRITE = b"\x03"
_LOCK = b"\x04"
_SEP = b"\x00"

_MAX_TS = (1 << 64) - 1


def _ts_bytes(timestamp: int) -> bytes:
    if not 0 <= timestamp <= _MAX_TS:
        raise ValueError(f"timestamp out of 64-bit range: {timestamp}")
    return timestamp.to_bytes(8, 'big')


def _check_user_key(key: bytes) -> bytes:
    if not isinstance(key, (bytes, bytearray, memoryview)):
        raise TypeError(f"key must be bytes, got {type(key).__name__}")
    key = bytes(key)
    if _SEP in key:
        raise ValueError("keys must not contain the 0x00 separator byte")
    return key


def _version_prefix(key: bytes) -> bytes:
    return _VERSION + key + _SEP


def _key_upper_bound(namespace: bytes, key: bytes) -> bytes:
    """Exclusive upper bound covering every entry of exactly ``key``.

    Everything for ``key`` starts with ``namespace || key || \\x00``, so
    ``namespace || key || \\x01`` is the tightest possible bound: it sorts after
    all of them and before any other key that merely shares a prefix.
    """
    return namespace + key + b"\x01"


class MVCCRecord:
    def __init__(self, value: bytes, timestamp: int, deleted: bool = False):
        self.value = value
        self.timestamp = timestamp
        self.deleted = deleted

    def __repr__(self) -> str:
        return f"MVCCRecord(timestamp={self.timestamp}, deleted={self.deleted})"


class WriteRecord:
    def __init__(self, start_ts: int, commit_ts: int):
        self.start_ts = start_ts
        self.commit_ts = commit_ts


class LockStatus:
    LOCKED = b"LOCKED"
    COMMITTED = b"COMMITTED"
    ABORTED = b"ABORTED"


class MVCCStorage:
    def __init__(self, engine: Optional[Engine] = None):
        self._engine = engine if engine is not None else MemoryEngine()
        # Serialises read-modify-write sequences that span several engine calls
        # (the engine itself only guarantees single-operation atomicity).
        self._lock = threading.RLock()

    @property
    def engine(self) -> Engine:
        return self._engine

    @staticmethod
    def _decode_version(raw: Optional[bytes], timestamp: int = 0) -> Optional[MVCCRecord]:
        if raw is None or raw == b"":
            return None
        flag, payload = raw[0], raw[1:]
        return MVCCRecord(payload, timestamp=timestamp, deleted=(flag == 0))

    def get(self, key: bytes, timestamp: int) -> Optional[bytes]:
        with self._lock:
            record = self._get_version_at(key, timestamp)
            if record is None or record.deleted:
                return None
            return record.value

    def _get_version_at(self, key: bytes, timestamp: int) -> Optional[MVCCRecord]:
        key = _check_user_key(key)
        prefix = _version_prefix(key)
        end = prefix + _ts_bytes(timestamp + 1) if timestamp < _MAX_TS else _key_upper_bound(_VERSION, key)

        rows = self._engine.scan(prefix, end)
        if not rows:
            return None

        # Rows are ordered by timestamp, so the last one is the newest version
        # visible at `timestamp`.
        last_key, last_value = rows[-1]
        version_ts = int.from_bytes(last_key[len(prefix):], 'big')
        return self._decode_version(last_value, version_ts)

    def set(self, key: bytes, value: bytes, timestamp: int) -> None:
        key = _check_user_key(key)
        with self._lock:
            self._engine.put(_version_prefix(key) + _ts_bytes(timestamp), b"\x01" + bytes(value))

    def delete(self, key: bytes, timestamp: int) -> None:
        key = _check_user_key(key)
        with self._lock:
            # A tombstone is a real version: it must shadow older versions for
            # every read at or after `timestamp`.
            self._engine.put(_version_prefix(key) + _ts_bytes(timestamp), b"\x00")

    def scan(self, start_key: bytes, end_key: bytes, timestamp: int) -> List[Tuple[bytes, bytes]]:
        start_key = _check_user_key(start_key)
        end_key = _check_user_key(end_key)
        with self._lock:
            rows = self._engine.scan(_VERSION + start_key, _VERSION + end_key)

            result: List[Tuple[bytes, bytes]] = []
            current_key: Optional[bytes] = None
            current_value: Optional[bytes] = None

            for row_key, raw in rows:
                user_key, version_ts = self._split_version_key(row_key)
                if user_key != current_key:
                    if current_key is not None and current_value is not None:
                        result.append((current_key, current_value))
                    current_key, current_value = user_key, None

                if version_ts <= timestamp:
                    record = self._decode_version(raw)
                    # Newest visible version wins; a tombstone hides the key.
                    current_value = None if record is None or record.deleted else record.value

            if current_key is not None and current_value is not None:
                result.append((current_key, current_value))

            return result

    @staticmethod
    def _split_version_key(row_key: bytes) -> Tuple[bytes, int]:
        body = row_key[1:]
        # The timestamp is always the trailing 8 bytes, so the separator sits at
        # a fixed offset.  Searching with rindex would instead find a 0x00 byte
        # *inside* the timestamp whenever the value is small.
        separator = len(body) - 9
        if separator < 0:
            raise ValueError(f"malformed MVCC version key: {row_key!r}")
        return body[:separator], int.from_bytes(body[separator + 1:], 'big')

    def get_latest_version(self, key: bytes) -> Optional[MVCCRecord]:
        key = _check_user_key(key)
        with self._lock:
            rows = self._engine.scan(_version_prefix(key), _key_upper_bound(_VERSION, key))
            if not rows:
                return None
            row_key, raw = rows[-1]
            _, version_ts = self._split_version_key(row_key)
            return self._decode_version(raw, version_ts)

    def get_latest_write(self, key: bytes) -> Optional[Dict[str, int]]:
        key = _check_user_key(key)
        with self._lock:
            rows = self._engine.scan(_WRITE + key + _SEP, _key_upper_bound(_WRITE, key))
            if not rows:
                return None
            row_key, raw = rows[-1]
            commit_ts = int.from_bytes(row_key[len(_WRITE) + len(key) + 1:], 'big')
            return {"commit_ts": commit_ts, "start_ts": int.from_bytes(raw, 'big')}

    # -- lock records ------------------------------------------------------
    #
    # A lock is the durable form of a write intent: it carries the value that a
    # commit will publish, the primary key that decides the transaction's fate,
    # and the moment the lock was taken so the TTL logic survives a restart.
    # Keeping locks in the engine rather than in a dict on the state machine is
    # what lets a restarted node still commit or clean up a transaction it had
    # already prewritten.

    def put_lock(self, key: bytes, start_ts: int, status: bytes, primary_key: bytes,
                 lock_time: float, value: bytes) -> None:
        key = _check_user_key(key)
        payload = msgpack.packb(
            {"s": status, "p": bytes(primary_key), "t": lock_time, "v": bytes(value)},
            use_bin_type=True,
        )
        with self._lock:
            self._engine.put(_LOCK + key + _SEP + _ts_bytes(start_ts), payload)

    @staticmethod
    def _decode_lock(raw: Optional[bytes], key: bytes, start_ts: int) -> Optional[Dict[str, Any]]:
        if raw is None:
            return None
        data = msgpack.unpackb(raw, raw=False)
        return {
            "status": data["s"],
            "primary_key": data["p"],
            "lock_time": data["t"],
            "value": data["v"],
            "key": key,
            "start_ts": start_ts,
        }

    def get_lock(self, key: bytes, start_ts: int) -> Optional[Dict[str, Any]]:
        key = _check_user_key(key)
        with self._lock:
            raw = self._engine.get(_LOCK + key + _SEP + _ts_bytes(start_ts))
        return self._decode_lock(raw, key, start_ts)

    def get_newest_lock(self, key: bytes) -> Optional[Dict[str, Any]]:
        """The lock with the highest ``start_ts`` for ``key``, if there is one."""
        key = _check_user_key(key)
        with self._lock:
            rows = self._engine.scan(_LOCK + key + _SEP, _key_upper_bound(_LOCK, key))
        if not rows:
            return None
        row_key, raw = rows[-1]
        _, start_ts = self._split_version_key(row_key)
        return self._decode_lock(raw, key, start_ts)

    def remove_lock(self, key: bytes, start_ts: int) -> None:
        key = _check_user_key(key)
        with self._lock:
            self._engine.delete(_LOCK + key + _SEP + _ts_bytes(start_ts))

    def iter_locks(self) -> List[Tuple[bytes, Dict[str, Any]]]:
        """Every lock currently held, ordered by key and then ``start_ts``."""
        with self._lock:
            rows = self._engine.scan(_LOCK, b"\x05")
        locks = []
        for row_key, raw in rows:
            key, start_ts = self._split_version_key(row_key)
            record = self._decode_lock(raw, key, start_ts)
            if record is not None:
                locks.append((key, record))
        return locks

    # -- raw write-intent access ------------------------------------------
    #
    # These three predate lock records and are kept so existing callers keep
    # working; they expose only the lock's value field.

    def set_write_intent(self, key: bytes, value: bytes, start_ts: int) -> None:
        self.put_lock(key, start_ts, LockStatus.LOCKED, primary_key=key,
                      lock_time=time.time(), value=value)

    def get_write_intent(self, key: bytes, start_ts: int) -> Optional[bytes]:
        lock = self.get_lock(key, start_ts)
        return None if lock is None else lock["value"]

    def remove_write_intent(self, key: bytes, start_ts: int) -> None:
        self.remove_lock(key, start_ts)

    def write_write_record(self, key: bytes, start_ts: int, commit_ts: int) -> None:
        key = _check_user_key(key)
        with self._lock:
            self._engine.put(_WRITE + key + _SEP + _ts_bytes(commit_ts), _ts_bytes(start_ts))

    def close(self) -> None:
        self._engine.close()

    def clear(self) -> None:
        with self._lock:
            self._engine.delete_range(b"\x01", b"\x05")
