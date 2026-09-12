"""Durable state for a Raft node: ``currentTerm``/``votedFor``, the log and
``commitIndex``.

Two things matter for correctness here, and both were missing before:

1. ``commitIndex`` must be persisted, otherwise a restarted node cannot tell
   committed entries from merely replicated ones and would hand uncommitted
   entries to the state machine (violating the Raft state-machine safety rule).
2. The log must support *truncation*.  When a follower discovers a conflicting
   suffix it deletes those entries; if that deletion is not written through,
   the entries reappear on the next restart and the node silently diverges.

It also stores the newest *snapshot*: the state of the state machine at some
index, which is what makes it safe to drop the log entries the snapshot already
contains (``compact_log``).  Without that the log grows for ever.

:class:`EngineRaftStorage` is the engine-backed implementation (see plan E) and
the one to use for new code.  :class:`JSONFileStorage` is kept because existing
tests and tools construct it, and it now honours the same contract; it rewrites
the whole file per append, so treat it as a development aid rather than a
durable store.
"""

import json
import os
import tempfile
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

import msgpack

from oxidedb.storage.engine import Engine, create_engine
from .node import LogEntry


class RaftStorage(ABC):
    @abstractmethod
    def save_meta(self, current_term: int, voted_for: Optional[int]) -> None:
        pass

    @abstractmethod
    def load_meta(self) -> tuple:
        pass

    @abstractmethod
    def append_log_entry(self, entry: LogEntry) -> None:
        pass

    @abstractmethod
    def truncate_log(self, from_index: int) -> None:
        """Drop every entry with ``index >= from_index``."""
        pass

    @abstractmethod
    def compact_log(self, last_included_index: int) -> None:
        """Drop every entry with ``index <= last_included_index``.

        Called once a snapshot covering those entries is stored: replaying them
        afterwards would be redundant, and keeping them would defeat the point.
        """
        pass

    @abstractmethod
    def load_log(self) -> List[LogEntry]:
        pass

    @abstractmethod
    def save_snapshot(self, last_included_index: int, last_included_term: int, data: bytes) -> None:
        pass

    @abstractmethod
    def load_snapshot(self) -> Optional[Tuple[int, int, bytes]]:
        """``(last_included_index, last_included_term, data)``, or ``None``."""
        pass

    @abstractmethod
    def save_commit_index(self, commit_index: int) -> None:
        pass

    @abstractmethod
    def load_commit_index(self) -> int:
        pass

    @abstractmethod
    def clear(self) -> None:
        pass


class JSONFileStorage(RaftStorage):
    """Legacy JSON-file storage, kept for backwards compatibility."""

    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._meta_path = os.path.join(data_dir, "raft_meta.json")
        self._log_path = os.path.join(data_dir, "raft_log.json")
        self._snapshot_path = os.path.join(data_dir, "raft_snapshot.bin")

    def _atomic_write(self, path: str, data: dict) -> None:
        fd, temp_path = tempfile.mkstemp(dir=self._data_dir)
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(data, f)
            os.replace(temp_path, path)
        except Exception:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise

    def _read_log(self) -> list:
        if not os.path.exists(self._log_path):
            return []
        with open(self._log_path, 'r') as f:
            return json.load(f).get("entries", [])

    def _write_log(self, entries: list) -> None:
        self._atomic_write(self._log_path, {"entries": entries})

    def save_meta(self, current_term: int, voted_for: Optional[int]) -> None:
        data = {
            "current_term": current_term,
            "voted_for": voted_for,
        }
        if os.path.exists(self._meta_path):
            with open(self._meta_path, 'r') as f:
                data["commit_index"] = json.load(f).get("commit_index", 0)
        self._atomic_write(self._meta_path, data)

    def load_meta(self) -> tuple:
        if not os.path.exists(self._meta_path):
            return 0, None
        with open(self._meta_path, 'r') as f:
            data = json.load(f)
        return data.get("current_term", 0), data.get("voted_for", None)

    def append_log_entry(self, entry: LogEntry) -> None:
        entries = [e for e in self._read_log() if e["index"] < entry.index]
        entries.append({
            "term": entry.term,
            "index": entry.index,
            "command": entry.command.decode('latin-1'),
        })
        self._write_log(entries)

    def truncate_log(self, from_index: int) -> None:
        self._write_log([e for e in self._read_log() if e["index"] < from_index])

    def compact_log(self, last_included_index: int) -> None:
        self._write_log([e for e in self._read_log() if e["index"] > last_included_index])

    def load_log(self) -> List[LogEntry]:
        entries = []
        for item in self._read_log():
            entries.append(LogEntry(
                term=item["term"],
                index=item["index"],
                command=item["command"].encode('latin-1'),
            ))
        entries.sort(key=lambda e: e.index)
        return entries

    def save_snapshot(self, last_included_index: int, last_included_term: int, data: bytes) -> None:
        with open(self._snapshot_path, 'wb') as f:
            f.write(data)
        meta = {}
        if os.path.exists(self._meta_path):
            with open(self._meta_path, 'r') as f:
                meta = json.load(f)
        meta["snapshot_index"] = last_included_index
        meta["snapshot_term"] = last_included_term
        self._atomic_write(self._meta_path, meta)

    def load_snapshot(self) -> Optional[Tuple[int, int, bytes]]:
        if not os.path.exists(self._snapshot_path) or not os.path.exists(self._meta_path):
            return None
        with open(self._meta_path, 'r') as f:
            meta = json.load(f)
        index = meta.get("snapshot_index")
        if index is None:
            return None
        with open(self._snapshot_path, 'rb') as f:
            return index, meta.get("snapshot_term", 0), f.read()

    def save_commit_index(self, commit_index: int) -> None:
        data = {"current_term": 0, "voted_for": None, "commit_index": commit_index}
        if os.path.exists(self._meta_path):
            with open(self._meta_path, 'r') as f:
                data.update(json.load(f))
            data["commit_index"] = commit_index
        self._atomic_write(self._meta_path, data)

    def load_commit_index(self) -> int:
        if not os.path.exists(self._meta_path):
            return 0
        with open(self._meta_path, 'r') as f:
            return json.load(f).get("commit_index", 0)

    def clear(self) -> None:
        if os.path.exists(self._meta_path):
            os.remove(self._meta_path)
        if os.path.exists(self._log_path):
            os.remove(self._log_path)
        if os.path.exists(self._snapshot_path):
            os.remove(self._snapshot_path)


class EngineRaftStorage(RaftStorage):
    """Raft state on top of a byte-ordered :class:`~oxidedb.storage.engine.Engine`.

    Keyspace (single-byte namespaces so that range scans stay cheap):

    * ``\\x01 || index(8B big-endian)`` -> msgpack ``{term, command}``
    * ``\\x02``                         -> msgpack ``{current_term, voted_for}``
    * ``\\x03``                         -> ``commit_index`` as 8 bytes big-endian
    * ``\\x04``                         -> msgpack ``{index, term, data}`` snapshot
    """

    _LOG = b"\x01"
    _META = b"\x02"
    _COMMIT = b"\x03"
    _SNAPSHOT = b"\x04"

    def __init__(self, engine: Optional[Engine] = None, data_dir: Optional[str] = None):
        if engine is None:
            engine = create_engine(data_dir, name="raft")
        self._engine = engine

    @staticmethod
    def _log_key(index: int) -> bytes:
        return EngineRaftStorage._LOG + index.to_bytes(8, 'big')

    def save_meta(self, current_term: int, voted_for: Optional[int]) -> None:
        self._engine.put(self._META, msgpack.packb(
            {"current_term": current_term, "voted_for": voted_for}, use_bin_type=True
        ))

    def load_meta(self) -> tuple:
        raw = self._engine.get(self._META)
        if raw is None:
            return 0, None
        data = msgpack.unpackb(raw, raw=False)
        return data.get("current_term", 0), data.get("voted_for", None)

    def append_log_entry(self, entry: LogEntry) -> None:
        self._engine.put(self._log_key(entry.index), msgpack.packb(
            {"term": entry.term, "command": entry.command}, use_bin_type=True
        ))

    def truncate_log(self, from_index: int) -> None:
        # delete_range is what makes truncation durable in one shot: with the old
        # append-only file the deleted suffix came back after a restart.
        self._engine.delete_range(self._log_key(from_index), b"\x02")

    def compact_log(self, last_included_index: int) -> None:
        # The whole compacted prefix goes in one range delete; the log's own
        # lower bound is index 0, so _log_key(0) is the start of the keyspace.
        self._engine.delete_range(self._log_key(0), self._log_key(last_included_index + 1))

    def load_log(self) -> List[LogEntry]:
        entries = []
        for key, value in self._engine.scan(self._LOG, b"\x02"):
            data = msgpack.unpackb(value, raw=False)
            entries.append(LogEntry(
                term=data["term"],
                index=int.from_bytes(key[1:9], 'big'),
                command=data["command"],
            ))
        entries.sort(key=lambda e: e.index)
        return entries

    def save_commit_index(self, commit_index: int) -> None:
        self._engine.put(self._COMMIT, commit_index.to_bytes(8, 'big'))

    def load_commit_index(self) -> int:
        raw = self._engine.get(self._COMMIT)
        return 0 if raw is None else int.from_bytes(raw, 'big')

    def save_snapshot(self, last_included_index: int, last_included_term: int, data: bytes) -> None:
        # One record, so a crash cannot leave a snapshot payload behind without
        # its index (which would make the log truncation below wrong).
        self._engine.put(self._SNAPSHOT, msgpack.packb(
            {"index": last_included_index, "term": last_included_term, "data": data},
            use_bin_type=True,
        ))

    def load_snapshot(self) -> Optional[Tuple[int, int, bytes]]:
        raw = self._engine.get(self._SNAPSHOT)
        if raw is None:
            return None
        record = msgpack.unpackb(raw, raw=False)
        return record["index"], record["term"], record["data"]

    def clear(self) -> None:
        self._engine.delete_range(b"\x01", b"\x05")

    def close(self) -> None:
        self._engine.close()


def create_raft_storage(data_dir: Optional[str] = None, engine: Optional[Engine] = None) -> RaftStorage:
    """Engine-backed storage; ``data_dir=None`` gives an in-memory one."""
    return EngineRaftStorage(engine=engine, data_dir=data_dir)
