import time
from abc import ABC, abstractmethod
from typing import Optional, List, Tuple, Dict, Any
from oxidedb.storage.mvcc import LockStatus, MVCCStorage
import msgpack


class CommandType:
    SET = b"SET"
    DELETE = b"DELETE"
    PREWRITE = b"PREWRITE"
    COMMIT = b"COMMIT"
    ROLLBACK = b"ROLLBACK"
    CLEAN_LOCK = b"CLEAN_LOCK"


class ErrorCode:
    SUCCESS = 0
    ERR_UNKNOWN = 1
    ERR_APPLY_ERROR = 2
    ERR_LOCKED = 101
    ERR_WRITE_CONFLICT = 102
    ERR_NO_LOCK = 201
    ERR_TIMESTAMP_MISMATCH = 202
    ERR_LOCK_STATUS_INVALID = 203
    ERR_NOT_LEADER = 301
    ERR_LEADERSHIP_LOST = 302
    ERR_TIMEOUT = 303
    ERR_ENTRY_OVERWRITTEN = 304
    ERR_SPLIT_IN_PROGRESS = 305
    #: The same refusal for the other operation that reads a shard's rows at one moment
    #: and moves them somewhere else.  A caller that hears "split" waits for this shard to
    #: come back - it will still answer for the half below the split point - and one that
    #: hears "migrating" has to look the range up again, because this group will not answer
    #: for it at all.
    ERR_MIGRATING = 306


#: The commands that add rows to a shard, as opposed to finishing work that is
#: already in flight over it.  A shard whose rows are being copied into a new
#: shard refuses the first kind and still accepts the second: a commit or a
#: rollback is how a transaction that prewrote before the copy finishes, and
#: dropping one of those would drop a write that had already been promised.
DATA_COMMANDS = frozenset({CommandType.SET, CommandType.DELETE, CommandType.PREWRITE})


def is_not_leader(error_code: Optional[int]) -> bool:
    """Whether a refusal is the one a caller answers by asking the leader.

    ``ERR_NOT_LEADER`` and ``ERR_LEADERSHIP_LOST`` are one answer from the outside: the node
    either was not leading, or stopped leading while the call was in flight, and either way
    the caller's next move is to ask whoever leads now.  Every service that classifies a
    refusal asks this - the shard's client service and the two group services - so there is
    one place that decides it rather than one per service.
    """
    return error_code in (ErrorCode.ERR_NOT_LEADER, ErrorCode.ERR_LEADERSHIP_LOST)


def writes_new_data(command: bytes) -> bool:
    """Whether ``command``, proposed to a shard, adds rows to it.

    A command that cannot be read at all is treated as one that does: the only
    thing a caller can do with an unreadable proposal is refuse it, and a shard
    in the middle of copying its rows out is the one place where guessing the
    other way loses a row.
    """
    try:
        kind = msgpack.unpackb(command).get("type")
    except Exception:
        return True
    return kind in DATA_COMMANDS


def serialize_command(cmd_type: bytes, **kwargs) -> bytes:
    """The bytes of a command: what ``apply`` parses and what a client builds.

    A function rather than a state machine method, because no state machine appears
    in it.  The client is the side that builds a command - the coordinator is the
    one that knows a transaction is being committed - and those bytes have to be the
    same on both sides of the seam between a client and a node, so there is exactly
    one implementation and the machines call it too.
    """
    return msgpack.packb({"type": cmd_type, **kwargs})


class ApplyResult:
    def __init__(self, success: bool, error_code: Optional[int] = None, error_msg: Optional[str] = None,
                 data: Optional[bytes] = None, index: Optional[int] = None,
                 leader_address: Optional[str] = None):
        self.success = success
        self.error_code = error_code
        self.error_msg = error_msg
        self.data = data
        #: Where the command landed in the log, when it landed somewhere.  A reader that
        #: has to see a write has to have applied this index, and only the node knows
        #: it: the machine knows what it applied and not where it was written down.
        self.index = index
        #: Where the leader is, when the node answering is not it and knows who is.  A
        #: refusal that names nowhere leaves the caller to read the routing table again;
        #: one that names a shard's leader lets it ask over there instead.  It is empty
        #: for every node in this process - a caller holding the cluster can see for
        #: itself - and it is what the wire version of a refusal carries.
        self.leader_address = leader_address
    
    @staticmethod
    def success(data: Optional[bytes] = None, index: Optional[int] = None):
        return ApplyResult(True, ErrorCode.SUCCESS, data=data, index=index)
    
    @staticmethod
    def failure(error_code: int, error_msg: str, leader_address: Optional[str] = None):
        return ApplyResult(False, error_code, error_msg, leader_address=leader_address)


class ReadResult:
    def __init__(self, success: bool, value: Optional[bytes] = None, error_code: Optional[int] = None,
                 error_msg: Optional[str] = None, leader_address: Optional[str] = None):
        self.success = success
        self.value = value
        self.error_code = error_code
        self.error_msg = error_msg
        #: Where the leader is, when the node answering this read is not it and knows
        #: who is.  See the same field on :class:`ApplyResult`.
        self.leader_address = leader_address
    
    @staticmethod
    def success(value: Optional[bytes]):
        return ReadResult(True, value)
    
    @staticmethod
    def failure(error_code: int, error_msg: str, leader_address: Optional[str] = None):
        return ReadResult(False, error_code=error_code, error_msg=error_msg,
                          leader_address=leader_address)
    
    @staticmethod
    def locked():
        return ReadResult(False, error_code=ErrorCode.ERR_LOCKED, error_msg="Key is locked by another transaction")


class ScanRefused(RuntimeError):
    """A range read that will not guess.

    ``scan`` answers with rows, so there is no error code to put a refusal in, and
    an empty list is a legitimate answer: a refusal that came back as "no keys
    matched" would be indistinguishable from a right answer, and a key left out
    because it was locked looks exactly like a key that is not there.  So a range
    read that cannot answer raises this instead, carrying the ``ErrorCode`` a caller
    would need to react to (``ERR_LOCKED`` with the ``.key`` that is in the way, or
    ``ERR_NOT_LEADER``), and an empty list from ``scan`` means the range is empty.
    """

    def __init__(self, error_code: int, error_msg: str, key: Optional[bytes] = None,
                 leader_address: Optional[str] = None):
        super().__init__(error_msg)
        self.error_code = error_code
        self.error_msg = error_msg
        self.key = key
        #: Where the leader is, when a node that is not it refused this range read and
        #: knows who is.  See the same field on :class:`ReadResult`.
        self.leader_address = leader_address


class StateMachine(ABC):
    @abstractmethod
    def apply(self, command: bytes) -> ApplyResult:
        pass

    @abstractmethod
    def get(self, key: bytes, timestamp: Optional[int] = None) -> ReadResult:
        pass

    @abstractmethod
    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None) -> List[Tuple[bytes, bytes]]:
        """Every key in ``[start_key, end_key)``, at ``timestamp``.

        The same timestamp ``get`` takes: None means the newest version, and a
        transaction passes its own ``start_ts``.  A range read that cannot answer
        without guessing raises :class:`ScanRefused` rather than leaving a key out.
        """
        pass
    
    @abstractmethod
    def serialize_command(self, cmd_type: bytes, **kwargs) -> bytes:
        pass

    def snapshot(self) -> bytes:
        """Serialise the whole applied state, for log compaction.

        Raising :class:`NotImplementedError` (the default) marks this state
        machine as unsupported; a node then never compacts its log rather than
        silently discarding state it cannot rebuild.
        """
        raise NotImplementedError

    def restore(self, data: bytes) -> None:
        """Replace the whole state with whatever :meth:`snapshot` produced."""
        raise NotImplementedError


class MVCCStateMachine(StateMachine):
    def __init__(self, storage=None):
        """``storage`` may be an :class:`Engine`, or an already built
        :class:`MVCCStorage`.  Omitted, the state machine keeps its original
        in-memory behaviour."""
        if isinstance(storage, MVCCStorage):
            self._storage = storage
        else:
            self._storage = MVCCStorage(engine=storage)
        self._last_applied_timestamp = 0
    
    def apply(self, command: bytes) -> ApplyResult:
        try:
            cmd = msgpack.unpackb(command)
            cmd_type = cmd.get("type")
            
            if cmd_type == CommandType.SET:
                return self._apply_set(cmd)
            
            elif cmd_type == CommandType.DELETE:
                return self._apply_delete(cmd)
            
            elif cmd_type == CommandType.PREWRITE:
                return self._apply_prewrite(cmd)
            
            elif cmd_type == CommandType.COMMIT:
                return self._apply_commit(cmd)
            
            elif cmd_type == CommandType.ROLLBACK:
                return self._apply_rollback(cmd)
            
            elif cmd_type == CommandType.CLEAN_LOCK:
                return self._apply_clean_lock(cmd)
            
            else:
                return ApplyResult.failure(1, f"Unknown command type: {cmd_type}")
        
        except Exception as e:
            return ApplyResult.failure(2, f"Apply error: {str(e)}")
    
    def _apply_set(self, cmd: Dict[str, Any]) -> ApplyResult:
        key = cmd.get("key")
        value = cmd.get("value")
        timestamp = cmd.get("timestamp", self._last_applied_timestamp + 1)
        self._storage.set(key, value, timestamp)
        # A row moved here by a split is not a new write: it carries the write
        # record of the transaction that committed it, so a read-set validation on
        # this shard reads the same history the shard it came from reads.
        start_ts = cmd.get("start_ts")
        if start_ts is not None:
            self._storage.write_write_record(key, start_ts, timestamp)
        if timestamp > self._last_applied_timestamp:
            self._last_applied_timestamp = timestamp
        return ApplyResult.success()
    
    def _apply_delete(self, cmd: Dict[str, Any]) -> ApplyResult:
        key = cmd.get("key")
        timestamp = cmd.get("timestamp", self._last_applied_timestamp + 1)
        self._storage.delete(key, timestamp)
        if timestamp > self._last_applied_timestamp:
            self._last_applied_timestamp = timestamp
        return ApplyResult.success()
    
    def _apply_prewrite(self, cmd: Dict[str, Any]) -> ApplyResult:
        key = cmd.get("key")
        value = cmd.get("value")
        start_ts = cmd.get("start_ts")
        primary_key = cmd.get("primary_key")
        
        lock = self._storage.get_newest_lock(key)
        if lock is not None and lock["status"] == LockStatus.LOCKED:
            return ApplyResult.failure(101, f"Key {key} is locked by transaction {lock['start_ts']}")
        
        existing_write = self._storage.get_latest_write(key)
        if existing_write and existing_write["commit_ts"] > start_ts:
            return ApplyResult.failure(102, f"Key {key} has newer write with commit_ts {existing_write['commit_ts']}")
        
        existing_version = self._storage.get_latest_version(key)
        if existing_version and existing_version.timestamp > start_ts:
            return ApplyResult.failure(102, f"Key {key} has newer version with timestamp {existing_version.timestamp}")
        
        # The lock *is* the write intent.  Keeping it in the engine instead of a
        # dict is what lets a restarted replica still commit (or clean up) a
        # transaction it had prewritten before the crash.
        self._storage.put_lock(key, start_ts, LockStatus.LOCKED, primary_key, time.time(), value)
        
        return ApplyResult.success()
    
    def _apply_commit(self, cmd: Dict[str, Any]) -> ApplyResult:
        key = cmd.get("key")
        start_ts = cmd.get("start_ts")
        commit_ts = cmd.get("commit_ts")
        
        lock = self._storage.get_lock(key, start_ts)
        if lock is None:
            newest = self._storage.get_newest_lock(key)
            if newest is None:
                return ApplyResult.failure(201, f"No lock found for key {key}")
            return ApplyResult.failure(
                ErrorCode.ERR_TIMESTAMP_MISMATCH,
                f"Start timestamp mismatch: expected {start_ts}, got {newest['start_ts']}",
            )

        if lock["status"] != LockStatus.LOCKED:
            return ApplyResult.failure(203, f"Lock status is {lock['status']}, expected LOCKED")

        value = lock["value"]
        self._storage.set(key, value, commit_ts)
        self._storage.write_write_record(key, start_ts, commit_ts)

        self._storage.remove_lock(key, start_ts)
        
        if commit_ts > self._last_applied_timestamp:
            self._last_applied_timestamp = commit_ts
        
        return ApplyResult.success()
    
    def _apply_rollback(self, cmd: Dict[str, Any]) -> ApplyResult:
        key = cmd.get("key")
        start_ts = cmd.get("start_ts")
        
        lock = self._storage.get_lock(key, start_ts)
        if lock is None:
            newest = self._storage.get_newest_lock(key)
            if newest is None:
                return ApplyResult.success()
            return ApplyResult.failure(
                ErrorCode.ERR_TIMESTAMP_MISMATCH,
                f"Start timestamp mismatch: expected {start_ts}, got {newest['start_ts']}",
            )

        self._storage.remove_lock(key, start_ts)
        
        return ApplyResult.success()
    
    def _apply_clean_lock(self, cmd: Dict[str, Any]) -> ApplyResult:
        key = cmd.get("key")
        start_ts = cmd.get("start_ts")
        
        lock = self._storage.get_newest_lock(key)
        if lock is None:
            return ApplyResult.success()

        if start_ts is None or lock["start_ts"] == start_ts:
            self._storage.remove_lock(key, lock["start_ts"])

        return ApplyResult.success()
    
    def get(self, key: bytes, timestamp: Optional[int] = None) -> ReadResult:
        """Read ``key`` at ``timestamp``, or at the newest version when it is None.

        None is what a linearizable read wants: the freshest version this replica
        has applied.  A transaction passes its own ``start_ts`` instead, and then
        the read is a snapshot read - the version that was committed before the
        transaction started, the same one every time it is repeated.

        Which locks block a read depends on the timestamp, for the same reason.  A
        lock only hides the key from a reader that could have seen the version the
        lock holds, so a lock taken by a transaction that started *after* this
        snapshot is ignored: it cannot have committed into it, and blocking on it
        would let a concurrent writer defeat the snapshot.  A lock whose start_ts
        *is* this snapshot is the reader's own write intent, and the value it holds
        is the one that transaction wrote.

        A lock that does block is reported, not judged.  Whether the transaction that
        left it committed is not in the lock - it is in the primary key's write record
        - and releasing the lock goes through the Raft log, so deciding it here would
        be a guess and a divergence.  `LockResolver` asks the question, and applies
        the TTL.
        """
        lock = self._storage.get_newest_lock(key)
        if lock is not None:
            lock_ts = lock["start_ts"]

            if timestamp is not None and lock_ts == timestamp:
                return ReadResult.success(lock["value"])

            if timestamp is None or lock_ts < timestamp:
                return ReadResult.locked()

        read_timestamp = self._last_applied_timestamp if timestamp is None else timestamp
        value = self._storage.get(key, read_timestamp)
        return ReadResult.success(value)
    
    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None) -> List[Tuple[bytes, bytes]]:
        """Every key in the range, read the way :meth:`get` reads one key.

        None is the newest version; a transaction passes its own ``start_ts`` and
        gets the snapshot it started with.

        A lock in the range is judged per key by the same rule as in ``get``.  One
        older than the timestamp may hold a version this snapshot is owed, and the
        lock does not say whether it does, so the scan refuses to answer rather
        than leave the key out - an omitted key and a key that is not there are the
        same thing to a caller.  A lock at the timestamp is the reader's own write
        intent, so its value is the answer; a newer one cannot have committed into
        this snapshot and is ignored.
        """
        read_timestamp = self._last_applied_timestamp if timestamp is None else timestamp
        rows = dict(self._storage.scan(start_key, end_key, read_timestamp))

        for key, lock in self._storage.iter_locks():
            if not start_key <= key < end_key:
                continue
            if timestamp is None or lock["start_ts"] < timestamp:
                raise ScanRefused(
                    ErrorCode.ERR_LOCKED,
                    f"Key {key!r} is locked by transaction {lock['start_ts']}",
                    key=key,
                )
            if lock["start_ts"] == timestamp:
                rows[key] = lock["value"]

        return sorted(rows.items())
    
    def serialize_command(self, cmd_type: bytes, **kwargs) -> bytes:
        return serialize_command(cmd_type, **kwargs)
    
    def get_lock_status(self, key: bytes) -> Optional[Dict[str, Any]]:
        return self._storage.get_newest_lock(key)

    def get_write_record(self, key: bytes) -> Optional[Dict[str, Any]]:
        """The newest committed version's write record, or None when there is none.

        The other half of what the lock resolver asks: ``get_lock_status`` says what
        is holding a key, and this says what became of the transaction that left the
        lock - which transaction wrote the key and when it was published.  A lock
        whose transaction never committed has no write record here, and that is how a
        reader tells the two apart without the row itself being visible.
        """
        return self._storage.get_latest_write(key)

    # -- snapshots ---------------------------------------------------------

    def snapshot(self) -> bytes:
        # The read timestamp travels with the state: a restored machine that
        # still thought it had applied timestamp 0 would read nothing back.
        return msgpack.packb(
            {"ts": self._last_applied_timestamp, "storage": self._storage.dump()},
            use_bin_type=True,
        )

    def restore(self, data: bytes) -> None:
        if not data:
            self._storage.load(b"")
            self._last_applied_timestamp = 0
            return
        record = msgpack.unpackb(data, raw=False)
        self._storage.load(record["storage"])
        self._last_applied_timestamp = record["ts"]
