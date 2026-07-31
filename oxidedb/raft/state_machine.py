import time
from abc import ABC, abstractmethod
from typing import Optional, List, Tuple, Dict, Any
from oxidedb.storage.mvcc import MVCCStorage
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


class ApplyResult:
    def __init__(self, success: bool, error_code: Optional[int] = None, error_msg: Optional[str] = None, data: Optional[bytes] = None):
        self.success = success
        self.error_code = error_code
        self.error_msg = error_msg
        self.data = data
    
    @staticmethod
    def success(data: Optional[bytes] = None):
        return ApplyResult(True, ErrorCode.SUCCESS, data=data)
    
    @staticmethod
    def failure(error_code: int, error_msg: str):
        return ApplyResult(False, error_code, error_msg)


class LockStatus:
    LOCKED = b"LOCKED"
    COMMITTED = b"COMMITTED"
    ABORTED = b"ABORTED"


class ReadResult:
    def __init__(self, success: bool, value: Optional[bytes] = None, error_code: Optional[int] = None, error_msg: Optional[str] = None):
        self.success = success
        self.value = value
        self.error_code = error_code
        self.error_msg = error_msg
    
    @staticmethod
    def success(value: Optional[bytes]):
        return ReadResult(True, value)
    
    @staticmethod
    def failure(error_code: int, error_msg: str):
        return ReadResult(False, error_code=error_code, error_msg=error_msg)
    
    @staticmethod
    def locked():
        return ReadResult(False, error_code=ErrorCode.ERR_LOCKED, error_msg="Key is locked by another transaction")


class StateMachine(ABC):
    @abstractmethod
    def apply(self, command: bytes) -> ApplyResult:
        pass

    @abstractmethod
    def get(self, key: bytes) -> ReadResult:
        pass

    @abstractmethod
    def scan(self, start_key: bytes, end_key: bytes) -> List[Tuple[bytes, bytes]]:
        pass
    
    @abstractmethod
    def serialize_command(self, cmd_type: bytes, **kwargs) -> bytes:
        pass


class MVCCStateMachine(StateMachine):
    def __init__(self):
        self._storage = MVCCStorage()
        self._last_applied_timestamp = 0
        self._locks: Dict[bytes, Dict[str, Any]] = {}
    
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
        
        if key in self._locks:
            lock = self._locks[key]
            if lock["status"] == LockStatus.LOCKED:
                return ApplyResult.failure(101, f"Key {key} is locked by transaction {lock['start_ts']}")
        
        existing_write = self._storage.get_latest_write(key)
        if existing_write and existing_write["commit_ts"] > start_ts:
            return ApplyResult.failure(102, f"Key {key} has newer write with commit_ts {existing_write['commit_ts']}")
        
        existing_version = self._storage.get_latest_version(key)
        if existing_version and existing_version.timestamp > start_ts:
            return ApplyResult.failure(102, f"Key {key} has newer version with timestamp {existing_version.timestamp}")
        
        self._locks[key] = {
            "primary_key": primary_key,
            "start_ts": start_ts,
            "status": LockStatus.LOCKED,
            "value": value,
            "lock_time": time.time(),
        }
        
        self._storage.set_write_intent(key, value, start_ts)
        
        return ApplyResult.success()
    
    def _apply_commit(self, cmd: Dict[str, Any]) -> ApplyResult:
        key = cmd.get("key")
        start_ts = cmd.get("start_ts")
        commit_ts = cmd.get("commit_ts")
        
        if key not in self._locks:
            return ApplyResult.failure(201, f"No lock found for key {key}")
        
        lock = self._locks[key]
        if lock["start_ts"] != start_ts:
            return ApplyResult.failure(202, f"Start timestamp mismatch: expected {start_ts}, got {lock['start_ts']}")
        
        if lock["status"] != LockStatus.LOCKED:
            return ApplyResult.failure(203, f"Lock status is {lock['status']}, expected LOCKED")
        
        value = lock["value"]
        self._storage.set(key, value, commit_ts)
        self._storage.write_write_record(key, start_ts, commit_ts)
        
        del self._locks[key]
        
        if commit_ts > self._last_applied_timestamp:
            self._last_applied_timestamp = commit_ts
        
        return ApplyResult.success()
    
    def _apply_rollback(self, cmd: Dict[str, Any]) -> ApplyResult:
        key = cmd.get("key")
        start_ts = cmd.get("start_ts")
        
        if key not in self._locks:
            return ApplyResult.success()
        
        lock = self._locks[key]
        if lock["start_ts"] != start_ts:
            return ApplyResult.failure(301, f"Start timestamp mismatch: expected {start_ts}, got {lock['start_ts']}")
        
        lock["status"] = LockStatus.ABORTED
        self._storage.remove_write_intent(key, start_ts)
        
        del self._locks[key]
        
        return ApplyResult.success()
    
    def _apply_clean_lock(self, cmd: Dict[str, Any]) -> ApplyResult:
        key = cmd.get("key")
        start_ts = cmd.get("start_ts")
        
        if key not in self._locks:
            return ApplyResult.success()
        
        lock = self._locks[key]
        if start_ts is None or lock["start_ts"] == start_ts:
            self._storage.remove_write_intent(key, lock["start_ts"])
            del self._locks[key]
        
        return ApplyResult.success()
    
    def get(self, key: bytes) -> ReadResult:
        lock = self._locks.get(key)
        if lock is not None:
            lock_time = lock.get("lock_time", time.time())
            if time.time() - lock_time < 5:
                return ReadResult.locked()
            
            self._try_clean_expired_lock(key, lock)
        
        value = self._storage.get(key, self._last_applied_timestamp)
        return ReadResult.success(value)
    
    def _try_clean_expired_lock(self, key: bytes, lock: Dict[str, Any]):
        pass
    
    def scan(self, start_key: bytes, end_key: bytes) -> List[Tuple[bytes, bytes]]:
        return self._storage.scan(start_key, end_key, self._last_applied_timestamp)
    
    def serialize_command(self, cmd_type: bytes, **kwargs) -> bytes:
        return msgpack.packb({"type": cmd_type, **kwargs})
    
    def get_lock_status(self, key: bytes) -> Optional[Dict[str, Any]]:
        return self._locks.get(key)