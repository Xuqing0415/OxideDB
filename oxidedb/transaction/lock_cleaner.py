import threading
import time
import hashlib
from typing import Dict, Optional, Callable
from ..raft.node import MemoryRaftNode, NodeState
from ..raft.state_machine import CommandType, LockStatus


class LockCleaner:
    def __init__(self, shard_cluster, poll_interval: int = 10):
        self._shard_cluster = shard_cluster
        self._poll_interval = poll_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
    
    def _get_shard_id(self, key: bytes) -> int:
        range_map = self._shard_cluster._range_map
        for shard_id, (start, end) in range_map.items():
            if start <= key < end:
                return shard_id
        return 0
    
    def _get_shard_leader(self, shard_id: int) -> Optional[MemoryRaftNode]:
        for server in self._shard_cluster._shard_servers.values():
            node = server.get_shard_node(shard_id)
            if node and node.state == NodeState.LEADER:
                return node
        return None
    
    def _get_primary_status(self, primary_key: bytes, start_ts: int) -> str:
        primary_shard_id = self._get_shard_id(primary_key)
        leader = self._get_shard_leader(primary_shard_id)
        
        if leader is None:
            return "UNKNOWN"
        
        write_record = leader._state_machine._storage.get_latest_write(primary_key)
        if write_record is not None and write_record["start_ts"] == start_ts:
            return "COMMITTED"
        
        lock = leader._state_machine.get_lock_status(primary_key)
        if lock is not None and lock["start_ts"] == start_ts:
            lock_time = lock.get("lock_time", 0)
            if time.time() - lock_time < 5:
                return "LOCKED"
        
        return "ABORTED"
    
    def _clean_expired_locks(self):
        for server in self._shard_cluster._shard_servers.values():
            for shard_id in range(server._num_shards):
                node = server.get_shard_node(shard_id)
                if node is None or node.state != NodeState.LEADER:
                    continue
                
                state_machine = node._state_machine
                locks_to_clean = []
                
                for key, lock in list(state_machine._locks.items()):
                    status = lock.get('status')
                    if status != LockStatus.LOCKED:
                        continue
                    
                    lock_time = lock.get("lock_time", 0)
                    if time.time() - lock_time >= 5:
                        locks_to_clean.append((key, lock))
                
                for key, lock in locks_to_clean:
                    self._process_expired_lock(node, key, lock)
    
    def _process_expired_lock(self, leader: MemoryRaftNode, key: bytes, lock: Dict):
        primary_key = lock["primary_key"]
        start_ts = lock["start_ts"]
        
        status = self._get_primary_status(primary_key, start_ts)
        
        if status == "COMMITTED":
            commit_ts = self._get_commit_ts(primary_key, start_ts)
            if commit_ts is not None:
                commit_cmd = leader._state_machine.serialize_command(
                    CommandType.COMMIT,
                    key=key,
                    start_ts=start_ts,
                    commit_ts=commit_ts,
                )
                leader.propose(commit_cmd)
        
        elif status == "ABORTED":
            rollback_cmd = leader._state_machine.serialize_command(
                CommandType.ROLLBACK,
                key=key,
                start_ts=start_ts,
            )
            leader.propose(rollback_cmd)
    
    def _get_commit_ts(self, primary_key: bytes, start_ts: int) -> Optional[int]:
        primary_shard_id = self._get_shard_id(primary_key)
        leader = self._get_shard_leader(primary_shard_id)
        
        if leader is None:
            return None
        
        write_record = leader._state_machine._storage.get_latest_write(primary_key)
        if write_record is not None and write_record["start_ts"] == start_ts:
            return write_record["commit_ts"]
        
        return None
    
    def start(self):
        with self._lock:
            if self._running:
                return
            self._running = True
        
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    
    def _run(self):
        while self._running:
            try:
                self._clean_expired_locks()
            except Exception:
                pass
            
            time.sleep(self._poll_interval)
    
    def stop(self):
        with self._lock:
            self._running = False
        
        if self._thread is not None:
            self._thread.join(timeout=5)