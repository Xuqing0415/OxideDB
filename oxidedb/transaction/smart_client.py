import time
import random
from typing import Optional, Tuple, Dict, Any

from ..raft.node import MemoryRaftNode, NodeState
from ..raft.state_machine import ReadResult, ErrorCode
from ..tso.tso import TSOClient
from .coordinator import TransactionCoordinator


class SmartClient:
    def __init__(self, tso_client: TSOClient, shard_cluster):
        self._tso_client = tso_client
        self._shard_cluster = shard_cluster
        self._coordinator = TransactionCoordinator(tso_client, shard_cluster)
        self._retry_backoff_base = 0.01
        self._retry_max_backoff = 0.1
        self._retry_max_attempts = 3
    
    def _get_shard_leader(self, key: bytes) -> Optional[MemoryRaftNode]:
        result = self._shard_cluster.get_leader_for_key(key)
        if result is not None:
            return result[1]
        return None
    
    def _refresh_leader_cache(self):
        pass
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        last_error = None
        for attempt in range(self._retry_max_attempts):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                last_error = e
                if attempt < self._retry_max_attempts - 1:
                    backoff = min(
                        self._retry_backoff_base * (2 ** attempt),
                        self._retry_max_backoff
                    )
                    time.sleep(backoff + random.uniform(0, 0.005))
        
        raise last_error
    
    def get(self, key: bytes) -> Optional[bytes]:
        def _do_get():
            leader = self._get_shard_leader(key)
            if leader is None:
                raise RuntimeError("No leader found")
            
            result = leader.get(key)
            
            if result.error_code == ErrorCode.ERR_NOT_LEADER:
                self._refresh_leader_cache()
                raise RuntimeError(f"Not leader, refreshing cache")
            
            if result.error_code == ErrorCode.ERR_LOCKED:
                raise RuntimeError(f"Key is locked")
            
            return result.value
        
        return self._retry_with_backoff(_do_get)
    
    def begin(self) -> int:
        txn_id, _ = self._coordinator.begin()
        return txn_id
    
    def add_write(self, txn_id: int, key: bytes, value: bytes):
        self._coordinator.add_write(txn_id, key, value)
    
    def commit(self, txn_id: int) -> Tuple[bool, Optional[int]]:
        def _do_commit():
            success, commit_ts = self._coordinator.commit(txn_id)
            
            if not success:
                txn = self._coordinator.get_transaction(txn_id)
                if txn is not None:
                    raise RuntimeError(f"Commit failed: {txn.status}")
                raise RuntimeError("Commit failed")
            
            return success, commit_ts
        
        try:
            return self._retry_with_backoff(_do_commit)
        except RuntimeError:
            return False, None
    
    def rollback(self, txn_id: int) -> bool:
        return self._coordinator.rollback(txn_id)
    
    def put(self, key: bytes, value: bytes) -> bool:
        txn_id = self.begin()
        self.add_write(txn_id, key, value)
        success, _ = self.commit(txn_id)
        return success