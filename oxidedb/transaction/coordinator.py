import threading
import time
from enum import Enum
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..raft.node import MemoryRaftNode, NodeState
from ..raft.state_machine import CommandType, ApplyResult
from ..shard.router import locate
from ..tso.tso import TSOClient


class TxnStatus(Enum):
    PENDING = "pending"
    PREWRITTEN = "prewritten"
    COMMITTED = "committed"
    ABORTED = "aborted"


class Transaction:
    def __init__(self, txn_id: int, start_ts: int):
        self.txn_id = txn_id
        self.start_ts = start_ts
        self.commit_ts = None
        self.keys: List[Tuple[bytes, bytes]] = []
        self.primary_key: Optional[bytes] = None
        self.status = TxnStatus.PENDING
    
    def add_key(self, key: bytes, value: bytes):
        self.keys.append((key, value))
        if self.primary_key is None:
            self.primary_key = key


class TransactionCoordinator:
    def __init__(self, tso_client: TSOClient, shard_server):
        self._tso_client = tso_client
        self._shard_server = shard_server
        self._transactions: Dict[int, Transaction] = {}
        self._txn_id_counter = 0
        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(max_workers=10)
    
    def _get_shard_id(self, key: bytes) -> int:
        return locate(self._shard_server._range_map, key)
    
    def _get_shard_leader(self, shard_id: int) -> Optional[MemoryRaftNode]:
        for server in self._shard_server._shard_servers.values():
            node = server.get_shard_node(shard_id)
            if node and node.state == NodeState.LEADER:
                return node
        return None
    
    def begin(self) -> Tuple[int, int]:
        with self._lock:
            self._txn_id_counter += 1
            txn_id = self._txn_id_counter
        
        start_ts = self._tso_client.get_timestamp()
        
        with self._lock:
            self._transactions[txn_id] = Transaction(txn_id, start_ts)
        
        return txn_id, start_ts
    
    def add_write(self, txn_id: int, key: bytes, value: bytes):
        with self._lock:
            txn = self._transactions.get(txn_id)
            if txn is None:
                raise RuntimeError(f"Transaction {txn_id} not found")
            if txn.status != TxnStatus.PENDING:
                raise RuntimeError(f"Transaction {txn_id} is {txn.status}")
            txn.add_key(key, value)
    
    def commit(self, txn_id: int) -> Tuple[bool, Optional[int]]:
        with self._lock:
            txn = self._transactions.get(txn_id)
            if txn is None:
                return False, None
            if txn.status != TxnStatus.PENDING:
                return False, None
            if not txn.keys:
                txn.status = TxnStatus.COMMITTED
                txn.commit_ts = txn.start_ts
                return True, txn.start_ts
        
        shard_groups = self._group_keys_by_shard(txn)
        
        prewrite_success = self._prewrite_all_shards(txn, shard_groups)
        if not prewrite_success:
            with self._lock:
                txn.status = TxnStatus.ABORTED
            return False, None
        
        with self._lock:
            txn.status = TxnStatus.PREWRITTEN
        
        commit_ts = self._tso_client.get_timestamp()
        
        with self._lock:
            txn.commit_ts = commit_ts
        
        primary_shard_id = self._get_shard_id(txn.primary_key)
        primary_leader = self._get_shard_leader(primary_shard_id)
        
        if primary_leader is None:
            self._rollback_all_shards(txn, shard_groups)
            with self._lock:
                txn.status = TxnStatus.ABORTED
            return False, None
        
        primary_commit_result = self._commit_key(primary_leader, txn.primary_key, txn.start_ts, commit_ts)
        
        if not primary_commit_result.success:
            self._rollback_all_shards(txn, shard_groups)
            with self._lock:
                txn.status = TxnStatus.ABORTED
            return False, None
        
        with self._lock:
            txn.status = TxnStatus.COMMITTED
        
        self._commit_secondary_shards(txn, shard_groups, commit_ts)
        
        return True, commit_ts
    
    def _group_keys_by_shard(self, txn: Transaction) -> Dict[int, List[Tuple[bytes, bytes]]]:
        shard_groups = {}
        for key, value in txn.keys:
            shard_id = self._get_shard_id(key)
            if shard_id not in shard_groups:
                shard_groups[shard_id] = []
            shard_groups[shard_id].append((key, value))
        return shard_groups
    
    def _prewrite_all_shards(self, txn: Transaction, shard_groups: Dict[int, List[Tuple[bytes, bytes]]]) -> bool:
        futures = {}
        
        for shard_id, keys in shard_groups.items():
            leader = self._get_shard_leader(shard_id)
            if leader is None:
                return False
            
            future = self._executor.submit(self._prewrite_shard, leader, txn, keys)
            futures[future] = shard_id
        
        results = {}
        for future in as_completed(futures):
            shard_id = futures[future]
            try:
                results[shard_id] = future.result()
            except Exception:
                results[shard_id] = False
        
        if not all(results.values()):
            self._rollback_all_shards(txn, shard_groups)
            return False
        
        return True
    
    def _prewrite_shard(self, leader: MemoryRaftNode, txn: Transaction, keys: List[Tuple[bytes, bytes]]) -> bool:
        for key, value in keys:
            command = leader._state_machine.serialize_command(
                CommandType.PREWRITE,
                key=key,
                value=value,
                start_ts=txn.start_ts,
                primary_key=txn.primary_key,
            )
            
            result = leader.propose(command)
            if not result.success:
                return False
        
        return True
    
    def _rollback_all_shards(self, txn: Transaction, shard_groups: Dict[int, List[Tuple[bytes, bytes]]]):
        for shard_id, keys in shard_groups.items():
            leader = self._get_shard_leader(shard_id)
            if leader is None:
                continue
            
            for key, _ in keys:
                command = leader._state_machine.serialize_command(
                    CommandType.ROLLBACK,
                    key=key,
                    start_ts=txn.start_ts,
                )
                leader.propose(command)
    
    def _commit_key(self, leader: MemoryRaftNode, key: bytes, start_ts: int, commit_ts: int) -> ApplyResult:
        command = leader._state_machine.serialize_command(
            CommandType.COMMIT,
            key=key,
            start_ts=start_ts,
            commit_ts=commit_ts,
        )
        return leader.propose(command)
    
    def _commit_secondary_shards(self, txn: Transaction, shard_groups: Dict[int, List[Tuple[bytes, bytes]]], commit_ts: int):
        for shard_id, keys in shard_groups.items():
            leader = self._get_shard_leader(shard_id)
            if leader is None:
                continue
            
            for key, _ in keys:
                if key == txn.primary_key:
                    continue
                
                self._executor.submit(self._commit_key, leader, key, txn.start_ts, commit_ts)
    
    def rollback(self, txn_id: int) -> bool:
        with self._lock:
            txn = self._transactions.get(txn_id)
            if txn is None:
                return False
            if txn.status == TxnStatus.COMMITTED:
                return False
            if txn.status == TxnStatus.PENDING:
                txn.status = TxnStatus.ABORTED
                return True
        
        shard_groups = self._group_keys_by_shard(txn)
        self._rollback_all_shards(txn, shard_groups)
        
        with self._lock:
            txn.status = TxnStatus.ABORTED
        
        return True
    
    def get_transaction(self, txn_id: int) -> Optional[Transaction]:
        with self._lock:
            return self._transactions.get(txn_id)
    
    def shutdown(self):
        self._executor.shutdown(wait=True)