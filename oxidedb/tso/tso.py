import msgpack
import threading
import time
from enum import Enum
from typing import Optional
from ..raft.node import MemoryRaftNode, NodeState
from ..raft.state_machine import ApplyResult, StateMachine


class TSOCommandType(Enum):
    ALLOCATE = b"allocate"


class TSOSMStateMachine(StateMachine):
    def __init__(self):
        self._current_ts = 0
        self._lock = threading.RLock()
    
    def apply(self, command: bytes) -> ApplyResult:
        try:
            cmd = msgpack.unpackb(command)
            cmd_type = cmd.get("type")
            
            if cmd_type == TSOCommandType.ALLOCATE.value:
                batch_size = cmd.get("batch_size", 1000)
                return self._apply_allocate(batch_size)
            
            return ApplyResult.success()
        except Exception as e:
            return ApplyResult.failure(1, f"Apply error: {str(e)}")
    
    def _apply_allocate(self, batch_size: int) -> ApplyResult:
        with self._lock:
            start_ts = self._current_ts + 1
            end_ts = self._current_ts + batch_size
            self._current_ts = end_ts
            result_data = {"start_ts": start_ts, "end_ts": end_ts}
            return ApplyResult.success(data=msgpack.packb(result_data))
    
    def get_current_ts(self) -> int:
        with self._lock:
            return self._current_ts
    
    def serialize_command(self, cmd_type, **kwargs) -> bytes:
        cmd_type_value = cmd_type.value if hasattr(cmd_type, 'value') else cmd_type
        return msgpack.packb({"type": cmd_type_value, **kwargs})
    
    def get(self, key: bytes) -> Optional[bytes]:
        return None
    
    def scan(self, start_key: bytes, end_key: bytes) -> list:
        return []


class TSOClient:
    def __init__(self, tso_node: MemoryRaftNode, batch_size: int = 1000):
        self._tso_node = tso_node
        self._batch_size = batch_size
        self._local_start = 0
        self._local_end = 0
        self._lock = threading.RLock()
    
    def get_timestamp(self) -> int:
        with self._lock:
            if self._local_start >= self._local_end:
                self._fetch_batch()
            
            ts = self._local_start
            self._local_start += 1
            return ts
    
    def _fetch_batch(self):
        if self._tso_node.state != NodeState.LEADER:
            raise RuntimeError("TSO node is not leader")
        
        command = self._tso_node._state_machine.serialize_command(
            TSOCommandType.ALLOCATE,
            batch_size=self._batch_size
        )
        
        result = self._tso_node.propose(command)
        
        if not result.success:
            raise RuntimeError(f"Failed to allocate timestamp: {result.error_msg}")
        
        try:
            result_data = msgpack.unpackb(result.data) if result.data else {}
            # 服务端返回的 start_ts/end_ts 均为 inclusive（[start_ts, end_ts] 共 batch_size 个号），
            # 客户端按半开区间 [start_ts, end_ts) 消费，因此 end 需 +1 转成 exclusive 上界，
            # 否则当 _local_start 增长到 end_ts 时会提前触发新批次，跳过最后一个号。
            self._local_start = result_data.get("start_ts", 0)
            self._local_end = result_data.get("end_ts", 0) + 1
        except Exception:
            self._local_start = 0
            self._local_end = 0
    
    def batch_get_timestamps(self, count: int) -> list:
        timestamps = []
        for _ in range(count):
            timestamps.append(self.get_timestamp())
        return timestamps


class TSOCluster:
    def __init__(self, num_nodes: int = 3):
        self._num_nodes = num_nodes
        self._cluster = None
    
    def start(self, peer_addresses: dict, storage_factory=None):
        from ..raft.node import RaftCluster
        
        self._cluster = RaftCluster(num_nodes=self._num_nodes)
        self._cluster.start_network(
            state_machine_factory=lambda: TSOSMStateMachine(),
            peer_addresses=peer_addresses,
            storage_factory=storage_factory,
        )
    
    def get_client(self) -> Optional[TSOClient]:
        if self._cluster is None:
            return None
        
        leader_id = self._cluster.get_leader()
        if leader_id is None:
            return None
        
        leader = self._cluster.get_node(leader_id)
        return TSOClient(leader)
    
    def shutdown(self):
        if self._cluster is not None:
            self._cluster.shutdown()