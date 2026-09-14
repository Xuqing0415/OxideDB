import msgpack
import threading
import time
from enum import Enum
from typing import Optional
from ..raft.node import MemoryRaftNode, NodeState
from ..groups import refusal
from ..proto import groups_pb2
from ..proto.groups_pb2_grpc import TSOServiceServicer
from ..raft.state_machine import ApplyResult, ErrorCode, StateMachine


class TSOCommandType(Enum):
    ALLOCATE = b"allocate"


#: How many timestamps one proposal hands out.  A run costs one entry in the group's log
#: whatever it is worth, so a timestamp is allocated in runs and consumed one at a time:
#: here by a caller that takes a run and hands the numbers out itself, and below by the
#: servicer, which answers a client that would rather not hold a run of its own.
DEFAULT_BATCH_SIZE = 1000


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
    
    def get(self, key: bytes, timestamp: Optional[int] = None) -> Optional[bytes]:
        return None
    
    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None) -> list:
        # The timestamp group is a counter, not a keyspace: there is nothing here
        # for a range read to find, at any timestamp.
        return []

    def snapshot(self) -> bytes:
        # This state machine is a counter, so the snapshot is the counter.  It
        # is also the reason the counter must never go backwards: nothing here
        # may hand out a timestamp twice.
        with self._lock:
            return msgpack.packb({"current_ts": self._current_ts})

    def restore(self, data: bytes) -> None:
        with self._lock:
            self._current_ts = msgpack.unpackb(data, raw=False)["current_ts"] if data else 0


class TSOClient:
    def __init__(self, tso_node: MemoryRaftNode,
                 batch_size: int = DEFAULT_BATCH_SIZE):
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


class TSOServicer(TSOServiceServicer):
    """The group's clock, answered to a client that is not in this process.

    Two calls for one thing, and what differs between them is the size of the run.
    ``GetTimestamp`` allocates a run of one, so the number it answers with is everything
    the group handed out for it; ``GetTimestampBatch`` allocates what the caller asked
    for and answers with both ends of that run.  A caller that reads one number at a time
    should not be handed a thousand it will not use - which is the reason both calls are
    in the contract rather than one with a default - and a caller that wants a thousand
    should not pay a thousand log entries for them.

    Neither call decides that this node leads: the proposal is what answers that, so a
    node that has stopped leading refuses here the way it refuses anywhere else.
    """

    def __init__(self, node, leader_address=None):
        self._node = node
        self._leader_address = leader_address

    def GetTimestamp(self, request, context):
        data, refused = self._allocate(1)
        if refused is not None:
            return groups_pb2.GetTimestampResponse(**refused)
        return groups_pb2.GetTimestampResponse(error_code=groups_pb2.OK,
                                               timestamp=data.get("start_ts", 0))

    def GetTimestampBatch(self, request, context):
        if request.count < 1:
            # A run of none is not a range and cannot be answered with one: start_ts
            # and end_ts would have to name a boundary that does not exist.  Refused
            # rather than rounded up, because a caller asking for none is a caller that
            # has lost track of what it is asking for.
            return groups_pb2.GetTimestampBatchResponse(**refusal(
                f"a batch is at least one timestamp, and this asks for {request.count}",
                ErrorCode.ERR_UNKNOWN, None))

        data, refused = self._allocate(int(request.count))
        if refused is not None:
            return groups_pb2.GetTimestampBatchResponse(**refused)
        return groups_pb2.GetTimestampBatchResponse(error_code=groups_pb2.OK,
                                                    start_ts=data.get("start_ts", 0),
                                                    end_ts=data.get("end_ts", 0))

    def _allocate(self, count: int):
        """One run of ``count`` timestamps, or the refusal to answer with instead.

        The run comes back as the allocate command wrote it - both ends inclusive, in
        msgpack - and is passed on as it is: a servicer that adjusted the numbers would
        be a second place the group's arithmetic lives, and the one place a client reads
        them has to agree with the command it came from.
        """
        command = self._node._state_machine.serialize_command(
            TSOCommandType.ALLOCATE, batch_size=count)
        result = self._node.propose(command)
        if not result.success:
            return None, refusal(
                result.error_msg or "the clock cannot allocate a timestamp",
                result.error_code, self._leader_address)
        return msgpack.unpackb(result.data, raw=False) if result.data else {}, None


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
