# EXPERIMENTAL: routing not wired to a metadata service, do not use
import hashlib
import time
from typing import Dict, List, Optional, Callable
from .node import MemoryRaftNode, RaftCluster, NodeState
from .state_machine import StateMachine, CommandType, ApplyResult
from .storage import RaftStorage, JSONFileStorage


class ShardServer:
    def __init__(self, node_id: int, num_shards: int, total_nodes: int):
        self._node_id = node_id
        self._num_shards = num_shards
        self._total_nodes = total_nodes
        self._shards: Dict[int, MemoryRaftNode] = {}
        self._shard_peers: Dict[int, List[int]] = {}
        self._base_port = 50051
        self._range_map: Dict[int, tuple] = {}
    
    def set_range_map(self, range_map: Dict[int, tuple]):
        self._range_map = range_map
    
    def _get_shard_id(self, key: bytes) -> int:
        for shard_id, (start, end) in self._range_map.items():
            if start <= key < end:
                return shard_id
        return 0
    
    def _get_shard_port(self, shard_id: int) -> int:
        return self._base_port + shard_id * 100 + self._node_id
    
    def _get_peer_nodes(self, shard_id: int) -> List[int]:
        peers = []
        for node_id in range(1, self._total_nodes + 1):
            if node_id != self._node_id:
                peers.append(node_id)
        return peers
    
    def start_shards(self, state_machine_factory: Callable[[], StateMachine], 
                     storage_factory: Optional[Callable[[int, int], RaftStorage]] = None):
        for shard_id in range(self._num_shards):
            state_machine = state_machine_factory()
            
            storage = None
            if storage_factory is not None:
                storage = storage_factory(self._node_id, shard_id)
            
            peers = self._get_peer_nodes(shard_id)
            
            node = MemoryRaftNode(
                node_id=self._node_id,
                peers=peers,
                state_machine=state_machine,
                storage=storage,
            )
            
            self._shards[shard_id] = node
            self._shard_peers[shard_id] = peers
        
        print(f"ShardServer {self._node_id} started with {self._num_shards} shards")
    
    def start_shards_network(self, state_machine_factory: Callable[[], StateMachine],
                             peer_addresses: Dict[int, str],
                             storage_factory: Optional[Callable[[int, int], RaftStorage]] = None):
        import grpc
        from .raft_servicer import RaftServicer
        from .network_client import RaftNetworkClient
        from oxidedb.proto.raft_pb2_grpc import add_RaftServiceServicer_to_server
        from concurrent.futures import ThreadPoolExecutor
        
        for shard_id in range(self._num_shards):
            state_machine = state_machine_factory()
            
            storage = None
            if storage_factory is not None:
                storage = storage_factory(self._node_id, shard_id)
            
            peers = self._get_peer_nodes(shard_id)
            
            shard_peer_addresses = {}
            for peer_id in peers:
                base_addr = peer_addresses[peer_id]
                host, port = base_addr.split(":")
                peer_port = int(port) + shard_id * 100
                shard_peer_addresses[peer_id] = f"{host}:{peer_port}"
            
            network_client = RaftNetworkClient(shard_peer_addresses)
            
            node = MemoryRaftNode(
                node_id=self._node_id,
                peers=peers,
                state_machine=state_machine,
                storage=storage,
                network_client=network_client,
            )
            
            self._shards[shard_id] = node
            self._shard_peers[shard_id] = peers
            
            server = grpc.server(ThreadPoolExecutor(max_workers=10))
            servicer = RaftServicer(node)
            add_RaftServiceServicer_to_server(servicer, server)
            
            host, port = peer_addresses[self._node_id].split(":")
            shard_port = int(port) + shard_id * 100
            address = f"{host}:{shard_port}"
            server.add_insecure_port(address)
            server.start()
            
            node._grpc_server = server
        
        print(f"ShardServer {self._node_id} started with {self._num_shards} shards (network mode)")
    
    def get_shard_node(self, shard_id: int) -> Optional[MemoryRaftNode]:
        return self._shards.get(shard_id)
    
    def get_shard_for_key(self, key: bytes) -> Optional[MemoryRaftNode]:
        shard_id = self._get_shard_id(key)
        return self._shards.get(shard_id)
    
    def get_leader_shard_for_key(self, key: bytes) -> Optional[MemoryRaftNode]:
        shard_id = self._get_shard_id(key)
        node = self._shards.get(shard_id)
        if node and node.state == NodeState.LEADER:
            return node
        return None
    
    def shutdown(self):
        for node in self._shards.values():
            node.shutdown()


class ShardedRaftCluster:
    def __init__(self, num_nodes: int, num_shards: int):
        self._num_nodes = num_nodes
        self._num_shards = num_shards
        self._shard_servers: Dict[int, ShardServer] = {}
        self._range_map: Dict[int, tuple] = self._create_default_range_map()
    
    def _create_default_range_map(self) -> Dict[int, tuple]:
        range_map = {}
        if self._num_shards <= 1:
            range_map[0] = (b'', b'\xff')
            return range_map
        
        chunk = 256 // self._num_shards
        for i in range(self._num_shards):
            start = bytes([i * chunk])
            end = bytes([(i + 1) * chunk]) if i < self._num_shards - 1 else b'\xff'
            range_map[i] = (start, end)
        return range_map
    
    def update_range_map(self, range_map: Dict[int, tuple]):
        self._range_map = range_map
        for server in self._shard_servers.values():
            server.set_range_map(range_map)
    
    def start(self, state_machine_factory: Callable[[], StateMachine],
              storage_factory: Optional[Callable[[int, int], RaftStorage]] = None):
        for node_id in range(1, self._num_nodes + 1):
            server = ShardServer(node_id, self._num_shards, self._num_nodes)
            server.set_range_map(self._range_map)
            server.start_shards(state_machine_factory, storage_factory)
            self._shard_servers[node_id] = server
        
        print(f"Sharded cluster started with {self._num_nodes} nodes and {self._num_shards} shards")
    
    def start_network(self, state_machine_factory: Callable[[], StateMachine],
                      peer_addresses: Dict[int, str],
                      storage_factory: Optional[Callable[[int, int], RaftStorage]] = None):
        for node_id in range(1, self._num_nodes + 1):
            server = ShardServer(node_id, self._num_shards, self._num_nodes)
            server.set_range_map(self._range_map)
            server.start_shards_network(state_machine_factory, peer_addresses, storage_factory)
            self._shard_servers[node_id] = server
        
        print(f"Sharded cluster started with {self._num_nodes} nodes and {self._num_shards} shards (network mode)")
    
    def get_shard_server(self, node_id: int) -> Optional[ShardServer]:
        return self._shard_servers.get(node_id)
    
    def get_leader_for_key(self, key: bytes) -> Optional[tuple]:
        for server in self._shard_servers.values():
            node = server.get_leader_shard_for_key(key)
            if node:
                return (server._node_id, node)
        return None
    
    def split_shard(self, shard_id: int, split_key: bytes) -> bool:
        old_range = self._range_map.get(shard_id)
        if old_range is None:
            return False
        
        start, end = old_range
        if split_key <= start or split_key >= end:
            return False
        
        leader_info = None
        for server in self._shard_servers.values():
            node = server.get_shard_node(shard_id)
            if node and node.state == NodeState.LEADER:
                leader_info = (server._node_id, node)
                break
        
        if leader_info is None:
            return False
        
        _, leader = leader_info
        
        scan_result = leader._state_machine.scan(start, end)
        
        self._num_shards += 1
        new_shard_id = self._num_shards - 1
        
        new_range_map = dict(self._range_map)
        new_range_map[shard_id] = (start, split_key)
        new_range_map[new_shard_id] = (split_key, end)
        
        for server in self._shard_servers.values():
            peers = server._get_peer_nodes(new_shard_id)
            
            state_machine = server._shards[shard_id]._state_machine.__class__()
            
            node = type(server._shards[shard_id])(
                node_id=server._node_id,
                peers=peers,
                state_machine=state_machine,
                storage=None,
                get_peer_node=lambda x, s=server, nid=server._node_id: self._shard_servers.get(x, s).get_shard_node(new_shard_id) if x != nid else None,
            )
            
            server._shards[new_shard_id] = node
            server._shard_peers[new_shard_id] = peers
        
        self.update_range_map(new_range_map)
        
        time.sleep(1)
        
        for key, value in scan_result:
            if split_key <= key:
                leader_info = self.get_leader_for_key(key)
                if leader_info is not None:
                    _, new_leader = leader_info
                    command = new_leader._state_machine.serialize_command(
                        CommandType.SET,
                        key=key,
                        value=value,
                        timestamp=self._get_next_timestamp(),
                    )
                    new_leader.propose(command)
        
        time.sleep(0.5)
        
        return True
    
    def _get_next_timestamp(self) -> int:
        import time
        return int(time.time() * 1000000)
    
    def shutdown(self):
        for server in self._shard_servers.values():
            server.shutdown()
