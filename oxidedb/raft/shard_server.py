# EXPERIMENTAL: sharding is frozen - a split moves rows without publishing the new
# range map, do not use
import time
from typing import Dict, List, Optional, Callable, Tuple
from ..metadata.publisher import DEFAULT_PUBLISH_INTERVAL, MetadataPublisher
from ..shard.router import default_range_map, locate
from ..transaction.lock_cleaner import LockCleaner
from ..transaction.lock_resolver import DEFAULT_LOCK_TTL

#: How often the cluster's own lock cleaner looks for abandoned locks, in seconds.
DEFAULT_LOCK_CLEANER_INTERVAL = 30.0

#: Shard ``s`` of a node listens at that node's base port plus ``s * this``, in both
#: ``_get_shard_port`` and ``start_shards_network``, so the two cannot drift apart.
SHARD_PORT_STRIDE = 100
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
        #: Where each shard of this node actually listens, for a networked server.  The
        #: table publishes these rather than recomputing them: an address a client is
        #: sent to has to be the one the server bound.
        self._shard_addresses: Dict[int, str] = {}
    
    def set_range_map(self, range_map: Dict[int, tuple]):
        self._range_map = range_map
    
    def _get_shard_id(self, key: bytes) -> int:
        return locate(self._range_map, key)
    
    @property
    def node_id(self) -> int:
        return self._node_id

    def _get_shard_port(self, shard_id: int) -> int:
        return self._base_port + shard_id * SHARD_PORT_STRIDE + self._node_id

    
    def _get_peer_nodes(self, shard_id: int) -> List[int]:
        peers = []
        for node_id in range(1, self._total_nodes + 1):
            if node_id != self._node_id:
                peers.append(node_id)
        return peers
    
    def shard_replica_ids(self, shard_id: int) -> List[int]:
        """Every node serving ``shard_id``, this one included."""
        return sorted([self._node_id] + list(self._shard_peers.get(shard_id, [])))

    def shard_address(self, shard_id: int) -> Optional[str]:
        """Where this node serves ``shard_id``, or None in process."""
        return self._shard_addresses.get(shard_id)

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
                peer_port = int(port) + shard_id * SHARD_PORT_STRIDE
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
            shard_port = int(port) + shard_id * SHARD_PORT_STRIDE
            address = f"{host}:{shard_port}"
            server.add_insecure_port(address)
            server.start()
            self._shard_addresses[shard_id] = address
            
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
        self._lock_cleaner = None
        self._metadata_client = None
        self._metadata_publisher: Optional[MetadataPublisher] = None
    
    def _create_default_range_map(self) -> Dict[int, tuple]:
        return default_range_map(self._num_shards)
    
    def update_range_map(self, range_map: Dict[int, tuple]):
        self._range_map = range_map
        for server in self._shard_servers.values():
            server.set_range_map(range_map)
    
    def start(self, state_machine_factory: Callable[[], StateMachine],
              storage_factory: Optional[Callable[[int, int], RaftStorage]] = None,
              lock_cleaner_interval: Optional[float] = DEFAULT_LOCK_CLEANER_INTERVAL,
              lock_cleaner_ttl: float = DEFAULT_LOCK_TTL,
              metadata=None,
              metadata_publish_interval: float = DEFAULT_PUBLISH_INTERVAL):
        for node_id in range(1, self._num_nodes + 1):
            server = ShardServer(node_id, self._num_shards, self._num_nodes)
            server.set_range_map(self._range_map)
            server.start_shards(state_machine_factory, storage_factory)
            self._shard_servers[node_id] = server

        self.start_lock_cleaner(lock_cleaner_interval, lock_cleaner_ttl)
        self.start_metadata_publisher(metadata, metadata_publish_interval)
        print(f"Sharded cluster started with {self._num_nodes} nodes and {self._num_shards} shards")
    
    def start_network(self, state_machine_factory: Callable[[], StateMachine],
                      peer_addresses: Dict[int, str],
                      storage_factory: Optional[Callable[[int, int], RaftStorage]] = None,
                      lock_cleaner_interval: Optional[float] = DEFAULT_LOCK_CLEANER_INTERVAL,
                      lock_cleaner_ttl: float = DEFAULT_LOCK_TTL,
                      metadata=None,
                      metadata_publish_interval: float = DEFAULT_PUBLISH_INTERVAL):
        for node_id in range(1, self._num_nodes + 1):
            server = ShardServer(node_id, self._num_shards, self._num_nodes)
            server.set_range_map(self._range_map)
            server.start_shards_network(state_machine_factory, peer_addresses, storage_factory)
            self._shard_servers[node_id] = server
        
        self.start_lock_cleaner(lock_cleaner_interval, lock_cleaner_ttl)
        self.start_metadata_publisher(metadata, metadata_publish_interval)
        print(f"Sharded cluster started with {self._num_nodes} nodes and {self._num_shards} shards (network mode)")

    def start_lock_cleaner(self, interval: Optional[float] = DEFAULT_LOCK_CLEANER_INTERVAL,
                           lock_ttl: float = DEFAULT_LOCK_TTL) -> None:
        """Start the background cleaner that resolves abandoned locks.

        A coordinator that dies before its primary commit leaves locks that nobody
        is going to resolve, and the read path can only report them as locked.  The
        cleaner re-derives each one from the primary key's write record - committed
        if there is one there, aborted otherwise - which is the decision a reader
        would make for itself if it had the protocol.  ``interval=None`` leaves it
        off.
        """
        if interval is None or self._lock_cleaner is not None:
            return

        self._lock_cleaner = LockCleaner(self, poll_interval=interval, lock_ttl=lock_ttl)
        self._lock_cleaner.start()
    
    def get_shard_server(self, node_id: int) -> Optional[ShardServer]:
        return self._shard_servers.get(node_id)

    def range_map(self) -> Dict[int, tuple]:
        return dict(self._range_map)

    def shard_ids(self) -> List[int]:
        return sorted(self._range_map)

    def shard_replica_ids(self, shard_id: int) -> List[int]:
        """The nodes whose Raft group serves ``shard_id``."""
        for server in self._shard_servers.values():
            if server.get_shard_node(shard_id) is not None:
                return server.shard_replica_ids(shard_id)
        return []

    def shard_addresses(self, shard_id: int) -> Dict[int, str]:
        """Where each node serves ``shard_id``, as the servers themselves bound it.

        Empty for an in-process cluster: nothing listens, so there is no address to
        publish.  The addresses come from the servers rather than from a second copy of
        the port arithmetic - an address a client is sent to has to be one that is
        actually accepting connections.
        """
        addresses = {}
        for node_id, server in sorted(self._shard_servers.items()):
            address = server.shard_address(shard_id)
            if address is not None:
                addresses[node_id] = address
        return addresses

    def shard_leader(self, shard_id: int) -> Optional[Tuple[int, int]]:
        """The node leading ``shard_id``, and the term it leads at, if one does."""
        for server in self._shard_servers.values():
            node = server.get_shard_node(shard_id)
            if node is not None and node.state == NodeState.LEADER:
                return (server.node_id, node.current_term)
        return None

    def start_metadata_publisher(self, metadata,
                                 interval: float = DEFAULT_PUBLISH_INTERVAL) -> None:
        """Start the thread that publishes this cluster's placement.

        ``metadata`` is a ``MetadataCluster`` or an already built ``MetadataClient``.
        None leaves the table unwritten, which is what a cluster started without a
        metadata service does.  The publisher gets a client of its own so that its
        writes drop its own cache and nobody else's.
        """
        if metadata is None or self._metadata_publisher is not None:
            return

        client = metadata.get_client() if hasattr(metadata, "get_client") else metadata
        self._metadata_client = client
        self._metadata_publisher = MetadataPublisher(client, self, poll_interval=interval)
        self._metadata_publisher.start()

    def metadata_client(self):
        """The client this cluster publishes through, if a publisher is running."""
        return self._metadata_client
    
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
        
        # Committed state, and not while a transaction is in flight over the range.
        # A lock in there may be a commit that has not been applied yet, and a copy
        # taken without it would be a write lost at the moment the row moved, so the
        # split refuses rather than race the coordinator that is making it.
        state_machine = leader._state_machine
        storage = state_machine._storage
        if any(start <= key < end for key, _ in storage.iter_locks()):
            return False

        scan_result = storage.scan(start, end, state_machine._last_applied_timestamp)
        
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
            if key < split_key:
                continue  # this row stays where it is

            target = self.get_leader_for_key(key)
            if target is not None:
                _, new_leader = target
                self._move_row(leader, new_leader, key, value)
        
        time.sleep(0.5)
        
        return True

    def _move_row(self, source_leader: MemoryRaftNode, target_leader: MemoryRaftNode,
                  key: bytes, value: bytes) -> None:
        """Copy one row into the new shard *as the version it already is*.

        The row keeps the timestamp it was committed at, and the write record of
        the transaction that committed it.  A copy stamped with the moment of the
        move - which is what a wall clock gives you, and what this used to do - is
        the newest thing that has ever happened to that key: a snapshot read at
        any timestamp a client can hold does not see it, and every prewrite
        against it is refused as a write conflict, because the copy is newer than
        the start timestamp the TSO has just handed out.  Moving a row is not a
        write to the key, and a shard that answers for the row differently from
        the shard it came from is a shard the row cannot be moved to.
        """
        storage = source_leader._state_machine._storage
        version = storage.get_latest_version(key)
        if version is None:
            return

        record = storage.get_latest_write(key)
        command = target_leader._state_machine.serialize_command(
            CommandType.SET,
            key=key,
            value=value,
            timestamp=version.timestamp,
            start_ts=(record["start_ts"] if record is not None
                      and record["commit_ts"] == version.timestamp else None),
        )
        target_leader.propose(command)
    
    def shutdown(self):
        # Before the servers: the cleaner proposes entries onto shard leaders, so
        # stopping it first keeps it from racing the shutdown.  The publisher needs
        # the same treatment one layer up - it reads leaders and proposes into the
        # metadata group - and it goes first because the servers it reads are next.
        if self._metadata_publisher is not None:
            self._metadata_publisher.stop()
            self._metadata_publisher = None

        if self._lock_cleaner is not None:
            self._lock_cleaner.stop()
            self._lock_cleaner = None

        for server in self._shard_servers.values():
            server.shutdown()
