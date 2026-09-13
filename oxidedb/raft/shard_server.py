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
    def __init__(self, node_id: int, num_shards: int, total_nodes: int,
                 get_peer_shard_node: Optional[Callable[[int, int], MemoryRaftNode]] = None):
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
        #: How to build one more shard.  A split asks for a group the node was not
        #: started with, and it has to be the same kind of group as the others: the
        #: same state machine, the same storage, the same peers, and - in network mode
        #: - a port of its own at the address the routing table will publish.
        self._state_machine_factory: Optional[Callable[[], StateMachine]] = None
        self._storage_factory: Optional[Callable[[int, int], RaftStorage]] = None
        self._peer_addresses: Optional[Dict[int, str]] = None
        self._base_address: Optional[str] = None
        self._get_peer_shard_node = get_peer_shard_node
    
    def set_range_map(self, range_map: Dict[int, tuple]):
        self._range_map = range_map
    
    def _get_shard_id(self, key: bytes) -> int:
        return locate(self._range_map, key)
    
    @property
    def node_id(self) -> int:
        return self._node_id

    def _get_shard_port(self, shard_id: int) -> int:
        """Where shard ``shard_id`` of this node listens.

        One copy of the arithmetic for either mode: an in-process server derives it
        from the base port and the node id, a networked one from the address the node
        was given.  A split that worked the new shard's port out a second way would
        publish an address nothing is listening on.
        """
        if self._base_address is None:
            return self._base_port + shard_id * SHARD_PORT_STRIDE + self._node_id
        return self._shard_port_of(self._base_address, shard_id)

    @staticmethod
    def _shard_port_of(base_address: str, shard_id: int) -> int:
        """The port the node at ``base_address`` serves ``shard_id`` on."""
        return int(base_address.split(":")[1]) + shard_id * SHARD_PORT_STRIDE

    def _peer_address(self, base_address: str, shard_id: int) -> str:
        host, _ = base_address.split(":")
        return f"{host}:{self._shard_port_of(base_address, shard_id)}"

    def _shard_address_for(self, shard_id: int) -> Optional[str]:
        """Where this node serves ``shard_id``, or None for an in-process server."""
        if self._base_address is None:
            return None
        host, _ = self._base_address.split(":")
        return f"{host}:{self._get_shard_port(shard_id)}"

    def _peer_node_lookup(self, shard_id: int) -> Optional[Callable[[int], MemoryRaftNode]]:
        """How an in-process peer of ``shard_id`` is reached, or None if there is none."""
        if self._get_peer_shard_node is None:
            return None
        return lambda peer_id: self._get_peer_shard_node(shard_id, peer_id)

    
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
                     storage_factory: Optional[Callable[[int, int], RaftStorage]] = None,
                     peer_addresses: Optional[Dict[int, str]] = None):
        """Start this node's shards, in process unless ``peer_addresses`` is given.

        The factories and the addresses are kept rather than used and dropped: a split
        asks for one more shard, and :meth:`add_shard` has to build it the way these
        were built.
        """
        self._state_machine_factory = state_machine_factory
        self._storage_factory = storage_factory
        self._peer_addresses = peer_addresses
        if peer_addresses is not None:
            self._base_address = peer_addresses[self._node_id]

        for shard_id in range(self._num_shards):
            self.add_shard(shard_id)

        mode = " (network mode)" if peer_addresses is not None else ""
        print(f"ShardServer {self._node_id} started with {self._num_shards} shards{mode}")

    def start_shards_network(self, state_machine_factory: Callable[[], StateMachine],
                             peer_addresses: Dict[int, str],
                             storage_factory: Optional[Callable[[int, int], RaftStorage]] = None):
        self.start_shards(state_machine_factory, storage_factory, peer_addresses)

    def add_shard(self, shard_id: int) -> MemoryRaftNode:
        """Build one more Raft group for ``shard_id`` on this node, and serve it.

        This is how the groups the node started with are built, and a split goes
        through it rather than growing its own copy: the new shard has to have the
        same state machine and storage as its peers, and in network mode a port of its
        own.  A group that existed only in memory would be a shard whose published
        address nothing answers on - a range no client could reach.
        """
        if self._state_machine_factory is None:
            raise RuntimeError("start the shards on this server before adding one")

        storage = None
        if self._storage_factory is not None:
            storage = self._storage_factory(self._node_id, shard_id)

        peers = self._get_peer_nodes(shard_id)

        network_client = None
        if self._peer_addresses is not None:
            import grpc
            from .raft_servicer import RaftServicer
            from .network_client import RaftNetworkClient
            from oxidedb.proto.raft_pb2_grpc import add_RaftServiceServicer_to_server
            from concurrent.futures import ThreadPoolExecutor

            network_client = RaftNetworkClient({
                peer_id: self._peer_address(self._peer_addresses[peer_id], shard_id)
                for peer_id in peers
            })

        node = MemoryRaftNode(
            node_id=self._node_id,
            peers=peers,
            state_machine=self._state_machine_factory(),
            storage=storage,
            network_client=network_client,
            get_peer_node=None if network_client is not None else self._peer_node_lookup(shard_id),
        )

        self._shards[shard_id] = node
        self._shard_peers[shard_id] = peers
        self._num_shards = max(self._num_shards, shard_id + 1)

        address = self._shard_address_for(shard_id)
        if address is not None:
            server = grpc.server(ThreadPoolExecutor(max_workers=10))
            add_RaftServiceServicer_to_server(RaftServicer(node), server)
            server.add_insecure_port(address)
            server.start()
            self._shard_addresses[shard_id] = address
            node._grpc_server = server

        return node

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

    def _get_peer_shard_node(self, shard_id: int, peer_id: int) -> Optional[MemoryRaftNode]:
        """The node ``peer_id`` holds for ``shard_id``, for an in-process cluster.

        Networked nodes reach each other by address instead; this is what lets an
        in-process cluster be a cluster at all rather than a set of nodes that never
        hear from each other.
        """
        server = self._shard_servers.get(peer_id)
        return None if server is None else server.get_shard_node(shard_id)
    
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
            server = ShardServer(node_id, self._num_shards, self._num_nodes,
                                 get_peer_shard_node=self._get_peer_shard_node)
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
            server = ShardServer(node_id, self._num_shards, self._num_nodes,
                                 get_peer_shard_node=self._get_peer_shard_node)
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

        # Step 1: freeze.  Everything below reads this shard's rows at one
        # moment, and that moment only exists if nothing is writing: a row
        # proposed after the copy was taken would sit in the shard the table is
        # about to stop sending anyone to.  Freezing first and looking at the
        # locks afterwards is the order that matters - the other way round, a
        # prewrite could land between the look and the freeze.
        self._freeze_shard(shard_id)
        try:
            if not self._drain_shard(shard_id):
                return False

            # Committed state, and not while a transaction is in flight over the
            # range.  A lock in there may be a commit that has not been applied
            # yet, and a copy taken without it would be a write lost at the
            # moment the row moved, so the split refuses rather than race the
            # coordinator that is making it.  Nothing new can arrive while the
            # shard is frozen, so what is left in flight is a transaction that
            # prewrote before it and has a commit on the way.
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
            
            # The new shard is built by each server the same way it built the shards it
            # started with - same state machine, same storage, and in network mode a
            # port at the address the table is about to publish.
            for server in self._shard_servers.values():
                server.add_shard(new_shard_id)
            
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
        finally:
            self._unfreeze_shard(shard_id)

    def _shard_nodes(self, shard_id: int) -> List[MemoryRaftNode]:
        """Every replica of ``shard_id`` this cluster is holding."""
        nodes = [server.get_shard_node(shard_id) for server in self._shard_servers.values()]
        return [node for node in nodes if node is not None]

    def _freeze_shard(self, shard_id: int) -> None:
        """Stop the shard taking new rows, on every replica.  See MemoryRaftNode."""
        for node in self._shard_nodes(shard_id):
            node.freeze_writes()

    def _unfreeze_shard(self, shard_id: int) -> None:
        for node in self._shard_nodes(shard_id):
            node.resume_writes()

    def _drain_shard(self, shard_id: int) -> bool:
        """Wait out the writes that were admitted before the freeze."""
        return all(node.wait_for_writes_to_drain() for node in self._shard_nodes(shard_id))

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
