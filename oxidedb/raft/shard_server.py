# EXPERIMENTAL: sharding is frozen - a split moves rows without publishing the new
# range map, do not use
import time
from typing import Any, Dict, List, Optional, Callable, Tuple
from ..metadata.publisher import DEFAULT_PUBLISH_INTERVAL, MetadataPublisher
from ..shard.router import default_range_map, locate
from ..transaction.lock_cleaner import LockCleaner
from ..transaction.lock_resolver import DEFAULT_LOCK_TTL

#: How often the cluster's own lock cleaner looks for abandoned locks, in seconds.
DEFAULT_LOCK_CLEANER_INTERVAL = 30.0

#: How long a split waits for the shard it created to elect a leader, in seconds.
#: An election takes a few hundred milliseconds; a group that has not held one by
#: now is a group this split cannot finish, not something to wait for for ever.
SPLIT_LEADER_TIMEOUT = 10.0

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
        #: Splits whose rows are in the new shard's group but whose range the routing
        #: table has not been told about, keyed by the shard being split.  A retry has
        #: to finish the split that is in here rather than start a second one: the rows
        #: are already in the new group, and a second id would point the table at a
        #: group nothing was ever copied into.
        self._pending_splits: Dict[int, Dict[str, Any]] = {}
        #: Why the last split could not be published, if it could not be.
        self._last_split_error: Optional[str] = None
    
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
        """Split ``shard_id`` at ``split_key``: rows first, the routing table last.

        The order is the whole of it.  The new shard's group is built, the rows above
        the split point are copied into it, and only then is the table told - because
        the table is what clients route by, so a range it hands out has to be a range
        whose data is already there.  Proposing the split early would be a faster way
        to lose rows, not a feature.

        The source shard is frozen for all of it and thawed only once the table knows,
        which is the one state a retry can start from.  Rows that have been copied but
        not published live in a group no client is sent to, and a write let into the
        source in the meantime would live in a range the table is about to give away.

        A ``False`` answer means the table was not told: the shard is still frozen and
        the split is remembered, so calling again asks for that same split - the same
        id, the same rows, the same proposal - rather than for a second one.  Returns
        whether the split is now what the table says.
        """
        pending = self._pending_splits.get(shard_id)
        if pending is not None:
            if pending["split_key"] != split_key:
                # A different split of a shard that is already half-split.  There is no
                # answer this could give that would not lose one of the two.
                return False
            return self._finish_split(pending)

        old_range = self._range_map.get(shard_id)
        if old_range is None:
            return False

        start, end = old_range
        if split_key <= start or split_key >= end:
            return False

        leader = self._shard_leader_node(shard_id)
        if leader is None:
            return False

        # Step 1: freeze.  Everything below reads this shard's rows at one moment, and
        # that moment only exists if nothing is writing: a row proposed after the copy
        # was taken would sit in the shard the table is about to stop sending anyone
        # to.  Freezing first and looking at the locks afterwards is the order that
        # matters - the other way round, a prewrite could land between the look and the
        # freeze.
        self._freeze_shard(shard_id)
        copied = False
        try:
            if not self._drain_shard(shard_id):
                return False

            # Committed state, and not while a transaction is in flight over the
            # range.  A lock in there may be a commit that has not been applied yet,
            # and a copy taken without it would be a write lost at the moment the row
            # moved, so the split refuses rather than race the coordinator making it.
            # Nothing new can arrive while the shard is frozen, so what is left in
            # flight is a transaction that prewrote before it with a commit on the way.
            if self._locks_in_range(leader, start, end):
                return False

            rows = [(key, value) for key, value in self._committed_rows(leader, start, end)
                    if key >= split_key]

            new_shard_id = self._num_shards
            self._num_shards += 1
            # The new shard is built by each server the same way it built the shards it
            # started with - same state machine, same storage, and in network mode a
            # port at the address the table is about to publish.
            for server in self._shard_servers.values():
                server.add_shard(new_shard_id)

            pending = {"shard_id": shard_id, "split_key": split_key,
                       "new_shard_id": new_shard_id, "rows": rows}
            self._pending_splits[shard_id] = pending
            copied = True
            return self._finish_split(pending)
        finally:
            if not copied:
                # Nothing of this split exists anywhere, so there is nothing a write
                # could land outside of: the shard goes back to answering as it was.
                self._unfreeze_shard(shard_id)

    def _finish_split(self, pending: Dict[str, Any]) -> bool:
        """Copy what the new shard is missing, then tell the routing table.

        Called for the first attempt and for every retry of it.  A retry that finds the
        rows already in the new shard - which is what a split whose proposal was
        refused, or whose response was lost, comes back to - does not copy them again:
        the source has been frozen since they were read, so the copy that is already
        there is the copy.  What is left is the proposal, which the group recognises as
        the split it already applied.
        """
        new_shard_id = pending["new_shard_id"]
        new_leader = self._wait_for_shard_leader(new_shard_id)
        if new_leader is None:
            self._last_split_error = f"shard {new_shard_id} has no leader"
            return False

        if not self._new_shard_holds_them(pending):
            source = self._shard_leader_node(pending["shard_id"])
            if source is None:
                self._last_split_error = f"shard {pending['shard_id']} has no leader"
                return False
            for key, value in pending["rows"]:
                self._move_row(source, new_leader, key, value)

        if not self._publish_split(pending):
            return False

        self._pending_splits.pop(pending["shard_id"], None)
        self._apply_split_locally(pending)
        self._unfreeze_shard(pending["shard_id"])
        return True

    def _publish_split(self, pending: Dict[str, Any]) -> bool:
        """Tell the routing table that the new shard owns the right half.

        A refusal is not the end of the split: the shard stays frozen and the caller
        comes back through :meth:`_finish_split`, which is the same proposal again.
        What must not happen is a thaw.  The rows are in a group the table has not
        been told about, and a client routing by the old table would be sent to the
        source shard for keys whose data has already been copied out of it.
        """
        client = self._metadata_client
        if client is None:
            # No table to tell.  An in-process cluster has its own range map and
            # nothing else; that map is the whole world to it.
            return True

        new_shard_id = pending["new_shard_id"]
        result = client.split_shard(
            pending["shard_id"], pending["split_key"], new_shard_id,
            self.shard_replica_ids(new_shard_id), self.shard_addresses(new_shard_id),
        )
        if result.success:
            return True

        self._last_split_error = result.error_msg
        return False

    def _apply_split_locally(self, pending: Dict[str, Any]) -> None:
        """Re-range this process too, now that the table says so.

        The servers route by this map, so until it moves they would keep answering
        for the range the table has already given away.
        """
        new_range_map = dict(self._range_map)
        start, end = new_range_map[pending["shard_id"]]
        new_range_map[pending["shard_id"]] = (start, pending["split_key"])
        new_range_map[pending["new_shard_id"]] = (pending["split_key"], end)
        self.update_range_map(new_range_map)

    def _new_shard_holds_them(self, pending: Dict[str, Any]) -> bool:
        """Whether the new shard already has every row of the right half, as it is.

        What a retry needs to know before it copies anything again.  The source has
        been frozen since the rows were read, so the row has not moved on: the version
        already in the new shard, at the same timestamp, is the row - and copying it
        again would be work for nothing.  The timestamp is what makes it the same
        version; a row rewritten with the same value would be a later one.
        """
        target = self._wait_for_shard_leader(pending["new_shard_id"])
        source = self._shard_leader_node(pending["shard_id"])
        if target is None or source is None:
            return False

        source_storage = source._state_machine._storage
        target_storage = target._state_machine._storage
        for key, _value in pending["rows"]:
            expected = source_storage.get_latest_version(key)
            if expected is None:
                continue
            actual = target_storage.get_latest_version(key)
            if actual is None or actual.timestamp != expected.timestamp:
                return False
        return True

    def _committed_rows(self, leader: MemoryRaftNode, start: bytes, end: bytes):
        """The shard's committed rows in ``[start, end)``."""
        state_machine = leader._state_machine
        return state_machine._storage.scan(start, end, state_machine._last_applied_timestamp)

    def _locks_in_range(self, leader: MemoryRaftNode, start: bytes, end: bytes) -> bool:
        """Whether a transaction holds a lock anywhere in ``[start, end)``."""
        return any(start <= key < end for key, _ in leader._state_machine._storage.iter_locks())

    def _shard_leader_node(self, shard_id: int) -> Optional[MemoryRaftNode]:
        """The node object leading ``shard_id``, if one does."""
        leader = self.shard_leader(shard_id)
        if leader is None:
            return None
        return self._shard_servers[leader[0]].get_shard_node(shard_id)

    def _wait_for_shard_leader(self, shard_id: int,
                               timeout: float = SPLIT_LEADER_TIMEOUT) -> Optional[MemoryRaftNode]:
        """The node leading ``shard_id``, waiting a bounded while for an election.

        A shard that has just been created has no leader yet - the group has to elect
        one before anything can be copied into it - and a copy sent to a shard with no
        leader is a copy that never happened.
        """
        deadline = time.time() + timeout
        while True:
            leader = self._shard_leader_node(shard_id)
            if leader is not None:
                return leader
            if time.time() >= deadline:
                return None
            time.sleep(0.05)

    def split_error(self) -> Optional[str]:
        """Why the last split could not be published, if it could not be."""
        return self._last_split_error

    def pending_splits(self) -> Dict[int, Dict[str, Any]]:
        """The splits that are waiting for the routing table, for callers that look."""
        return dict(self._pending_splits)

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
