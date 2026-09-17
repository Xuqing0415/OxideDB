# EXPERIMENTAL: sharding is wired end to end for a placement that never has to change -
# no follower read, and no move that finishes, so do not build a deployment on it
import time

import msgpack

from dataclasses import dataclass, field

from typing import Any, Dict, List, Optional, Callable, Tuple
from ..metadata.publisher import DEFAULT_PUBLISH_INTERVAL, MetadataPublisher
from ..shard.router import default_range_map, locate
from ..transaction.lock_cleaner import LockCleaner
from ..transaction.lock_resolver import DEFAULT_LOCK_TTL

#: How often the cluster's own lock cleaner looks for abandoned locks, in seconds.
DEFAULT_LOCK_CLEANER_INTERVAL = 30.0

#: Where a shard's in-progress split is written down, within that shard's storage.
SPLIT_RECORD_PREFIX = "split"

#: The same, for a shard that is being moved to another group.
MIGRATION_RECORD_PREFIX = "migrate"

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
        #: Which nodes serve each shard, when this server was told.  A shard with no
        #: entry is served by every node of the cluster, which is how a cluster that was
        #: not given a placement is built - and how the shard a split creates is built.
        #: A move needs the other answer: the group it builds is on the nodes it was
        #: given and on no others.
        self._shard_members: Dict[int, List[int]] = {}
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
        """The other nodes of ``shard_id``'s group: the ones it was placed on, or all."""
        members = self._shard_members.get(shard_id)
        if members is None:
            members = range(1, self._total_nodes + 1)
        return [node_id for node_id in members if node_id != self._node_id]
    
    def shard_replica_ids(self, shard_id: int) -> List[int]:
        """Every node serving ``shard_id``, this one included."""
        return sorted([self._node_id] + list(self._shard_peers.get(shard_id, [])))

    def shard_address(self, shard_id: int) -> Optional[str]:
        """Where this node serves ``shard_id``, or None in process."""
        return self._shard_addresses.get(shard_id)

    def leader_address(self, shard_id: int) -> Optional[str]:
        """Where this node believes ``shard_id``'s leader can be reached, if it can say.

        Only a node that is not the leader, and that has heard from one, can answer: this
        node's own address is the one a caller has just been refused at, and a node that
        thought it led is exactly the node whose belief a caller cannot use.  None
        everywhere else, which is what leaves a client to read the routing table - and
        None is the only possible answer for an in-process server, since nothing is
        listening at an address to send anyone to.
        """
        if self._peer_addresses is None:
            return None

        node = self._shards.get(shard_id)
        if node is None:
            return None
        leader_id = node.leader_id
        if leader_id is None or leader_id == self._node_id:
            return None

        base_address = self._peer_addresses.get(leader_id)
        if base_address is None:
            return None
        return self._peer_address(base_address, shard_id)

    def start_shards(self, state_machine_factory: Callable[[], StateMachine],
                     storage_factory: Optional[Callable[[int, int], RaftStorage]] = None,
                     peer_addresses: Optional[Dict[int, str]] = None,
                     shard_nodes: Optional[Dict[int, List[int]]] = None):
        """Start this node's shards, in process unless ``peer_addresses`` is given.

        The factories and the addresses are kept rather than used and dropped: a split
        asks for one more shard, and :meth:`add_shard` has to build it the way these
        were built.

        ``shard_nodes`` says which nodes serve each shard, and a shard this node is not
        in is not built here at all: a node holding a group it is not a member of would
        be a group that answers to nobody, and - in network mode - a port bound for a
        shard this node does not serve.  A shard with no entry is served by every node,
        which is what every caller that does not pass this gets.
        """
        self._state_machine_factory = state_machine_factory
        self._storage_factory = storage_factory
        self._peer_addresses = peer_addresses
        if shard_nodes:
            self._shard_members = {shard_id: sorted(set(members))
                                   for shard_id, members in shard_nodes.items()}
        if peer_addresses is not None:
            self._base_address = peer_addresses[self._node_id]

        for shard_id in range(self._num_shards):
            if self._serves(shard_id):
                self.add_shard(shard_id)

        mode = " (network mode)" if peer_addresses is not None else ""
        print(f"ShardServer {self._node_id} started with {len(self._shards)} of "
              f"{self._num_shards} shards{mode}")

    def start_shards_network(self, state_machine_factory: Callable[[], StateMachine],
                             peer_addresses: Dict[int, str],
                             storage_factory: Optional[Callable[[int, int], RaftStorage]] = None,
                             shard_nodes: Optional[Dict[int, List[int]]] = None):
        self.start_shards(state_machine_factory, storage_factory, peer_addresses, shard_nodes)

    def _serves(self, shard_id: int) -> bool:
        """Whether this node is one of ``shard_id``'s members.

        Everything is a member of every shard when nothing was said, which is the model
        this started with and what a shard created by a split still gets.
        """
        members = self._shard_members.get(shard_id)
        return members is None or self._node_id in members

    def add_shard(self, shard_id: int, members: Optional[List[int]] = None,
                  ) -> MemoryRaftNode:
        """Build one more Raft group for ``shard_id`` on this node, and serve it.

        This is how the groups the node started with are built, and a split goes
        through it rather than growing its own copy: the new shard has to have the
        same state machine and storage as its peers, and in network mode a port of its
        own.  A group that existed only in memory would be a shard whose published
        address nothing answers on - a range no client could reach.

        ``members`` is the other caller: a move builds the group that will own a shard
        on the nodes it was given, which is a set this node was not started with.  What
        it is given is remembered for the shard, so the ports, the storage and the peers
        of the group are all worked out from the one answer.
        """
        if self._state_machine_factory is None:
            raise RuntimeError("start the shards on this server before adding one")

        if members is not None:
            self._shard_members[shard_id] = sorted(set(members))

        storage = None
        if self._storage_factory is not None:
            storage = self._storage_factory(self._node_id, shard_id)

        peers = self._get_peer_nodes(shard_id)

        network_client = None
        if self._peer_addresses is not None:
            import grpc
            from .raft_servicer import RaftServicer
            from .client_servicer import ClientServicer
            from .network_client import RaftNetworkClient
            from oxidedb.proto.raft_pb2_grpc import add_RaftServiceServicer_to_server
            from oxidedb.proto.client_pb2_grpc import add_ClientServiceServicer_to_server
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
            # The client's six primitives on the same port: a shard's address is where
            # that shard is, and a client sent to one of them should not need a second
            # address to ask it anything.  The hint a refusal carries is therefore an
            # address of exactly this kind - another node's shard port.
            add_ClientServiceServicer_to_server(
                ClientServicer(node, leader_address=lambda: self.leader_address(shard_id)),
                server)
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


class MigrationPhase:
    """Where a move of a shard has got to.

    A shard being moved is a shard in two places at once, and which of these a move is in
    is how a caller - and a cluster that has just come back - tells "nothing has happened
    yet" from "the rows are in the new group and the switch is owed".  The order is the
    protocol: freeze, copy, propose, done.
    """

    FREEZING = "freezing"
    COPYING = "copying"
    PROPOSING = "proposing"
    DONE = "done"


@dataclass
class MigrationState:
    """One move of one shard, as the cluster making it remembers it."""

    shard_id: int
    #: Where the shard is going: a replica set, named by the caller of ``move_shard``.
    target_nodes: List[int]
    #: Where it is coming from, which is the set that answers for it - and the one the
    #: routing table names - until the proposal lands.
    source_nodes: List[int] = field(default_factory=list)
    phase: str = MigrationPhase.FREEZING
    #: The keys this move has copied into the new group so far.
    copied_keys: List[bytes] = field(default_factory=list)
    #: The rows as they were read at the freeze, which is the one moment at which they
    #: are the whole truth about the range.  In this process only: a cluster that comes
    #: back reads them out of the source again rather than out of a note.
    rows: List[Tuple[bytes, bytes]] = field(default_factory=list)


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
        #: Moves of a shard that have not finished, keyed by the shard being moved.  A
        #: shard with one of these has two groups for a moment - the one the routing
        #: table names and the one it is about to - and this is what says which of them
        #: a lookup means, and what a restarted cluster finds waiting for it.
        self._migrations: Dict[int, MigrationState] = {}
        #: Why the last move could not be started or could not be copied.
        self._last_migration_error: Optional[str] = None
    
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
              shard_nodes: Optional[Dict[int, List[int]]] = None,
              lock_cleaner_interval: Optional[float] = DEFAULT_LOCK_CLEANER_INTERVAL,
              lock_cleaner_ttl: float = DEFAULT_LOCK_TTL,
              metadata=None,
              metadata_publish_interval: float = DEFAULT_PUBLISH_INTERVAL):
        for node_id in range(1, self._num_nodes + 1):
            server = ShardServer(node_id, self._num_shards, self._num_nodes,
                                 get_peer_shard_node=self._get_peer_shard_node)
            server.set_range_map(self._range_map)
            server.start_shards(state_machine_factory, storage_factory,
                                shard_nodes=shard_nodes)
            self._shard_servers[node_id] = server

        self.start_lock_cleaner(lock_cleaner_interval, lock_cleaner_ttl)
        # The notes are read before anything publishes and acted on after: a publisher
        # that started first could see the table a step ahead of this cluster and take
        # this cluster's own split for someone else's keyspace.
        self._load_pending_splits()
        self._load_pending_migrations()
        self.start_metadata_publisher(metadata, metadata_publish_interval)
        self._finish_pending_splits()
        print(f"Sharded cluster started with {self._num_nodes} nodes and {self._num_shards} shards")
    
    def start_network(self, state_machine_factory: Callable[[], StateMachine],
                      peer_addresses: Dict[int, str],
                      storage_factory: Optional[Callable[[int, int], RaftStorage]] = None,
                      shard_nodes: Optional[Dict[int, List[int]]] = None,
                      lock_cleaner_interval: Optional[float] = DEFAULT_LOCK_CLEANER_INTERVAL,
                      lock_cleaner_ttl: float = DEFAULT_LOCK_TTL,
                      metadata=None,
                      metadata_publish_interval: float = DEFAULT_PUBLISH_INTERVAL):
        for node_id in range(1, self._num_nodes + 1):
            server = ShardServer(node_id, self._num_shards, self._num_nodes,
                                 get_peer_shard_node=self._get_peer_shard_node)
            server.set_range_map(self._range_map)
            server.start_shards_network(state_machine_factory, peer_addresses,
                                        storage_factory, shard_nodes)
            self._shard_servers[node_id] = server
        
        self.start_lock_cleaner(lock_cleaner_interval, lock_cleaner_ttl)
        self._load_pending_splits()
        self._load_pending_migrations()
        self.start_metadata_publisher(metadata, metadata_publish_interval)
        self._finish_pending_splits()
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
        """The nodes whose Raft group serves ``shard_id``.

        For a shard being moved that is the group it is leaving: until the routing table
        is told about the move, that is the group which answers for the range, and the
        new one is a group nothing routes to yet.  See :meth:`_serving_nodes`.
        """
        for node_id in self._serving_nodes(shard_id):
            server = self._shard_servers[node_id]
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
        for node_id in self._serving_nodes(shard_id):
            address = self._shard_servers[node_id].shard_address(shard_id)
            if address is not None:
                addresses[node_id] = address
        return addresses

    def possible_ranges(self) -> List[Dict[int, tuple]]:
        """The range maps this cluster's own keyspace could be in, right now.

        A cluster that is splitting has two answers for a moment: the map its servers
        route by, and the map the table gets once the split lands.  Both are this
        cluster's own placement, which is what the publisher needs to tell apart from
        somebody else's - so it is given both rather than one and left to guess.

        One entry when nothing is in flight, which is the usual case.
        """
        now = self.range_map()
        settled = dict(now)
        for pending in self._pending_splits.values():
            start, end = settled.get(pending["shard_id"], (None, None))
            if start is None:
                continue
            settled[pending["shard_id"]] = (start, pending["split_key"])
            settled[pending["new_shard_id"]] = (pending["split_key"], end)
        return [now] if settled == now else [now, settled]

    def shard_leader(self, shard_id: int) -> Optional[Tuple[int, int]]:
        """The node leading ``shard_id``, and the term it leads at, if one does."""
        for node_id in self._serving_nodes(shard_id):
            node = self._shard_servers[node_id].get_shard_node(shard_id)
            if node is not None and node.state == NodeState.LEADER:
                return (node_id, node.current_term)
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
        shard_id = locate(self._range_map, key)
        for node_id in self._serving_nodes(shard_id):
            node = self._shard_servers[node_id].get_leader_shard_for_key(key)
            if node:
                return (node_id, node)
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

            rows = self._rows_above(leader, shard_id, split_key)

            new_shard_id = self._num_shards
            self._num_shards += 1
            pending = {"shard_id": shard_id, "split_key": split_key,
                       "new_shard_id": new_shard_id, "rows": rows}
            self._pending_splits[shard_id] = pending

            # Written down before anything is moved: a process that dies in the middle
            # of the copy has to come back knowing what it was doing.  Then the new
            # shard is built the way each server built the shards it started with -
            # same state machine, same storage, and in network mode a port at the
            # address the table is about to publish.
            self._remember_split(pending)
            self._ensure_shard(new_shard_id)

            copied = True
            return self._finish_split(pending)
        finally:
            if not copied:
                # Nothing of this split exists anywhere, so there is nothing a write
                # could land outside of: the shard goes back to answering as it was.
                self._unfreeze_shard(shard_id)

    def _finish_split(self, pending: Dict[str, Any]) -> bool:
        """Copy what the new shard is missing, then tell the routing table.

        Called for the first attempt and for every retry of it.  A retry that finds rows
        already in the new shard - which is what a split whose proposal was refused, or
        whose response was lost, or that died part way through the copy, comes back to -
        copies only the rest: the source has been frozen since they were read, so a row
        that is already there is the row, and copying it again would be work for nothing.
        What is left is the proposal, which the group recognises as the split it already
        applied.
        """
        if not self._copy_what_is_missing(pending):
            return False

        if not self._publish_split(pending):
            return False

        # Re-range this process first: from here on it is the map the table has, and
        # the publisher - which compares the two - sees a split that is finished rather
        # than a cluster whose ranges disagree with it.
        self._apply_split_locally(pending)
        self._pending_splits.pop(pending["shard_id"], None)
        self._forget_split(pending)
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

    def _copy_what_is_missing(self, pending: Dict[str, Any]) -> bool:
        """Move the rows of the right half that the new shard does not already hold.

        A retry starts from whatever the process that died had managed to copy, so the
        question is asked per row rather than per split: a split that got one row across
        before it stopped has one row less to move, and one that got none has all of
        them.  Asking it per split would re-copy the rows that did make it, which is the
        same data written twice and a second timestamp on a row that already had one.

        The version already in the new shard, at the same timestamp, is the row - the
        source has been frozen since they were read, so it cannot have moved on.  A row
        the source no longer has is skipped: it was deleted, and there is nothing left to
        move.  A row the new shard has at another timestamp is not the same row, and is
        copied over.

        The new shard is read through a leader that has committed an entry of its own
        term: a row being in the group's log and the group's leader being able to see it
        are two different things straight after a restart (see
        :meth:`_wait_for_shard_to_catch_up`), and believing the second when only the
        first is true would lose the row.
        """
        target = self._wait_for_shard_to_catch_up(pending["new_shard_id"])
        source = self._shard_leader_node(pending["shard_id"])
        if target is None or source is None:
            self._last_split_error = (f"shard {pending['new_shard_id']} or "
                                      f"{pending['shard_id']} has no leader")
            return False

        source_storage = source._state_machine._storage
        target_storage = target._state_machine._storage
        for key, value in pending["rows"]:
            expected = source_storage.get_latest_version(key)
            if expected is None:
                continue
            actual = target_storage.get_latest_version(key)
            if actual is not None and actual.timestamp == expected.timestamp:
                continue
            self._move_row(source, target, key, value)
        return True

    def move_shard(self, shard_id: int, target_nodes: List[int]) -> bool:
        """Move ``shard_id`` to a new group on ``target_nodes``: freeze it, copy its rows.

        Called to start a move and to look again at one that is already under way.

        The first half of a move, in the order the split uses and for the same reason:
        the source is frozen before its rows are read, the rows are copied into the group
        that will own them, and only after that could the routing table be told - because
        the table is what clients route by, and a range it hands out has to be a range
        whose data is already there.  The last step is not wired yet, and a move that has
        copied its rows says so rather than pretending: it raises ``NotImplementedError``
        with the shard left frozen and the move remembered, which is the only state the
        completion can start from.

        ``target_nodes`` is the replica set the shard is to have, named by the caller and
        derived from nothing here: which nodes a shard should end up on is a policy - a
        rebalancer, an operator - and a cluster that worked it out for itself would have
        to guess a replica count as well as a place.  The set has to be disjoint from the
        one serving the shard now, which is the rule the routing table checks a
        ``MOVE_SHARD`` proposal against: a set that overlapped would need a group replaced
        under an id it already holds, and this project's Raft does not change a group's
        membership in place.

        Looking again at a shard that is already moving is how the move is retried: the
        same target set asks for the copy to be finished, which is idempotent because a
        row already in the new group at the same timestamp is the row.  A different target
        set is refused - there is no answer to that which would not lose one of the two.

        A ``False`` answer means nothing was copied and nothing is frozen: the rows are
        where they were and the shard answers as it did.  Why is in
        :meth:`migration_error`.
        """
        target_nodes = sorted({int(node_id) for node_id in target_nodes})
        if not target_nodes:
            self._last_migration_error = "a move needs somewhere to move the shard to"
            return False

        in_flight = self._migrations.get(shard_id)
        if in_flight is not None:
            # A shard that is already moving.  A caller asking for the same move is
            # picking up the one in flight - the same rows, the same new group, the same
            # proposal - and a caller asking for a different one has no answer here that
            # would not lose one of the two.
            if in_flight.target_nodes != target_nodes:
                self._last_migration_error = (
                    f"shard {shard_id} is already moving to {in_flight.target_nodes}")
                return False
            return self._finish_move(in_flight)

        old_range = self._range_map.get(shard_id)
        if old_range is None:
            self._last_migration_error = f"shard {shard_id} is not in this cluster's ranges"
            return False

        unknown = [node_id for node_id in target_nodes
                   if node_id not in self._shard_servers]
        if unknown:
            self._last_migration_error = f"nodes {unknown} are not nodes of this cluster"
            return False

        source_nodes = self.shard_replica_ids(shard_id)
        overlap = sorted(set(target_nodes) & set(source_nodes))
        if overlap:
            self._last_migration_error = (
                f"nodes {overlap} already serve shard {shard_id}, and a node cannot hold "
                f"two groups for one shard")
            return False

        leader = self._shard_leader_node(shard_id)
        if leader is None:
            self._last_migration_error = f"shard {shard_id} has no leader"
            return False

        start, end = old_range
        state = MigrationState(shard_id=shard_id, target_nodes=target_nodes,
                               source_nodes=source_nodes)
        self._migrations[shard_id] = state

        # Freeze first and look at the locks afterwards, which is the order that matters:
        # the other way round, a prewrite could land between the look and the freeze, and
        # a copy taken with a commit in flight is a write lost at the moment the row
        # moved.  Nothing new can arrive while the shard is frozen, so what is left in
        # flight is a transaction that prewrote before it.
        self._freeze_shard(shard_id, reason="migration", node_ids=source_nodes)
        copied = False
        try:
            if not self._drain_shard(shard_id, node_ids=source_nodes):
                self._last_migration_error = f"shard {shard_id} did not stop taking writes"
                return False

            if self._locks_in_range(leader, start, end):
                self._last_migration_error = (
                    f"a transaction holds a lock in shard {shard_id}; its rows cannot be "
                    f"read at one moment while one is in flight")
                return False

            state.rows = self._committed_rows(leader, start, end)

            # Written down before a row is moved: a process that dies in the middle of
            # the copy has to come back knowing which shard was moving, and where to.
            self._remember_migration(state)

            copied = True
            return self._finish_move(state)
        finally:
            if not copied:
                # Nothing of this move exists anywhere: no group was built and no row was
                # copied, so the shard goes back to answering as it was.
                self._migrations.pop(shard_id, None)
                self._unfreeze_shard(shard_id, node_ids=source_nodes)

    def _finish_move(self, state: MigrationState) -> bool:
        """Copy the rows into the new group, then stop at the proposal that is owed.

        Called for the first attempt and for every retry of it, and idempotent for the
        reason the split's is: a row already in the new group at the same timestamp is
        the row, and copying it again would be a second timestamp on a row that already
        has one - the source has been frozen since the rows were read, so nothing it
        holds has moved on.

        The group is built here rather than before the freeze because the rows were read
        first: a target node that already served the shard is a node this refuses to move
        onto (see :meth:`move_shard`), so building it is adding a group where there was
        none - and the group it replaces nothing of is the one the copy is written into.
        """
        shard_id = state.shard_id
        source = self._wait_for_leader_on(state.source_nodes, shard_id)
        if source is None:
            self._last_migration_error = f"shard {shard_id} has no leader"
            return False

        if not state.rows:
            # A move this process did not start: a cluster that came back found the note,
            # which says which shard was moving and where to, and nothing else - so the
            # rows are read out of the source, which has been frozen since it started and
            # is the only place they exist, rather than out of the note.
            start, end = self._range_map[shard_id]
            state.rows = self._committed_rows(source, start, end)

        for node_id in state.target_nodes:
            server = self._shard_servers[node_id]
            if server.get_shard_node(shard_id) is None:
                server.add_shard(shard_id, members=state.target_nodes)

        target = self._wait_for_leader_on(state.target_nodes, shard_id)
        if target is None:
            self._last_migration_error = (
                f"the new group for shard {shard_id} on {state.target_nodes} elected no "
                f"leader")
            return False

        state.phase = MigrationPhase.COPYING
        if not self._copy_rows(source, target, state):
            return False

        state.phase = MigrationPhase.PROPOSING
        raise NotImplementedError(
            f"shard {shard_id} has its rows copied into {state.target_nodes} and is still "
            f"frozen: the proposal that makes that group the shard's is not wired yet")

    def _copy_rows(self, source_leader: MemoryRaftNode, target_leader: MemoryRaftNode,
                   state: MigrationState) -> bool:
        """Copy the rows the new group does not already have, as the versions they are."""
        source_storage = source_leader._state_machine._storage
        target_storage = target_leader._state_machine._storage
        for key, value in state.rows:
            expected = source_storage.get_latest_version(key)
            if expected is None:
                continue
            actual = target_storage.get_latest_version(key)
            if actual is not None and actual.timestamp == expected.timestamp:
                continue
            result = self._move_row(source_leader, target_leader, key, value)
            if result is not None and not result.success:
                # A row the new group refused is a row it does not have, and a move that
                # carried on would leave a shard whose table entry names a group that is
                # missing one of its rows.
                self._last_migration_error = (
                    f"the new group refused a row of shard {state.shard_id}: "
                    f"{result.error_msg}")
                return False
            state.copied_keys.append(key)
        return True

    def recover_splits(self) -> List[int]:
        """Finish the splits this cluster was in the middle of when it stopped.

        A split writes down what it is doing before it does it - the shard, the split
        point, the id of the shard it is creating - and the note is dropped only once
        the routing table has the new range.  A cluster that comes back and finds one
        is a cluster whose rows may already be in the new shard's group, or may not be,
        if it stopped before the copy had finished; either way the answer is the same
        one a retry gets: freeze the source, copy whatever the new shard is missing,
        and make the proposal.

        The rows are read back out of the source shard's own state rather than out of
        the note, so what gets copied is what the shard would answer with.  A note
        carrying a copy of the rows would be a second copy of the shard, taken at some
        earlier moment, and a copy of a copy is how a split loses a row.

        Returns the shards whose split it finished.  A split it could not finish - the
        shard has not elected a leader yet, the table cannot be reached - is left
        frozen and still remembered, and the next call picks it up.
        """
        self._load_pending_splits()
        return self._finish_pending_splits()

    def _load_pending_splits(self) -> None:
        """Read the split notes this cluster's shards left behind, without acting."""
        for shard_id in sorted(self._range_map):
            if shard_id in self._pending_splits:
                continue
            pending = self._load_split_record(shard_id)
            if pending is not None:
                self._pending_splits[shard_id] = pending

    def _load_pending_migrations(self) -> None:
        """Read the move notes this cluster's shards left behind, and freeze them.

        A note means a move of that shard was in flight when the process stopped: its
        rows may already be in the new group, and the routing table may or may not name
        it.  The one thing that must not happen while that is unknown is the shard taking
        rows - a row written into it now would be one the new group does not have, on a
        shard the table may already have given away - so the source goes back to being
        frozen here, and stays frozen until the move is finished.  Finishing one is a
        caller's job for now: nothing resumes a move on its own yet.
        """
        for shard_id in sorted(self._range_map):
            if shard_id in self._migrations:
                continue
            state = self._load_migration_record(shard_id)
            if state is None:
                continue
            self._migrations[shard_id] = state
            self._freeze_shard(shard_id, reason="migration", node_ids=state.source_nodes)

    def _finish_pending_splits(self) -> List[int]:
        """Finish every split this cluster knows it was in the middle of."""
        return [shard_id for shard_id in sorted(self._pending_splits)
                if self._resume_split(shard_id)]

    def _resume_split(self, shard_id: int) -> bool:
        """Pick a split back up.  See :meth:`recover_splits`."""
        pending = self._pending_splits.get(shard_id)
        if pending is None:
            return False

        # It was copied, or it was about to be.  Either way the shard may not take new
        # rows until the table says where they go, and a restarted node has to be told
        # again: a freeze is a local fact, and it died with the process that held it.
        self._freeze_shard(shard_id)
        self._ensure_shard(pending["new_shard_id"])

        source = self._wait_for_shard_leader(shard_id)
        if source is None:
            self._last_split_error = f"shard {shard_id} has no leader"
            return False

        pending["rows"] = self._rows_above(source, shard_id, pending["split_key"])
        return self._finish_split(pending)

    def _remember_split(self, pending: Dict[str, Any]) -> None:
        """Write a split down, on every replica of the shard being split.

        The window between the copy and the proposal is the one place this cluster
        holds state that exists nowhere else: rows in the new shard's group, and a
        freeze that nothing has recorded.  A process that dies there comes back with no
        idea it was doing anything, so the intent goes into the source shard's own
        storage - the thing that does survive a restart - before the first row moves.

        Every replica writes its own copy.  The note says which shard is being split,
        and any replica holding that shard can be the one that finds it.
        """
        record = msgpack.packb({"shard_id": pending["shard_id"],
                                "split_key": pending["split_key"],
                                "new_shard_id": pending["new_shard_id"]},
                               use_bin_type=True)
        key = self._split_record_key(pending["shard_id"])
        for node in self._shard_nodes(pending["shard_id"]):
            if node._storage is not None:
                node._storage.save_admin(key, record)

    def _forget_split(self, pending: Dict[str, Any]) -> None:
        """Drop the note: the table has the range, so there is nothing to pick up."""
        key = self._split_record_key(pending["shard_id"])
        for node in self._shard_nodes(pending["shard_id"]):
            if node._storage is not None:
                node._storage.delete_admin(key)

    def _load_split_record(self, shard_id: int) -> Optional[Dict[str, Any]]:
        """The split this shard was in the middle of, as one of its replicas wrote it."""
        key = self._split_record_key(shard_id)
        for node in self._shard_nodes(shard_id):
            if node._storage is None:
                continue
            raw = node._storage.load_admin(key)
            if raw is None:
                continue
            record = msgpack.unpackb(raw, raw=False)
            if int(record["shard_id"]) != shard_id:
                # A note for another shard, which is what a storage reused by id would
                # look like.  Nothing here can act on it.
                continue
            return {"shard_id": shard_id,
                    "split_key": bytes(record["split_key"]),
                    "new_shard_id": int(record["new_shard_id"]),
                    "rows": []}
        return None

    @staticmethod
    def _split_record_key(shard_id: int) -> str:
        return f"{SPLIT_RECORD_PREFIX}/{shard_id}"

    def _remember_migration(self, state: MigrationState) -> None:
        """Write a move down, on every replica of the shard being moved.

        The window this covers is the split's, one size larger: between the copy and the
        proposal the rows are in a group the routing table has never heard of, and a
        process that dies there comes back with no idea that a shard of its own is in two
        places.  So the intent goes into the source shard's own storage - the thing that
        does survive a restart - before the first row moves.

        What is not in the note is how far the move got.  A phase that said "done" would
        be a lie the moment the process writing it died; what a cluster that comes back
        knows is that the move did not finish, and what it does about that is freeze the
        source and copy again, which is what a retry does anyway.
        """
        record = msgpack.packb({"shard_id": state.shard_id,
                                "source_nodes": list(state.source_nodes),
                                "target_nodes": list(state.target_nodes)},
                               use_bin_type=True)
        key = self._migration_record_key(state.shard_id)
        for node in self._nodes_on(state.source_nodes, state.shard_id):
            if node._storage is not None:
                node._storage.save_admin(key, record)

    def _load_migration_record(self, shard_id: int) -> Optional[MigrationState]:
        """The move this shard was in the middle of, as one of its replicas wrote it."""
        key = self._migration_record_key(shard_id)
        for node in self._shard_nodes(shard_id):
            if node._storage is None:
                continue
            raw = node._storage.load_admin(key)
            if raw is None:
                continue
            record = msgpack.unpackb(raw, raw=False)
            if int(record["shard_id"]) != shard_id:
                # A note for another shard, which is what a storage reused by an id would
                # look like.  Nothing here can act on it.
                continue
            return MigrationState(
                shard_id=shard_id,
                target_nodes=[int(node_id) for node_id in record["target_nodes"]],
                source_nodes=[int(node_id) for node_id in record["source_nodes"]])
        return None

    @staticmethod
    def _migration_record_key(shard_id: int) -> str:
        return f"{MIGRATION_RECORD_PREFIX}/{shard_id}"

    def _ensure_shard(self, shard_id: int) -> None:
        """Make sure every node of the cluster serves ``shard_id``.

        A restarted cluster builds the shards it was started with and nothing else - it
        does not read its ranges from the table - so the shard a split created has to
        be built again before that split can be finished.
        """
        for server in self._shard_servers.values():
            if server.get_shard_node(shard_id) is None:
                server.add_shard(shard_id)
        self._num_shards = max(self._num_shards, shard_id + 1)

    def _rows_above(self, leader: MemoryRaftNode, shard_id: int, split_key: bytes):
        """The rows of ``shard_id``'s range that belong to the shard above the point."""
        start, end = self._range_map[shard_id]
        return [(key, value) for key, value in self._committed_rows(leader, start, end)
                if key >= split_key]

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

    def _wait_for_shard_to_catch_up(self, shard_id: int,
                                    timeout: float = SPLIT_LEADER_TIMEOUT
                                    ) -> Optional[MemoryRaftNode]:
        """The node leading ``shard_id``, once it knows its own commit index.

        Stronger than :meth:`_wait_for_shard_leader`, and needed only by a caller that
        reads the leader's state machine.  A node that has just been elected cannot yet
        answer for the rows in its log: whether they committed is a question a majority
        acknowledging an entry of its own term answers, and until they have, the state
        machine is behind the group.  A split that read the shard there would take a row
        that is already copied for one that is not, and copy it a second time.
        """
        deadline = time.time() + timeout
        while True:
            leader = self._shard_leader_node(shard_id)
            if leader is not None and leader.has_committed_in_its_own_term():
                return leader
            if time.time() >= deadline:
                return None
            time.sleep(0.05)

    def split_error(self) -> Optional[str]:
        """Why the last split could not be published, if it could not be."""
        return self._last_split_error

    def migration_error(self) -> Optional[str]:
        """Why the last move could not be started or copied, if it could not be."""
        return self._last_migration_error

    def pending_splits(self) -> Dict[int, Dict[str, Any]]:
        """The splits that are waiting for the routing table, for callers that look."""
        return dict(self._pending_splits)

    def migration_state(self, shard_id: int) -> Optional[MigrationState]:
        """The move of ``shard_id`` this cluster is in the middle of, if there is one."""
        return self._migrations.get(shard_id)

    def migrations(self) -> Dict[int, MigrationState]:
        """Every move this cluster has not finished, for callers that look."""
        return dict(self._migrations)

    def _shard_nodes(self, shard_id: int) -> List[MemoryRaftNode]:
        """Every replica of ``shard_id`` this cluster is holding.

        Including the group a move has built for it and not yet switched to, which is
        why every caller that is not a split asks for its own nodes: see
        :meth:`_nodes_on`.
        """
        nodes = [server.get_shard_node(shard_id) for server in self._shard_servers.values()]
        return [node for node in nodes if node is not None]

    def _serving_nodes(self, shard_id: int) -> List[int]:
        """The nodes whose group is the shard's, as the routing table has it.

        A shard being moved has two groups for a moment, on two disjoint sets of nodes:
        the one the table names and the one the table is about to.  Everything that
        answers a question about *the* shard - who serves it, where its leader is, which
        address to publish - means the first of them until the proposal lands, because
        that is the one clients are being routed to.  Every other shard is served by
        every node, which is the answer this gives when nothing is in flight.
        """
        state = self._migrations.get(shard_id)
        if state is None:
            return sorted(self._shard_servers)
        return list(state.source_nodes)

    def _nodes_on(self, node_ids: List[int], shard_id: int) -> List[MemoryRaftNode]:
        """The nodes of ``node_ids`` that are holding a group for ``shard_id``."""
        nodes = [self._shard_servers[node_id].get_shard_node(shard_id)
                 for node_id in node_ids if node_id in self._shard_servers]
        return [node for node in nodes if node is not None]

    def _leader_on(self, node_ids: List[int], shard_id: int) -> Optional[MemoryRaftNode]:
        """The node among ``node_ids`` leading ``shard_id``, if one of them does.

        Asked for a set of nodes rather than for the shard, because a shard being moved
        has a leader on each side of the move and each step of it means one of them.
        """
        for node_id in node_ids:
            server = self._shard_servers.get(node_id)
            node = None if server is None else server.get_shard_node(shard_id)
            if node is not None and node.state == NodeState.LEADER:
                return node
        return None

    def _wait_for_leader_on(self, node_ids: List[int], shard_id: int,
                            timeout: float = SPLIT_LEADER_TIMEOUT) -> Optional[MemoryRaftNode]:
        """The leader among ``node_ids``, once it knows its own commit index.

        The same wait the split makes for the shard it creates, for the same reason: a
        node that has just been elected cannot yet answer for the rows in its log, and a
        copy that believed it could would take a row that is already there for one that
        is not - or, worse, write into a group that has not agreed on anything yet.
        """
        deadline = time.time() + timeout
        while True:
            leader = self._leader_on(node_ids, shard_id)
            if leader is not None and leader.has_committed_in_its_own_term():
                return leader
            if time.time() >= deadline:
                return None
            time.sleep(0.05)

    def _freeze_shard(self, shard_id: int, reason: str = "split",
                      node_ids: Optional[List[int]] = None) -> None:
        """Stop the shard taking new rows, on every replica.  See MemoryRaftNode.

        ``node_ids`` is for a shard being moved: it has two groups for a moment, and the
        freeze belongs on the one whose rows are being read - the one the table names -
        rather than on the group that is being copied into.
        """
        nodes = self._shard_nodes(shard_id) if node_ids is None else self._nodes_on(node_ids, shard_id)
        for node in nodes:
            node.freeze_writes(reason)

    def _unfreeze_shard(self, shard_id: int, node_ids: Optional[List[int]] = None) -> None:
        nodes = self._shard_nodes(shard_id) if node_ids is None else self._nodes_on(node_ids, shard_id)
        for node in nodes:
            node.resume_writes()

    def _drain_shard(self, shard_id: int, node_ids: Optional[List[int]] = None) -> bool:
        """Wait out the writes that were admitted before the freeze."""
        nodes = self._shard_nodes(shard_id) if node_ids is None else self._nodes_on(node_ids, shard_id)
        return all(node.wait_for_writes_to_drain() for node in nodes)

    def _move_row(self, source_leader: MemoryRaftNode, target_leader: MemoryRaftNode,
                  key: bytes, value: bytes) -> Optional[ApplyResult]:
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
        return target_leader.propose(command)
    
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
