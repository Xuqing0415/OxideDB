# EXPERIMENTAL: sharding is wired end to end for a placement that never has to change -
# no migration and no follower read, so do not build a deployment on it
import time

import msgpack

from typing import Any, Dict, List, Optional, Callable, Tuple
from ..metadata.publisher import DEFAULT_PUBLISH_INTERVAL, MetadataPublisher
from ..shard.router import default_range_map, locate
from ..transaction.lock_cleaner import LockCleaner
from ..transaction.lock_resolver import DEFAULT_LOCK_TTL

#: How often the cluster's own lock cleaner looks for abandoned locks, in seconds.
DEFAULT_LOCK_CLEANER_INTERVAL = 30.0

#: Where a shard's in-progress split is written down, within that shard's storage.
SPLIT_RECORD_PREFIX = "split"

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
        # The notes are read before anything publishes and acted on after: a publisher
        # that started first could see the table a step ahead of this cluster and take
        # this cluster's own split for someone else's keyspace.
        self._load_pending_splits()
        self.start_metadata_publisher(metadata, metadata_publish_interval)
        self._finish_pending_splits()
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
        self._load_pending_splits()
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
