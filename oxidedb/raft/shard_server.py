# EXPERIMENTAL: sharding is wired end to end for a placement that never has to change -
# no follower read, and no move that finishes, so do not build a deployment on it
import os
import time

from typing import Any, Dict, List, Optional, Callable, Tuple
from ..client.node_client import (LocalNodeClient, LocalNodeClientFactory,
                                  NodeClient)
from ..client.routing import ShardLeaders
from ..metadata.publisher import DEFAULT_PUBLISH_INTERVAL, MetadataPublisher
from ..metadata.service import PROPOSE_ATTEMPTS, RETRY_BACKOFF
from ..shard.router import default_range_map, locate
from ..transaction.lock_cleaner import LockCleaner
from ..transaction.lock_resolver import DEFAULT_LOCK_TTL
from .recovery_notes import (MIGRATION_RECORD_PREFIX, SPLIT_RECORD_PREFIX, PendingNote,
                             forget_note, read_note, write_note)
from .recovery_runner import (LEADER_TIMEOUT, MIGRATION_DRAIN_SECONDS, MigrationPhase,
                              MigrationState, ProposalOutcome, ProposalResult,
                              RecoveryRunner)

#: How often the cluster's own lock cleaner looks for abandoned locks, in seconds.
DEFAULT_LOCK_CLEANER_INTERVAL = 30.0

#: How many ports of a node belong to its shards: shard ``s`` listens at that node's base
#: port plus ``s``, here and in :func:`oxidedb.launcher.ports_for`, so the two cannot
#: drift apart.  The two group ports are the two above this segment, which makes it the
#: bound on how many shards a node can have as well: one more would want the port the
#: routing table's group is holding.
SHARD_SEGMENT = 1000
from .node import MemoryRaftNode, RaftCluster, NodeState
from .state_machine import StateMachine, ApplyResult, ErrorCode
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
            # In-process, where the base port is one number for the whole cluster: each
            # node gets a block of the segment, the way one base port per node gives it
            # one on real ports, so two nodes' shards cannot land on one number.
            return self._base_port + self._node_id * SHARD_SEGMENT + shard_id
        return self._shard_port_of(self._base_address, shard_id)

    @staticmethod
    def _shard_port_of(base_address: str, shard_id: int) -> int:
        """The port the node at ``base_address`` serves ``shard_id`` on."""
        return int(base_address.split(":")[1]) + shard_id

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
    
    def shutdown_shard(self, shard_id: int) -> bool:
        """Stop serving one shard: its group, its port and its storage go together.

        The group a move leaves behind.  Its rows are in the group the routing table now
        names, so this one has nothing left to answer - and it has to stop answering,
        because a client still holding the old table would otherwise be answered by a
        group that no longer owns the range.

        The storage goes with it, which :meth:`shutdown` does not do: a whole cluster
        closing hands its storages to whoever built them, and this is one shard going on
        its own with nobody else to release the file.  That matters to the caller that
        means to move the directory out from under it - on Windows it has no choice, and
        a directory still being written to would not be a faithful copy anyway.

        False means this node was not serving the shard, which is what closing one twice
        looks like: a caller that died in the middle of closing things comes back and
        closes them again, and the second close is an answer rather than an error.
        """
        node = self._shards.pop(shard_id, None)
        if node is None:
            return False

        self._shard_peers.pop(shard_id, None)
        self._shard_addresses.pop(shard_id, None)
        node.shutdown()
        if node._storage is not None:
            node._storage.close()
        return True

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
        #: The recovery's own body, run against this cluster: the splits and the moves
        #: that are in flight, and why the last attempt at either could not finish.  It
        #: belongs to the recovery rather than to a side - ``docs/recovery.md``, section
        #: 3 - so this cluster reaches it through here, and so does the beginning of a
        #: split or a move, which puts the entry in it that the note is the disk copy of.
        self._recovery_runner = RecoveryRunner(self)
        #: Which nodes serve each shard, when they are not every node of the cluster.  A
        #: shard with no entry is served by all of them, which is the model this started
        #: with; a move is what makes an entry, and this is how the cluster's own answer
        #: about a shard follows the table without asking it.
        self._placed_shards: Dict[int, List[int]] = {}
        #: The data directories this cluster has moved aside, in the order it did.  Kept
        #: for callers that look - a test, an operator - and never read back: nothing
        #: here opens one.
        self._orphan_dirs: List[str] = []
        #: The handles this cluster hands out for its own nodes, built when something
        #: first asks: one wrapper per (shard, node), which is what the two lookups
        #: below reach a group through, and the reason a caller that asked twice does
        #: not pay for the same handle twice.
        self._node_clients: Optional[LocalNodeClientFactory] = None
        #: The lookup that turns "who leads this shard" into a handle, built when
        #: something first asks.  Kept because a `ShardLeaders` remembers the refusals
        #: it has been walked through, which is worth keeping between two calls.
        self._leaders: Optional[ShardLeaders] = None
    
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
    
    def _remember_placement(self, shard_nodes: Optional[Dict[int, List[int]]]) -> None:
        """Take the placement this cluster was started with as its own answer for it.

        A node's servers know which shards they serve; the cluster has to know it too,
        because the cluster is what answers "who serves this shard" - to a publisher, to
        a client's route, to the rest of a move.  A shard nobody placed is served by every
        node, which is what an entry that is not here means.
        """
        if shard_nodes:
            self._placed_shards = {shard_id: sorted(set(members))
                                   for shard_id, members in shard_nodes.items()}

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
        self._remember_placement(shard_nodes)
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
        self._recovery_runner.load_pending_splits()
        self._recovery_runner.load_pending_migrations()
        self.start_metadata_publisher(metadata, metadata_publish_interval)
        self._recovery_runner.finish_pending_splits()
        self._recovery_runner.finish_pending_migrations()
        print(f"Sharded cluster started with {self._num_nodes} nodes and {self._num_shards} shards")
    
    def start_network(self, state_machine_factory: Callable[[], StateMachine],
                      peer_addresses: Dict[int, str],
                      storage_factory: Optional[Callable[[int, int], RaftStorage]] = None,
                      shard_nodes: Optional[Dict[int, List[int]]] = None,
                      lock_cleaner_interval: Optional[float] = DEFAULT_LOCK_CLEANER_INTERVAL,
                      lock_cleaner_ttl: float = DEFAULT_LOCK_TTL,
                      metadata=None,
                      metadata_publish_interval: float = DEFAULT_PUBLISH_INTERVAL):
        self._remember_placement(shard_nodes)
        for node_id in range(1, self._num_nodes + 1):
            server = ShardServer(node_id, self._num_shards, self._num_nodes,
                                 get_peer_shard_node=self._get_peer_shard_node)
            server.set_range_map(self._range_map)
            server.start_shards_network(state_machine_factory, peer_addresses,
                                        storage_factory, shard_nodes)
            self._shard_servers[node_id] = server
        
        self.start_lock_cleaner(lock_cleaner_interval, lock_cleaner_ttl)
        self._recovery_runner.load_pending_splits()
        self._recovery_runner.load_pending_migrations()
        self.start_metadata_publisher(metadata, metadata_publish_interval)
        self._recovery_runner.finish_pending_splits()
        self._recovery_runner.finish_pending_migrations()
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

    def node_ids(self) -> List[int]:
        """Every node of this cluster, in a fixed order.

        The set a split's new shard is served by.  It is this cluster's whole membership
        rather than the nodes holding the shard, and a process answers the same list, so
        the members the two sides each build are the members of one group.

        See :class:`RecoveryView.node_ids`.
        """
        return sorted(self._shard_servers)

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

        One entry when nothing is in flight, which is the usual case.  See
        :meth:`RecoveryRunner.possible_ranges`: the splits in flight are the recovery's
        state, so the second map is asked of it rather than worked out here.
        """
        return self._recovery_runner.possible_ranges()

    def shard_leader(self, shard_id: int) -> Optional[Tuple[int, int]]:
        """The node leading ``shard_id``, and the term it leads at, if one does."""
        for node_id in self._serving_nodes(shard_id):
            node = self._shard_servers[node_id].get_shard_node(shard_id)
            if node is not None and node.state == NodeState.LEADER:
                return (node_id, node.current_term)
        return None

    def leader_client(self, shard_id: int) -> Optional[NodeClient]:
        """Who leads ``shard_id``, as a client rather than as a node object.

        The same lookup the coordinator and the resolver ask, over this cluster's own
        nodes: :meth:`shard_leader` answers with an id, the id goes into a factory, and
        what comes back is a ``NodeClient`` - so a recovery that runs here reads and
        proposes exactly the way one that runs in a process of its own does, and the
        two can be compared.  The set it asks is :meth:`_serving_nodes`: the group the
        routing table names, which for a shard being moved is the one its rows are
        coming from.

        None while nobody leads it, which is an answer and not a failure.
        """
        if self._leaders is None:
            self._leaders = ShardLeaders(self, factory=self._node_client_factory())
        return self._leaders.leader_for_shard(shard_id)

    def leader_client_for_nodes(self, shard_id: int,
                                nodes: List[int]) -> Optional[NodeClient]:
        """The client for whichever of ``nodes`` leads ``shard_id``, or None.

        The question :meth:`leader_client` cannot answer, and the one a move asks: the
        group its rows are being copied *into* is not the group the routing table names
        yet, so the set comes from the caller that built it rather than from the table.
        A call of its own and not a default on the other one, because the two answer
        different questions and a caller that forgot the set would be handed the wrong
        group's leader by a signature that let it forget - see ``docs/recovery.md``,
        section 5.

        Each node is asked with the call the client service already has for it: a
        follower read index is answered by a leader that has confirmed an entry of its
        own term with a quorum, and refused by every other node, so the first node that
        answers with an index is the one to talk to.  Asked that way for the reason
        :meth:`RecoveryRunner._wait_for_group` waits the same way - a node that merely
        believes it leads is exactly the node whose belief is in question, and here it is
        the one that would be handed a copy.
        leads is exactly the node whose belief is in question, and here it is the one
        that would be handed a copy.

        No hint is followed, although a node that is not the leader answers with one:
        every member of the group is in ``nodes``, so the walk over the set is the
        whole answer, and the set is the caller's.  A handle onto a group that has gone
        away - the target of a move that was refused and is being tried again - answers
        as a follower for the same reason, so it is skipped rather than used.
        """
        factory = self._node_client_factory()
        for node_id in nodes:
            client = factory.get_client(shard_id, node_id)
            if client is None:
                continue
            read_index, _ = client.follower_read_index()
            if read_index is not None:
                return client
        return None

    def _client_for_node(self, shard_id: int, node: MemoryRaftNode) -> NodeClient:
        """The seam's handle for a node this cluster already has in its hand.

        Where the two worlds meet on the in-process side: the copy is written against
        ``NodeClient`` and nothing else, and something has to hand it one.  A process asks
        a factory, because it has ids and addresses and nothing else; this cluster has the
        node object, and asking the factory which node it is would be a question whose
        answer it already knows.  So the handle is built rather than looked up, and it is
        the handle the factory would have handed back for the pair - a wrapper holds
        nothing but the node - so nothing downstream can tell which of the two produced
        it.

        Not part of anything a recovery is handed, for that same reason: the argument is a
        node object, which is the one thing a process does not have.
        """
        return LocalNodeClient(node)

    def _node_client_factory(self) -> LocalNodeClientFactory:
        """The one factory the two lookups above build their handles through.

        Shared rather than built per call so that a wrapper is built once per
        (shard, node), which is what a factory is for - and so that both lookups hand
        out the same object for the same node rather than two that are equal in
        everything but identity.
        """
        if self._node_clients is None:
            self._node_clients = LocalNodeClientFactory(self)
        return self._node_clients

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
        pending = self._recovery_runner.pending_split(shard_id)
        if pending is not None:
            if pending["split_key"] != split_key:
                # A different split of a shard that is already half-split.  There is no
                # answer this could give that would not lose one of the two.
                return False
            return self._recovery_runner.finish_split(
                pending, self.leader_client(shard_id))

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
            # Written down before anything is moved: a process that dies in the middle
            # of the copy has to come back knowing what it was doing - and the note and
            # the copy of it in memory are one act, which is why the recovery writes
            # both.  Then the new shard is built the way each server built the shards it
            # started with - same state machine, same storage, and in network mode a
            # port at the address the table is about to publish.
            self._recovery_runner.remember_split(pending)
            self.ensure_serving(new_shard_id, sorted(self._shard_servers))

            copied = True
            return self._recovery_runner.finish_split(
                pending, self.leader_client(shard_id))
        finally:
            if not copied:
                # Nothing of this split exists anywhere, so there is nothing a write
                # could land outside of: the shard goes back to answering as it was.
                self._unfreeze_shard(shard_id)

    def apply_split_locally(self, shard_id: int, split_key: bytes,
                            new_shard_id: int) -> None:
        """Re-range this cluster, now that the routing table says so.

        One call and one map, taken by the side that holds the map: the servers route by
        it, so until it moves they would keep answering for the range the table has
        already given away, and two edits would leave a moment in which a live range
        belongs to neither shard and its keys are routed to shard zero without anything
        failing.

        See :class:`RecoveryView.apply_split_locally`.
        """
        new_range_map = dict(self._range_map)
        start, end = new_range_map[shard_id]
        new_range_map[shard_id] = (start, split_key)
        new_range_map[new_shard_id] = (split_key, end)
        self.update_range_map(new_range_map)

    def apply_move_locally(self, shard_id: int, nodes: List[int]) -> None:
        """Write down that the routing table now names ``nodes`` for ``shard_id``.

        The move's half of :meth:`apply_split_locally`: both are a write the routing table
        has just made, applied on the side that has to live with it, in one call because a
        caller that left a step out would be answering about a shard two groups are holding.
        Two of this cluster's answers are about the shard, and both become the new set's:

        * ``_placed_shards``, which is who serves the shard until the placement this cluster
          was moved to replaces the one it was started with - and until this call it has
          been answering with the group the move is leaving.
        * what the publisher last published about who *leads* the shard, which was about
          that same group, and the two groups' terms are not comparable: left alone, the
          publisher would not name the new group's leader until its term passed a term
          belonging to another group entirely.

        What is not here is the groups themselves: this call changes what this cluster says
        about the shard, and :meth:`ensure_serving` - the next step, after the drain window -
        changes what it holds.  Between the two they disagree on purpose, because the table
        has moved on and a client that cached it has not.

        See :class:`RecoveryView.apply_move_locally`.
        """
        self._placed_shards[shard_id] = sorted(set(nodes))
        if self._metadata_publisher is not None:
            self._metadata_publisher.forget_leader(shard_id)

    def move_shard(self, shard_id: int, target_nodes: List[int],
                   drain: float = MIGRATION_DRAIN_SECONDS) -> bool:
        """Move ``shard_id`` to a new group on ``target_nodes``: freeze, copy, tell, let go.

        Called to start a move and to look again at one that is already under way.

        The order is the split's, and for the same reason: the source is frozen before its
        rows are read, the rows are copied into the group that will own them, and the
        routing table is told only after that - because the table is what clients route by,
        and a range it hands out has to be a range whose data is already there.  Once the
        table agrees, the group that was left behind is let go
        (:meth:`RecoveryRunner._commit_move`).

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

        ``drain`` is how long the group the shard leaves goes on answering once the table
        has moved on, handed to :meth:`RecoveryRunner._commit_move`: a client routes by a
        table it cached, and the node it was sent to a moment ago is one it may still ask.
        A caller making
        and the node it was sent to a moment ago is one it may still ask.  A caller making
        a move waits it out; a cluster finishing one it found already written down does
        not, which is why it is a parameter rather than only a constant.

        A ``True`` answer means the table names the new group and the old one is closed.
        A ``False`` one means the move did not get there, and there are two ways that
        happens, told apart by :meth:`migration_error` and by whether the shard is frozen:

        * nothing was copied - the move could not be started, or its rows could not be read
          at one moment - so the shard is not frozen and answers exactly as it did;
        * the rows are copied and the table could not be reached, so the shard stays frozen
          with the move written down.  Asking again with the same target set finishes it,
          which is the same call: nothing about a move is resumed on its own.
        """
        target_nodes = sorted({int(node_id) for node_id in target_nodes})
        if not target_nodes:
            self._recovery_runner.record_migration_error(
                "a move needs somewhere to move the shard to")
            return False

        in_flight = self._recovery_runner.migration_state(shard_id)
        if in_flight is not None:
            # A shard that is already moving.  A caller asking for the same move is
            # picking up the one in flight - the same rows, the same new group, the same
            # proposal - and a caller asking for a different one has no answer here that
            # would not lose one of the two.
            if in_flight.target_nodes != target_nodes:
                self._recovery_runner.record_migration_error(
                    f"shard {shard_id} is already moving to {in_flight.target_nodes}")
                return False
            return self._recovery_runner.finish_move(in_flight, drain=drain)

        old_range = self._range_map.get(shard_id)
        if old_range is None:
            self._recovery_runner.record_migration_error(
                f"shard {shard_id} is not in this cluster's ranges")
            return False

        unknown = [node_id for node_id in target_nodes
                   if node_id not in self._shard_servers]
        if unknown:
            self._recovery_runner.record_migration_error(
                f"nodes {unknown} are not nodes of this cluster")
            return False

        source_nodes = self.shard_replica_ids(shard_id)
        overlap = sorted(set(target_nodes) & set(source_nodes))
        if overlap:
            self._recovery_runner.record_migration_error(
                f"nodes {overlap} already serve shard {shard_id}, and a node cannot hold "
                f"two groups for one shard")
            return False

        leader = self._shard_leader_node(shard_id)
        if leader is None:
            self._recovery_runner.record_migration_error(
                f"shard {shard_id} has no leader")
            return False

        start, end = old_range
        state = MigrationState(shard_id=shard_id, target_nodes=target_nodes,
                               source_nodes=source_nodes)
        self._recovery_runner.hold_migration(state)

        # Freeze first and look at the locks afterwards, which is the order that matters:
        # the other way round, a prewrite could land between the look and the freeze, and
        # a copy taken with a commit in flight is a write lost at the moment the row
        # moved.  Nothing new can arrive while the shard is frozen, so what is left in
        # flight is a transaction that prewrote before it.
        self._freeze_shard(shard_id, reason="migration", node_ids=source_nodes)
        copied = False
        try:
            if not self._drain_shard(shard_id, node_ids=source_nodes):
                self._recovery_runner.record_migration_error(
                    f"shard {shard_id} did not stop taking writes")
                return False

            if self._locks_in_range(leader, start, end):
                self._recovery_runner.record_migration_error(
                    f"a transaction holds a lock in shard {shard_id}; its rows cannot be "
                    f"read at one moment while one is in flight")
                return False

            state.rows = self._committed_rows(leader, start, end)

            # Written down before a row is moved: a process that dies in the middle of
            # the copy has to come back knowing which shard was moving, and where to.
            self._recovery_runner.remember_migration(state)

            copied = True
            return self._recovery_runner.finish_move(state, drain=drain)
        finally:
            if not copied:
                # Nothing of this move exists anywhere: no group was built and no row was
                # copied, so the shard goes back to answering as it was.
                self._recovery_runner.drop_migration(shard_id)
                self._unfreeze_shard(shard_id, node_ids=source_nodes)



    def addresses_on(self, shard_id: int, nodes: List[int]) -> Dict[int, str]:
        """Where each of ``nodes`` serves ``shard_id``, as those nodes bound it.

        Asked of a set, because the nodes a move is going to are not the nodes the shard
        is served by yet.

        See :class:`RecoveryView.addresses_on`.
        """
        return self._addresses_on(nodes, shard_id)

    def _addresses_on(self, node_ids: List[int], shard_id: int) -> Dict[int, str]:
        """Where each of ``node_ids`` serves ``shard_id``, as the servers bound it.

        The same answer :meth:`shard_addresses` gives, asked of a set of nodes rather
        than of the shard.  The nodes a shard is moving to are not the nodes it is served
        by yet, so a proposal that took its addresses from the shard's own answer would
        hand the table the addresses of the group it is leaving.
        """
        addresses = {}
        for node_id in node_ids:
            server = self._shard_servers.get(node_id)
            address = None if server is None else server.shard_address(shard_id)
            if address is not None:
                addresses[node_id] = address
        return addresses





    def ensure_serving(self, shard_id: int, nodes: List[int]) -> List[int]:
        """Make every node that serves ``shard_id`` one of ``nodes``, and no other.

        The cluster's own answer to the call the recovery makes, and one call for both
        directions for the same reason the process side's is one: a shard's placement and
        the groups that answer for it are two halves of one fact, so a caller that moved
        one without the other would leave a shard with two live groups, or with none.

        Two things, in this order:

        * every node in ``nodes`` that holds no group for the shard builds one, with the
          set it was given as its members.  A restarted cluster builds the shards it was
          told to serve and nothing else, so the group a split created, or the one a move
          went to, has to be built again before it can answer anything - and building it
          is also what reads back the rows a move committed through it.
        * every node that holds a group and is not in ``nodes`` closes it and puts its
          storage aside, which is what a move leaves behind: the range belongs to a group
          this one is not, and a client still routing by the old table must not be
          answered by a group that no longer owns it.

        ``nodes`` is the whole set and not a part of it, which is the caller's to name -
        the replica set a move is going to is a policy (see :meth:`move_shard`).  The
        whole cluster is the other answer there is, and the one a split's new shard gets;
        it is left unsaid rather than written down, because that is how a shard server
        already spells it (see ``ShardServer._serves``).

        What this call is not is the step that builds a group a move is still copying
        into.  That one closes nothing, and its members are the nodes it is given - which
        is a different set from "the nodes that should end up holding the shard", because
        the source is holding its own group at that moment and its members are its own.
        It is :meth:`_ensure_group_on`, a move calls it before the proposal and this call
        after, and the two are not one call for the reason a process does not have: one
        process holds one of those groups, and a cluster object holds both.

        What it returns is the nodes whose group it closed, so an empty list is a
        placement that was already in force - an answer rather than a failure, and the
        difference a caller that ran twice sees.

        What it does not touch is this cluster's own answer for who serves the shard,
        which is ``_placed_shards``: that is the routing table's placement and it moves
        when the table does, so a move builds its new group here before the proposal and
        goes on answering with the group it is leaving until the table agrees.
        """
        members = sorted(set(nodes))
        # Left unsaid when the set is the whole cluster, which is what a shard built at
        # start-up and a shard a split creates are: a shard server's own reading of "no
        # set was given" is every node, and naming them would be a second way of saying
        # it - which ``ShardServer._serves`` and ``ShardServer._get_peer_nodes`` both
        # answer the same way, so this is spelling rather than meaning.
        named = None if members == sorted(self._shard_servers) else members
        # Counted as one of the cluster's own, the way a shard it was started with is.
        self._num_shards = max(self._num_shards, shard_id + 1)
        for node_id in members:
            server = self._shard_servers[node_id]
            if server.get_shard_node(shard_id) is None:
                server.add_shard(shard_id, members=named)
        left = [node_id for node_id in sorted(self._shard_servers)
                if node_id not in members]
        return self._close_group_on(left, shard_id)

    def ensure_group_on(self, shard_id: int, nodes: List[int]) -> None:
        """Make sure every node of ``nodes`` holds a group for ``shard_id``.

        The build half on its own, which is what a move needs and a split does not: a
        move builds the group its rows are going into while the source is still the group
        the table names, so the nodes holding that shard are not one replica set.

        See :class:`RecoveryView.ensure_group_on`.
        """
        self._ensure_group_on(nodes, shard_id)

    def close_group_on(self, shard_id: int, nodes: List[int]) -> List[int]:
        """Close the group on exactly these nodes, and put their storage aside.

        The set is the caller's and the routing table is not: the placement is read by
        :meth:`serving_nodes` and this call does not go near it, where
        :meth:`ensure_serving` answers to whatever the table names.

        See :class:`RecoveryView.close_group_on`.
        """
        return self._close_group_on(nodes, shard_id)

    def _ensure_group_on(self, node_ids: List[int], shard_id: int) -> None:
        """Make sure every node of ``node_ids`` is holding a group for ``shard_id``.

        The set a move is going to, which is not a set this cluster was started with: a
        restarted cluster builds the shards it was told to serve and nothing else, so the
        group the routing table names has to be built here before it can answer anything.

        The build half of :meth:`ensure_serving` on its own, and the members are exactly
        ``node_ids``: a move builds this group while the source is still the group the
        table names, so the set of nodes that hold the shard is not a set that any one
        replica set describes - see the note there.

        What it comes back with is the copy.  A move's rows are committed through the group
        they are carried into, so the storage under the new group's name on each of its
        nodes is where they are; building the group is what reads them back, and it is the
        only reason a restarted cluster can serve a shard it was not placed on.
        """
        for node_id in node_ids:
            server = self._shard_servers[node_id]
            if server.get_shard_node(shard_id) is None:
                server.add_shard(shard_id, members=node_ids)

    def _close_group_on(self, node_ids: List[int], shard_id: int) -> List[int]:
        """Stop serving a shard on exactly ``node_ids``, and put their storage aside.

        The nodes are the caller's, and this is the primitive rather than the call:
        :meth:`ensure_serving` closes through it on the nodes a placement leaves out, and
        :meth:`RecoveryRunner._abort_move` names a set directly - the group a refused move
        built, which
        is the only one that call is entitled to close, since a refusal does not say what
        the table names instead.

        What it returns is the ones that actually had a group, which is how a caller tells
        "I closed it" from "it was already closed" - a group that is not there is an answer
        rather than an error, because a caller that died in the middle of this comes back
        and runs the whole of it again.
        """
        closed = []
        for node_id in sorted(set(node_ids)):
            server = self._shard_servers.get(node_id)
            node = None if server is None else server.get_shard_node(shard_id)
            if node is None:
                continue
            # Asked for before the close: the group is where its storage is known.
            directory = node._storage.data_dir if node._storage is not None else None
            if not server.shutdown_shard(shard_id):
                continue
            closed.append(node_id)
            orphan = self._rename_storage(directory, shard_id)
            if orphan is not None:
                self._orphan_dirs.append(orphan)
        return closed

    def _rename_storage(self, directory: Optional[str], shard_id: int) -> Optional[str]:
        """Move a retired shard's directory aside, under a name nothing reads.

        None means there was nothing to move: a storage that keeps nothing on disk, or one
        whose directory is already gone, which is what a second run finds.  A rename that
        fails is left to raise - it is the one thing here that a repeat would not turn
        into a no-op, and a caller told why is better than one told the move finished
        while the data sits under the name clients look in.
        """
        if directory is None or not os.path.isdir(directory):
            return None
        base = os.path.join(os.path.dirname(os.path.abspath(directory)),
                            f"orphan-shard-{shard_id}-{int(time.time())}")
        target, suffix = base, 1
        while os.path.exists(target):
            # Two shards retiring into one directory within the same second: only a test
            # does this, and a name that collided would be a rename that failed.
            target = f"{base}-{suffix}"
            suffix += 1
        os.rename(directory, target)
        return target

    def orphan_dirs(self) -> List[str]:
        """The directories this cluster has moved aside, in the order it did."""
        return list(self._orphan_dirs)

    def recover_splits(self) -> List[int]:
        """Finish the splits this cluster was in the middle of when it stopped.

        See :meth:`RecoveryRunner.recover_splits`, where the flow is written: this
        cluster and a node in a process of its own run the same body over themselves,
        and what differs between them is only what each of them holds.  The two start-up
        paths call :meth:`RecoveryRunner.load_pending_splits` and
        :meth:`RecoveryRunner.finish_pending_splits` around the publisher instead, which
        is the order they need and the order ``docs/recovery.md`` section 6 explains.
        """
        return self._recovery_runner.recover_splits()

    def recover_migrations(self) -> List[int]:
        """Finish the moves this cluster was in the middle of when it stopped.

        See :meth:`RecoveryRunner.recover_migrations`, where the flow is written: this
        cluster and a node in a process of its own run the same body over themselves,
        and what differs between them is only what each of them holds.  The two start-up
        paths call :meth:`RecoveryRunner.load_pending_migrations` and
        :meth:`RecoveryRunner.finish_pending_migrations` around the publisher instead,
        which is the order they need.
        """
        return self._recovery_runner.recover_migrations()




    def pending_notes(self, shard_id: int) -> List[PendingNote]:
        """The notes this cluster holds for ``shard_id``: a split's or a move's.

        Written on every replica of the shard they are about, and read back off whichever
        of them is still open - a replica whose storage has been closed cannot be asked,
        which is why a note there is not a failure.  At most one today: a shard cannot be
        splitting and moving at once, and a second would be a state neither flow can have
        been in.

        See :class:`RecoveryView.pending_notes`.
        """
        storages = self._storages_of(shard_id)
        notes = []
        for prefix in (SPLIT_RECORD_PREFIX, MIGRATION_RECORD_PREFIX):
            note = read_note(storages, shard_id, prefix)
            if note is not None:
                notes.append(note)
        return notes

    def remember_note(self, note: PendingNote) -> None:
        """Write ``note`` on every replica this cluster holds of the shard it is about.

        Which shard that is is the note's own answer rather than a second argument, and
        writing the same note twice is the same bytes.

        See :class:`RecoveryView.remember_note`.
        """
        write_note(self._storages_of(note.shard_id), note)

    def forget_note(self, shard_id: int) -> None:
        """Drop every note this cluster holds for ``shard_id``, of either kind.

        ``forget_note`` below is the module's function and not this method: a method body
        looks its globals up rather than its own class, so the two do not shadow each
        other and the call means what it says.

        See :class:`RecoveryView.forget_note`.
        """
        storages = self._storages_of(shard_id)
        for prefix in (SPLIT_RECORD_PREFIX, MIGRATION_RECORD_PREFIX):
            forget_note(storages, shard_id, prefix)



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
        """Whether a transaction holds a lock anywhere in ``[start, end)``.

        Asked by every caller about to read a whole range out of a shard and put it
        somewhere else: the two that begin a split or a move, and the two that finish one
        out of a note.  A lock in there may be a commit that has not been applied yet, and
        an intent is not in the version space, so a read that went ahead would answer with
        the old version of a row a transaction is in the middle of changing - the copy would
        be of a row that is about to be different, and nothing downstream could tell.  A
        caller that gets a yes refuses and comes back later: the lock belongs to a
        transaction, so it clears by itself, and the shard waits for that frozen.
        """
        return any(start <= key < end for key, _ in leader._state_machine._storage.iter_locks())

    def _shard_leader_node(self, shard_id: int) -> Optional[MemoryRaftNode]:
        """The node object leading ``shard_id``, if one does."""
        leader = self.shard_leader(shard_id)
        if leader is None:
            return None
        return self._shard_servers[leader[0]].get_shard_node(shard_id)

    def split_error(self) -> Optional[str]:
        """Why the last split could not finish, if it could not."""
        return self._recovery_runner.split_error()

    def migration_error(self) -> Optional[str]:
        """Why the last move could not be started or copied, if it could not be."""
        return self._recovery_runner.migration_error()

    def pending_splits(self) -> Dict[int, Dict[str, Any]]:
        """The splits that are waiting for the routing table, for callers that look."""
        return self._recovery_runner.pending_splits()

    def migration_state(self, shard_id: int) -> Optional[MigrationState]:
        """The move of ``shard_id`` this cluster is in the middle of, if there is one."""
        return self._recovery_runner.migration_state(shard_id)

    def migrations(self) -> Dict[int, MigrationState]:
        """Every move this cluster has not finished, for callers that look."""
        return self._recovery_runner.migrations()

    def _shard_nodes(self, shard_id: int) -> List[MemoryRaftNode]:
        """Every replica of ``shard_id`` this cluster is holding.

        Including the group a move has built for it and not yet switched to, which is
        why every caller that is not a split asks for its own nodes: see
        :meth:`_nodes_on`.
        """
        nodes = [server.get_shard_node(shard_id) for server in self._shard_servers.values()]
        return [node for node in nodes if node is not None]

    def serving_nodes(self, shard_id: int) -> Optional[List[int]]:
        """The set the routing table names for ``shard_id``, or None if it names none.

        Read out of the table where this cluster has one.  The placement this cluster keeps
        (``_placed_shards``) is the routing it publishes *from*, and a recovery answered out
        of it would be answered out of this side's own belief about a table somebody else
        may have moved - which is the difference the process side has always drawn, and
        this side draws it too now.  A cluster with no table at all, which is a cluster
        started without a metadata service and the in-process tests, answers with its own
        placement instead: such a cluster is the whole world to itself and that placement is
        the table's.

        None is the table saying it names no such shard, which both sides can be told.  A
        table that cannot be read raises rather than answering: nothing was read, so nothing
        is known.

        See :class:`RecoveryView.serving_nodes`.
        """
        client = self._metadata_client
        if client is None:
            return list(self._serving_nodes(shard_id))

        placement = client.table(refresh=True).shard(shard_id)
        return None if placement is None else list(placement.nodes)

    def _serving_nodes(self, shard_id: int) -> List[int]:
        """The nodes whose group is the shard's, as the routing table has it.

        A shard being moved has two groups for a moment, on two disjoint sets of nodes:
        the one the table names and the one the table is about to.  Everything that
        answers a question about *the* shard - who serves it, where its leader is, which
        address to publish - means the first of them until the proposal lands, because
        that is the one clients are being routed to.

        After the proposal lands it means the second of them, which is the placement this
        cluster was told or moved to (``_placed_shards``).  What says the proposal has
        landed is the move's *phase* and not the move's going: the record outlives that
        moment by the whole drain window, because while this side is still holding the
        group the shard left, a publisher that read this side's placement would be
        describing the shard out of a group the table has already given away - see
        :meth:`RecoveryRunner._commit_move`, steps 1 and 6.

        A shard nothing here has an entry for is served by every node, which is the model
        this started with and what a shard a split created still gets.
        """
        state = self._recovery_runner.migration_state(shard_id)
        if state is not None and state.phase != MigrationPhase.DONE:
            return list(state.source_nodes)
        if shard_id in self._placed_shards:
            return list(self._placed_shards[shard_id])
        return sorted(self._shard_servers)

    def _nodes_on(self, node_ids: List[int], shard_id: int) -> List[MemoryRaftNode]:
        """The nodes of ``node_ids`` that are holding a group for ``shard_id``."""
        nodes = [self._shard_servers[node_id].get_shard_node(shard_id)
                 for node_id in node_ids if node_id in self._shard_servers]
        return [node for node in nodes if node is not None]

    def _storages_of(self, shard_id: int,
                     node_ids: Optional[List[int]] = None) -> List[RaftStorage]:
        """The storages of the replicas this cluster holds for ``shard_id``.

        The note layer takes storages rather than nodes: a note is written into a
        replica's storage and read back out of it, and the one thing the two start-up
        paths agree on is that there is a storage per replica.  A replica whose storage
        has been closed for good is dropped rather than failing at the call - a note on
        a group that is gone cannot be reached, whatever it says.
        """
        nodes = (self._shard_nodes(shard_id) if node_ids is None
                 else self._nodes_on(node_ids, shard_id))
        return [node._storage for node in nodes if node._storage is not None]

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
                            timeout: float = LEADER_TIMEOUT) -> Optional[MemoryRaftNode]:
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

    def freeze(self, shard_id: int, reason: str, nodes: List[int]) -> None:
        """Stop the shard taking new rows, on this cluster's groups in ``nodes``.

        The set is the caller's and is not worked out here: a shard being moved has two
        groups at once, and which of them a copy is about to read is written in the note
        that copy came from.  :meth:`_serving_nodes` answers where clients are being sent,
        and it also answers with the source set while a move is in flight - a second route
        to the same answer, which agrees only while the note has been loaded and nothing
        has moved on.  Naming the set leaves one route to it.

        See :class:`RecoveryView.freeze`.
        """
        self._freeze_shard(shard_id, reason=reason, node_ids=nodes)

    def unfreeze(self, shard_id: int, nodes: List[int]) -> None:
        """Let the shard take rows again, on this cluster's groups in ``nodes``.

        See :class:`RecoveryView.unfreeze`.  A node of ``nodes`` holding no group, or one
        that was never frozen, is a no-op rather than a mistake.
        """
        self._unfreeze_shard(shard_id, node_ids=nodes)

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

    def _move_row(self, source: NodeClient, target: NodeClient, key: bytes,
                  value: bytes, version: int) -> ApplyResult:
        """Write one row into the new shard.  See :meth:`RecoveryRunner._move_row`.

        Left over from when the move's copy was written in this class: both branches'
        copies are the recovery's now, and nothing here calls this.  It goes with the
        rest of the dead leaves.
        """
        return self._recovery_runner._move_row(source, target, key, value, version)

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
