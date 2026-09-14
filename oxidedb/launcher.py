"""One node of a cluster, as a process.

Everything else here builds a cluster inside one process: ``RaftCluster`` and
``ShardedRaftCluster`` are objects that hold every node, which is what the tests want and
what nothing outside them can use.  This is the other half of that story - a node that a
process can be, so a cluster can be several processes and a client can be a program that
is not one of them.

A node runs three kinds of Raft group, and each of them elects on its own:

* its shards, at ``base + 100 * shard_id``, served by ``ShardServer`` - the Raft service
  and the client's six primitives on the same port;
* the metadata group, at ``base + 100 * num_shards``, whose table a client routes by;
* the TSO group, at ``base + 100 * (num_shards + 1)``, which hands out timestamps.

That arithmetic lives in :func:`group_port` and nowhere else, so an address this node
publishes is one another node derives the same way.

Two things a node here does that a node object cannot: it reports its placement to the
metadata group (``MetadataPublisher``, which is also what installs the starting ranges -
the first writer wins and every other node is refused, which is why all of them may try),
and it sweeps the locks of the shards it leads (``LockCleaner``).  Both are background
threads, and both are stopped before the groups they talk to are.

It says what it is doing on stdout, because a process started by another program has to
be able to say when it is ready: ``READY <host> <port>`` once every port is bound and the
groups are electing, ``SHUTDOWN`` when a stop is asked for, ``STOPPED`` when nothing is
listening any more.  A caller waits for ``READY`` rather than for a number of seconds.
"""

import argparse
import os
import signal
import sys
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .metadata.publisher import MetadataPublisher
from .metadata.service import MetadataClient, MetadataStateMachine
from .raft.node import MemoryRaftNode, NodeState
from .raft.shard_server import (DEFAULT_LOCK_CLEANER_INTERVAL, SHARD_PORT_STRIDE,
                                ShardServer)
from .raft.state_machine import MVCCStateMachine, StateMachine
from .raft.storage import RaftStorage, create_raft_storage
from .shard.router import RangeMap, default_range_map
from .transaction.lock_cleaner import LockCleaner
from .tso.tso import TSOSMStateMachine

DEFAULT_HOST = "127.0.0.1"

#: How many shards a node serves when it is not told.  Two, as the tests use: enough that
#: a key can land somewhere other than where the next one landed.
DEFAULT_NUM_SHARDS = 2

#: How many of the cluster's nodes take part in the metadata and the TSO group.  The
#: groups are small because they are not on the data path - they answer routing and
#: timestamp questions - and a cluster of fewer nodes uses all of them.
DEFAULT_GROUP_SIZE = 3

#: The line a caller waits for, and the two that bound a stop.  These are a contract with
#: whatever started this process, not log messages.
READY = "READY"
SHUTDOWN = "SHUTDOWN"
STOPPED = "STOPPED"

#: Which of a node's non-shard groups a port belongs to, in :func:`group_port`.
METADATA_GROUP = 0
TSO_GROUP = 1


def group_port(base_port: int, num_shards: int, group: int) -> int:
    """Where a node's metadata group (0) or TSO group (1) listens.

    The shards take the block from ``base_port`` upwards, one
    :data:`~oxidedb.raft.shard_server.SHARD_PORT_STRIDE` apart, so the groups sit above
    the last of them.  One copy of the arithmetic, and one direction of it: a node's
    block is ``(num_shards + 2) * SHARD_PORT_STRIDE`` ports wide, and every node derives
    another node's group ports from that node's base port exactly as it derives its own.
    """
    return base_port + (num_shards + group) * SHARD_PORT_STRIDE


def _address(host: str, port: int) -> str:
    return f"{host}:{port}"


@dataclass(frozen=True)
class Peer:
    """Another node of this cluster, as it is named on this node's command line."""

    node_id: int
    host: str
    port: int

    @property
    def address(self) -> str:
        """Where that node's shard 0 listens, which is what its other ports derive from."""
        return _address(self.host, self.port)

    @classmethod
    def parse(cls, text: str) -> "Peer":
        """``2@127.0.0.1:8002`` - a node id, and where that node's shard 0 listens."""
        try:
            node_id, rest = text.split("@", 1)
            host, port = rest.rsplit(":", 1)
            return cls(int(node_id), host, int(port))
        except ValueError:
            raise ValueError(
                f"a peer is written <node-id>@<host>:<port>, and {text!r} is not that"
            ) from None


@dataclass(frozen=True)
class ClusterConfig:
    """Everything one node needs to know, and nothing it has to guess.

    Handed in rather than read from the environment: several of these processes run on one
    machine at the same time, and an environment variable would be shared by all of them.
    """

    node_id: int
    port: int
    host: str = DEFAULT_HOST
    peers: Tuple[Peer, ...] = ()
    #: Where this node keeps what it must not forget.  Omitted, the node is volatile: its
    #: groups still replicate and still elect, but a restart is a new cluster member.
    data_dir: Optional[str] = None
    num_shards: int = DEFAULT_NUM_SHARDS
    metadata_group_size: int = DEFAULT_GROUP_SIZE
    tso_group_size: int = DEFAULT_GROUP_SIZE
    #: Whether this node tells the metadata group what the cluster looks like.  It is the
    #: publisher that does it, and it is what installs the starting ranges: every node may
    #: try, the first one wins, and the rest are refused - which is the ordinary case
    #: rather than an error, because no node can know whether it started first.  A node
    #: whose business the table is not can be started with this off.
    bootstrap: bool = True
    #: Whether a line on stdin, or stdin closing, stops this node.  This is what makes a
    #: node stoppable by the program that started it on Windows, where one process cannot
    #: send another a signal: ``TerminateProcess`` ends a process without running any of
    #: ours, so a caller that wants a graceful stop has to ask over the one channel it has.
    watch_stdin: bool = True

    # -- where everything is ------------------------------------------------

    @property
    def address(self) -> str:
        """Where this node's shard 0 listens: the address other nodes know it by."""
        return _address(self.host, self.port)

    def node_ids(self) -> List[int]:
        """Every node of the cluster, this one included, in a fixed order."""
        return sorted([self.node_id] + [peer.node_id for peer in self.peers])

    def base_addresses(self) -> Dict[int, str]:
        """``{node_id: "host:port"}`` for the whole cluster, from this node's view."""
        addresses = {self.node_id: self.address}
        for peer in self.peers:
            addresses[peer.node_id] = peer.address
        return addresses

    def _base_of(self, node_id: Optional[int]) -> Tuple[str, int]:
        """``node_id``'s host and base port, its own when none is named."""
        host, port = self.base_addresses()[
            self.node_id if node_id is None else node_id].rsplit(":", 1)
        return host, int(port)

    def shard_address(self, shard_id: int, node_id: Optional[int] = None) -> str:
        """Where ``shard_id`` is served, which is where ``ShardServer`` binds it.

        Not :func:`group_port`: a shard is not one of the groups, it is one of the strides
        above the base port, and this is the arithmetic the shard server itself uses.
        """
        host, base = self._base_of(node_id)
        return _address(host, base + shard_id * SHARD_PORT_STRIDE)

    def _group_address(self, group: int, node_id: Optional[int]) -> str:
        host, base = self._base_of(node_id)
        return _address(host, group_port(base, self.num_shards, group))

    def metadata_address(self, node_id: Optional[int] = None) -> str:
        return self._group_address(METADATA_GROUP, node_id)

    def tso_address(self, node_id: Optional[int] = None) -> str:
        return self._group_address(TSO_GROUP, node_id)

    def voters(self, group_size: int) -> List[int]:
        """Which nodes take part in a group that wants ``group_size`` of them.

        A prefix of the sorted node ids, so that every node works the same set out of its
        own command line: a membership that depended on which node was asked would be two
        majorities wearing one name.
        """
        return self.node_ids()[:max(1, group_size)]

    def validate(self) -> None:
        """Refuse a configuration this node could not serve, before it binds anything."""
        if self.node_id < 1:
            raise ValueError(f"a node id starts at 1, and this one is {self.node_id}")
        if self.num_shards < 1:
            raise ValueError(f"a node has at least one shard, not {self.num_shards}")
        if self.metadata_group_size < 1 or self.tso_group_size < 1:
            raise ValueError("a group has at least one node in it")

        ids = [peer.node_id for peer in self.peers]
        if len(set(ids)) != len(ids) or self.node_id in ids:
            raise ValueError("two peers share a node id, or one of them is this node")

        every = self.node_ids()
        if every != list(range(1, len(every) + 1)):
            raise ValueError(
                f"node ids are 1..N with none missing, and this node has {every} - "
                f"a node that names the wrong peers is a node in a cluster of its own")

        block_end = group_port(self.port, self.num_shards, TSO_GROUP)
        if self.port < 1 or block_end > 65535:
            raise ValueError(
                f"shard 0 at {self.port} needs the {(self.num_shards + 2) * SHARD_PORT_STRIDE} "
                f"ports up to {block_end} free, and that is not a port")

    # -- what this node would print about itself ----------------------------

    def describe(self) -> str:
        """One line naming every port this node serves, for a human reading the output."""
        shards = ", ".join(self.shard_address(shard_id)
                           for shard_id in range(self.num_shards))
        return (f"node {self.node_id}: shards at {shards}, "
                f"metadata at {self.metadata_address()}, tso at {self.tso_address()}")


class NodeClusterView:
    """This one node, shaped like the cluster the rest of the code asks questions of.

    ``LockCleaner``, ``LockResolver``, ``ShardLeaders`` and ``MetadataPublisher`` were all
    written against ``ShardedRaftCluster``: they ask a cluster which nodes serve a shard,
    where those nodes listen, which of them leads it, and - the cleaner, directly - for the
    servers themselves.  A node in a process of its own has one of those answers for
    certain, its own, and can work the rest out from the addresses it was given.

    It is deliberately not a cluster.  ``_shard_servers`` holds this node, and that is the
    name ``LockCleaner`` reaches for, so the cleaner sweeps the locks of the shards this
    node leads and no others - which is the only thing a process could do about another
    process's locks in any case.
    """

    def __init__(self, config: ClusterConfig, range_map: RangeMap):
        self._config = config
        self._range_map: RangeMap = dict(range_map)
        #: Named as the cleaner expects to find it: a mapping of node id to server.
        self._shard_servers: Dict[int, ShardServer] = {}
        self._metadata_client: Optional[MetadataClient] = None

    def serve(self, server: ShardServer) -> None:
        """Record the server this node runs, which is the one it can answer for."""
        self._shard_servers[self._config.node_id] = server

    def set_metadata_client(self, client: MetadataClient) -> None:
        self._metadata_client = client

    # -- what the client-side lookups ask a cluster -------------------------

    def metadata_client(self) -> Optional[MetadataClient]:
        return self._metadata_client

    def get_shard_server(self, node_id: int) -> Optional[ShardServer]:
        return self._shard_servers.get(node_id)

    def range_map(self) -> RangeMap:
        return dict(self._range_map)

    # -- what the publisher asks a cluster ----------------------------------

    def shard_ids(self) -> List[int]:
        return sorted(self._range_map)

    def shard_replica_ids(self, shard_id: int) -> List[int]:
        """Every node serving ``shard_id``, from the group this node keeps for it."""
        server = self._shard_servers.get(self._config.node_id)
        return [] if server is None else server.shard_replica_ids(shard_id)

    def shard_addresses(self, shard_id: int) -> Dict[int, str]:
        """Where each replica of ``shard_id`` listens, for the table to publish.

        This node's own address is the one its server actually bound; a peer's is derived
        from that peer's base address, because there is no object here to ask.  Both sides
        of that are :func:`group_port`, so the derived address is the one the peer bound
        rather than a second guess at it.
        """
        addresses = {self._config.node_id: self._config.shard_address(shard_id)}
        for node_id in self.shard_replica_ids(shard_id):
            if node_id != self._config.node_id:
                addresses[node_id] = self._config.shard_address(shard_id, node_id)
        return addresses

    def shard_leader(self, shard_id: int) -> Optional[Tuple[int, int]]:
        """The node leading ``shard_id`` and the term it leads at, when it is this one.

        Only this node can be reported from here: a peer's leadership is a fact about the
        peer's own group, and what this node knows is what it has heard - which is what
        the publishers of the other nodes are for.
        """
        server = self._shard_servers.get(self._config.node_id)
        if server is None:
            return None
        node = server.get_shard_node(shard_id)
        if node is None or node.state != NodeState.LEADER:
            return None
        return (self._config.node_id, node.current_term)


class ClusterNode:
    """One node: its groups, its servers, its background threads, and its stop."""

    def __init__(self, config: ClusterConfig):
        config.validate()
        self._config = config
        self._range_map: RangeMap = default_range_map(config.num_shards)
        self._view = NodeClusterView(config, self._range_map)
        self._shard_server: Optional[ShardServer] = None
        self._metadata_node: Optional[MemoryRaftNode] = None
        self._tso_node: Optional[MemoryRaftNode] = None
        self._metadata_client: Optional[MetadataClient] = None
        self._publisher: Optional[MetadataPublisher] = None
        self._lock_cleaner: Optional[LockCleaner] = None
        self._storages: List[RaftStorage] = []
        self._stop = threading.Event()
        self._started = False
        self._signal_handlers: Dict[int, Any] = {}

    # -- start --------------------------------------------------------------

    def start(self) -> None:
        """Build this node, bind its ports, and say it is ready."""
        if self._started:
            raise RuntimeError("this node is already started")

        if self._config.data_dir is not None:
            os.makedirs(self._config.data_dir, exist_ok=True)

        self._install_signal_handlers()
        # The groups first, so that a shard's table entry has somewhere to be published
        # and a timestamp has somewhere to come from.
        self._start_metadata_group()
        self._start_tso_group()
        self._start_shards()
        self._start_background()

        if self._config.watch_stdin:
            threading.Thread(target=self._watch_stdin, name="stdin-watch",
                             daemon=True).start()

        self._started = True
        print(self._config.describe(), flush=True)
        print(f"{READY} {self._config.host} {self._config.port}", flush=True)

    def _start_metadata_group(self) -> None:
        """Serve this node's member of the routing table's group, if it has one."""
        voters = self._config.voters(self._config.metadata_group_size)
        if self._config.node_id not in voters:
            print(f"metadata: this node is not one of {voters}, serving no member",
                  flush=True)
            return

        self._metadata_node = self._start_group(
            "metadata",
            self._config.metadata_address(),
            {node_id: self._config.metadata_address(node_id)
             for node_id in voters if node_id != self._config.node_id},
            MetadataStateMachine,
            self._group_storage("metadata"),
        )
        self._metadata_client = MetadataClient(self._metadata_leader)
        self._view.set_metadata_client(self._metadata_client)

    def _start_tso_group(self) -> None:
        """Serve this node's member of the timestamp group, if it has one."""
        voters = self._config.voters(self._config.tso_group_size)
        if self._config.node_id not in voters:
            print(f"tso: this node is not one of {voters}, serving no member", flush=True)
            return

        self._tso_node = self._start_group(
            "tso",
            self._config.tso_address(),
            {node_id: self._config.tso_address(node_id)
             for node_id in voters if node_id != self._config.node_id},
            TSOSMStateMachine,
            self._group_storage("tso"),
        )

    def _start_shards(self) -> None:
        """Serve every shard this node holds, and every client primitive on that port."""
        config = self._config
        server = ShardServer(config.node_id, config.num_shards, len(config.node_ids()))
        server.set_range_map(self._range_map)
        server.start_shards_network(
            state_machine_factory=self._state_machine,
            peer_addresses=config.base_addresses(),
            storage_factory=self._shard_storage,
        )
        self._shard_server = server
        self._view.serve(server)

    def _state_machine(self) -> StateMachine:
        """A shard's state machine.

        In memory, and rebuilt from the Raft log on the way up: ``ShardServer`` calls this
        factory once per shard without telling it which shard it is building for, so a
        durable state machine per shard is not something this interface can express.  The
        log is durable, so replaying it is what brings the rows back - a durable state
        machine would be faster to start and is a change to that interface, not to this.
        """
        return MVCCStateMachine()

    def _shard_storage(self, node_id: int, shard_id: int) -> RaftStorage:
        """A shard's log and metadata, under the node's own data directory."""
        return self._group_storage(f"shard-{shard_id}")

    def _group_storage(self, name: str) -> RaftStorage:
        """Durable storage for one group, or a volatile one without a data directory."""
        directory = None
        if self._config.data_dir is not None:
            directory = os.path.join(self._config.data_dir, name)
        storage = create_raft_storage(directory)
        self._storages.append(storage)
        return storage

    def _start_group(self, name: str, address: str, peers: Dict[int, str],
                     state_machine_factory: Callable[[], StateMachine],
                     storage: RaftStorage) -> MemoryRaftNode:
        """Run one member of one group, with a server of its own at ``address``."""
        import grpc
        from concurrent.futures import ThreadPoolExecutor

        from oxidedb.proto.raft_pb2_grpc import add_RaftServiceServicer_to_server
        from .raft.network_client import RaftNetworkClient
        from .raft.raft_servicer import RaftServicer

        node = MemoryRaftNode(
            node_id=self._config.node_id,
            peers=sorted(peers),
            state_machine=state_machine_factory(),
            storage=storage,
            network_client=RaftNetworkClient(peers),
        )

        server = grpc.server(ThreadPoolExecutor(max_workers=10))
        add_RaftServiceServicer_to_server(RaftServicer(node), server)
        server.add_insecure_port(address)
        server.start()
        # The server is handed to the node as well, because ``node.shutdown()`` is what
        # stops it - the same hand-off ``ShardServer`` and ``RaftCluster`` make.  A server
        # kept only here would hold a socket and a thread pool for the life of the process.
        node._grpc_server = server

        print(f"{name}: {address} (peers {sorted(peers)})", flush=True)
        return node

    def _metadata_leader(self) -> Optional[MemoryRaftNode]:
        """The metadata member to ask, or None while this node is not the leader."""
        node = self._metadata_node
        if node is None or node.state != NodeState.LEADER:
            return None
        return node

    def _start_background(self) -> None:
        """Start the threads that keep the table current and the locks cleaned up.

        The publisher is also the bootstrap: its first pass proposes the starting ranges,
        and a refusal from a table that already holds them is the ordinary case rather
        than an error - see :class:`~oxidedb.metadata.publisher.MetadataPublisher`.  A
        second INIT_ROUTES here would make a refusal mean two different things.
        """
        if self._config.bootstrap and self._metadata_node is not None:
            self._publisher = MetadataPublisher(self._metadata_client, self._view)
            self._publisher.start()

        self._lock_cleaner = LockCleaner(self._view,
                                         poll_interval=DEFAULT_LOCK_CLEANER_INTERVAL)
        self._lock_cleaner.start()

    # -- stop ---------------------------------------------------------------

    def request_stop(self) -> None:
        """Ask this node to stop, from wherever the request came from."""
        self._stop.set()

    def wait(self) -> None:
        """Block until something asks this node to stop."""
        try:
            self._stop.wait()
        except KeyboardInterrupt:
            self.request_stop()

    def shutdown(self) -> None:
        """Stop in the one order that keeps the background threads from talking to nothing.

        The publisher and the cleaner propose to Raft groups, so they go first: a cleaner
        that ran on after its group had stopped would spend the time reporting failures,
        and the point of the order is that nothing writes once nothing is listening.

        The shards go next, and each of them stops its gRPC server and its node together -
        ``ShardServer.shutdown`` is one call for both, so "stop accepting" and "stop the
        node" are one step here rather than two.  It is the same order for the two groups
        below, and then the storage is closed.
        """
        if not self._started:
            return

        print(SHUTDOWN, flush=True)
        for stopper in (self._publisher, self._lock_cleaner):
            if stopper is not None:
                stopper.stop()

        if self._shard_server is not None:
            self._shard_server.shutdown()

        for node in (self._metadata_node, self._tso_node):
            if node is not None:
                node.shutdown()
                node.wait_for_shutdown()

        for storage in self._storages:
            close = getattr(storage, "close", None)
            if close is not None:
                close()

        self._restore_signal_handlers()
        self._started = False
        print(STOPPED, flush=True)

    def _watch_stdin(self) -> None:
        """Stop when the program that started this node says so, or goes away.

        A line reading ``stop``, or stdin closing - which is what a dropped pipe looks
        like - stops the node.  This is the stoppable half of the READY contract on
        Windows, where there is no signal for one process to send another.
        """
        for line in sys.stdin:
            if line.strip().lower() == "stop":
                break
        self.request_stop()

    def _install_signal_handlers(self) -> None:
        """Turn a signal into a stop, where this platform and thread allow one."""
        for name in ("SIGINT", "SIGTERM"):
            which = getattr(signal, name, None)
            if which is None:
                continue
            try:
                self._signal_handlers[which] = signal.getsignal(which)
                signal.signal(which, lambda *_: self.request_stop())
            except (ValueError, OSError):
                # Not the main thread, or a signal this interpreter will not let us have.
                self._signal_handlers.pop(which, None)

    def _restore_signal_handlers(self) -> None:
        for which, handler in self._signal_handlers.items():
            try:
                signal.signal(which, handler)
            except (ValueError, OSError):
                pass
        self._signal_handlers.clear()


def run_cluster(config: ClusterConfig) -> int:
    """Start a node, wait for a stop, stop it - all of what a node's process does."""
    node = ClusterNode(config)
    node.start()
    try:
        node.wait()
    finally:
        node.shutdown()
    return 0


def parse_peers(text: str) -> Tuple[Peer, ...]:
    """``2@127.0.0.1:8002,3@127.0.0.1:8003`` into peers, and nothing into none."""
    return tuple(Peer.parse(part.strip()) for part in text.split(",") if part.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m oxidedb.launcher",
        description="Run one node of an OxideDB cluster until it is told to stop.",
    )
    parser.add_argument("--node-id", type=int, required=True,
                        help="this node's id; node ids are 1..N with none missing")
    parser.add_argument("--port", type=int, required=True, metavar="PORT",
                        help="where this node's shard 0 listens; its other shards, the "
                             "metadata group and the TSO group take the port block above it")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help=f"the interface to bind (default {DEFAULT_HOST})")
    parser.add_argument("--peers", default="", metavar="ID@HOST:PORT,...",
                        help="every other node, by its shard 0 address; a single-node "
                             "cluster names none")
    parser.add_argument("--data-dir", default=None, metavar="DIR",
                        help="keep each group's log under DIR, so a restart is the same "
                             "node rather than a new one; omitted, nothing survives")
    parser.add_argument("--num-shards", type=int, default=DEFAULT_NUM_SHARDS,
                        help="how many shards this node serves (default "
                             f"{DEFAULT_NUM_SHARDS}); every node must be told the same")
    parser.add_argument("--metadata-group-size", type=int, default=DEFAULT_GROUP_SIZE,
                        help="how many nodes hold the routing table (default "
                             f"{DEFAULT_GROUP_SIZE})")
    parser.add_argument("--tso-group-size", type=int, default=DEFAULT_GROUP_SIZE,
                        help="how many nodes hold the timestamp group (default "
                             f"{DEFAULT_GROUP_SIZE})")
    parser.add_argument("--no-bootstrap", dest="bootstrap", action="store_false",
                        help="do not publish this node's placement; for a node whose "
                             "table somebody else is keeping")
    parser.add_argument("--no-stdin-watch", dest="watch_stdin", action="store_false",
                        help="do not stop when stdin closes or reads 'stop'")
    return parser


def config_from_args(argv: Optional[Sequence[str]] = None) -> ClusterConfig:
    args = build_parser().parse_args(argv)
    return ClusterConfig(
        node_id=args.node_id,
        port=args.port,
        host=args.host,
        peers=parse_peers(args.peers),
        data_dir=args.data_dir,
        num_shards=args.num_shards,
        metadata_group_size=args.metadata_group_size,
        tso_group_size=args.tso_group_size,
        bootstrap=args.bootstrap,
        watch_stdin=args.watch_stdin,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        config = config_from_args(argv)
    except ValueError as error:
        print(f"cannot start: {error}", file=sys.stderr, flush=True)
        return 2

    try:
        return run_cluster(config)
    except ValueError as error:
        print(f"cannot start: {error}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
