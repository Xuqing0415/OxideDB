"""Where the shards are, and who leads them.

Sharding was frozen because nothing owned the answer to "which shard holds this key".
The routing table was built when the cluster started and every process kept its own
copy of it, so nothing could change the answer and a client had no one to ask.  This
module is that owner: an ordinary Raft group - the same machinery as the timestamp
group, with a routing table instead of a counter - so the answer is a replicated
decision rather than a local belief, and it survives losing the node that wrote it.

Three things are in the table:

* the ranges: shard id -> ``(start, end)``.  ``shard.router.locate`` is still the one
  routing rule; this is the table it was always missing as an input.
* the placement: which node ids serve a shard, and the address each one listens on.
* the leader: which of those nodes currently leads, and the term it claimed.  Terms are
  kept so a report from a node that has since been deposed cannot write itself back in.

The whole table is one key and therefore one read.  A client that read the ranges and
the leaders at two different moments would route on a table that never existed, and the
read goes through ``MemoryRaftNode.get`` - the ReadIndex path - so a metadata leader
that cannot reach a quorum refuses to serve a table it cannot justify.

What is not here yet: a command to split a range.  A split has to move the rows it cuts
off before the table may say they belong somewhere else, so it arrives with that
migration rather than ahead of it.  Nothing in the cluster publishes to this table
either - that wiring is the next step, and until it lands a cluster still builds its
routing table locally.
"""

import threading
import time
from typing import Any, Callable, Dict, List, Optional

import msgpack

from ..groups import refusal
from ..proto import groups_pb2
from ..proto.groups_pb2_grpc import MetadataServiceServicer
from ..raft.node import MemoryRaftNode, RaftCluster
from ..raft.state_machine import ApplyResult, ErrorCode, ReadResult, StateMachine
from ..shard.router import RangeMap, locate

#: The table is not a keyspace, it is one document, and it is read in one call.
TABLE_KEY = b"routing_table"

#: How many times a read or a write follows the group's leader before giving up.
PROPOSE_ATTEMPTS = 3
#: Base backoff between those attempts, multiplied by the attempt number.
RETRY_BACKOFF = 0.05


class MetadataCommandType:
    INIT_ROUTES = b"init_routes"
    SET_SHARD_NODES = b"set_shard_nodes"
    REPORT_LEADER = b"report_leader"
    #: Split one shard's range in two.  This is the only command that changes which
    #: range a shard answers for, and it is proposed after the rows have moved, so
    #: the group checks it against the table rather than believing the caller.
    SPLIT = b"split"


class ShardPlacement:
    """One shard as the table describes it."""

    def __init__(self, shard_id: int, start: bytes, end: bytes,
                 nodes: Optional[List[int]] = None,
                 addresses: Optional[Dict[int, str]] = None,
                 leader_id: Optional[int] = None,
                 leader_term: int = 0):
        self.shard_id = shard_id
        self.start = start
        self.end = end
        #: The node ids whose Raft group serves this shard.
        self.nodes = list(nodes or [])
        #: Where each of them listens, for a client that has to send an RPC.
        self.addresses = dict(addresses or {})
        #: The node that last reported leading this shard, and the term it reported.
        self.leader_id = leader_id
        self.leader_term = leader_term

    def leader_address(self) -> Optional[str]:
        if self.leader_id is None:
            return None
        return self.addresses.get(self.leader_id)

    def __repr__(self) -> str:
        return (f"ShardPlacement(shard_id={self.shard_id}, "
                f"range=({self.start!r}, {self.end!r}), nodes={self.nodes}, "
                f"leader_id={self.leader_id}, leader_term={self.leader_term})")


class RoutingTable:
    """Every shard, all of it read at one version."""

    def __init__(self, version: int, shards: Dict[int, ShardPlacement]):
        self.version = version
        self.shards = shards
        self._ranges: RangeMap = {
            shard_id: (placement.start, placement.end)
            for shard_id, placement in shards.items()
        }

    def routes(self) -> RangeMap:
        return dict(self._ranges)

    def route_for(self, key: bytes) -> int:
        """The shard that owns ``key``, by the one routing rule."""
        return locate(self._ranges, key)

    def shard(self, shard_id: int) -> Optional[ShardPlacement]:
        return self.shards.get(shard_id)

    def shard_for(self, key: bytes) -> Optional[ShardPlacement]:
        return self.shards.get(self.route_for(key))

    def __repr__(self) -> str:
        return f"RoutingTable(version={self.version}, shards={sorted(self.shards)})"


def _table_payload(version: int, shards: Dict[int, ShardPlacement]) -> Dict[str, Any]:
    """The table as msgpack-able plain data, with string keys throughout.

    String keys are not decoration: ``msgpack.unpackb`` rejects anything else by
    default, so an int-keyed map would fail at the far end of a round trip.
    """
    return {
        "version": version,
        "shards": [
            {
                "shard_id": placement.shard_id,
                "start": placement.start,
                "end": placement.end,
                "nodes": placement.nodes,
                "addresses": [[node_id, address]
                              for node_id, address in sorted(placement.addresses.items())],
                "leader_id": placement.leader_id,
                "leader_term": placement.leader_term,
            }
            for _, placement in sorted(shards.items())
        ],
    }


def _parse_table(payload: Dict[str, Any]) -> RoutingTable:
    shards = {}
    for entry in payload.get("shards", []):
        shard_id = int(entry["shard_id"])
        shards[shard_id] = ShardPlacement(
            shard_id,
            bytes(entry["start"]),
            bytes(entry["end"]),
            nodes=[int(node_id) for node_id in entry.get("nodes", [])],
            addresses={int(node_id): str(address)
                       for node_id, address in entry.get("addresses", [])},
            leader_id=(None if entry.get("leader_id") is None else int(entry["leader_id"])),
            leader_term=int(entry.get("leader_term", 0)),
        )
    return RoutingTable(int(payload.get("version", 0)), shards)


class MetadataStateMachine(StateMachine):
    """The shard map, replicated.

    Commands are the three ways the table changes; the read is the whole table under
    :data:`TABLE_KEY`.  A command that cannot be applied is refused with a code rather
    than applied halfway, because a table that is locally wrong is worse than a write
    that failed: the whole point of the group is that everyone reading it reads the
    same answer.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._shards: Dict[int, ShardPlacement] = {}
        self._version = 0

    # -- commands ----------------------------------------------------------

    def apply(self, command: bytes) -> ApplyResult:
        try:
            cmd = msgpack.unpackb(command)
            cmd_type = cmd.get("type")

            if cmd_type == MetadataCommandType.INIT_ROUTES:
                return self._apply_init_routes(cmd)
            if cmd_type == MetadataCommandType.SET_SHARD_NODES:
                return self._apply_set_shard_nodes(cmd)
            if cmd_type == MetadataCommandType.REPORT_LEADER:
                return self._apply_report_leader(cmd)
            if cmd_type == MetadataCommandType.SPLIT:
                return self._apply_split(cmd)

            return ApplyResult.failure(1, f"Unknown metadata command: {cmd_type!r}")
        except Exception as error:
            return ApplyResult.failure(2, f"Apply error: {error}")

    def _apply_init_routes(self, cmd: Dict[str, Any]) -> ApplyResult:
        ranges = {int(entry[0]): (bytes(entry[1]), bytes(entry[2]))
                  for entry in cmd["ranges"]}
        with self._lock:
            if self._shards:
                # A bootstrap that arrives twice - a restarted cluster, a second
                # proposer - must not undo a table that has since been changed.
                return ApplyResult.failure(3, "the routing table is already initialised")
            self._shards = {
                shard_id: ShardPlacement(shard_id, start, end)
                for shard_id, (start, end) in ranges.items()
            }
            self._version += 1
        return ApplyResult.success()

    def _apply_set_shard_nodes(self, cmd: Dict[str, Any]) -> ApplyResult:
        shard_id = int(cmd["shard_id"])
        nodes = [int(node_id) for node_id in cmd["nodes"]]
        addresses = {int(entry[0]): str(entry[1]) for entry in cmd["addresses"]}
        with self._lock:
            placement = self._shards.get(shard_id)
            if placement is None:
                return ApplyResult.failure(4, f"shard {shard_id} is not in the routing table")
            placement.nodes = nodes
            placement.addresses = addresses
            if placement.leader_id is not None and placement.leader_id not in nodes:
                # The node that last reported itself leader is not in the replica set
                # any more, so the table would be pointing at a stranger.
                placement.leader_id = None
                placement.leader_term = 0
            self._version += 1
        return ApplyResult.success()

    def _apply_report_leader(self, cmd: Dict[str, Any]) -> ApplyResult:
        shard_id = int(cmd["shard_id"])
        node_id = int(cmd["node_id"])
        term = int(cmd["term"])
        with self._lock:
            placement = self._shards.get(shard_id)
            if placement is None:
                return ApplyResult.failure(4, f"shard {shard_id} is not in the routing table")
            if placement.nodes and node_id not in placement.nodes:
                return ApplyResult.failure(5, f"node {node_id} does not serve shard {shard_id}")
            if placement.leader_id is not None and term <= placement.leader_term:
                # Not an error to shout about: two reports racing is normal - it is a
                # message that lost, and applying it would move the table backwards.
                return ApplyResult.failure(
                    6, f"shard {shard_id} already has a leader at term {placement.leader_term}")
            placement.leader_id = node_id
            placement.leader_term = term
            self._version += 1
        return ApplyResult.success()

    def _apply_split(self, cmd: Dict[str, Any]) -> ApplyResult:
        """Replace one shard's range with the two halves that partition it.

        The group is the arbiter here, not the caller's notebook.  A split arrives
        after the rows have already moved - that is the only order that is safe, see
        design.md section 7 - so applying one the caller got wrong would point clients
        at a range that is not the range whose data was copied.  Every check is made
        against the table as it stands, and a failure is refused whole: a half-applied
        split would leave a range nobody owns.

        The refusals, each with its own code because they mean different things to a
        caller deciding whether to retry or to give up:

        * 7 - the shard to split is not in the table.
        * 8 - the split point is not strictly inside that shard's range, so one half
          would be empty or the point is outside the range altogether.
        * 9 - the new shard's id is already in the table, and not as the shard this
          proposal would have created.
        * 10 - the new shard's id is the shard this proposal did create, but it holds
          a replica set that is not the one being proposed now.
        * 11 - either half would overlap a range another shard already owns.

        A retry of a split that already applied is a success rather than a conflict.
        The caller cannot tell a lost response from a lost proposal, so the group has to
        answer both the same way; what identifies the retry is the geometry - the left
        half ending exactly at the split point, the new shard beginning exactly there -
        together with the replica set that was asked for.

        The left half keeps the old shard's id.  It is the same Raft group with less to
        answer for, so its replica set, its addresses and a leader that has already
        reported stay as they are - only the range it answers for changes.
        """
        shard_id = int(cmd["shard_id"])
        new_shard_id = int(cmd["new_shard_id"])
        split_key = bytes(cmd["split_key"])
        nodes = [int(node_id) for node_id in cmd["nodes"]]
        addresses = {int(entry[0]): str(entry[1]) for entry in cmd["addresses"]}

        with self._lock:
            if new_shard_id in self._shards:
                return self._split_again(shard_id, new_shard_id, split_key, nodes)

            placement = self._shards.get(shard_id)
            if placement is None:
                return ApplyResult.failure(
                    7, f"shard {shard_id} is not in the routing table")

            if not placement.start < split_key < placement.end:
                return ApplyResult.failure(
                    8, f"split point {split_key!r} is not inside shard {shard_id}'s "
                       f"range ({placement.start!r}, {placement.end!r})")

            left = ShardPlacement(shard_id, placement.start, split_key,
                                  nodes=placement.nodes, addresses=placement.addresses,
                                  leader_id=placement.leader_id,
                                  leader_term=placement.leader_term)
            right = ShardPlacement(new_shard_id, split_key, placement.end,
                                   nodes=nodes, addresses=addresses)

            for other_id, other in self._shards.items():
                if other_id == shard_id:
                    continue
                for half in (left, right):
                    if other.start < half.end and half.start < other.end:
                        return ApplyResult.failure(
                            11, f"shard {other_id} ({other.start!r}, {other.end!r}) "
                                f"overlaps the new range ({half.start!r}, "
                                f"{half.end!r})")

            self._shards[shard_id] = left
            self._shards[new_shard_id] = right
            self._version += 1
        return ApplyResult.success()

    def _split_again(self, shard_id: int, new_shard_id: int, split_key: bytes,
                     nodes: List[int]) -> ApplyResult:
        """The new id is taken: is this proposal the split that took it?

        Called with the lock held.  Anything that is not recognisably that same split is
        refused, because a caller that cannot tell a lost response from a lost proposal
        must not be able to make the second one real.
        """
        left = self._shards.get(shard_id)
        right = self._shards[new_shard_id]
        if left is not None and left.end == split_key and right.start == split_key:
            if right.nodes == nodes:
                return ApplyResult.success()
            return ApplyResult.failure(
                10, f"shard {new_shard_id} was created by a split at {split_key!r} "
                    f"with replica set {right.nodes}, not {nodes}")
        return ApplyResult.failure(
            9, f"shard {new_shard_id} is already in the routing table "
               f"({right.start!r}, {right.end!r})")

    # -- reads -------------------------------------------------------------

    def get(self, key: bytes, timestamp: Optional[int] = None) -> ReadResult:
        """The whole table, as msgpack, under :data:`TABLE_KEY`.

        The timestamp is not used.  There is no history here to read: the table is one
        current decision about where the shards are, and the version it carries is
        there so a client can tell a refresh from a no-op.
        """
        if key != TABLE_KEY:
            return ReadResult.failure(ErrorCode.ERR_UNKNOWN, f"No metadata key {key!r}")
        return ReadResult.success(self.encode())

    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None) -> list:
        # One table, not a keyspace: there is nothing for a range read to find.
        return []

    def encode(self) -> bytes:
        return msgpack.packb(self._table_payload(), use_bin_type=True)

    def _table_payload(self) -> Dict[str, Any]:
        with self._lock:
            return _table_payload(self._version, self._shards)

    def table(self) -> RoutingTable:
        """The table as this replica sees it, for in-process callers."""
        return _parse_table(self._table_payload())

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def serialize_command(self, cmd_type: bytes, **kwargs) -> bytes:
        return msgpack.packb({"type": cmd_type, **kwargs}, use_bin_type=True)

    # -- snapshots ---------------------------------------------------------

    def snapshot(self) -> bytes:
        return self.encode()

    def restore(self, data: bytes) -> None:
        if not data:
            with self._lock:
                self._shards = {}
                self._version = 0
            return
        table = _parse_table(msgpack.unpackb(data, raw=False))
        with self._lock:
            self._shards = table.shards
            self._version = table.version


class MetadataClient:
    """The routing table: reads for a client, writes for the cluster.

    Reads are what a client does with it.  The table is cached, so routing a key costs
    nothing until the table changes; ``refresh`` re-reads it, and any successful write
    drops the cache so the next read cannot serve a table from before it.  The read
    itself is linearizable - it goes through the group's ReadIndex path - and a group
    that cannot reach a quorum raises instead of answering from a stale copy.

    Writes are what the cluster does with it: ``init_routes`` once at bootstrap, and
    then ``set_shard_nodes`` and ``report_leader`` as shards come up and their leaders
    change.  A write follows the leader if it moved, and a leader report carries the
    term that was claimed, so the table keeps the newest claim and an old leader cannot
    write itself back in.
    """

    def __init__(self, get_leader_node: Callable[[], Optional[MemoryRaftNode]]):
        self._get_leader_node = get_leader_node
        self._lock = threading.RLock()
        self._table: Optional[RoutingTable] = None

    # -- reads -------------------------------------------------------------

    def refresh(self) -> RoutingTable:
        """Read the whole table from the group's leader, and cache it."""
        last_error = None
        for attempt in range(PROPOSE_ATTEMPTS):
            node = self._get_leader_node()
            if node is None:
                last_error = "the metadata group has no leader"
            else:
                result = node.get(TABLE_KEY)
                if result.success:
                    table = _parse_table(msgpack.unpackb(result.value, raw=False))
                    with self._lock:
                        self._table = table
                    return table
                last_error = result.error_msg
                if result.error_code != ErrorCode.ERR_NOT_LEADER:
                    break
            time.sleep(RETRY_BACKOFF * (attempt + 1))

        raise RuntimeError(f"the routing table cannot be read: {last_error}")

    def table(self, refresh: bool = False) -> RoutingTable:
        """The cached table, reading it the first time and when asked to."""
        with self._lock:
            cached = self._table
        if cached is not None and not refresh:
            return cached
        return self.refresh()

    def get_shard_for(self, key: bytes) -> int:
        """Which shard owns ``key``, from the cached table."""
        return self.table().route_for(key)

    def get_shard_leader(self, shard_id: int) -> Optional[int]:
        """Which node leads ``shard_id``, if any node has said so."""
        placement = self.table().shard(shard_id)
        return None if placement is None else placement.leader_id

    def get_leader_for_key(self, key: bytes) -> Optional[int]:
        placement = self.table().shard_for(key)
        return None if placement is None else placement.leader_id

    def list_shards(self) -> List[ShardPlacement]:
        return list(self.table().shards.values())

    # -- writes ------------------------------------------------------------

    def init_routes(self, ranges: RangeMap) -> ApplyResult:
        """Install the starting ranges.  Refused if the table already has any."""
        return self._propose(
            MetadataCommandType.INIT_ROUTES,
            ranges=[[shard_id, start, end]
                    for shard_id, (start, end) in sorted(ranges.items())],
        )

    def set_shard_nodes(self, shard_id: int, nodes: List[int],
                        addresses: Optional[Dict[int, str]] = None) -> ApplyResult:
        """Record which nodes serve a shard, and where they listen."""
        return self._propose(
            MetadataCommandType.SET_SHARD_NODES,
            shard_id=shard_id,
            nodes=list(nodes),
            addresses=[[node_id, address]
                       for node_id, address in sorted((addresses or {}).items())],
        )

    def split_shard(self, shard_id: int, split_key: bytes, new_shard_id: int,
                    nodes: List[int],
                    addresses: Optional[Dict[int, str]] = None) -> ApplyResult:
        """Record that ``shard_id``'s range is now two ranges, one of them new.

        The rows have to have been moved before this is proposed: the table is what
        clients route by, so a range it hands out is a range whose data has to be
        there.  Proposing a split early is a faster way to lose data, not a feature.
        A retry is safe - the group recognises the split it already applied.
        """
        return self._propose(
            MetadataCommandType.SPLIT,
            shard_id=shard_id,
            split_key=split_key,
            new_shard_id=new_shard_id,
            nodes=list(nodes),
            addresses=[[node_id, address]
                       for node_id, address in sorted((addresses or {}).items())],
        )

    def report_leader(self, shard_id: int, node_id: int, term: int) -> ApplyResult:
        """Record that ``node_id`` leads ``shard_id`` at ``term``.

        A ``False`` result here is not necessarily a problem: a report that arrives
        after a newer one is meant to lose.
        """
        return self._propose(
            MetadataCommandType.REPORT_LEADER,
            shard_id=shard_id, node_id=node_id, term=term,
        )

    def _propose(self, cmd_type: bytes, **kwargs) -> ApplyResult:
        """Propose one command, following the leader if it moved under us."""
        last_error = None
        for attempt in range(PROPOSE_ATTEMPTS):
            node = self._get_leader_node()
            if node is None:
                last_error = "the metadata group has no leader"
            else:
                command = node._state_machine.serialize_command(cmd_type, **kwargs)
                result = node.propose(command)
                if result.success:
                    with self._lock:
                        self._table = None
                    return result
                last_error = result.error_msg
                if result.error_code not in (ErrorCode.ERR_NOT_LEADER,
                                             ErrorCode.ERR_LEADERSHIP_LOST):
                    # The command itself was refused.  Proposing it again would be
                    # refused the same way.
                    return result
            time.sleep(RETRY_BACKOFF * (attempt + 1))

        return ApplyResult.failure(ErrorCode.ERR_NOT_LEADER, str(last_error))


def shard_record(placement: ShardPlacement) -> groups_pb2.ShardRecord:
    """One placement as the wire names it: field for field, nothing tidied.

    ``leader_address`` is set only when the table knows both halves of it - which node
    reported leading, and where that node listens - because the message has no way to say
    "the id without the address": an absent field is the second half missing, and a
    client that needs an address reads it the way it reads a shard whose leader has not
    published one yet.
    """
    record = groups_pb2.ShardRecord(
        shard_id=placement.shard_id,
        start_key=placement.start,
        end_key=placement.end,
        nodes=placement.nodes,
        leader_term=placement.leader_term,
    )
    for node_id, address in sorted(placement.addresses.items()):
        record.addresses[node_id] = address
    if placement.leader_id is not None:
        record.leader_id = placement.leader_id
        address = placement.leader_address()
        if address:
            record.leader_address = address
    return record


class MetadataServicer(MetadataServiceServicer):
    """The table, answered to a client that is not in this process.

    This node does not decide that it is the leader, and does not read its own state to
    find out: the read goes through the node's own ``get``, which is the ReadIndex path,
    so a member that cannot reach a quorum refuses here rather than serving the copy it
    happens to hold.  What that leaves for this file is the translation, and the one
    thing a refusal carries that a code cannot: where the leader is, when this node has
    heard from one.  ``leader_address`` is a callable for the reason
    ``ClientServicer``'s is - elections happen while a servicer lives.
    """

    def __init__(self, node, leader_address=None):
        self._node = node
        self._leader_address = leader_address

    def ListShards(self, request, context):
        result = self._node.get(TABLE_KEY)
        if not result.success:
            return groups_pb2.ListShardsResponse(**refusal(
                result.error_msg or "the routing table cannot be read",
                result.error_code, self._leader_address))

        table = _parse_table(msgpack.unpackb(result.value, raw=False))
        response = groups_pb2.ListShardsResponse(
            error_code=groups_pb2.OK, version=table.version)
        response.shards.extend(shard_record(placement)
                               for placement in table.shards.values())
        return response


class MetadataCluster:
    """The Raft group that holds the table - three nodes, like everything else."""

    def __init__(self, num_nodes: int = 3):
        self._num_nodes = num_nodes
        self._cluster: Optional[RaftCluster] = None

    def start(self, peer_addresses: Optional[Dict[int, str]] = None,
              storage_factory=None) -> None:
        """Start the group, in process unless addresses are given.

        The metadata group is not on the data path, so a test that only cares about
        routing rules does not have to pay for three more listening sockets.
        """
        self._cluster = RaftCluster(num_nodes=self._num_nodes)
        if peer_addresses is None:
            self._cluster.start(lambda: MetadataStateMachine(), storage_factory)
        else:
            self._cluster.start_network(
                lambda: MetadataStateMachine(), peer_addresses, storage_factory)

    def get_node(self, node_id: int) -> Optional[MemoryRaftNode]:
        if self._cluster is None:
            return None
        return self._cluster.get_node(node_id)

    def get_leader_node(self) -> Optional[MemoryRaftNode]:
        if self._cluster is None:
            return None
        leader_id = self._cluster.get_leader()
        if leader_id is None:
            return None
        return self._cluster.get_node(leader_id)

    def get_client(self) -> MetadataClient:
        """A client with its own table cache.  Two callers that must not share a
        cache - a test asserting on someone else's view - ask for two of these."""
        return MetadataClient(self.get_leader_node)

    def shutdown(self) -> None:
        if self._cluster is not None:
            self._cluster.shutdown()
            self._cluster = None