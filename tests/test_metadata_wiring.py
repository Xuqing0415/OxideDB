"""The cluster publishes its placement, so a client can route without being told.

The metadata group was a service nobody wrote to, which makes it a library.  Now the
cluster publishes through ``MetadataPublisher``: the ranges once, each shard's replica set
once, and a leader report whenever a shard's leader or its term moves.  That is the join
that makes routing usable end to end - a client asks the table which shard owns a key and
which node leads it, and both answers come from the cluster that owns the data.

What these tests pin: the table a client reads names the shard's real leader, at its real
term, at an address that is really listening; the publisher writes on change and not on a
timer, because every write is a reason for every client's cache to refresh; a leader that
dies is replaced in the table by the node that took over; a cluster that restarts and
re-proposes the same ranges is not a disagreement; and a table that holds *different*
ranges is not overwritten; and a shard a move is in flight for is left to the move rather
than described by the publisher, because the two of them would disagree about which group
serves it.
"""

import socket

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_metadata_client, wait_until
from oxidedb.metadata.publisher import MetadataPublisher
from oxidedb.metadata.service import MetadataCluster
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine
from oxidedb.shard.router import default_range_map, locate
from oxidedb.tso.tso import TSOCluster

KEY_A = b"key0"      # first byte 0x6b -> shard 0
KEY_B = b"\x80key1"  # first byte 0x80 -> shard 1


class _FakeCluster:
    """The publisher's whole view of a cluster - five methods, plus two it may not have.

    Having them in one place is the point: this is the entire coupling between the
    publisher and ``ShardedRaftCluster``, so a test can swap the cluster for this and
    still exercise every branch of the publishing logic.  ``possible_ranges`` and
    ``migrations`` are the two the publisher asks for only if the cluster can answer them,
    and both are about a placement in flight rather than the one in force.
    """

    def __init__(self, ranges, nodes=(1, 2, 3), addresses=None, possible=None,
                 moving=()):
        self._ranges = dict(ranges)
        self._nodes = list(nodes)
        self._addresses = dict(addresses or {})
        #: The extra range maps this cluster could have published - the one a split it
        #: is in the middle of will produce.  Absent is the ordinary cluster, which has
        #: only the map it routes by.
        self._possible = [dict(entry) for entry in (possible or [])]
        #: The shards a move in this cluster is in charge of, as ``migrations`` returns
        #: them - the ids, which is all the publisher asks about.
        self.moving = list(moving)
        self.leaders = {}

    def range_map(self):
        return dict(self._ranges)

    def shard_ids(self):
        return sorted(self._ranges)

    def shard_replica_ids(self, shard_id):
        return list(self._nodes)

    def shard_addresses(self, shard_id):
        return dict(self._addresses)

    def shard_leader(self, shard_id):
        return self.leaders.get(shard_id)

    def possible_ranges(self):
        return [self.range_map()] + [dict(entry) for entry in self._possible]

    def migrations(self):
        return {shard_id: None for shard_id in self.moving}


def _started_metadata():
    cluster = MetadataCluster(num_nodes=3)
    cluster.start()
    return cluster


def _cluster_with_metadata(num_shards=2):
    metadata = _started_metadata()

    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())

    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=num_shards)
    shard_cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(),
        lock_cleaner_interval=None,
        metadata=metadata,
    )
    return metadata, tso_cluster, shard_cluster


def _published_table(client, shard_ids):
    """The table, once every shard in ``shard_ids`` has a leader in it."""
    def ready():
        table = client.table(refresh=True)
        for shard_id in shard_ids:
            placement = table.shard(shard_id)
            if placement is None or placement.leader_id is None:
                return None
        return table

    return wait_until(ready, message="the cluster never published a leader for every shard")


def test_the_publisher_writes_on_change_and_not_on_a_timer():
    """Every write to the table is a reason for every client's cache to refresh."""
    metadata = _started_metadata()
    try:
        client = wait_for_metadata_client(metadata)
        cluster = _FakeCluster(default_range_map(2), addresses={1: "127.0.0.1:50051"})
        cluster.leaders = {0: (1, 4), 1: (2, 4)}
        publisher = MetadataPublisher(metadata.get_client(), cluster, poll_interval=0.05)

        publisher.publish_once()
        first = client.table(refresh=True)
        assert sorted(first.routes()) == [0, 1]
        assert first.shard(0).leader_id == 1 and first.shard(0).leader_term == 4
        assert first.shard(1).leader_id == 2
        assert first.shard(0).nodes == [1, 2, 3]
        assert first.shard(0).leader_address() == "127.0.0.1:50051"

        # Nothing moved, so nothing is written: the version stands still.
        publisher.publish_once()
        publisher.publish_once()
        assert client.table(refresh=True).version == first.version

        # A new term moves the table once, and once only.
        cluster.leaders[0] = (2, 5)
        publisher.publish_once()
        moved = client.table(refresh=True)
        assert moved.version == first.version + 1, "one change, one write"
        assert moved.shard(0).leader_id == 2 and moved.shard(0).leader_term == 5

        assert publisher.error is None
        assert publisher.leaders() == {0: (2, 5), 1: (2, 4)}
    finally:
        metadata.shutdown()


def test_a_shard_a_move_is_in_flight_for_is_left_to_the_move():
    """Which group serves a moving shard is the move's to write, not the publisher's.

    A shard being moved has two groups for a moment.  The cluster's own answer for it is the
    one it is leaving - that is where clients are still being routed - while the table may
    already name the group the rows went to, because the proposal landed and the rest of the
    move did not.  A publisher that wrote the cluster's answer would undo that proposal, and
    would do it on the first pass of a process that has just come back.
    """
    metadata = _started_metadata()
    try:
        client = wait_for_metadata_client(metadata)
        cluster = _FakeCluster(default_range_map(2), addresses={1: "127.0.0.1:50051"},
                               moving=[0])
        cluster.leaders = {0: (1, 4), 1: (2, 4)}
        publisher = MetadataPublisher(metadata.get_client(), cluster, poll_interval=0.05)

        publisher.publish_once()

        table = client.table(refresh=True)
        assert sorted(table.routes()) == [0, 1], "the ranges are published all the same"
        assert table.shard(0).nodes == [], "but not a set a move is still deciding"
        assert table.shard(0).leader_id is None
        assert table.shard(1).nodes == [1, 2, 3], "the shard that is not moving is described"
        assert table.shard(1).leader_id == 2

        # Not a permanent silence either: once the move is finished with the shard, the
        # publisher describes it like any other.
        cluster.moving = []
        publisher.publish_once()
        table = client.table(refresh=True)
        assert table.shard(0).nodes == [1, 2, 3]
        assert table.shard(0).leader_id == 1 and table.shard(0).leader_term == 4
    finally:
        metadata.shutdown()


def test_a_restart_publishing_the_same_ranges_is_not_a_disagreement():
    """A cluster that restarts re-proposes its bootstrap.  That is not a conflict."""
    metadata = _started_metadata()
    try:
        client = wait_for_metadata_client(metadata)
        ranges = default_range_map(2)
        assert client.init_routes(ranges).success, "the table survived the restart"

        cluster = _FakeCluster(ranges)
        cluster.leaders = {0: (1, 2), 1: (1, 2)}
        publisher = MetadataPublisher(metadata.get_client(), cluster, poll_interval=0.05)
        publisher.publish_once()

        assert publisher.error is None
        assert publisher.leaders() == {0: (1, 2), 1: (1, 2)}
        assert client.table(refresh=True).shard(1).leader_id == 1
    finally:
        metadata.shutdown()


def test_a_table_holding_the_ranges_of_our_own_split_is_ours():
    """A split can reach the group before the publisher's first pass reaches it.

    The cluster is then routing by the map it started with while the table holds the two
    ranges its own split created - and the second map is not a stranger's keyspace.  A
    publisher that stopped here would leave a range in the table with no replica set and
    no leader: a range every client routes to and nobody answers for.
    """
    metadata = _started_metadata()
    try:
        client = wait_for_metadata_client(metadata)
        before = {0: (b"", b"\xff")}
        after = {0: (b"", b"n"), 1: (b"n", b"\xff")}
        assert client.init_routes(before).success
        assert client.split_shard(0, b"n", 1, [2, 3], {2: "127.0.0.1:50052"}).success

        cluster = _FakeCluster(before, possible=[after])
        cluster.leaders = {0: (1, 3)}
        publisher = MetadataPublisher(metadata.get_client(), cluster, poll_interval=0.05)
        publisher.publish_once()

        assert publisher.error is None, "our own split is not somebody else's keyspace"
        assert sorted(client.table(refresh=True).routes()) == [0, 1]
        assert publisher.leaders() == {0: (1, 3)}, "the shard it still leads is published"
    finally:
        metadata.shutdown()


def test_a_table_that_disagrees_with_the_cluster_is_not_overwritten():
    """Two clusters publishing one keyspace is a mistake to see, not one to hide."""
    metadata = _started_metadata()
    try:
        client = wait_for_metadata_client(metadata)
        assert client.init_routes({0: (b"", b"\xff")}).success

        cluster = _FakeCluster(default_range_map(2))
        publisher = MetadataPublisher(metadata.get_client(), cluster, poll_interval=0.05)
        publisher.publish_once()

        assert publisher.error is not None and "different ranges" in publisher.error
        assert sorted(client.table(refresh=True).routes()) == [0], "the table is untouched"
    finally:
        metadata.shutdown()


def test_a_client_finds_the_shard_and_its_leader_through_the_cluster():
    """The end of the road: routing and leadership, from the cluster's own report."""
    metadata, tso_cluster, shard_cluster = _cluster_with_metadata()
    try:
        wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
        client = wait_for_metadata_client(metadata)
        table = _published_table(client, (0, 1))

        for key in (KEY_A, KEY_B):
            placement = table.shard_for(key)
            assert placement.shard_id == locate(shard_cluster.range_map(), key), key
            assert placement.nodes == [1, 2, 3]

            leader_id, term = shard_cluster.shard_leader(placement.shard_id)
            assert placement.leader_id == leader_id, "the table names the shard's leader"
            assert placement.leader_term == term
            assert shard_cluster.get_leader_for_key(key)[1].node_id == leader_id

            # The address it published is one that is really listening: this is the
            # hop a client would take, so it is worth opening the socket.
            address = placement.leader_address()
            assert address == shard_cluster.get_shard_server(leader_id).shard_address(
                placement.shard_id)
            host, port = address.split(":")
            with socket.create_connection((host, int(port)), timeout=2):
                pass
    finally:
        shard_cluster.shutdown()
        tso_cluster.shutdown()
        metadata.shutdown()


def test_the_table_follows_a_leader_change():
    """A leader that dies is replaced in the table by the node that took over.

    A table that still names the dead node is worse than no table: the client keeps
    asking it, and the answer is a connection refused rather than a leader.
    """
    metadata, tso_cluster, shard_cluster = _cluster_with_metadata()
    try:
        wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
        client = wait_for_metadata_client(metadata)
        before = _published_table(client, (0, 1))

        stopped = before.shard(0).leader_id
        affected = [shard_id for shard_id in (0, 1)
                    if before.shard(shard_id).leader_id == stopped]

        # Stopping the node takes its copy of every shard with it, which is how a
        # leader is actually lost.
        shard_cluster.get_shard_server(stopped).shutdown()

        def moved():
            table = client.table(refresh=True)
            for shard_id in affected:
                placement = table.shard(shard_id)
                if placement is None or placement.leader_id in (None, stopped):
                    return None
                if placement.leader_term <= before.shard(shard_id).leader_term:
                    return None
            return table

        after = wait_until(moved, message="the table never followed the leader change")
        for shard_id in affected:
            leader_id, term = shard_cluster.shard_leader(shard_id)
            assert after.shard(shard_id).leader_id == leader_id
            assert after.shard(shard_id).leader_term == term
            assert after.shard(shard_id).leader_id != stopped
    finally:
        shard_cluster.shutdown()
        tso_cluster.shutdown()
        metadata.shutdown()