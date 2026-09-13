"""The routing table has an owner now.

Sharding was frozen because every process built its own copy of the table at startup and
nobody owned the answer to "which shard holds this key": a split could not change it, and
a client had nobody to ask.  The table now lives in its own Raft group - one group for the
whole cluster, the same shape as the timestamp group - and a client reads it from there
and caches it, so routing a key costs nothing until the table changes.

What these tests pin: the client routes keys by the one routing rule, on the table it
read; the ranges, the placement and the leaders arrive in the same read at one version;
the table is replicated, so it is a decision rather than a belief; a leader report from an
older term loses; a node that does not serve a shard cannot claim it; a second bootstrap
does not undo a table that has since changed; and a group without a quorum refuses to
serve the table rather than answering from a local copy.
"""

import pytest

from _wait import wait_for_metadata_client, wait_until
from oxidedb.metadata.service import (MetadataCluster, MetadataCommandType,
                                      MetadataStateMachine, TABLE_KEY)
from oxidedb.shard.router import default_range_map, locate

ADDRESSES = {1: "127.0.0.1:50051", 2: "127.0.0.1:50151", 3: "127.0.0.1:50251"}


def _started_cluster():
    cluster = MetadataCluster(num_nodes=3)
    cluster.start()
    return cluster


def _bootstrapped_client(cluster):
    client = wait_for_metadata_client(cluster)
    ranges = default_range_map(2)
    assert client.init_routes(ranges).success
    for shard_id in ranges:
        assert client.set_shard_nodes(shard_id, [1, 2, 3], ADDRESSES).success
    return client


def test_a_client_routes_keys_by_the_one_routing_rule():
    """Two shards, and every key goes where ``router.locate`` puts it."""
    cluster = _started_cluster()
    try:
        client = wait_for_metadata_client(cluster)
        ranges = default_range_map(2)
        assert client.init_routes(ranges).success

        table = client.table(refresh=True)
        assert table.version >= 1
        assert sorted(table.routes()) == sorted(ranges)
        for key in (b"", b"a", b"\x7f", b"\x80", b"\x90key", b"\xfe", b"\xff"):
            assert client.get_shard_for(key) == locate(ranges, key), key
    finally:
        cluster.shutdown()


def test_placement_and_leaders_arrive_in_one_read():
    """The table is one version, so its ranges and its leaders cannot disagree."""
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)

        assert client.report_leader(0, 3, 7).success
        assert client.report_leader(1, 2, 4).success

        after = client.table(refresh=True)
        assert after.version > before.version, "a write has to move the version"

        shard_0 = after.shard(0)
        assert (shard_0.start, shard_0.end) == (bytes([0]), bytes([128]))
        assert shard_0.nodes == [1, 2, 3]
        assert shard_0.leader_id == 3 and shard_0.leader_term == 7
        assert shard_0.leader_address() == ADDRESSES[3]
        assert after.shard(1).leader_id == 2
        assert after.shard(1).leader_address() == ADDRESSES[2]

        # The same read answers "which shard" and "which leader" for a key.
        placement = after.shard_for(b"\x90key")
        assert after.route_for(b"\x90key") == placement.shard_id == 1
        assert placement.leader_id == 2

        # Every replica applied it: the table is a decision, not a local belief.
        for node_id in (1, 2, 3):
            state_machine = cluster.get_node(node_id)._state_machine
            wait_until(
                lambda sm=state_machine: sm.table().version == after.version,
                message=f"node {node_id} never applied the table at version {after.version}",
            )
            assert state_machine.table().shard(0).leader_id == 3
    finally:
        cluster.shutdown()


def test_a_leader_report_from_an_older_term_loses():
    """Terms are kept, so a leader that has been deposed cannot write itself back in."""
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)

        assert client.report_leader(0, 1, 9).success
        assert client.get_shard_leader(0) == 1

        stale = client.report_leader(0, 2, 8)
        assert not stale.success and "term" in stale.error_msg
        assert not client.report_leader(0, 1, 9).success, "the same term is not news either"
        assert client.get_shard_leader(0) == 1, "a stale report must not move the table"

        assert client.report_leader(0, 2, 10).success
        assert client.get_shard_leader(0) == 2

        # A node outside the replica set cannot claim the shard at any term.
        outsider = client.report_leader(0, 9, 11)
        assert not outsider.success and "does not serve" in outsider.error_msg
        assert client.get_shard_leader(0) == 2
    finally:
        cluster.shutdown()


def test_a_second_bootstrap_does_not_undo_the_table():
    """A restarted cluster re-proposes its bootstrap; the table keeps what it has."""
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.report_leader(0, 2, 5).success

        second = client.init_routes({0: (b"", b"\xff")})
        assert not second.success and "already initialised" in second.error_msg

        table = client.table(refresh=True)
        assert sorted(table.routes()) == [0, 1], "the ranges are the ones that were there"
        assert table.shard(0).nodes == [1, 2, 3]
        assert table.shard(0).leader_id == 2
    finally:
        cluster.shutdown()


def test_a_group_without_a_quorum_refuses_to_serve_the_table():
    """The read is the ReadIndex read, so a partitioned leader cannot hand one out.

    The alternative - answering from the local copy - is how a client ends up routing
    on a table that has since been replaced.  A client that already read the table
    keeps routing on it, which is the difference between a justified cache and a guess.
    """
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.get_shard_for(b"a") == locate(default_range_map(2), b"a")

        for node_id in (1, 2, 3):
            cluster.get_node(node_id)._get_peer_node = lambda _peer_id: None

        with pytest.raises(RuntimeError, match="cannot be read"):
            client.refresh()
        assert client.get_shard_for(b"a") == locate(default_range_map(2), b"a")
    finally:
        cluster.shutdown()


def test_the_table_survives_a_snapshot_round_trip():
    """A node that restores a snapshot restores the table, not an empty one."""
    state_machine = MetadataStateMachine()
    for command in (
        state_machine.serialize_command(
            MetadataCommandType.INIT_ROUTES,
            ranges=[[0, b"\x00", b"\x80"], [1, b"\x80", b"\xff"]]),
        state_machine.serialize_command(
            MetadataCommandType.SET_SHARD_NODES,
            shard_id=1, nodes=[1, 2, 3], addresses=[[1, ADDRESSES[1]]]),
        state_machine.serialize_command(
            MetadataCommandType.REPORT_LEADER, shard_id=1, node_id=1, term=12),
    ):
        assert state_machine.apply(command).success

    version = state_machine.version
    restored = MetadataStateMachine()
    restored.restore(state_machine.snapshot())

    table = restored.table()
    assert table.version == version, "the version has to travel with the table"
    assert sorted(table.routes()) == [0, 1]
    assert table.shard(1).leader_id == 1 and table.shard(1).leader_term == 12
    assert restored.get(TABLE_KEY).success