"""The one command that changes which nodes serve a shard, and nothing else about it.

A move is proposed *after* the rows have been copied to the new group, so the table is the
last thing to learn about it and the first thing a client believes - the same order a split
takes, and for the same reason (``docs/design.md`` section 7).  That makes the group the
arbiter of the proposal rather than the caller's notebook: what the caller believes is
serving the shard is checked against the table before anything is written, because two moves
computed from one table would otherwise both apply, and the second would name a group that
never received the data.

What these tests pin: a legal move replaces the replica set and its addresses, drops the
leader claim the group that left had made, and leaves the range alone; a shard that is not in
the table cannot be moved; a replica set that is empty, repeats a node, or leaves one of its
nodes without an address is refused; a proposal whose expectation of the current replica set
does not hold is refused; a set that overlaps the one leaving is refused, because a node
cannot hold two groups for the same shard; a retry of a move that applied is a success rather
than a second move, while a retry carrying other addresses is not that move; every replica
applies the same table; and a refused proposal leaves the table exactly as it was, version
included.
"""

from _wait import wait_for_metadata_client
from oxidedb.metadata.service import MetadataCluster
from oxidedb.shard.router import default_range_map, locate

ADDRESSES = {1: "127.0.0.1:50051", 2: "127.0.0.1:50151", 3: "127.0.0.1:50251"}

#: The nodes a move in these tests goes to, and where they listen.  None of them serves the
#: shard now: a move replaces the group, and a set that overlapped the one leaving it would
#: be a member change, which needs the group that is already serving (see code 15 below).
MOVED_TO = {4: "127.0.0.1:51151", 5: "127.0.0.1:51251", 6: "127.0.0.1:51351"}

#: The same nodes listening somewhere else, which is a different placement of them.
ELSEWHERE = {4: "127.0.0.1:52151", 5: "127.0.0.1:52251", 6: "127.0.0.1:52351"}

#: A set sharing node 3 with the one serving the shard: that is a member change, not a move.
OVERLAPPING = {3: "127.0.0.1:53151", 4: "127.0.0.1:53251", 5: "127.0.0.1:53351"}

#: Shard 0 of the default two-shard map owns (0x00, 0x80), and is served by all three nodes.
SERVING = [1, 2, 3]
MOVING = 0


def _started_cluster():
    cluster = MetadataCluster(num_nodes=3)
    cluster.start()
    return cluster


def _bootstrapped_client(cluster):
    client = wait_for_metadata_client(cluster)
    ranges = default_range_map(2)
    assert client.init_routes(ranges).success
    for shard_id in ranges:
        assert client.set_shard_nodes(shard_id, SERVING, ADDRESSES).success
    return client


def _placement_key(table):
    """Everything the table says about its shards, as plain data to compare."""
    return {shard_id: (placement.start, placement.end, placement.nodes,
                       dict(placement.addresses), placement.leader_id,
                       placement.leader_term)
            for shard_id, placement in table.shards.items()}


def _assert_untouched(client, before):
    """A refused proposal is not a write: same ranges, same placements, same version."""
    after = client.table(refresh=True)
    assert after.version == before.version, "a refusal must not move the version"
    assert _placement_key(after) == _placement_key(before)


def test_a_move_replaces_the_replica_set_and_its_addresses():
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)
        old = before.shard(MOVING)

        result = client.move_shard(MOVING, SERVING, [4, 5, 6], MOVED_TO)
        assert result.success, result.error_msg

        after = client.table(refresh=True)
        assert after.version == before.version + 1, "one move, one write"

        moved = after.shard(MOVING)
        assert moved.nodes == [4, 5, 6]
        assert moved.addresses == MOVED_TO
        assert (moved.start, moved.end) == (old.start, old.end), "a move is not a re-range"
        assert after.routes() == before.routes(), "and the keys a shard answers for hold"

        # The shard nobody moved keeps everything it had, including who serves it.
        assert after.shard(1).nodes == SERVING
        assert after.shard(1).addresses == ADDRESSES
    finally:
        cluster.shutdown()


def test_a_shard_that_is_not_in_the_table_cannot_be_moved():
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)

        result = client.move_shard(7, SERVING, [4, 5, 6], MOVED_TO)
        assert not result.success
        assert result.error_code == 12, result.error_msg
        _assert_untouched(client, before)
    finally:
        cluster.shutdown()


def test_a_replica_set_that_could_not_serve_the_shard_is_refused():
    """Empty, repeated, or a node with no address: the table cannot send a client there.

    The last one is the one that matters: the point of the move is that clients are sent to
    these nodes now, so a set the table cannot name an address for is no set at all.
    """
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)

        unusable = [([], MOVED_TO), ([4, 5, 5], MOVED_TO),
                    ([4, 5, 6], {4: MOVED_TO[4], 5: MOVED_TO[5]})]
        for nodes, addresses in unusable:
            result = client.move_shard(MOVING, SERVING, nodes, addresses)
            assert not result.success, nodes
            assert result.error_code == 13, (nodes, result.error_msg)
            _assert_untouched(client, before)
    finally:
        cluster.shutdown()


def test_a_move_whose_expectation_does_not_hold_is_refused():
    """What the caller thinks is serving the shard is the guard, so it has to be checked.

    The table is written directly here, which is what a second move computed from the old
    table looks like from the group's side: the caller is naming a replica set that is not
    there any more, so its proposal is built on rows it did not read.
    """
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.set_shard_nodes(MOVING, [1, 2],
                                      {1: ADDRESSES[1], 2: ADDRESSES[2]}).success
        before = client.table(refresh=True)

        result = client.move_shard(MOVING, SERVING, [4, 5, 6], MOVED_TO)
        assert not result.success, "the table says [1, 2], not the [1, 2, 3] proposed"
        assert result.error_code == 14, result.error_msg
        _assert_untouched(client, before)
    finally:
        cluster.shutdown()


def test_a_move_onto_a_node_that_already_serves_the_shard_is_refused():
    """A set that partly overlaps is a member change, which needs the group that is serving.

    This group does not implement one, and a node cannot hold two Raft groups for the same
    shard - so the proposal is refused rather than half-built into a cluster that would have
    one node receiving rows it is also the source of.
    """
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)

        result = client.move_shard(MOVING, SERVING, [3, 4, 5], OVERLAPPING)
        assert not result.success, "node 3 is serving this shard right now"
        assert result.error_code == 15, result.error_msg
        _assert_untouched(client, before)
    finally:
        cluster.shutdown()


def test_the_same_move_twice_is_a_success_and_not_a_second_move():
    """A retry has to be indistinguishable from the first attempt, to the caller."""
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.move_shard(MOVING, SERVING, [4, 5, 6], MOVED_TO).success
        once = client.table(refresh=True)

        again = client.move_shard(MOVING, SERVING, [4, 5, 6], MOVED_TO)
        assert again.success, "the same proposal twice is the same move"
        after = client.table(refresh=True)
        assert after.version == once.version, "and not a second write"
        assert _placement_key(after) == _placement_key(once)
    finally:
        cluster.shutdown()


def test_a_retry_carrying_other_addresses_is_not_that_move():
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.move_shard(MOVING, SERVING, [4, 5, 6], MOVED_TO).success
        after = client.table(refresh=True)

        result = client.move_shard(MOVING, SERVING, [4, 5, 6], ELSEWHERE)
        assert not result.success, "the table has those nodes at other addresses"
        assert result.error_code == 16, result.error_msg
        _assert_untouched(client, after)
    finally:
        cluster.shutdown()


def test_the_leader_of_the_group_that_left_is_not_carried_over():
    """The new group has its own election, so the claim the old one made cannot stay.

    The claim is made at a term nothing else here reaches, so a table that kept it would
    name a node that does not serve the shard any more - and, worse, would turn the leader
    of the group that does serve it away at the term gate.
    """
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.report_leader(MOVING, 1, 5).success
        assert client.table(refresh=True).shard(MOVING).leader_id == 1

        assert client.move_shard(MOVING, SERVING, [4, 5, 6], MOVED_TO).success

        moved = client.table(refresh=True).shard(MOVING)
        assert moved.leader_id is None
        assert moved.leader_term == 0, "the term went with the group that led"

        # The group that does serve it can name its leader at the first term it reaches.
        assert client.report_leader(MOVING, 4, 1).success
        assert client.table(refresh=True).shard(MOVING).leader_id == 4
    finally:
        cluster.shutdown()


def test_every_replica_applies_the_move():
    """The table is a decision, so the move has to be in all three copies of it."""
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.move_shard(MOVING, SERVING, [4, 5, 6], MOVED_TO).success
        table = client.table(refresh=True)

        for node_id in (1, 2, 3):
            replica = cluster.get_node(node_id)._state_machine.table()
            assert replica.version == table.version, node_id
            assert replica.routes() == table.routes(), node_id
            assert replica.shard(MOVING).nodes == [4, 5, 6], node_id
            assert replica.shard(MOVING).addresses == MOVED_TO, node_id

        # ...and it is the same answer the one routing rule would give, unchanged.
        for key in (b"\x00", b"\x3f", b"\x7f", b"\x80", b"\xfe"):
            assert table.route_for(key) == locate(table.routes(), key), key
    finally:
        cluster.shutdown()
