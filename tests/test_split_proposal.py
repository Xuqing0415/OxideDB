"""The one command that changes which range a shard answers for.

A split is proposed *after* the rows have moved, so the table is the last thing to learn
about it and the first thing a client believes.  That makes the group the arbiter rather
than the caller's notebook: a proposal it applied and the caller got wrong would send
every client to a range whose data was never copied there.  So every check is made
against the table as it stands, and a failure is refused whole - a half-applied split is
a range nobody owns.

What these tests pin: a legal split replaces one range with the two that partition it and
changes nothing else; a split point outside the range, or on either end of it, is refused;
a shard that is not in the table, or a new id that is already taken, is refused; a retry
of the split that was applied is a success - the caller cannot tell a lost response from a
lost proposal, so the group has to answer both the same way - but a retry carrying a
different replica set is not that split; every replica applies the same table; and a
refused proposal leaves the table exactly as it was, version included.
"""

from _wait import wait_for_metadata_client
from oxidedb.metadata.service import MetadataCluster
from oxidedb.shard.router import default_range_map, locate

ADDRESSES = {1: "127.0.0.1:50051", 2: "127.0.0.1:50151", 3: "127.0.0.1:50251"}

#: Shard 0 of the default two-shard map owns (0x00, 0x80); shard 1 owns (0x80, 0xff).
SPLIT_POINT = b"\x40"
NEW_SHARD = 2


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


def _assert_untouched(client, before):
    """A refused proposal is not a write: same ranges, same version."""
    after = client.table(refresh=True)
    assert after.version == before.version, "a refusal must not move the version"
    assert after.routes() == before.routes()


def test_a_split_replaces_one_range_with_the_two_that_partition_it():
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)
        old = before.shard(0)

        result = client.split_shard(0, SPLIT_POINT, NEW_SHARD, [1, 2, 3], ADDRESSES)
        assert result.success, result.error_msg

        after = client.table(refresh=True)
        assert after.version == before.version + 1, "one split, one write"
        assert sorted(after.routes()) == [0, 1, NEW_SHARD]

        left, right = after.shard(0), after.shard(NEW_SHARD)
        assert (left.start, left.end) == (old.start, SPLIT_POINT)
        assert (right.start, right.end) == (SPLIT_POINT, old.end)
        assert len(after.shards) == 3, "the old range is gone, not kept alongside"

        # The left half is the same shard with less to answer for, so the table still
        # knows who serves it; the right half is new, and nobody leads it yet.
        assert left.nodes == old.nodes and left.addresses == old.addresses
        assert right.nodes == [1, 2, 3] and right.addresses == ADDRESSES
        assert right.leader_id is None

        # Every key still has exactly one shard, and the ones above the split point have
        # changed hands.
        assert after.route_for(b"\x3f") == 0
        assert after.route_for(SPLIT_POINT) == NEW_SHARD
        assert after.route_for(b"\x60") == NEW_SHARD
        assert after.route_for(b"\x80") == 1
        assert after.route_for(b"\xfe") == 1
    finally:
        cluster.shutdown()


def test_a_split_point_outside_the_range_is_refused():
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)

        for split_key in (b"", b"\xff", b"\x80" + b"\x00"):  # below, and above
            result = client.split_shard(0, split_key, NEW_SHARD, [1, 2, 3], ADDRESSES)
            assert not result.success, split_key
            assert result.error_code == 8, result.error_msg
            _assert_untouched(client, before)
    finally:
        cluster.shutdown()


def test_a_split_point_on_an_end_of_the_range_is_refused():
    """Either half would be empty, and an empty range is a shard answering for nothing."""
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)
        old = before.shard(0)

        for split_key in (old.start, old.end):
            result = client.split_shard(0, split_key, NEW_SHARD, [1, 2, 3], ADDRESSES)
            assert not result.success, split_key
            assert result.error_code == 8, result.error_msg
            _assert_untouched(client, before)
    finally:
        cluster.shutdown()


def test_a_new_id_that_is_already_taken_is_refused():
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)

        result = client.split_shard(0, SPLIT_POINT, 1, [1, 2, 3], ADDRESSES)
        assert not result.success, "shard 1 is already in the table"
        assert result.error_code == 9, result.error_msg
        _assert_untouched(client, before)
    finally:
        cluster.shutdown()


def test_a_shard_that_is_not_in_the_table_cannot_be_split():
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        before = client.table(refresh=True)

        result = client.split_shard(7, SPLIT_POINT, NEW_SHARD, [1, 2, 3], ADDRESSES)
        assert not result.success
        assert result.error_code == 7, result.error_msg
        _assert_untouched(client, before)
    finally:
        cluster.shutdown()


def test_the_same_split_twice_is_a_success_and_not_a_second_split():
    """A retry has to be indistinguishable from the first attempt, to the caller."""
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.split_shard(0, SPLIT_POINT, NEW_SHARD, [1, 2, 3], ADDRESSES).success
        once = client.table(refresh=True)

        again = client.split_shard(0, SPLIT_POINT, NEW_SHARD, [1, 2, 3], ADDRESSES)
        assert again.success, "the same proposal twice is the same split"
        assert client.table(refresh=True).version == once.version, "and not a second write"
        assert client.table(refresh=True).routes() == once.routes()
    finally:
        cluster.shutdown()


def test_a_retry_with_a_different_replica_set_is_not_that_split():
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.split_shard(0, SPLIT_POINT, NEW_SHARD, [1, 2, 3], ADDRESSES).success
        after = client.table(refresh=True)

        result = client.split_shard(0, SPLIT_POINT, NEW_SHARD, [1, 2], ADDRESSES)
        assert not result.success, "that range was created with a different replica set"
        assert result.error_code == 10, result.error_msg
        _assert_untouched(client, after)
    finally:
        cluster.shutdown()


def test_a_split_that_would_overlap_another_shard_is_refused():
    """The group will not hand out a range that a shard already answers for.

    The table this starts from is not a partition - it was installed that way, since the
    group cannot know what the caller means by the whole keyspace - and the point is that
    a split does not make a bad table worse: a range two shards both claim is exactly what
    a client cannot route by.
    """
    cluster = _started_cluster()
    try:
        client = wait_for_metadata_client(cluster)
        assert client.init_routes({0: (b"\x00", b"\xff"),
                                   1: (b"\x80", b"\xff")}).success
        before = client.table(refresh=True)

        # The right half would be (0x40, 0xff), and shard 1 owns (0x80, 0xff).
        result = client.split_shard(0, b"\x40", NEW_SHARD, [1, 2, 3], ADDRESSES)
        assert not result.success
        assert result.error_code == 11, result.error_msg
        _assert_untouched(client, before)
    finally:
        cluster.shutdown()

def test_every_replica_applies_the_split():
    """The table is a decision, so the split has to be in all three copies of it."""
    cluster = _started_cluster()
    try:
        client = _bootstrapped_client(cluster)
        assert client.split_shard(0, SPLIT_POINT, NEW_SHARD, [1, 2, 3], ADDRESSES).success
        table = client.table(refresh=True)

        for node_id in (1, 2, 3):
            replica = cluster.get_node(node_id)._state_machine.table()
            assert replica.version == table.version, node_id
            assert replica.routes() == table.routes(), node_id
            assert replica.shard(NEW_SHARD).nodes == [1, 2, 3], node_id

        # ...and it is the same answer the one routing rule would give.
        for key in (b"\x00", b"\x3f", b"\x40", b"\x7f", b"\x80", b"\xfe"):
            assert table.route_for(key) == locate(table.routes(), key), key
    finally:
        cluster.shutdown()
