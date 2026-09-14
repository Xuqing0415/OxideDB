"""A split reaches the routing table only after its rows are in the new shard.

The table is what clients route by, so a range it hands out has to be a range whose data
is already there: rows first, the table last.  The other order - the table first, or the
two at once - sends a client to a shard whose state machine has never seen the key, and
the answer it gets back (no such key) is indistinguishable from the row never having
existed.  Nothing in the table's own checks can catch that: the split the group is asked
to apply is a legal split of a legal range.

So what these tests pin is the order, from the one place it is visible - between the copy
and the proposal - and what the cluster does when the proposal cannot be made.  A split
that copied its rows and could not tell the table stays frozen, remembers what it was
doing, and finishes that same split when it is called again: the same id, the same rows,
the same proposal.  A retry that copied the rows a second time, or a second attempt that
a second split's worth of ranges could be built from, would lose exactly the rows the
freeze exists to protect.
"""

import socket

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_metadata_client, wait_until
from oxidedb.client import LocalNodeClientFactory
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import MetadataCluster
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import ApplyResult, CommandType, ErrorCode, MVCCStateMachine
from oxidedb.shard.router import locate

KEPT_KEY = b"a_key"    # below the split point: stays in shard 0
MOVED_KEY = b"z_key"   # above it: the new shard's row
NEW_SHARD = 1


def _set(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp)


def _cluster_with_metadata(num_shards=1):
    metadata = MetadataCluster(num_nodes=3)
    metadata.start()

    cluster = ShardedRaftCluster(num_nodes=3, num_shards=num_shards)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        # Room for the shard the split binds a port for; see _ports.free_addresses.
        peer_addresses=free_addresses(num_shards=2),
        lock_cleaner_interval=None,
        metadata=metadata,
    )
    return metadata, cluster


def _write_both_rows(cluster):
    """Two rows in the shard about to be split, one on each side of the point."""
    wait_for_keys_leader(cluster, [KEPT_KEY, MOVED_KEY])
    leader = cluster.get_leader_for_key(MOVED_KEY)[1]
    assert leader.propose(_set(leader._state_machine, KEPT_KEY, b"kept", 1)).success
    assert leader.propose(_set(leader._state_machine, MOVED_KEY, b"moved", 2)).success
    return leader


def _published(client, shard_ids):
    """The table, once the publisher has filled in every one of ``shard_ids``."""
    def ready():
        table = client.table(refresh=True)
        for shard_id in shard_ids:
            placement = table.shard(shard_id)
            if placement is None or placement.leader_id is None:
                return None
        return table

    return wait_until(ready, message="the cluster never published a leader for every shard")


class _WatchingClient:
    """The cluster's own metadata client, with a look taken at the split proposal.

    The order a split claims - rows copied, then the table told - is only observable
    between the two, which is where this looks.
    """

    def __init__(self, inner, cluster, seen):
        self._inner = inner
        self._cluster = cluster
        self._seen = seen

    def split_shard(self, shard_id, split_key, new_shard_id, nodes, addresses):
        leader_id, _term = self._cluster.shard_leader(new_shard_id)
        new_leader = self._cluster.get_shard_server(leader_id).get_shard_node(new_shard_id)
        storage = new_leader._state_machine._storage
        self._seen["moved_key_in_new_shard"] = (
            storage.get_latest_version(MOVED_KEY) is not None)
        self._seen["kept_key_in_new_shard"] = (
            storage.get_latest_version(KEPT_KEY) is not None)
        self._seen["cluster_routes_moved_key_to"] = locate(self._cluster.range_map(), MOVED_KEY)
        self._seen["table_routes"] = sorted(self._inner.table(refresh=True).routes())
        return self._inner.split_shard(shard_id, split_key, new_shard_id, nodes, addresses)


class _RefusingClient:
    """Stands in for a metadata group that cannot be reached at all.

    The split does all of its own work and the one call that would make it the clients'
    answer does not get through, which is the state the freeze has to survive.
    """

    def __init__(self, inner, refusals=1):
        self._inner = inner
        self._refusals = refusals
        self.calls = 0

    def split_shard(self, *args, **kwargs):
        self.calls += 1
        if self._refusals > 0:
            self._refusals -= 1
            return ApplyResult.failure(ErrorCode.ERR_NOT_LEADER,
                                       "the metadata group has no leader")
        return self._inner.split_shard(*args, **kwargs)


class _LostResponseClient:
    """The proposal lands, and the answer is lost on the way back.

    The caller cannot tell this from a proposal that never landed, so the group has to
    be able to answer both the same way - which it does by recognising the split it
    already applied.  Only the first answer is lost; the retry hears the real one.
    """

    def __init__(self, inner, loses=1):
        self._inner = inner
        self._loses = loses

    def split_shard(self, *args, **kwargs):
        self._inner.split_shard(*args, **kwargs)
        if self._loses > 0:
            self._loses -= 1
            return ApplyResult.failure(ErrorCode.ERR_NOT_LEADER, "the answer was lost")
        return ApplyResult.success()


def test_a_split_publishes_two_ranges_and_the_rows_are_there_first():
    metadata, cluster = _cluster_with_metadata()
    try:
        _write_both_rows(cluster)
        client = wait_for_metadata_client(metadata)
        before = _published(client, (0,))

        seen = {}
        cluster._metadata_client = _WatchingClient(cluster.metadata_client(), cluster, seen)
        assert cluster.split_shard(0, b"n"), cluster.split_error()

        # Seen from inside the proposal: the rows are in the new shard's group, the
        # half that stays is not, and neither the cluster nor the table has given the
        # range away yet.
        assert seen["moved_key_in_new_shard"] is True, "the table was told too early"
        assert seen["kept_key_in_new_shard"] is False, "only the right half is copied"
        assert seen["cluster_routes_moved_key_to"] == 0, "the cluster re-ranged too early"
        assert seen["table_routes"] == [0], "the table was re-ranged too early"

        table = client.table(refresh=True)
        assert sorted(table.routes()) == [0, NEW_SHARD], "one range became two"
        left, right = table.shard(0), table.shard(NEW_SHARD)
        assert (left.start, left.end) == (before.shard(0).start, b"n")
        assert (right.start, right.end) == (b"n", before.shard(0).end)
        assert left.nodes == before.shard(0).nodes, "the half that stayed is the same shard"
        assert right.nodes == [1, 2, 3]

        # The publisher fills in what the split could not know: who leads the new
        # shard, and at which address.  That address has to be answering.
        table = _published(client, (0, NEW_SHARD))
        assert table.shard(NEW_SHARD).leader_id is not None
        host, port = table.shard(NEW_SHARD).leader_address().split(":")
        with socket.create_connection((host, int(port)), timeout=2):
            pass

        # A client that routes by the table now reads the moved row from the new
        # shard, and the source shard is taking writes for the range it kept.
        # The route and the handle behind it: the client the cache hands out is the
        # one for the node the table names, which is the pair the factory is keyed by.
        factory = LocalNodeClientFactory(cluster)
        cache = RoutingCache(cluster, client, factory=factory)
        leader = cache.leader_for_key(MOVED_KEY)
        assert leader is factory.get_client(NEW_SHARD, table.shard(NEW_SHARD).leader_id)
        assert leader.get(MOVED_KEY).value == b"moved"

        source = cluster.get_shard_server(left.nodes[0]).get_shard_node(0)
        assert source is not None and not source.writes_frozen
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_split_the_table_never_hears_keeps_the_shard_frozen_and_is_retried():
    metadata, cluster = _cluster_with_metadata()
    try:
        source = _write_both_rows(cluster)
        client = wait_for_metadata_client(metadata)
        before = _published(client, (0,))

        refuser = _RefusingClient(cluster.metadata_client(), refusals=1)
        cluster._metadata_client = refuser

        assert not cluster.split_shard(0, b"n"), "the table was never told"
        assert refuser.calls == 1
        assert source.writes_frozen, "the rows are copied, so the shard has to stay frozen"
        refusal = source.propose(_set(source._state_machine, b"m_new", b"v", 5))
        assert not refusal.success and refusal.error_code == ErrorCode.ERR_SPLIT_IN_PROGRESS
        assert client.table(refresh=True).routes() == before.routes(), "the table is untouched"
        assert cluster.range_map() == before.routes(), "and so is the cluster's own map"
        assert cluster.pending_splits(), "the split is remembered, not thrown away"

        # Coming back is coming back to that same split, and it finishes it.
        assert cluster.split_shard(0, b"n"), cluster.split_error()
        assert not source.writes_frozen, "and now the shard can take writes again"
        assert not cluster.pending_splits()

        table = client.table(refresh=True)
        assert sorted(table.routes()) == [0, NEW_SHARD]
        assert table.shard(0).nodes == before.shard(0).nodes
        assert table.shard(NEW_SHARD).nodes == [1, 2, 3]

        table = _published(client, (NEW_SHARD,))
        leader = RoutingCache(cluster, client).leader_for_key(MOVED_KEY)
        assert leader is not None
        assert leader.get(MOVED_KEY).value == b"moved", "the copied row is in the new shard"
        assert leader.get(b"m_new").value is None, "the refused row is nowhere"
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_split_whose_answer_is_lost_is_not_applied_a_second_time(monkeypatch):
    metadata, cluster = _cluster_with_metadata()
    copies = []
    original = ShardedRaftCluster._move_row

    def counting_move_row(self, source_leader, target_leader, key, value):
        copies.append(key)
        return original(self, source_leader, target_leader, key, value)

    monkeypatch.setattr(ShardedRaftCluster, "_move_row", counting_move_row)
    try:
        source = _write_both_rows(cluster)
        client = wait_for_metadata_client(metadata)
        _published(client, (0,))

        cluster._metadata_client = _LostResponseClient(cluster.metadata_client())
        assert not cluster.split_shard(0, b"n"), "the answer was lost"

        applied = client.table(refresh=True)
        assert sorted(applied.routes()) == [0, NEW_SHARD], "the split did land"
        assert source.writes_frozen
        assert copies == [MOVED_KEY], "the first attempt copied the one moved row"

        assert cluster.split_shard(0, b"n"), cluster.split_error()
        assert client.table(refresh=True).version == applied.version, "one split, one write"
        assert copies == [MOVED_KEY], "the retry did not copy the row again"
        assert not source.writes_frozen
        assert locate(cluster.range_map(), MOVED_KEY) == NEW_SHARD

        _published(client, (NEW_SHARD,))
        leader = RoutingCache(cluster, client).leader_for_key(MOVED_KEY)
        assert leader is not None and leader.get(MOVED_KEY).value == b"moved"
    finally:
        cluster.shutdown()
        metadata.shutdown()
