"""A client routes by the table, and goes back to it when a shard says it is wrong.

The metadata group was a service nobody wrote to, and the table it holds was read by
nobody until a client routed by it.  This is that last join: a client reads the table
once - one linearizable read instead of a lookup per key - and treats what it read as a
decision it can be sent back to revisit, which is what a cached table has to be for the
cache to be safe rather than merely fast.

Two kinds of staleness, two ways back to the table.  The table can name a node that has
since stepped down, and the cache can see that for itself because the node itself says
so; and a shard can refuse a read because the node the client reached is no longer its
leader, which only the shard knows.  The first is a lookup that reads the table again,
the second is the client's retry, and a third test is the same leader change with a real
cluster, a real metadata group and a real node that dies.

The rest of the file pins what "routes by the table" has to mean: a client with a table
never looks at the cluster's own nodes, and a coordinator reads and writes by the same
table rather than by a scan of its own.
"""

from _ports import free_addresses
from _wait import (wait_for_keys_leader, wait_for_metadata_client,
                   wait_for_tso_client, wait_until)
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import MetadataCluster, RoutingTable, ShardPlacement
from oxidedb.raft.node import NodeState
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import ErrorCode, MVCCStateMachine, ReadResult
from oxidedb.transaction.coordinator import TransactionCoordinator
from oxidedb.transaction.smart_client import SmartClient
from oxidedb.tso.tso import TSOCluster

KEY_A = b"key0"      # first byte 0x6b -> shard 0
KEY_B = b"\x80key1"  # first byte 0x80 -> shard 1


# -- a cluster, a table and a node, small enough to be unit tests ---------------

class _FakeNode:
    """A shard node that answers with a value or refuses the way a deposed leader does:
    still claiming to lead, which is exactly what a client cannot detect."""

    def __init__(self, value=None, error=None, state=NodeState.LEADER):
        self._value = value
        self._error = error
        self.state = state

    def get(self, key, timestamp=None):
        if self._error is not None:
            return ReadResult.failure(self._error, "Not leader")
        return ReadResult.success(self._value)


class _FakeServer:
    def __init__(self, nodes):
        self._nodes = dict(nodes)

    def get_shard_node(self, shard_id):
        return self._nodes.get(shard_id)


class _FakeCluster:
    """Just enough cluster to hand out a node object per node id."""

    def __init__(self, servers):
        self._servers = dict(servers)

    def get_shard_server(self, node_id):
        return self._servers.get(node_id)


class _ShardlessCluster:
    """A cluster with no local shard nodes: what a client outside it has.

    ``_range_map`` is the one thing the table could not tell it and the group still
    needs, so a transaction can find which shard a key belongs to; the nodes are gone,
    so anything that resolves a leader by looking at them resolves nothing.
    """

    _range_map = {0: (b"", b"\xff")}
    _shard_servers = {}


class _FakeTableSource:
    """A metadata client, backed by one table that can be moved on.

    ``move_on`` is the cluster's publisher and the shards' elections rolled into one
    call: the stored table changes and nobody tells the client, which is the situation
    every one of these tests is about.
    """

    def __init__(self, table):
        self._table = table

    def move_on(self, table):
        self._table = table

    def table(self, refresh: bool = False):
        return self._table


class _FakeTso:
    def __init__(self):
        self._next = 0

    def get_timestamp(self):
        self._next += 1
        return self._next


def _table(leader_id, version: int = 1, nodes=(1, 2, 3)) -> RoutingTable:
    placement = ShardPlacement(0, b"", b"\xff", nodes=list(nodes),
                               leader_id=leader_id, leader_term=1)
    return RoutingTable(version, {0: placement})


def _single_shard_cluster(nodes):
    return _FakeCluster({node_id: _FakeServer({0: node})
                         for node_id, node in nodes.items()})


# -- the staleness a cache can see for itself ----------------------------------

def test_a_table_that_names_a_node_which_stepped_down_is_read_again():
    """The one staleness the cache can detect on its own: its leader has gone."""
    leading = _FakeNode(value=b"v0")
    taken_over = _FakeNode(value=b"v1")
    cluster = _single_shard_cluster({1: leading, 2: taken_over})
    source = _FakeTableSource(_table(1))
    cache = RoutingCache(cluster, source)
    assert cache.leader_for_key(KEY_A) is leading, "the table's answer first"
    assert cache.refreshes == 0, "a table that is right costs nothing"

    # Node 1 steps down and node 2 wins the election, and nobody tells this client:
    # the node the table names is the evidence, and evidence the cache can read.
    leading.state = NodeState.FOLLOWER
    source.move_on(_table(2, version=2))
    assert cache.leader_for_key(KEY_A) is taken_over
    assert cache.refreshes == 1, "the table was read again exactly once"


def test_a_table_that_names_nobody_is_not_a_reason_to_read_it_again():
    """A shard with no leader is a fact about the shard, not staleness in the table."""
    cluster = _single_shard_cluster({1: _FakeNode()})
    cache = RoutingCache(cluster, _FakeTableSource(_table(None)))

    assert cache.leader_for_key(KEY_A) is None
    assert cache.refreshes == 0, "reading it again would learn the same thing"


# -- the staleness only the shard knows ----------------------------------------

def test_a_shard_that_refuses_a_read_sends_the_client_back_to_the_table():
    """A node can claim to lead and still be refused by its peers; the shard knows."""
    old = _FakeNode(error=ErrorCode.ERR_NOT_LEADER)
    new = _FakeNode(value=b"v1")
    cluster = _single_shard_cluster({1: old, 2: new})
    source = _FakeTableSource(_table(1))
    router = RoutingCache(cluster, source)
    client = SmartClient(None, cluster, router=router)
    assert router.leader_for_key(KEY_A) is old, "this client has a table already"

    source.move_on(_table(2, version=2))
    assert client.get(KEY_A) == b"v1", "the retry reads what the new leader has"
    assert router.refreshes == 1


def test_a_transaction_routes_by_the_table_and_not_by_the_cluster():
    """A commit reads and writes by the same placement the reader used.

    The cluster here has no shard nodes at all, so a coordinator that resolved leaders
    by scanning them would resolve nothing: everything this transaction knows comes
    from the table.
    """
    node = _FakeNode(value=b"v1")
    cluster = _single_shard_cluster({1: node})
    router = RoutingCache(cluster, _FakeTableSource(_table(1)))
    coordinator = TransactionCoordinator(_FakeTso(), _ShardlessCluster(), router=router)

    txn_id, _ = coordinator.begin()
    assert coordinator.read(txn_id, KEY_A) == b"v1"


# -- the same two things over a real cluster -----------------------------------

def _cluster_with_metadata(num_shards=2):
    metadata = MetadataCluster(num_nodes=3)
    metadata.start()

    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())

    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=num_shards)
    shard_cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(num_shards=num_shards),
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


def test_a_client_with_a_table_never_routes_by_scanning_the_cluster():
    """The table is the placement a client uses; the cluster's own nodes are not."""
    metadata, tso_cluster, shard_cluster = _cluster_with_metadata()
    try:
        wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
        tso_client = wait_for_tso_client(tso_cluster)
        table_client = wait_for_metadata_client(metadata)
        _published_table(table_client, (0, 1))

        router = RoutingCache.from_cluster(shard_cluster)
        assert router is not None, "the cluster publishes, so the client has a table"
        client = SmartClient(tso_client, shard_cluster, router=router)
        assert client.put(KEY_A, b"v1")

        # From here on, asking the cluster where a key lives is a bug: this client has
        # a table, and the table is what it routes by.  Reaching for the cluster's own
        # nodes would be reading placement a client outside it could not have - and it
        # would pass this suite whether or not the table was ever used.
        def _no_scan(key):
            raise AssertionError("the client scanned the cluster instead of its table")

        shard_cluster.get_leader_for_key = _no_scan

        assert client.get(KEY_A) == b"v1"
        assert client.get(KEY_B) is None
    finally:
        shard_cluster.shutdown()
        tso_cluster.shutdown()
        metadata.shutdown()


def test_the_client_follows_a_leader_change_without_being_told():
    """The leader dies while the client holds a table that names it.  The read answers
    anyway, with the value that was committed, because the client goes back to the table
    instead of to a node that has already refused it."""
    metadata, tso_cluster, shard_cluster = _cluster_with_metadata()
    try:
        wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
        tso_client = wait_for_tso_client(tso_cluster)
        table_client = wait_for_metadata_client(metadata)
        _published_table(table_client, (0, 1))

        router = RoutingCache.from_cluster(shard_cluster)
        client = SmartClient(tso_client, shard_cluster, router=router)
        assert client.put(KEY_A, b"v1")
        assert client.get(KEY_A) == b"v1"

        stopped = table_client.table(refresh=True).shard_for(KEY_A).leader_id
        shard_cluster.get_shard_server(stopped).shutdown()

        def moved():
            placement = table_client.table(refresh=True).shard_for(KEY_A)
            if placement is None or placement.leader_id in (None, stopped):
                return None
            return placement

        placement = wait_until(moved, message="the table never followed the leader change")
        refreshes_before = router.refreshes

        # This client's table still names the dead node - nothing told it - and the read
        # has to get past that on its own.
        assert client.get(KEY_A) == b"v1"
        assert router.refreshes > refreshes_before
        assert placement.leader_id != stopped
        assert shard_cluster.get_shard_server(placement.leader_id) is not None
    finally:
        shard_cluster.shutdown()
        tso_cluster.shutdown()
        metadata.shutdown()
