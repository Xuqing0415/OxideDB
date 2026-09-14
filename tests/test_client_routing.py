"""A client routes by the table, and goes back to it when a shard says it is wrong.

The metadata group was a service nobody wrote to, and the table it holds was read by
nobody until a client routed by it.  This is that last join: a client reads the table
once - one linearizable read instead of a lookup per key - and treats what it read as a
decision it can be sent back to revisit, which is what a cached table has to be for the
cache to be safe rather than merely fast.

One kind of staleness and one way back to the table.  A cached table can name a node that
has stopped leading, and the only evidence of that a client can get is the shard refusing
to answer: the node it reached is the one whose own belief is in question, since a leader
cut off from its peers goes on answering reads as if nothing had happened.  So the lookup
hands out the client for the placement the table published and inspects nothing behind it,
the refusal is what reads the table again, and the last two tests are the same leader
change with a real cluster, a real metadata group and a real node that dies.

The middle of the file is the mechanism that does the re-reading: ``ShardLeaders``, which
answers with a client from the table when there is one and from the cluster when there is
not, and ``ask_shard``, which is the one place that turns a refusal into a second
question.

The rest of the file pins what "routes by the table" has to mean: a client with a table
never looks at the cluster's own nodes, and a coordinator reads and writes by the same
table rather than by a scan of its own.
"""

import time

import pytest

from _ports import free_addresses
from _wait import (wait_for_keys_leader, wait_for_metadata_client,
                   wait_for_tso_client, wait_until)
from oxidedb.client import LocalNodeClient, LocalNodeClientFactory, ShardLeaders, ask_shard
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import MetadataCluster, RoutingTable, ShardPlacement
from oxidedb.raft.node import NodeState
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import (ErrorCode, MVCCStateMachine, ReadResult,
                                        ScanRefused)
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


# -- what a lookup answers, and what it does not check -------------------------

def test_a_lookup_hands_out_the_node_the_table_names_without_asking_it():
    """The cache does not check whether the node it names still leads.

    It used to, and the check was not evidence even when it fired: the node this client
    reached is the one whose belief is in question, and one that stepped down cleanly is
    the case where reading it would have been right anyway.  A leader cut off from its
    peers believes it leads right up until something refuses it, and that refusal is a
    fact about the shard - which is why it, and not the node's own state, is what sends
    this client back to the table (the next test).  What a lookup answers with, then, is
    the client for the placement the table published, as it stands.
    """
    stepped_down = _FakeNode(value=b"v0", state=NodeState.FOLLOWER)
    cluster = _single_shard_cluster({1: stepped_down, 2: _FakeNode(value=b"v1")})
    cache = RoutingCache(cluster, _FakeTableSource(_table(1)))

    client = cache.leader_for_key(KEY_A)

    assert client is not None and client.get(KEY_A).value == b"v0", "asked as it stands"
    assert cache.refreshes == 0, "a lookup is not a place to re-read the table"


def test_a_table_that_names_nobody_is_read_again_once_the_publisher_could_have_spoken():
    """A range with no leader yet is a table mid-publication, not a final answer.

    Placement is published a command at a time - the ranges, then a shard's replica set,
    then its leader - so a client whose first read lands in between holds a table that
    names a range nobody leads.  Believed for ever, that is a client that never routes
    again; re-read on every lookup, it is a metadata round trip per read for a shard that
    really has no leader.  Hence a bound, which is what this pins: not this read, not the
    next one, but the one after the publisher has had time to write.
    """
    cluster = _single_shard_cluster({1: _FakeNode()})
    source = _FakeTableSource(_table(None))
    cache = RoutingCache(cluster, source, missing_leader_refresh_interval=0.05)

    assert cache.leader_for_key(KEY_A) is None, "the table does name nobody"
    assert cache.refreshes == 0, "the table was read a moment ago; nobody could have written"

    time.sleep(0.06)
    assert cache.leader_for_key(KEY_A) is None
    assert cache.refreshes == 1, "the client went back to the table"


# -- the staleness only the shard knows ----------------------------------------

def test_a_shard_that_refuses_a_read_sends_the_client_back_to_the_table():
    """A node can claim to lead and still be refused by its peers; the shard knows."""
    old = _FakeNode(error=ErrorCode.ERR_NOT_LEADER)
    new = _FakeNode(value=b"v1")
    cluster = _single_shard_cluster({1: old, 2: new})
    source = _FakeTableSource(_table(1))
    router = RoutingCache(cluster, source)
    client = SmartClient(None, cluster, router=router)
    assert isinstance(router.leader_for_key(KEY_A), LocalNodeClient)
    assert router.refreshes == 0, "this client has a table already"

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


# -- the two sources a placement can come from ---------------------------------

class _ClusterThatSaysWhoLeads:
    """A cluster a client is inside: asked *which node* leads, never handed over itself.

    ``shard_leader`` and ``range_map`` are the whole of what ``ShardLeaders`` uses when
    there is no table, and both answer in ids - a node id goes to a factory and a client
    comes back - so nothing above that line ends up holding the node.
    """

    def __init__(self, shards, range_map=None):
        self._shards = {node_id: dict(groups) for node_id, groups in shards.items()}
        self._range_map = dict(range_map or {0: (b"", b"\xff")})

    def get_shard_server(self, node_id):
        groups = self._shards.get(node_id)
        return None if groups is None else _FakeServer(groups)

    def shard_leader(self, shard_id):
        for node_id in sorted(self._shards):
            if shard_id in self._shards[node_id]:
                return (node_id, 1)
        return None

    def range_map(self):
        return dict(self._range_map)


class _FakeScanningNode:
    """A node that answers a range read with rows, or with the refusal it was built on."""

    def __init__(self, rows=(), refusal=None):
        self._rows = list(rows)
        self._refusal = refusal

    def scan(self, start_key, end_key, timestamp=None):
        if self._refusal is not None:
            raise self._refusal
        return list(self._rows)


def test_without_a_table_the_cluster_is_asked_which_node_leads():
    """A client inside a cluster holds no placement: it asks, and gets a client back."""
    node = _FakeNode(value=b"v1")
    leaders = ShardLeaders(_ClusterThatSaysWhoLeads({1: {0: node}}))

    client = leaders.leader_for_shard(0)

    assert isinstance(client, LocalNodeClient)
    assert client.get(KEY_A).value == b"v1"
    assert leaders.shard_for_key(KEY_A) == 0, "the cluster's ranges, not a table's"
    assert leaders.leader_for_key(KEY_A) is client, "one handle per node, not one per ask"
    assert leaders.leader_for_shard(7) is None, "a shard no node in this cluster leads"


def test_ask_shard_reads_the_table_again_once_and_gives_up_after_that():
    """Two refusals in a row are not staleness, so the second one is the answer.

    The first refusal is the thing a cached table cannot know it will get, and it is
    worth reading the table for.  A refusal that survives that read is the shard's
    current answer, and asking a third time would only make a wrong answer slower.
    """
    always_refusing = _FakeNode(error=ErrorCode.ERR_NOT_LEADER)
    cluster = _single_shard_cluster({1: always_refusing})
    router = RoutingCache(cluster, _FakeTableSource(_table(1)))
    leaders = ShardLeaders(cluster, router=router)
    asked = []

    answer = ask_shard(leaders, 0,
                       lambda client: asked.append(client) or client.get(KEY_A))

    assert answer is not None and answer.error_code == ErrorCode.ERR_NOT_LEADER
    assert len(asked) == 2, "the leader was asked twice, and no more"
    assert router.refreshes == 1, "with exactly one read of the table in between"


def test_ask_shard_leaves_a_refusal_that_is_not_about_leading_alone():
    """A lock in the way of a range read is not something the table can fix."""
    locked = _FakeScanningNode(refusal=ScanRefused(ErrorCode.ERR_LOCKED, "locked"))
    cluster = _single_shard_cluster({1: locked})
    router = RoutingCache(cluster, _FakeTableSource(_table(1)))
    leaders = ShardLeaders(cluster, router=router)

    with pytest.raises(ScanRefused):
        ask_shard(leaders, 0, lambda client: client.scan(b"", b"\xff"))

    assert router.refreshes == 0, "the table had nothing to do with this one"


def test_ask_shard_reports_a_range_read_that_never_reached_a_leader():
    """A scan refuses by raising, so its last refusal is raised rather than returned."""
    refusing = _FakeScanningNode(
        refusal=ScanRefused(ErrorCode.ERR_NOT_LEADER, "Not leader"))
    cluster = _single_shard_cluster({1: refusing})
    router = RoutingCache(cluster, _FakeTableSource(_table(1)))
    leaders = ShardLeaders(cluster, router=router)

    with pytest.raises(ScanRefused):
        ask_shard(leaders, 0, lambda client: client.scan(b"", b"\xff"))

    assert router.refreshes == 1, "the range read went back to the table once too"


def test_invalidate_drops_the_handle_for_the_node_the_table_names():
    """A handle that stopped working is dropped; the placement is left alone."""
    cluster = _single_shard_cluster({1: _FakeNode(value=b"v1")})
    factory = LocalNodeClientFactory(cluster)
    cache = RoutingCache(cluster, _FakeTableSource(_table(1)), factory=factory)

    first = cache.leader_for_shard(0)
    cache.invalidate(0)
    second = cache.leader_for_shard(0)

    assert first is not second, "the dropped handle is not handed out again"
    assert second is not None and cache.refreshes == 0, "and the table was not re-read"

    # A shard the table names nobody for has no handle to drop, and that is not an
    # error: a caller that found one broken and one that never had one want the same
    # thing to happen.
    nameless = RoutingCache(_single_shard_cluster({}), _FakeTableSource(_table(None)))
    nameless.invalidate(0)


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
