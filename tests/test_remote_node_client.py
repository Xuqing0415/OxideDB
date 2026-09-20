"""The client on the other side of the wire, and the retry a hint buys.

Everything in this file talks to a shard the way a process outside the cluster would: over
a channel to an address, through RemoteNodeClient, with the cluster itself in this process
only because a test has to start one somewhere.

Three things are pinned.  The first is sameness: a client across a wire answers the same
questions as one holding the node, field for field, because a caller that can tell them
apart is a caller that has to be rewritten to cross the wire.  The second is what is left
when a node does not answer at all - a named failure and not a refusal, since there is
nothing in it for a caller to act on.  The third is the leader hint: a node that has stopped
leading says where the leader is, and the retry follows that address without reading the
routing table again.  That last one is about a write: a read sent to a node that does not
lead is answered by that node now - the servicer gets the index such a read has to be
answered at - so a read never reaches the point of refusing with a hint.
"""

import pytest

from _ports import allocate_port, free_addresses
from _wait import wait_for_keys_leader, wait_for_tso_client, wait_until
from oxidedb.client import (LocalNodeClient, NodeUnreachable, RemoteNodeClient,
                            RemoteNodeClientFactory)
from oxidedb.client.routing import ShardLeaders, ask_shard
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import RoutingTable, ShardPlacement
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import (CommandType, ErrorCode, MVCCStateMachine,
                                        ScanRefused, serialize_command)
from oxidedb.shard.router import default_range_map
from oxidedb.transaction.smart_client import SmartClient
from oxidedb.tso.tso import TSOCluster

KEY_A = b"key0"      # first byte 0x6b -> shard 0
KEY_B = b"\x80key1"  # first byte 0x80 -> shard 1

_BUILT_CLUSTERS = []
_BUILT_FACTORIES = []


@pytest.fixture(autouse=True)
def _close_what_the_test_opened():
    yield
    while _BUILT_FACTORIES:
        _BUILT_FACTORIES.pop().close()
    while _BUILT_CLUSTERS:
        _BUILT_CLUSTERS.pop().shutdown()


def _cluster(num_shards: int = 2) -> ShardedRaftCluster:
    """A cluster whose shards really listen, since a wire is the point of this file.

    The lock cleaner is off: a test here holds a lock on purpose, and a sweeper that
    resolved it half way through would be a test that failed for the wrong reason.
    """
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=num_shards)
    _BUILT_CLUSTERS.append(cluster)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(),
                          peer_addresses=free_addresses(),
                          lock_cleaner_interval=None)
    return cluster


def _factory() -> RemoteNodeClientFactory:
    factory = RemoteNodeClientFactory()
    _BUILT_FACTORIES.append(factory)
    return factory


def _settled_leader(cluster, shard_id: int = 0):
    """The node that leads a shard, and the term it leads at, once one does."""
    return wait_until(lambda: cluster.shard_leader(shard_id), timeout=20,
                      message=f"no leader for shard {shard_id}")


def _a_follower(cluster, leader_id: int) -> int:
    """A node that serves the same shard and is not its leader."""
    follower_id = next(node_id for node_id in sorted(cluster._shard_servers)
                       if node_id != leader_id)
    assert cluster.get_shard_server(follower_id) is not None
    return follower_id


def _leader_client(cluster, factory, shard_id: int = 0):
    """A remote client for whatever node leads a shard, and that node's id."""
    leader_id, _term = _settled_leader(cluster, shard_id)
    address = cluster.shard_addresses(shard_id)[leader_id]
    return factory.get_client(shard_id, leader_id, address), leader_id


def _read_shape(result):
    """Every field a read answers with, so equality is field by field."""
    return (result.success, result.value, result.commit_ts, result.error_code,
            result.error_msg)


def _table_over(cluster, leader_of, version: int = 1) -> RoutingTable:
    """The table as the cluster really is, with each shard's leader chosen by the caller.

    leader_of(shard_id) is where a test puts staleness: a placement that names a node
    which is no longer the leader, held still, which is what the publisher's poll interval
    looks like from inside a client.
    """
    return RoutingTable(version, {
        shard_id: ShardPlacement(shard_id, start, end,
                                 nodes=cluster.shard_replica_ids(shard_id),
                                 addresses=cluster.shard_addresses(shard_id),
                                 leader_id=leader_of(shard_id),
                                 leader_term=1)
        for shard_id, (start, end) in default_range_map(2).items()
    })


class _FixedTable:
    """A metadata client whose answer is the same every time it is asked."""

    def __init__(self, table):
        self._table = table

    def table(self, refresh: bool = False):
        return self._table


def _cache_over(cluster, table, factory) -> RoutingCache:
    """A routing cache over one table that never changes, so no test waits on a publisher.

    A real RoutingCache with a fixed source rather than a stand-in for one, so that
    everything else about it - the factory, the refresh counter, the refusal path - is the
    thing this file is about.
    """
    return RoutingCache(cluster, _FixedTable(table), factory=factory)


# -- the same answers, across a wire -------------------------------------------

def test_a_client_across_a_wire_answers_the_same_as_one_in_this_process():
    """Call by call, on a real shard with a lock and a committed version in it.

    Including the answers that are not values: a key with no version at this snapshot, and
    a key behind a lock.  Those are the distinctions the callers above this layer decide
    on, so they are the ones that have to survive being put in a message.
    """
    cluster = _cluster()
    factory = _factory()
    wait_for_keys_leader(cluster, [KEY_A])
    remote, leader_id = _leader_client(cluster, factory)
    local = LocalNodeClient(cluster.get_shard_server(leader_id).get_shard_node(0))

    assert _read_shape(remote.get(b"missing")) == _read_shape(local.get(b"missing"))

    prewrite = serialize_command(CommandType.PREWRITE, key=KEY_A, value=b"v1",
                                 start_ts=100, primary_key=KEY_A)
    assert remote.propose(prewrite).success
    assert remote.get_lock(KEY_A) == local.get_lock(KEY_A), (
        "the lock record, field for field, including the moment it was taken")

    reads = [(KEY_A, None), (KEY_A, 50), (KEY_A, 150), (b"missing", None)]
    for key, timestamp in reads:
        assert _read_shape(remote.get(key, timestamp)) == \
            _read_shape(local.get(key, timestamp)), (key, timestamp)

    # A range read with the lock still standing refuses on both sides rather than coming
    # back short: a key left out of the rows looks exactly like a key that is not there.
    with pytest.raises(ScanRefused) as here:
        local.scan(b"a", b"z")
    with pytest.raises(ScanRefused) as there:
        remote.scan(b"a", b"z")
    assert (there.value.error_code, there.value.key) == (here.value.error_code, here.value.key)

    committed = remote.propose(serialize_command(CommandType.COMMIT, key=KEY_A,
                                                 start_ts=100, commit_ts=200))
    assert committed.success
    assert committed.index is not None, "a landed write says where it landed"
    assert remote.scan(b"a", b"z") == local.scan(b"a", b"z"), (
        "with the lock committed away, the range reads the same on both sides")
    assert remote.scan_versions(b"a", b"z") == local.scan_versions(b"a", b"z")
    assert remote.get_write_record(KEY_A) == local.get_write_record(KEY_A)
    assert remote.get_write_record(KEY_A) == {"start_ts": 100, "commit_ts": 200}

    assert _read_shape(remote.get(KEY_A)) == _read_shape(local.get(KEY_A))
    assert remote.get(KEY_A).value == b"v1"
    # The version crosses too, and it is the one the commit published rather than a number
    # the two sides happened to agree on: this is what a copy cannot do without.
    assert remote.get(KEY_A).commit_ts == 200
    assert remote.scan_versions(b"a", b"z") == [(KEY_A, b"v1", 200)]
    assert remote.follower_read_index()[0] == local.follower_read_index()[0]


def test_a_transaction_over_a_wire_writes_and_reads_both_shards():
    """The coordinator's own path, with every command and read leaving the process.

    A put is a transaction - a prewrite and a commit - and the two keys are in different
    shards, so the commit reaches two different nodes on two different ports, chosen from
    the routing table rather than from a node object the caller is holding.
    """
    cluster = _cluster()
    factory = _factory()
    wait_for_keys_leader(cluster, [KEY_A, KEY_B])

    tso = TSOCluster(num_nodes=3)
    _BUILT_CLUSTERS.append(tso)
    tso.start(free_addresses())
    tso_client = wait_for_tso_client(tso)

    table = _table_over(cluster, lambda shard_id: _settled_leader(cluster, shard_id)[0])
    assert len(table.shard(0).addresses) == 3 and len(table.shard(1).addresses) == 3, (
        "every shard of every node listens, so the table has somewhere to point")
    assert table.shard_for(KEY_A).shard_id != table.shard_for(KEY_B).shard_id

    client = SmartClient(tso_client, cluster, router=_cache_over(cluster, table, factory))
    assert client.put(KEY_A, b"v1")
    assert client.put(KEY_B, b"v2")
    assert client.get(KEY_A) == b"v1"
    assert client.get(KEY_B) == b"v2"


# -- what the factory hands out -------------------------------------------------

def test_a_node_is_asked_for_by_address_and_kept_for_it():
    """The pair names a node, the address is how it is reached, and both are remembered."""
    cluster = _cluster()
    factory = _factory()
    wait_for_keys_leader(cluster, [KEY_A])
    leader_id, _term = _settled_leader(cluster)
    address = cluster.shard_addresses(0)[leader_id]

    assert factory.get_client(0, leader_id) is None, (
        "a pair with no address is a node this client cannot reach")

    first = factory.get_client(0, leader_id, address)
    assert first is not None
    assert factory.get_client(0, leader_id) is first, "the address was remembered"
    assert factory.get_client_at(0, address) is first, "one address, one channel"
    assert first.get(KEY_A).success

    factory.forget_client(0, leader_id)
    second = factory.get_client(0, leader_id)
    assert second is not first, "the handle was dropped"
    assert second.address == address, (
        "the placement did not change, only the way this client got there")


def test_a_node_that_does_not_answer_is_a_named_failure():
    """Nothing listening: not a refusal, and not the shard's code for one.

    A caller has to be able to tell this from an answer, because the recovery is different:
    a refusal names a leader to try, and silence leaves the table - where the address came
    from - as the only thing left to question.
    """
    client = RemoteNodeClient(f"127.0.0.1:{allocate_port(span=1)}", timeout=1.0)
    try:
        for call in (lambda: client.get(b"k"),
                     lambda: client.propose(b"command"),
                     lambda: client.scan(b"a", b"z"),
                     lambda: client.get_lock(b"k"),
                     lambda: client.get_write_record(b"k"),
                     lambda: client.follower_read_index()):
            with pytest.raises(NodeUnreachable):
                call()
    finally:
        client.close()


def test_a_refusal_over_a_wire_keeps_its_code_and_names_the_leader():
    """A follower refuses a write, and says where to go instead.

    A write is the shape a follower still refuses in, and it is the one a hint is for: the
    caller's next move is to ask whoever leads, and the address is a fact this node has and
    the caller does not.  A read is no longer among the shapes - a node that does not lead
    gets the index the read has to be answered at and answers the read here (pinned in
    ``test_client_servicer.py``), so there is no read refusal left to carry a hint on.
    """
    cluster = _cluster()
    factory = _factory()
    wait_for_keys_leader(cluster, [KEY_A])
    leader_id, _term = _settled_leader(cluster)
    addresses = cluster.shard_addresses(0)
    follower_id = _a_follower(cluster, leader_id)
    follower = factory.get_client(0, follower_id, addresses[follower_id])

    def the_refusal_is_ready():
        written = follower.propose(serialize_command(
            CommandType.SET, key=KEY_A, value=b"v1"))
        if (written.error_code == ErrorCode.ERR_NOT_LEADER
                and written.leader_address == addresses[leader_id]):
            return written
        return None

    refusal = wait_until(the_refusal_is_ready, timeout=20,
                         message="the follower never named the node that leads")
    assert refusal.leader_address == addresses[leader_id]


def test_a_refusal_the_shard_made_for_its_own_reason_arrives_as_refused():
    """The price of a five-value classification, pinned so that it cannot drift.

    A write conflict is the shard's own code - 102 - and it does not cross the wire: a client
    is told REFUSED and rebuilds it as the code for a command the machine would not apply,
    with the shard's prose still in the message.  Nothing above the seam branches on the
    difference today; the day something has to, this is the test that says the wire has to
    change with it.
    """
    cluster = _cluster()
    factory = _factory()
    wait_for_keys_leader(cluster, [KEY_A])
    remote, leader_id = _leader_client(cluster, factory)
    local = LocalNodeClient(cluster.get_shard_server(leader_id).get_shard_node(0))

    assert remote.propose(serialize_command(
        CommandType.SET, key=KEY_A, value=b"v1", timestamp=200)).success

    conflicting = serialize_command(CommandType.PREWRITE, key=KEY_A, value=b"v2",
                                    start_ts=150, primary_key=KEY_A)
    here = local.propose(conflicting)
    there = remote.propose(conflicting)
    assert here.error_code == ErrorCode.ERR_WRITE_CONFLICT, "one shard, one code"
    assert there.error_code == ErrorCode.ERR_APPLY_ERROR
    assert there.error_msg == here.error_msg, "the shard's own words still cross"


# -- the retry a hint buys -----------------------------------------------------

def test_a_retry_follows_the_hint_and_never_reads_the_table():
    """The table names a node that has stopped leading, and the write still lands.

    This is the case a hint exists for.  The publisher has not caught up with a leader
    change, so the table is wrong, and the node it names is alive and says so - with the
    address of the node that leads now.  Without hints the retry went back to the same wrong
    node and the write was refused; with them it goes straight to the leader, and the cache
    counter is the evidence that no metadata read happened on the way.

    A write and not a read, because that is where the hint is still handed out: a read sent
    to a node that does not lead is answered by that node now, which is a shorter path than
    refusing and being sent elsewhere.
    """
    cluster = _cluster()
    factory = _factory()
    wait_for_keys_leader(cluster, [KEY_A])
    _leader_id, _term = _settled_leader(cluster)
    follower_id = _a_follower(cluster, _leader_id)

    # A table that names the follower as shard 0's leader and never changes: the staleness a
    # real table has for one publisher interval, held still for the length of the test.
    table = _table_over(cluster, lambda shard_id: (
        follower_id if shard_id == 0 else _settled_leader(cluster, shard_id)[0]))
    cache = _cache_over(cluster, table, factory)
    leaders = ShardLeaders(cluster, factory=factory, router=cache)

    command = serialize_command(CommandType.SET, key=KEY_A, value=b"v1", timestamp=5)
    written = ask_shard(leaders, 0, lambda client: client.propose(command))

    assert written.success, written.error_msg
    assert cache.refreshes == 0, (
        "the client read the table again instead of following the address it was given")
