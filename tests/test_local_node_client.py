"""The primitives, and the claim that wrapping a node does not change it.

`NodeClient` is the seam every caller meets a shard through: these calls, none of which
is "be the client", so the node in this process and the node across a process can be
the same node to a coordinator.  What is easy to get wrong is the in-process
implementation - one that quietly answers differently from the object it wraps, or one
that hands the object out and lets a caller skip the seam entirely.  This file is the
evidence against both.

The first half compares the client against the node it wraps, call by call, on a real
state machine holding a committed key, a locked key and an empty snapshot: every
answer, including the refusals and the ones that are "no value" rather than a value,
has to be the same answer, field for field.

The second half is the one that matters most.  It drives a cross-shard transaction the
way `tests/test_cross_shard_transaction.py` does, but with every command and every read
going through a `LocalNodeClient`, so a path that only works while the caller is
holding the node object cannot pass.
"""

import time

import pytest

from _ports import free_addresses
from _wait import (DEFAULT_TIMEOUT, wait_for_keys_leader, wait_for_tso_client,
                   wait_until)
from oxidedb.client import (LocalNodeClient, LocalNodeClientFactory, NodeClient,
                            NodeClientFactory)
from oxidedb.raft.node import MemoryRaftNode, NodeState
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import (CommandType, ErrorCode, MVCCStateMachine,
                                        ScanRefused, is_not_leader, serialize_command)
from oxidedb.shard.router import locate
from oxidedb.tso.tso import TSOCluster

KEY_A = b"key0"      # first byte 0x6b -> shard 0
KEY_B = b"\x80key1"  # first byte 0x80 -> shard 1

COMMITTED_KEY = b"aaa"
LOCKED_KEY = b"zzz"


#: Every node a test in this file builds.  A node drives its elections from a thread of
#: its own (`MemoryRaftNode._tick`, started in its constructor), so a node nobody shuts
#: down leaves a thread behind for the rest of the session.
_BUILT_NODES = []


@pytest.fixture(autouse=True)
def _stop_the_nodes_the_test_built():
    yield
    while _BUILT_NODES:
        _BUILT_NODES.pop().shutdown()


def _build(node):
    """Keep a node, so that the end of the test stops its ticker thread."""
    _BUILT_NODES.append(node)
    return node


def _single_node_leader():
    """A one-node cluster that elects itself, so proposals commit.

    No peers and no storage: the shard is real, the node keeps its log in memory, and
    nothing here has to wait for a network.
    """
    node = _build(MemoryRaftNode(node_id=1, peers=[], state_machine=MVCCStateMachine(),
                                 get_peer_node=None,
                                 election_timeout_min=20, election_timeout_max=40))
    wait_until(lambda: node.state == NodeState.LEADER, timeout=10,
               message="the single node never became the leader")
    return node


def _quiet_follower(node_id=2):
    """A node whose election timer will not fire during the test."""
    node = _build(MemoryRaftNode(node_id=node_id, peers=[],
                                 state_machine=MVCCStateMachine(),
                                 get_peer_node=None,
                                 election_timeout_min=60000,
                                 election_timeout_max=60000))
    assert node.state == NodeState.FOLLOWER
    return node


def _pair():
    """A leader and the client that wraps it."""
    node = _single_node_leader()
    return node, LocalNodeClient(node)


def _with_a_committed_and_a_locked_key(node):
    """One key written and left alone, one key prewritten and left locked."""
    assert node.propose(serialize_command(
        CommandType.SET, key=COMMITTED_KEY, value=b"v1", timestamp=5)).success
    assert node.propose(serialize_command(
        CommandType.PREWRITE, key=LOCKED_KEY, value=b"v2", start_ts=10,
        primary_key=LOCKED_KEY)).success


def _read_shape(result):
    """Every field a read answers with, so equality is field by field."""
    return (result.success, result.value, result.commit_ts, result.error_code,
            result.error_msg)


def _apply_shape(result):
    """Every field a proposal answers with."""
    return (result.success, result.error_code, result.error_msg, result.data)


def test_the_client_is_the_protocol_and_nothing_else():
    """The calls, and no way through to the node behind them.

    A passthrough attribute would be the end of the seam: code that reaches the state
    machine works in this process and stops working silently the moment the node is a
    channel away.  `dir` catches a passthrough as well as a method.
    """
    client = LocalNodeClient(_single_node_leader())

    assert isinstance(client, NodeClient)
    assert {name for name in dir(client) if not name.startswith("_")} == {
        "get", "scan", "scan_versions", "propose", "get_lock", "get_write_record",
        "follower_read_index"}


def test_a_read_answers_the_same_through_the_client_as_it_does_directly():
    """Including the two cases that are not a value: nothing there, and a lock.

    A snapshot older than the version, a snapshot the lock is newer than, and the
    reader's own write intent all have to come back the same way - those are the
    distinctions the callers above this layer make decisions on.
    """
    node, client = _pair()
    _with_a_committed_and_a_locked_key(node)

    reads = [
        (COMMITTED_KEY, None),   # the newest version
        (COMMITTED_KEY, 5),      # the version it was written at
        (COMMITTED_KEY, 4),      # before it existed: no value, and not an error
        (b"missing", None),      # never written at all
        (LOCKED_KEY, None),      # behind a lock, and the lock is not the answer
        (LOCKED_KEY, 10),        # this reader's own write intent
        (LOCKED_KEY, 11),        # a snapshot older than the lock
        (LOCKED_KEY, 9),         # a snapshot the lock is newer than: ignored
    ]
    for key, timestamp in reads:
        assert _read_shape(client.get(key, timestamp)) == _read_shape(
            node.get(key, timestamp)), (key, timestamp)

    # And equality is not enough for the version, or two implementations that both dropped
    # it would pass: a committed row says which version it is, a key that is not there says
    # nothing, and a reader's own write intent is a value with no version at all.
    assert client.get(COMMITTED_KEY).commit_ts == 5
    assert client.get(b"missing").commit_ts == 0
    assert client.get(LOCKED_KEY, 10).commit_ts == 0


def test_a_range_read_answers_the_same_rows_and_refuses_the_same_way():
    """Rows where it can answer, `ScanRefused` where it cannot - on both sides.

    A range that holds the locked key may not come back short: a key left out and a
    key that is not there are the same thing to a caller, so the lock has to stop the
    read.  The refusal carries the key it stopped on, and that has to survive the
    client too.
    """
    node, client = _pair()
    _with_a_committed_and_a_locked_key(node)

    ranges = [(b"", b"zzz"), (COMMITTED_KEY, b"b"), (b"b", b"zzz"), (b"b", b"b")]
    for start_key, end_key in ranges:
        assert client.scan(start_key, end_key) == node.scan(start_key, end_key), (
            start_key, end_key)
        assert client.scan_versions(start_key, end_key) == \
            node.scan_versions(start_key, end_key), (start_key, end_key)

    # The same rows, with the version each value was written at - which is what a copy
    # into another group has to carry with it, or the row arrives as the newest thing
    # that has ever happened to the key.
    assert client.scan_versions(b"", b"zzz") == [(COMMITTED_KEY, b"v1", 5)]

    for scan in (node.scan, client.scan):
        with pytest.raises(ScanRefused) as caught:
            scan(b"", b"\xff")
        assert caught.value.error_code == ErrorCode.ERR_LOCKED
        assert caught.value.key == LOCKED_KEY


def test_a_refused_proposal_comes_back_the_same_way():
    """A command the machine cannot read is a result, not an exception.

    The two messages are compared to each other rather than to a literal: what has to
    hold is that both sides say the same thing, not that the wording is any
    particular string.
    """
    node, client = _pair()

    direct = node.propose(b"not a command")
    through = client.propose(b"not a command")

    assert not direct.success and not through.success
    assert _apply_shape(direct) == _apply_shape(through)
    assert direct.error_msg, "a refusal has to say something"


def test_a_write_is_the_same_write_whichever_side_makes_it():
    """The claim in both directions: written through one, read through the other."""
    node, client = _pair()

    assert client.propose(serialize_command(
        CommandType.SET, key=b"via_client", value=b"1", timestamp=7)).success
    assert _read_shape(node.get(b"via_client")) == (True, b"1", 7, None, None)

    assert node.propose(serialize_command(
        CommandType.SET, key=b"via_node", value=b"2", timestamp=8)).success
    assert _read_shape(client.get(b"via_node")) == (True, b"2", 8, None, None)


def test_the_two_reads_a_lock_resolver_needs_answer_the_same_way():
    """The lock, and the primary key's write record, before and after the commit.

    These are the two questions `LockResolver` asks of a shard, and the difference
    between the answers is how a lock whose transaction never committed is told from
    one whose transaction did: with no write record, there is nothing to roll
    forward.
    """
    node, client = _pair()
    _with_a_committed_and_a_locked_key(node)

    assert client.get_lock(LOCKED_KEY) == node._state_machine.get_lock_status(LOCKED_KEY)
    assert client.get_lock(LOCKED_KEY)["start_ts"] == 10
    assert client.get_lock(COMMITTED_KEY) is None

    # Nobody committed this one, so there is no write record to find.
    assert client.get_write_record(LOCKED_KEY) is None
    assert client.get_write_record(LOCKED_KEY) == node._state_machine.get_write_record(
        LOCKED_KEY)

    assert node.propose(serialize_command(
        CommandType.COMMIT, key=LOCKED_KEY, start_ts=10, commit_ts=20)).success

    committed = client.get_write_record(LOCKED_KEY)
    assert committed == node._state_machine.get_write_record(LOCKED_KEY)
    assert (committed["start_ts"], committed["commit_ts"]) == (10, 20)
    assert client.get_lock(LOCKED_KEY) is None


def test_the_read_index_is_the_same_answer_through_the_client():
    """How far this node has committed, which is what a follower read waits on."""
    node, client = _pair()
    _with_a_committed_and_a_locked_key(node)

    through = client.follower_read_index()
    assert through == node._read_index()
    assert through[1] is None and through[0] is not None


def test_a_node_that_does_not_lead_says_so_through_the_client_too():
    """A node that is not the leader reports that; it does not read and hope."""
    node = _quiet_follower()
    client = LocalNodeClient(node)

    assert client.follower_read_index() == node._read_index() == (None, "Not leader")
    assert _read_shape(client.get(b"any")) == _read_shape(node.get(b"any"))
    assert client.get(b"any").error_code == ErrorCode.ERR_NOT_LEADER


def test_propose_on_non_leader_returns_not_leader_code():
    """A proposal a node cannot take because it is not leading says so in the code.

    The message - "Not leader" - is prose, and a client that has to match on prose to
    decide whether to retry somewhere else is a client with a string comparison in its
    retry path.  Reads already answered this way; this is the proposal path agreeing
    with them, so a caller can dispatch on the code alone.  It used to be a bare 1,
    which is `ERR_UNKNOWN`: the same code a command the machine cannot read comes back
    with, and no way to tell "ask the leader" from "this command is broken".

    Asserted through the client as well, because the thing that will be doing the
    dispatching holds a client and not the node.
    """
    node = _quiet_follower()
    client = LocalNodeClient(node)
    command = serialize_command(CommandType.SET, key=b"k", value=b"v", timestamp=1)

    for result in (node.propose(command), client.propose(command)):
        assert not result.success
        assert result.error_code == ErrorCode.ERR_NOT_LEADER
        assert result.error_code != ErrorCode.ERR_UNKNOWN


class _ServerWithEveryShard:
    """One server: a node in *every* shard's Raft group, which is what a server is."""

    def __init__(self, node_id, num_shards):
        self.node_id = node_id
        self.shards = {shard_id: _quiet_follower(node_id)
                       for shard_id in range(num_shards)}

    def get_shard_node(self, shard_id):
        return self.shards.get(shard_id)


class _ClusterOfServers:
    """The part of a sharded cluster a factory uses, and nothing else."""

    def __init__(self, node_ids=(1, 2), num_shards=2):
        self.servers = {node_id: _ServerWithEveryShard(node_id, num_shards)
                        for node_id in node_ids}

    def get_shard_server(self, node_id):
        return self.servers.get(node_id)


def test_the_factory_is_the_protocol_and_answers_with_clients():
    factory = LocalNodeClientFactory(_ClusterOfServers())

    assert isinstance(factory, NodeClientFactory)
    for shard_id in (0, 1):
        for node_id in (1, 2):
            assert isinstance(factory.get_client(shard_id, node_id), LocalNodeClient)


def test_a_node_id_on_its_own_would_name_the_wrong_group():
    """The same id in two shards is two groups, and has to be two clients.

    A server holds one node per shard, so "node 1" is one Raft group in shard 0 and
    another in shard 1 - which is the whole reason the factory is keyed by a pair and
    not by the id a caller thinks it is asking about.
    """
    factory = LocalNodeClientFactory(_ClusterOfServers())

    first = factory.get_client(0, 1)
    second = factory.get_client(1, 1)

    assert first is not second
    assert first._node is not second._node


def test_the_factory_hands_out_one_client_per_node():
    """Asked twice, it answers with the client it built the first time."""
    factory = LocalNodeClientFactory(_ClusterOfServers())

    assert factory.get_client(0, 1) is factory.get_client(0, 1)


def test_the_factory_answers_none_for_a_node_it_has_no_handle_on():
    """None and not an exception: the table names nodes a client cannot reach."""
    factory = LocalNodeClientFactory(_ClusterOfServers(node_ids=(1,)))

    assert factory.get_client(0, 1) is not None
    assert factory.get_client(0, 7) is None, "no such server"
    assert factory.get_client(9, 1) is None, "no such shard on that server"


def test_the_factory_drops_a_client_it_is_told_to_forget():
    """A handle that has stopped working is dropped, and the next ask builds a new one.

    The node behind it is not dropped: nothing about where the shard is has changed,
    only the way this client reaches it.
    """
    factory = LocalNodeClientFactory(_ClusterOfServers())

    first = factory.get_client(0, 1)
    factory.forget_client(0, 1)
    second = factory.get_client(0, 1)

    assert first is not second, "the handle was dropped"
    assert second._node is first._node, "and the node behind it was not"


def test_forgetting_a_client_nobody_asked_for_is_not_an_error():
    """A caller that found one broken and one that never had one want the same thing."""
    factory = LocalNodeClientFactory(_ClusterOfServers())

    factory.forget_client(0, 1)
    factory.forget_client(9, 7)

    assert factory.get_client(0, 1) is not None


def _two_shard_cluster():
    """A TSO group and a two-shard cluster, both started but not yet settled.

    The lock cleaner is off: the refusal test holds a lock that has to stay put.
    """
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())

    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(),
        lock_cleaner_interval=None,
    )
    return tso_cluster, shard_cluster


def _assert_two_shards(cluster, *keys):
    """Fail loudly unless the keys really are in different shards and groups.

    Asserted before anything else, because a routing change that put these keys
    together would otherwise turn this into a single-shard test that still passes.
    """
    shards = [locate(cluster._range_map, key) for key in keys]
    assert len(set(shards)) == len(shards), (
        "the keys share a shard, so nothing here crosses one")

    first = cluster.get_leader_for_key(keys[0])[1]
    for key in keys[1:]:
        assert cluster.get_leader_for_key(key)[1] is not first, (
            "the keys are served by one Raft group")
    return shards


def _client_for(factory, cluster, key):
    """The client for whichever node leads the shard that owns ``key``.

    Through the factory, which is how a caller gets one, and for the pair the routing
    table names: a shard, and the id of the server serving it.
    """
    leader = cluster.get_leader_for_key(key)
    assert leader is not None, f"no shard leader for {key!r}"
    node_id, _ = leader

    client = factory.get_client(locate(cluster._range_map, key), node_id)
    assert client is not None, f"no client for {key!r}"
    return client


def _prewrite(client, key, value, start_ts, primary_key):
    """One key's prewrite, built the way the coordinator builds it."""
    return client.propose(serialize_command(
        CommandType.PREWRITE, key=key, value=value, start_ts=start_ts,
        primary_key=primary_key))


def _commit(client, key, start_ts, commit_ts):
    """One key's commit, built the way the coordinator builds it."""
    return client.propose(serialize_command(
        CommandType.COMMIT, key=key, start_ts=start_ts, commit_ts=commit_ts))


#: How long between two asks of a shard whose leader changed under the caller.
_BETWEEN_ASKS = 0.05


def _through_a_client_for(client, factory, cluster, key, call, timeout=DEFAULT_TIMEOUT):
    """Do ``call`` through a client for ``key``'s shard, following a changed leader.

    A caller holds a client pinned to whoever led when it asked for one, and a group
    elects on its own schedule: on a loaded machine the node behind a client can stop
    leading between two calls of one test.  The answer then is ``ERR_NOT_LEADER`` - a
    fact about the moment and not about the command - and the caller's next move is to
    ask whoever leads now, which is what ``write_when_ready`` in ``tests/_cluster.py``
    lets a test over the wire do.  Only that refusal is followed: a shard's own
    refusal, a lock that is not there and a version that does not match all come back
    untouched, and still fail the test that has a reason to.
    """
    deadline = time.monotonic() + timeout
    while True:
        answer = call(client)
        if answer.success or not is_not_leader(answer.error_code):
            return answer
        if time.monotonic() >= deadline:
            return answer
        wait_for_keys_leader(cluster, [key])
        client = _client_for(factory, cluster, key)
        time.sleep(_BETWEEN_ASKS)


def test_a_cross_shard_transaction_runs_through_the_clients():
    """Two shards, one transaction, and no node object touched by the caller.

    Not a second test of the coordinator - that is `test_cross_shard_transaction` -
    but of the seam: the same prewrite, commit and read, issued to a `NodeClient`
    for each shard, with the node behind each client asked afterwards to confirm it
    is the node's own state that moved.

    The proposals and the reads go through `_through_a_client_for`, because a client is
    pinned to whoever led when it was asked for and a group elects on its own schedule:
    the one refusal that means the pin has gone stale is followed, and nothing else is.
    """
    tso_cluster, shard_cluster = _two_shard_cluster()
    try:
        wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
        tso_client = wait_for_tso_client(tso_cluster)
        _assert_two_shards(shard_cluster, KEY_A, KEY_B)

        factory = LocalNodeClientFactory(shard_cluster)
        client_a = _client_for(factory, shard_cluster, KEY_A)
        client_b = _client_for(factory, shard_cluster, KEY_B)

        start_ts = tso_client.get_timestamp()
        commit_ts = tso_client.get_timestamp()
        assert commit_ts > start_ts

        assert _through_a_client_for(
            client_a, factory, shard_cluster, KEY_A,
            lambda client: _prewrite(client, KEY_A, b"value_a", start_ts,
                                     KEY_A)).success
        assert _through_a_client_for(
            client_b, factory, shard_cluster, KEY_B,
            lambda client: _prewrite(client, KEY_B, b"value_b", start_ts,
                                     KEY_B)).success

        # A prewrite is a lock, and the lock has to be readable through a client -
        # that is what a resolver on the other side of the seam would be handed.  The
        # factory hands back the same handle while the same node still leads.
        for key in (KEY_A, KEY_B):
            client = _client_for(factory, shard_cluster, key)
            lock = client.get_lock(key)
            assert lock is not None and lock["start_ts"] == start_ts

        committed = _through_a_client_for(
            client_a, factory, shard_cluster, KEY_A,
            lambda client: _commit(client, KEY_A, start_ts, commit_ts))
        assert committed.success, (committed.error_code, committed.error_msg)
        assert _through_a_client_for(
            client_b, factory, shard_cluster, KEY_B,
            lambda client: _commit(client, KEY_B, start_ts, commit_ts)).success

        for key, value in ((KEY_A, b"value_a"), (KEY_B, b"value_b")):
            # Whoever leads now and not the handle the transaction started with: the
            # reads below are about the state machine that has the committed row in it.
            client = _client_for(factory, shard_cluster, key)
            read = _through_a_client_for(
                client, factory, shard_cluster, key,
                lambda client: client.get(key, commit_ts))
            assert (read.success, read.value) == (True, value)
            assert client.get_lock(key) is None

            record = client.get_write_record(key)
            assert (record["start_ts"], record["commit_ts"]) == (start_ts, commit_ts)

            # The node behind the client answers the read the same way, so what moved
            # is the node's state and not a copy the wrapper kept.
            node = shard_cluster.get_leader_for_key(key)[1]
            assert _read_shape(node.get(key, commit_ts)) == _read_shape(read)
    finally:
        shard_cluster.shutdown()
        tso_cluster.shutdown()


def test_a_prewrite_one_shard_refuses_is_refused_through_the_client_too():
    """The refusal path, on both sides of the seam, and the rollback that follows.

    A foreign lock on the second shard makes its prewrite fail; the same command
    proposed to the node directly and through the client has to come back the same
    way, because a caller that had to catch an exception on one side and inspect a
    result on the other would have to know which side it was on.  The first shard's
    lock, taken through its own client, is then rolled back the same way - and the
    other transaction's lock has to survive it.
    """
    tso_cluster, shard_cluster = _two_shard_cluster()
    try:
        wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
        tso_client = wait_for_tso_client(tso_cluster)
        _assert_two_shards(shard_cluster, KEY_A, KEY_B)

        factory = LocalNodeClientFactory(shard_cluster)
        node_b = shard_cluster.get_leader_for_key(KEY_B)[1]
        client_b = LocalNodeClient(node_b)

        # Someone else holds the lock on KEY_B, so a prewrite for it will be refused.
        # Its timestamp comes from the TSO too, so it cannot collide with ours.
        other_ts = tso_client.get_timestamp()
        assert node_b.propose(serialize_command(
            CommandType.PREWRITE, key=KEY_B, value=b"someone else",
            start_ts=other_ts, primary_key=KEY_B)).success

        start_ts = tso_client.get_timestamp()
        blocked = serialize_command(CommandType.PREWRITE, key=KEY_B, value=b"ours",
                                    start_ts=start_ts, primary_key=KEY_A)

        refused = client_b.propose(blocked)
        assert _apply_shape(refused) == _apply_shape(node_b.propose(blocked))
        assert not refused.success and refused.error_code == ErrorCode.ERR_LOCKED

        # The other shard did take the lock, so the transaction is undone through the
        # client that took it.
        client_a = _client_for(factory, shard_cluster, KEY_A)
        assert _prewrite(client_a, KEY_A, b"value_a", start_ts, KEY_A).success
        assert client_a.get_lock(KEY_A) is not None

        assert client_a.propose(serialize_command(
            CommandType.ROLLBACK, key=KEY_A, start_ts=start_ts)).success
        assert client_a.get_lock(KEY_A) is None

        survivor = client_b.get_lock(KEY_B)
        assert survivor is not None and survivor["start_ts"] == other_ts, (
            "the rollback touched the other transaction's lock")
    finally:
        shard_cluster.shutdown()
        tso_cluster.shutdown()