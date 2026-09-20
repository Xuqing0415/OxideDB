"""A client routes by the table, and goes back to it when a shard says it is wrong.

The metadata group was a service nobody wrote to, and the table it holds was read by
nobody until a client routed by it.  This is that last join: a client reads the table
once - one linearizable read instead of a lookup per key - and treats what it read as a
decision it can be sent back to revisit, which is what a cached table has to be for the
cache to be safe rather than merely fast.

One kind of staleness, and two ways of answering it.  A cached table can name a node that
has stopped leading, and the only evidence of that a client can get is the shard refusing
to answer: the node it reached is the one whose own belief is in question, since a leader
cut off from its peers goes on answering reads as if nothing had happened.  So the lookup
hands out the client for the placement the table published and inspects nothing behind it,
and the refusal is what sends the client on - to the address the shard named, to the other
replicas of its set, and only then back to the table it read.  The tests at the end are the
same leader change with a real cluster, a real metadata group and a real node that dies,
one of them with a table frozen so that only a shard's own replicas can answer.

The middle of the file is the mechanism that does the re-reading: ``ShardLeaders``, which
answers with a client from the table when there is one and from the cluster when there is
not, and ``ask_shard``, which is the one place that turns a refusal into the next question:
to the address the shard named, then to the shard's other replicas, and only then back to
the table - which is why most of the file is about what a client may do before it is worth
asking the metadata group anything again.

The rest of the file pins what "routes by the table" has to mean: a client with a table
never looks at the cluster's own nodes, and a transaction reads, writes and looks for
leaders by that one table rather than by a scan of its own - which is what lets the
coordinator be handed a table and no cluster at all.
"""

import time
from typing import Dict, Tuple

import pytest

from _ports import free_addresses
from _wait import (wait_for_keys_leader, wait_for_metadata_client,
                   wait_for_tso_client, wait_until)
from oxidedb.client import (LocalNodeClient, LocalNodeClientFactory, NodeUnreachable,
                            RemoteNodeClientFactory, ShardLeaders, ask_shard)
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import MetadataCluster, RoutingTable, ShardPlacement
from oxidedb.raft.node import NodeState
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import (ErrorCode, MVCCStateMachine, ReadResult,
                                        ScanRefused)
from oxidedb.transaction.coordinator import TransactionCoordinator
from oxidedb.transaction.smart_client import Consistency, SmartClient
from oxidedb.tso.tso import TSOCluster

KEY_A = b"key0"      # first byte 0x6b -> shard 0
KEY_B = b"\x80key1"  # first byte 0x80 -> shard 1


# -- a cluster, a table and a node, small enough to be unit tests ---------------

class _FakeNode:
    """A shard node that answers with a value or refuses the way a deposed leader does:
    still claiming to lead, which is exactly what a client cannot detect."""

    def __init__(self, value=None, error=None, state=NodeState.LEADER, leader_address=None):
        self._value = value
        self._error = error
        self.state = state
        #: Where this node says the leader is, for the refusals it gives: a follower that
        #: has heard from one names it, and a node that only knows it is not the leader
        #: names nowhere.
        self._leader_address = leader_address

    def get(self, key, timestamp=None, read_index=None):
        if self._error is not None:
            return ReadResult.failure(self._error, "Not leader", self._leader_address)
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


def _table_with_addresses(leader_id, addresses, version: int = 1) -> RoutingTable:
    """A one-shard table that names its replica set and where each member serves it.

    ``addresses`` is node id to address, and the nodes of the placement are its keys, so
    the set the table names and the set a client can reach cannot drift apart in a test
    that meant them to be the same.
    """
    placement = ShardPlacement(0, b"", b"\xff", nodes=sorted(addresses),
                               addresses=dict(addresses), leader_id=leader_id,
                               leader_term=1)
    return RoutingTable(version, {0: placement})


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
    """A transaction reads, writes and settles locks by the placement it was handed.

    Which shard owns a key is the same lookup that finds that shard's leader, so a
    coordinator with a table needs no cluster object to work shards out of, and none is
    given one here.  It used to need one: the shard a key belonged to came from the
    cluster's own range map while the leader came from the table, which is two
    placements in one transaction - and the one a client outside the cluster cannot
    have, since what it holds is a table and no cluster at all.  The resolver, which
    settles the locks a transaction meets, asks the same leaders.
    """
    node = _FakeNode(value=b"v1")
    cluster = _single_shard_cluster({1: node})
    router = RoutingCache(cluster, _FakeTableSource(_table(1)))
    coordinator = TransactionCoordinator(_FakeTso(), None, router=router)

    assert coordinator._get_shard_id(KEY_A) == 0
    assert coordinator._resolver._get_shard_id(KEY_A) == 0, (
        "the lock resolver routes by the same placement")

    txn_id, _ = coordinator.begin()
    assert coordinator.read(txn_id, KEY_A) == b"v1"


# -- a refusal that leaves somewhere else to ask -------------------------------

class _AddressableClient(LocalNodeClient):
    """A handle that knows the address it was reached at.

    A handle over a wire knows one, and that is what keeps the node the table named out of
    the replicas a refusal queues: a round of addresses is keyed by address, and a handle
    that cannot say which one it is was never going to be in one.
    """

    def __init__(self, node, address):
        super().__init__(node)
        self.address = address


class _AddressBookFactory:
    """A factory over addresses, with a small cluster's nodes behind them.

    ``LocalNodeClientFactory`` answers None to every address, and that is honest for nodes
    in this process - nothing here answers at one - which is also why the walk over a
    shard's replicas cannot be tested with it.  This stands in for the factory a client
    over a wire holds: the same four calls, with ``get_client_at`` finding something.
    """

    def __init__(self, nodes_by_address):
        self._nodes = dict(nodes_by_address)
        self._addresses: Dict[Tuple[int, int], str] = {}
        self._clients: Dict[str, _AddressableClient] = {}

    def get_client(self, shard_id: int, node_id: int, address=None):
        if address is not None:
            self._addresses[(shard_id, node_id)] = address
        address = self._addresses.get((shard_id, node_id))
        return None if address is None else self.get_client_at(shard_id, address)

    def get_client_at(self, shard_id: int, address: str):
        node = self._nodes.get(address)
        if node is None:
            return None
        # One handle per address, the way a factory over a wire keeps one channel per
        # address: two callers asking about one node are asking about one node.
        return self._clients.setdefault(address, _AddressableClient(node, address))

    def forget_client(self, shard_id: int, node_id: int) -> None:
        self._addresses.pop((shard_id, node_id), None)


def test_a_refusal_walks_the_shard_s_other_replicas_before_the_table():
    """The table names one leader and a replica set, and only one member leads now.

    Node 1 is what the table says, node 1 refuses, and the answer is on node 3: so the
    client asks the two nodes it has not asked, one at a time, and the table is never read.
    That is the difference between a client that survives an election the publisher has not
    written down yet and one that sits out every election until it has.
    """
    addresses = {1: "node1:7001", 2: "node2:7001", 3: "node3:7001"}
    factory = _AddressBookFactory({addresses[1]: _FakeNode(error=ErrorCode.ERR_NOT_LEADER),
                                   addresses[2]: _FakeNode(error=ErrorCode.ERR_NOT_LEADER),
                                   addresses[3]: _FakeNode(value=b"v2")})
    source = _FakeTableSource(_table_with_addresses(1, addresses))
    router = RoutingCache(_FakeCluster({}), source, factory=factory)
    leaders = ShardLeaders(_FakeCluster({}), router=router)
    asked = []

    answer = ask_shard(leaders, 0, lambda client: asked.append(client) or client.get(KEY_A))

    assert answer.value == b"v2"
    assert [client.address for client in asked] == [addresses[1], addresses[2], addresses[3]]
    assert router.refreshes == 0, "the replicas answered it, so the table had nothing to add"


def test_a_refusal_that_names_the_leader_sends_the_client_there_before_any_replica():
    """The shard's own answer is the newest placement there is, so it goes first.

    Node 1 refuses and names node 3: following that costs one call and needs no read of the
    table, and it skips node 2 - a node this client can reach and has no reason to ask,
    because the node that refused it has just said who leads.
    """
    addresses = {1: "node1:7001", 2: "node2:7001", 3: "node3:7001"}
    factory = _AddressBookFactory({addresses[1]: _FakeNode(error=ErrorCode.ERR_NOT_LEADER,
                                                           leader_address=addresses[3]),
                                   addresses[2]: _FakeNode(value=b"the skipped replica"),
                                   addresses[3]: _FakeNode(value=b"v2")})
    source = _FakeTableSource(_table_with_addresses(1, addresses))
    router = RoutingCache(_FakeCluster({}), source, factory=factory)
    leaders = ShardLeaders(_FakeCluster({}), router=router)
    asked = []

    answer = ask_shard(leaders, 0, lambda client: asked.append(client) or client.get(KEY_A))

    assert answer.value == b"v2"
    assert [client.address for client in asked] == [addresses[1], addresses[3]]
    assert router.refreshes == 0


def test_a_walk_that_finds_nobody_sends_the_client_back_to_the_table():
    """Replicas are a second answer and not the last one: the table is still the record.

    Every node the old table names refuses, so the walk spends the set and the client is
    back where it started - and the table, which the publisher moved on before any of this,
    is the one thing that can name a node the round was never going to reach.  One read of
    it, and the write lands on the fourth node.
    """
    addresses = {1: "node1:7001", 2: "node2:7001", 3: "node3:7001"}
    nodes = {address: _FakeNode(error=ErrorCode.ERR_NOT_LEADER)
             for address in addresses.values()}
    nodes["node4:7001"] = _FakeNode(value=b"v3")
    factory = _AddressBookFactory(nodes)
    source = _FakeTableSource(_table_with_addresses(1, addresses))
    router = RoutingCache(_FakeCluster({}), source, factory=factory)
    leaders = ShardLeaders(_FakeCluster({}), router=router)
    asked = []

    assert leaders.leader_for_shard(0) is not None, "this client holds the old table already"
    source.move_on(_table_with_addresses(4, {4: "node4:7001"}))

    answer = ask_shard(leaders, 0, lambda client: asked.append(client) or client.get(KEY_A))

    assert answer.value == b"v3"
    assert [client.address for client in asked] == [addresses[1], addresses[2], addresses[3],
                                                    "node4:7001"]
    assert router.refreshes == 1, "the set was spent, so the table was read"


# -- a member of a set, for a read that need not lead --------------------------

class _RecordingFactory:
    """A factory that remembers what it was asked for, with a say in what it has.

    ``reachable`` is the set of node ids it holds a handle for, or None for all of them -
    which is the table's own answer when every member of a set is listening.
    """

    def __init__(self, reachable=None):
        self.asked = []
        self._reachable = reachable

    def get_client(self, shard_id, node_id, address=None):
        self.asked.append((shard_id, node_id, address))
        if self._reachable is not None and node_id not in self._reachable:
            return None
        return _AddressableClient(_FakeNode(value=b"v1"), address)

    def get_client_at(self, shard_id, address):
        return None

    def forget_client(self, shard_id, node_id):
        pass


def test_a_read_that_need_not_lead_is_given_a_member_of_the_shard_s_set():
    """The set the table publishes, one member at a time, instead of the leader.

    A read that carries its own index can be answered by any member, so the lookup that used
    to answer "the leader" answers "a member" - and which one is the point rather than an
    implementation detail: a client that read from the node the table names would be reading
    from the leader again, and many clients spreading over the set is the whole reason to
    read from a replica.  The address is the placement's, because a factory that opens a
    channel has nowhere else to learn one.

    The draw is random, so the spread is pinned over many of them rather than on one: what
    is asserted is that every member was drawn and nothing else ever was.
    """
    addresses = {1: "node1:5001", 2: "node2:5002", 3: "node3:5003"}
    factory = _RecordingFactory()
    cache = RoutingCache(None, _FakeTableSource(_table_with_addresses(1, addresses)),
                         factory=factory)

    handles = [cache.any_replica_for(0) for _ in range(60)]

    assert all(handle is not None for handle in handles)
    assert {(node_id, address) for _, node_id, address in factory.asked} == set(
        addresses.items()), "members of the set, at their own addresses, and all of them"


def test_a_member_this_client_has_no_handle_for_is_passed_over():
    """What is handed out is a member a client can reach, and None when that is nobody.

    The table names a replica set, which is not the same thing as a set of nodes this client
    can talk to: a member it has no address for yet, or one whose channel has gone, is a
    member to try and not a member to stop at.  A set with none of them reachable is None
    rather than a raise, the same answer the leader lookup gives a shard with no leader - and
    so is a shard the table does not name at all.
    """
    factory = _RecordingFactory(reachable={2})
    cache = RoutingCache(
        None, _FakeTableSource(_table_with_addresses(1, {1: "a:1", 2: "a:2"})),
        factory=factory)

    assert cache.any_replica_for(0).address == "a:2", (
        "the member with a handle, and not whichever was tried first")
    assert all(node_id in (1, 2) for _, node_id, _ in factory.asked)

    silent = RoutingCache(None, _FakeTableSource(_table_with_addresses(1, {1: "a:1"})),
                          factory=_RecordingFactory(reachable=set()))
    assert silent.any_replica_for(0) is None

    unnamed = RoutingCache(None, _FakeTableSource(_table(1, nodes=())))
    assert unnamed.any_replica_for(0) is None, "a shard the table names nobody for"


def test_a_read_is_served_by_this_machine_when_it_is_one_of_the_members():
    """The member a cache was told it is on comes first, and the others are not asked.

    A read answered here costs no hop, and only the cache knows which member this process
    is: the table names a set and nothing in it says which of them is asking, so the id is
    given rather than worked out.  What comes back is a handle on that member at that
    member's own address, and no other member is asked about - a lookup that asked them all
    and then picked one would be the random draw with a step in front of it.
    """
    addresses = {1: "node1:5001", 2: "node2:5002", 3: "node3:5003"}
    factory = _RecordingFactory()
    cache = RoutingCache(None, _FakeTableSource(_table_with_addresses(1, addresses)),
                         factory=factory, local_node_id=3)

    handles = [cache.any_replica_for(0) for _ in range(20)]

    assert all(handle is not None and handle.address == "node3:5003" for handle in handles)
    assert {node_id for _, node_id, _ in factory.asked} == {3}, (
        "the machine it is on, and no question about the members it is not on")


def test_a_machine_that_is_not_one_of_the_members_is_not_preferred():
    """An id outside the set is not a member to hand back, and not a reason to answer None.

    A node the shard is not served by is not a node to read that shard from, whoever is
    asking: the set is the table's answer and the preference is over it.  So the draw is
    what it was before there was a preference - every member, and nothing else.
    """
    addresses = {1: "node1:5001", 2: "node2:5002"}
    factory = _RecordingFactory()
    cache = RoutingCache(None, _FakeTableSource(_table_with_addresses(1, addresses)),
                         factory=factory, local_node_id=9)

    handles = [cache.any_replica_for(0) for _ in range(60)]

    assert all(handle is not None for handle in handles)
    assert {node_id for _, node_id, _ in factory.asked} == {1, 2}


def test_a_machine_this_client_has_no_handle_for_is_not_where_it_reads():
    """The preference is over the members a client can reach, the way the draw is.

    A local node whose channel has gone is a member to try and not a member to stop at,
    exactly as any other member is: what the preference is worth is a hop, and there is
    nothing to answer None over while the set holds a member that does answer.
    """
    addresses = {1: "a:1", 2: "a:2"}
    factory = _RecordingFactory(reachable={2})
    cache = RoutingCache(None, _FakeTableSource(_table_with_addresses(1, addresses)),
                         factory=factory, local_node_id=1)

    assert cache.any_replica_for(0).address == "a:2"


class _SilentNode:
    """A member that does not answer at all: a channel that has gone, not a refusal.

    Nothing here refuses, because there is no answer to refuse in - the situation ``ask_shard``
    treats as a refusal's equal with nothing in it.  A list, when one is given, is where this
    node records that it was asked: a walk over a set of these is told apart from a walk that
    happened to start elsewhere by the order the list is in.
    """

    def __init__(self, reached=None):
        self.asked = 0
        self._reached = reached

    def get(self, key, timestamp=None, read_index=None):
        self.asked += 1
        if self._reached is not None:
            self._reached.append(self)
        raise NodeUnreachable("nothing answered")


def test_a_member_that_does_not_answer_sends_the_read_to_another_member():
    """The level picks a member, and a pick that has gone quiet is one to walk past.

    A node that does not answer is the same situation as a refusal with no answer in it: the
    member this read was sent to is not serving it, so the next member of the set is asked.
    The member it picked is the one this client is on, and the one the table calls the leader
    is elsewhere - so an answer that came from the leader, with the member this read was sent
    to never asked, is exactly the read this level exists not to make.

    The silent member is asked once: asking each member of the set once is what a round of
    addresses is, and the client's retry of the whole read is not a second round.
    """
    addresses = {1: "node1:8001", 2: "node2:8001"}
    silent = _SilentNode()
    factory = _AddressBookFactory({addresses[1]: _FakeNode(value=b"v1"),
                                   addresses[2]: silent})
    router = RoutingCache(_FakeCluster({}),
                          _FakeTableSource(_table_with_addresses(1, addresses)),
                          factory=factory, local_node_id=2)
    client = SmartClient(None, _FakeCluster({}), router=router, factory=factory)

    assert client.get(KEY_A, consistency=Consistency.FOLLOWER) == b"v1"
    assert silent.asked == 1, "the member this read was sent to, once, before the leader"


def test_a_follower_read_that_no_member_answers_says_so():
    """Every member silent is the one outcome a caller has to be told about.

    Coming back with nothing would read as a shard with no leader, which is a different
    thing: the members are there and none of them is answering.  So the walk ends by raising
    what it heard, and only after it has spent the set: every member the table names asked,
    which is what tells a walk that ran out of places from one that stopped at the first.
    """
    addresses = {1: "node1:8001", 2: "node2:8001", 3: "node3:8001"}
    reached = []
    silent = {address: _SilentNode(reached) for address in addresses.values()}
    factory = _AddressBookFactory(silent)
    router = RoutingCache(_FakeCluster({}),
                          _FakeTableSource(_table_with_addresses(1, addresses)),
                          factory=factory, local_node_id=2)
    client = SmartClient(None, _FakeCluster({}), router=router, factory=factory)

    with pytest.raises(NodeUnreachable):
        client.get(KEY_A, consistency=Consistency.FOLLOWER)

    assert reached[0] is silent[addresses[2]], (
        "the member this client is on was asked first, and not the leader the table names")
    assert all(node.asked > 0 for node in silent.values()), (
        "every member of the set was asked before the client gave up")


class _NodeThatNamesItsBasis:
    """A node that answers at an index of its own, the way a node that served a read does.

    A refusal is not an answer as of anything, so this is the shape every read that reached a
    state machine comes back in - and the basis is the node\'s to fill in, because the machine
    it read from is handed entries to apply and not the positions they were written at.
    """

    def __init__(self, value, read_index):
        self._value = value
        self._read_index = read_index

    def get(self, key, timestamp=None, read_index=None):
        assert read_index is None, "a level the node obtains the index for names none"
        return ReadResult.success(self._value, read_index=self._read_index)


def test_a_read_keeps_the_index_the_node_answered_it_at():
    """The basis a client has just been told is the basis of the reads that will not ask.

    A cached read is answered at an index this client was given earlier, and the only place
    such an index can come from is a read that was answered at one.  So every read that
    reached a state machine keeps what came back, and not only the level that will read at
    it: a first follower read is what makes a cached one possible at all.
    """
    addresses = {1: "node1:9001", 2: "node2:9001"}
    factory = _AddressBookFactory({addresses[2]: _NodeThatNamesItsBasis(b"v1", 7)})
    router = RoutingCache(_FakeCluster({}),
                          _FakeTableSource(_table_with_addresses(1, addresses)),
                          factory=factory, local_node_id=2)
    client = SmartClient(None, _FakeCluster({}), router=router, factory=factory)

    assert client.get(KEY_A, consistency=Consistency.FOLLOWER) == b"v1"
    # The cache has no other handle: what a level will read at is not something a caller of a
    # read is meant to reach for, so the test reaches in rather than opening one.
    assert client._read_index_cache.cached_index(0) == 7, (
        "the index the answer came back with, kept by the shard it is an index of")


class _NodeThatAnswersARange:
    """A node that answers a range with rows, wherever it is asked from."""

    def __init__(self, rows):
        self._rows = rows

    def scan(self, start_key, end_key, timestamp=None, read_index=None):
        return list(self._rows)


def test_a_range_read_at_a_level_that_need_not_lead_goes_to_a_member():
    """A range is a piece per shard it crosses, and a piece is one read of that shard.

    So the level is the piece\'s and not the range\'s: each piece goes to a member of its own
    shard\'s set, and the caller asked for the level once.  What makes this worth its own test
    is that the pick is per piece - a range read is a loop over shards, and a loop is where a
    lookup gets made once and used for all of them.
    """
    addresses = {1: "node1:9001", 2: "node2:9001"}
    factory = _AddressBookFactory({addresses[2]: _NodeThatAnswersARange([(b"k", b"v")])})
    router = RoutingCache(_FakeCluster({}),
                          _FakeTableSource(_table_with_addresses(1, addresses)),
                          factory=factory, local_node_id=2)
    client = SmartClient(None, _FakeCluster({}), router=router, factory=factory)

    assert client.scan(b"a", b"z", consistency=Consistency.FOLLOWER) == [(b"k", b"v")]


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

    def scan(self, start_key, end_key, timestamp=None, read_index=None):
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


def test_a_killed_shard_leader_is_answered_by_the_other_replicas_of_its_set():
    """A table naming a leader that is gone, recovered without reading the table at all.

    What a client over a wire has and an in-process one does not is the address of every
    replica the table published, and this is what that buys: the write goes to the node the
    table named, gets no answer, and lands on the leader its own set elected, with the
    metadata group never asked.  The table is the cluster's real one, frozen at the moment
    before the kill - a publisher that has not written yet is the situation, and freezing
    the table is how this test says so without racing one - so a read of it would only hand
    this client the same dead node back.
    """
    metadata, tso_cluster, shard_cluster = _cluster_with_metadata()
    factory = RemoteNodeClientFactory()
    try:
        wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
        tso_client = wait_for_tso_client(tso_cluster)
        table_client = wait_for_metadata_client(metadata)
        table = _published_table(table_client, (0, 1))

        router = RoutingCache(shard_cluster, _FakeTableSource(table), factory=factory)
        client = SmartClient(tso_client, shard_cluster, router=router)
        assert client.put(KEY_A, b"v1")

        placement = table.shard_for(KEY_A)
        assert len(placement.addresses) > 1, "a walk needs a set to walk over"
        shard_cluster.get_shard_server(placement.leader_id).shutdown()
        wait_for_keys_leader(shard_cluster, [KEY_A])

        assert client.put(KEY_A, b"v2"), "the write never got past the node that was killed"
        assert router.refreshes == 0, "the table was not read to find the new leader"
    finally:
        factory.close()
        shard_cluster.shutdown()
        tso_cluster.shutdown()
        metadata.shutdown()


# -- the level that picks a member, over a real cluster ------------------------

class _RecordingRemoteFactory(RemoteNodeClientFactory):
    """A factory over a wire that remembers which node each handle was drawn for.

    How many members a follower read went through is not something the answer says, so it is
    said here: a read the member it asked went on to serve draws one handle, and a read that
    had to walk past a member that refused or went quiet draws more than one.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.drawn = []

    def get_client(self, shard_id, node_id, address=None):
        client = super().get_client(shard_id, node_id, address)
        if client is not None:
            self.drawn.append(node_id)
        return client


def test_a_follower_read_is_served_by_a_member_of_the_shard_s_set():
    """The level where the hop happens on the replica, and the reads spread over the set.

    A follower read is answered at the leader's index, and it is the node that answers that
    obtains it - so any member of the set can serve the read, and which member is the
    caller's draw.  Pinning that takes a client over a wire: a handle in this process has no
    channel to carry the index question over, so that member of the set is a member the
    client reaches the leader through, which is ``test_client_servicer.py``\'s subject.

    One handle per read is how a member is told from a walk: a member that had refused would
    have sent the client to another node, which is a second draw.  The value is asserted too,
    and it is the floor rather than the point - a spread over replicas that answered
    differently would be a client reading from several places at once.
    """
    metadata, tso_cluster, shard_cluster = _cluster_with_metadata(num_shards=1)
    factory = _RecordingRemoteFactory()
    try:
        wait_for_keys_leader(shard_cluster, [KEY_A])
        tso_client = wait_for_tso_client(tso_cluster)
        table_client = wait_for_metadata_client(metadata)
        table = _published_table(table_client, (0,))

        router = RoutingCache(shard_cluster, _FakeTableSource(table), factory=factory)
        client = SmartClient(tso_client, shard_cluster, router=router)
        assert client.put(KEY_A, b"v1")

        members = set(table.shard(0).nodes)
        assert len(members) == 3, "a set to spread over"

        factory.drawn.clear()
        for _ in range(60):
            assert client.get(KEY_A, consistency=Consistency.FOLLOWER) == b"v1"

        assert len(factory.drawn) == 60, (
            "one member served each read: a member that refused would have been walked past")
        assert set(factory.drawn) == members, "every member of the set, and nothing else"
    finally:
        factory.close()
        shard_cluster.shutdown()
        tso_cluster.shutdown()
        metadata.shutdown()
