"""The six primitives on a port, and what a refusal looks like when it crosses one.

``ShardServer`` puts a ``ClientServicer`` on the address a shard already answers Raft on,
and this file drives that servicer with a bare stub - no client wrapper in between - so that
what is pinned here is the wire shape itself: which field carries the answer, which fields
are absent rather than empty, and what a refusal is when it arrives as a message instead of
as a returned object.

A node that has stopped leading is the case that matters.  It answers, and the answer is no,
and the difference between that and a node that has gone quiet is the difference between a
retry and a failure - so a shut-down node is served on a port of this test's own making and
asked six times.

The other half of a refusal is where to go next.  A node that is not the leader and has
heard from one answers with the address of the node that leads: in one test the address is
made up, and in the last one it comes from a real cluster, a real election, and a follower
that has just been told who won.
"""

from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from _ports import allocate_port, free_addresses
from _wait import wait_for_keys_leader, wait_until
from oxidedb.client import RemoteNodeClientFactory
from oxidedb.proto import client_pb2
from oxidedb.proto.client_pb2_grpc import (ClientServiceStub,
                                           add_ClientServiceServicer_to_server)
from oxidedb.raft.client_servicer import ClientServicer
from oxidedb.raft.node import MemoryRaftNode, NodeState
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import (CommandType, MVCCStateMachine,
                                        serialize_command)

KEY = b"key0"          # first byte 0x6b -> shard 0
LOCKED = b"key_locked"
TIMEOUT = 5

#: Everything a test here built that owns a thread or a socket.  A node drives its
#: elections from a thread of its own and a server owns a pool, so anything left running
#: outlives the test that started it - except the servers, which are stopped in reverse
#: order to the one they were started in.
_BUILT_NODES = []
_BUILT_SERVERS = []
_BUILT_CHANNELS = []
_BUILT_CLUSTERS = []
_BUILT_FACTORIES = []


@pytest.fixture(autouse=True)
def _stop_what_the_test_started():
    yield
    for channel in _BUILT_CHANNELS:
        channel.close()
    del _BUILT_CHANNELS[:]
    for server, _address in _BUILT_SERVERS:
        server.stop(grace=0)
    del _BUILT_SERVERS[:]
    while _BUILT_NODES:
        _BUILT_NODES.pop().shutdown()
    while _BUILT_CLUSTERS:
        _BUILT_CLUSTERS.pop().shutdown()
    for factory in _BUILT_FACTORIES:
        factory.close()
    del _BUILT_FACTORIES[:]


def _leader():
    """A one-node shard that leads itself, so that proposals commit."""
    node = MemoryRaftNode(node_id=1, peers=[], state_machine=MVCCStateMachine(),
                          get_peer_node=None,
                          election_timeout_min=20, election_timeout_max=40)
    _BUILT_NODES.append(node)
    wait_until(lambda: node.state == NodeState.LEADER, timeout=10,
               message="the single node never became the leader")
    return node


def _served(node, leader_address=None, client_at_address=None) -> str:
    """The node's client primitives on a port of this test's own making.

    The server is the test's rather than the node's, which is what lets a node that has
    been shut down - and so has stopped leading - go on answering: the refusal tests are
    about a node that says no, not about one that has gone quiet.

    ``client_at_address`` is how the node answers a question it cannot answer itself:
    the forwarding tests hand in a way to reach another node, and the tests that do not
    leave the forwarding with nowhere to go.
    """
    address = f"127.0.0.1:{allocate_port(span=1)}"
    server = grpc.server(ThreadPoolExecutor(max_workers=4))
    add_ClientServiceServicer_to_server(
        ClientServicer(node, leader_address=leader_address,
                       client_at_address=client_at_address), server)
    server.add_insecure_port(address)
    server.start()
    _BUILT_SERVERS.append((server, address))
    return address


def _stub(address: str) -> ClientServiceStub:
    channel = grpc.insecure_channel(address)
    _BUILT_CHANNELS.append(channel)
    return ClientServiceStub(channel)


def _propose(stub, **command):
    """One command, through the port, as the servicer's own caller would send it."""
    return stub.Propose(client_pb2.ProposeRequest(
        command=serialize_command(**command)), timeout=TIMEOUT)


# -- the six primitives --------------------------------------------------------

def test_the_six_primitives_cross_a_wire():
    """Every answer in the field the proto put it in, absent where it is absent."""
    node = _leader()
    stub = _stub(_served(node))

    # A key that is not there: a read that answered, with no value rather than an empty
    # one.  That is what the optional field is for - empty bytes are a value a caller
    # could have written, and "no version at this snapshot" is not.
    missing = stub.Get(client_pb2.GetRequest(key=b"missing"), timeout=TIMEOUT)
    assert missing.error_code == client_pb2.OK
    assert not missing.HasField("value")
    assert missing.message == ""
    assert missing.commit_ts == 0, "a key with no value has no version either"

    written = _propose(stub, cmd_type=CommandType.SET, key=KEY, value=b"v1", timestamp=5)
    assert written.error_code == client_pb2.OK
    # Where the write landed, which the state machine cannot know: it is the node that
    # appended the entry, and a client waiting for its write to be readable waits for this.
    assert written.HasField("index") and written.index > 0
    assert written.term == node.current_term

    read = stub.Get(client_pb2.GetRequest(key=KEY), timeout=TIMEOUT)
    assert read.error_code == client_pb2.OK and read.value == b"v1"
    assert read.commit_ts == 5, "and which version it is, so a copy can keep it"

    older = stub.Get(client_pb2.GetRequest(key=KEY, timestamp=4), timeout=TIMEOUT)
    assert older.error_code == client_pb2.OK and not older.HasField("value")
    assert older.commit_ts == 0, "not visible yet is not a version either"

    rows = stub.Scan(client_pb2.ScanRequest(start_key=b"a", end_key=b"z"), timeout=TIMEOUT)
    assert rows.error_code == client_pb2.OK
    assert [(entry.key, entry.value) for entry in rows.entries] == [(KEY, b"v1")]
    assert [entry.commit_ts for entry in rows.entries] == [5], (
        "the stamp rides on the row, so no row arrives without it")

    assert not stub.GetLock(
        client_pb2.GetLockRequest(key=KEY), timeout=TIMEOUT).HasField("lock")

    index = stub.FollowerReadIndex(client_pb2.FollowerReadIndexRequest(), timeout=TIMEOUT)
    assert index.error_code == client_pb2.OK and index.read_index >= written.index


def test_a_lock_and_a_write_record_cross_a_wire_field_for_field():
    """What the storage holds for a key, in the message that carries it.

    The wire shapes are copies of the storage's own records - not a tidier design of them -
    because what a lock means is decided by the code that reads it, and a message that
    dropped a field would be a decision made here by accident.
    """
    node = _leader()
    stub = _stub(_served(node))

    assert _propose(stub, cmd_type=CommandType.PREWRITE, key=LOCKED, value=b"v2",
                    start_ts=10, primary_key=LOCKED).error_code == client_pb2.OK

    response = stub.GetLock(client_pb2.GetLockRequest(key=LOCKED), timeout=TIMEOUT)
    assert response.HasField("lock")
    stored = node._state_machine.get_lock_status(LOCKED)
    lock = response.lock
    assert (lock.key, lock.start_ts, lock.status, lock.primary_key, lock.value) == (
        stored["key"], stored["start_ts"], stored["status"], stored["primary_key"],
        stored["value"])
    assert lock.lock_time == stored["lock_time"], (
        "the moment the lock was taken is what a TTL is measured from, so it travels")

    # Read back by the transaction that left it, the intent is a value with no version: 0
    # in the field, because an intent is a lock rather than a row any commit published.
    intent = stub.Get(client_pb2.GetRequest(key=LOCKED, timestamp=10), timeout=TIMEOUT)
    assert (intent.value, intent.commit_ts) == (b"v2", 0)

    scanned = stub.Scan(client_pb2.ScanRequest(start_key=b"a", end_key=b"z",
                                               timestamp=10), timeout=TIMEOUT)
    assert [(entry.key, entry.value, entry.commit_ts)
            for entry in scanned.entries] == [(LOCKED, b"v2", 0)]

    assert _propose(stub, cmd_type=CommandType.COMMIT, key=LOCKED, start_ts=10,
                    commit_ts=20).error_code == client_pb2.OK

    assert not stub.GetLock(
        client_pb2.GetLockRequest(key=LOCKED), timeout=TIMEOUT).HasField("lock")
    record = stub.GetWriteRecord(client_pb2.GetWriteRecordRequest(key=LOCKED),
                                 timeout=TIMEOUT)
    assert (record.start_ts, record.commit_ts) == (10, 20), (
        "which transaction wrote it, and when it was published")


def test_a_range_read_behind_a_lock_names_the_key_that_is_in_the_way():
    """A refusal in the response, and the key in it.

    A dropped key would look exactly like a key that is not there, which is why the local
    scan raises rather than answering short - and on the wire the same fact has to travel,
    because the caller is the one that has to resolve it.
    """
    node = _leader()
    stub = _stub(_served(node))
    assert _propose(stub, cmd_type=CommandType.PREWRITE, key=LOCKED, value=b"v2",
                    start_ts=10, primary_key=LOCKED).error_code == client_pb2.OK

    blocked = stub.Scan(client_pb2.ScanRequest(start_key=b"a", end_key=b"z"),
                        timeout=TIMEOUT)
    assert blocked.error_code == client_pb2.LOCKED
    assert blocked.locked_key == LOCKED
    assert not blocked.entries


# -- refusals ------------------------------------------------------------------

def test_a_node_that_has_stopped_leading_refuses_in_the_response():
    """A refusal is an answer, not a transport failure.

    Nothing here raises a gRPC error: the call arrived, and the node's answer was that it
    is not the leader.  A caller has to be able to tell that from a wire that is down,
    because one of those is a retry somewhere else and the other is not a retry at all.
    """
    node = _leader()
    stub = _stub(_served(node))
    assert _propose(stub, cmd_type=CommandType.SET, key=KEY, value=b"v1").error_code ==         client_pb2.OK

    node.shutdown()  # it steps down; the port belongs to this test and stays up

    for response in (
        stub.Get(client_pb2.GetRequest(key=KEY), timeout=TIMEOUT),
        _propose(stub, cmd_type=CommandType.SET, key=KEY, value=b"v2"),
        stub.FollowerReadIndex(client_pb2.FollowerReadIndexRequest(), timeout=TIMEOUT),
    ):
        assert response.error_code == client_pb2.NOT_LEADER
        assert response.message, "a refusal says what it refused"

    refused = stub.Scan(client_pb2.ScanRequest(start_key=b"a", end_key=b"z"),
                        timeout=TIMEOUT)
    assert refused.error_code == client_pb2.NOT_LEADER
    assert not refused.entries


def test_a_refusal_names_the_leader_when_the_node_knows_where_it_is():
    """The hint, and its absence.

    Set for NOT_LEADER and unset otherwise, which is the whole contract: a client that has
    just been refused can go straight to the address instead of reading the routing table,
    whose answer is only as fresh as the publisher's last poll.  A node that cannot say
    leaves the field unset rather than filling in a guess - its own address, say, which is
    where the caller has just been refused.
    """
    node = _leader()
    stub = _stub(_served(node, leader_address=lambda: "127.0.0.1:50051"))
    node.shutdown()

    refusal = stub.Get(client_pb2.GetRequest(key=KEY), timeout=TIMEOUT)
    assert refusal.error_code == client_pb2.NOT_LEADER
    assert refusal.leader_address == "127.0.0.1:50051"

    # A node that leads answers, and a node that is asked something it can answer does not
    # get a hint pinned to the answer.
    answering = _leader()
    answering_stub = _stub(_served(answering, leader_address=lambda: "127.0.0.1:50051"))
    assert not answering_stub.Get(
        client_pb2.GetRequest(key=b"missing"), timeout=TIMEOUT).HasField("leader_address")

    # And a node with nowhere to send anyone: the field stays absent, which is what makes
    # the client read the table instead.
    silent = _leader()
    silent_stub = _stub(_served(silent, leader_address=lambda: None))
    silent.shutdown()
    assert not silent_stub.Get(
        client_pb2.GetRequest(key=KEY), timeout=TIMEOUT).HasField("leader_address")


def test_a_follower_names_the_node_that_leads_a_real_cluster():
    """The hint where it comes from: an election, a heartbeat, and a shard server.

    The address is the leader's own shard port - the one the routing table publishes - so a
    client that follows it reaches the same node it would have reached by reading the table,
    without reading it.
    """
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    _BUILT_CLUSTERS.append(cluster)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(),
                          peer_addresses=free_addresses())
    wait_for_keys_leader(cluster, [KEY])

    def only_followers_point_at_the_leader():
        leader = cluster.shard_leader(0)
        if leader is None:
            return None
        leader_id = leader[0]
        addresses = cluster.shard_addresses(0)
        for node_id, server in sorted(cluster._shard_servers.items()):
            expected = None if node_id == leader_id else addresses[leader_id]
            if server.leader_address(0) != expected:
                return None
        return addresses[leader_id]

    hint = wait_until(only_followers_point_at_the_leader, timeout=10,
                      message="the followers never named the node that leads shard 0")
    assert hint not in (None, ""), "a cluster in network mode has an address to name"


def test_a_follower_carries_a_read_index_question_to_the_leader():
    """The one read a node cannot answer on its own, and how it is answered anyway.

    A follower holds no index of its own to offer, and the caller that asked it did not
    pick it at random: that is where its read is going.  So the question is carried to
    the leader this node has heard of, and what comes back is this node's answer.

    Asked not to pass it on, the same node answers what it answered before it could
    carry anything: not the leader, and where the leader is.  Every walk over a group
    that is looking for its leader rests on that half - a node that carried the question
    would answer it as well, with somebody else's index.
    """
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    _BUILT_CLUSTERS.append(cluster)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(),
                          peer_addresses=free_addresses())
    wait_for_keys_leader(cluster, [KEY])

    leader_id = cluster.shard_leader(0)[0]
    addresses = cluster.shard_addresses(0)
    follower_id = next(node_id for node_id in addresses if node_id != leader_id)
    stub = _stub(addresses[follower_id])

    def both_answers():
        """Both, once this node has settled on somebody else leading.

        A node that began an election just before the winner did stays a candidate
        until the winner's first heartbeat arrives, and a candidate has no leader to
        carry a question to - so the pair is taken again until this is a real follower.
        """
        carried = stub.FollowerReadIndex(
            client_pb2.FollowerReadIndexRequest(), timeout=TIMEOUT)
        local = stub.FollowerReadIndex(
            client_pb2.FollowerReadIndexRequest(answer_locally=True), timeout=TIMEOUT)
        if carried.error_code != client_pb2.OK:
            return None
        if local.error_code != client_pb2.NOT_LEADER:
            return None
        return carried, local

    carried, local = wait_until(
        both_answers, timeout=10,
        message="the follower never carried a read index question to the leader")

    assert carried.read_index >= 1, "an index the leader had confirmed"
    assert not carried.HasField("leader_address"), "an answer carries no hint"
    assert local.leader_address == addresses[leader_id], (
        "the node that cannot answer names the one that can")


def test_a_follower_whose_leader_did_not_answer_refuses_without_a_hint():
    """A hop this node could not make is still an answer about leadership.

    The caller asked a node, and the node has the one useful thing to say: not me.  What
    it must not do is name the address it has just failed to reach - that is the one
    place the caller should not be sent next - so the hint is dropped and the address is
    left in the message, where whoever reads it can see where the question went.
    """
    quiet = f"127.0.0.1:{allocate_port(span=1)}"
    factory = RemoteNodeClientFactory(timeout=0.5)
    _BUILT_FACTORIES.append(factory)
    node = _leader()
    stub = _stub(_served(
        node, leader_address=lambda: quiet,
        client_at_address=lambda address: factory.get_client_at(0, address)))

    # A node that has stopped leading: it cannot confirm an index itself, and the
    # question goes to an address nothing is listening on.
    node.shutdown()

    response = stub.FollowerReadIndex(
        client_pb2.FollowerReadIndexRequest(), timeout=TIMEOUT)
    assert response.error_code == client_pb2.NOT_LEADER
    assert not response.HasField("leader_address"), (
        "the address that has just gone quiet is not somewhere to send the caller")
    assert quiet in response.message, "and where the question went is in the message"
