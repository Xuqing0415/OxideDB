"""The routing table and the clock, asked by a client in another process.

A shard's client service has been reachable from outside the cluster since
``tests/test_client_over_processes.py``.  What a transaction also needs is a ``start_ts``
and a node to send its writes to, and both of those come from a group that is not the shard:
the table says which shard a key lives in and which node leads it, and the clock hands out
the numbers a snapshot read and a commit are stamped with.  These are the tests for that
half, over a real socket, against nodes that are processes of their own.

What they can check that no in-process test can: bytes that have to survive a message, an
address that answers nothing at all, and a refusal that carries an address a client can
act on.  The one thing they do not take on faith is the hint - a client consumes it, so a
test that only used the client could not tell a hint that was followed from a seed that
happened to work.  Those two tests read the refusal with the raw stub first, assert what it
says, and then require the client to get the table out of exactly that address.

What the table says about a shard's leader is asserted twice, and differently, because in a
cluster of processes it does not always say anything.  Only the node that leads the
metadata group can propose to it - a member that does not lead that group refuses, and
nothing on the wire forwards a proposal - so a shard led by another node is published with
its replica set and its addresses and no leader.  The one-node cluster below is where the
leader column is checked on its own, because there the node leads everything and the answer
is not a race; the three-node cluster checks that a leader which *is* named is a real one.

A proposal is now something a client outside the cluster can make at all.  A member answers
one by refusing and naming the leader, which is the path a read already walked, so the table
can be written from a node that does not lead its group.  Nothing forwards a proposal on the
service's behalf - the caller is the one that moves - and the writes below are what that
buys: the placement the group already holds, written back through a member that does not
lead; a command the table will not take, answered as a refusal rather than as another "ask
the leader"; and a leader report that is what the table names afterwards.
"""

import socket

import grpc
import pytest

from _cluster import start_cluster
from _ports import allocate_port
from _wait import wait_until
from oxidedb.channels import ChannelPool
from oxidedb.client import NodeUnreachable, RemoteMetadataClient, RemoteTSOClient
from oxidedb.proto import groups_pb2
from oxidedb.proto.groups_pb2_grpc import MetadataServiceStub, TSOServiceStub
from oxidedb.raft.state_machine import ErrorCode
from oxidedb.shard.router import default_range_map


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    """Three nodes with two shards each: groups to lead, and members that do not lead them.

    Module-wide because a cluster costs three processes and three elections, and because
    no test here spoils another one's reading: the table only ever gains a leader report,
    and the clock only ever moves forward.
    """
    with start_cluster(num_nodes=3, num_shards=2,
                       base_dir=str(tmp_path_factory.mktemp("oxidedb-groups"))) as running:
        yield running


@pytest.fixture(scope="module")
def one_node(tmp_path_factory):
    """One node with two shards, which is the leader of every group it holds.

    Where the three-node cluster cannot promise that a shard's leader gets published at
    all - see the module docstring - this one can, so the leader column is checked here
    rather than behind a condition.
    """
    with start_cluster(num_nodes=1, num_shards=2,
                       base_dir=str(tmp_path_factory.mktemp("oxidedb-one-node"))) as running:
        yield running


@pytest.fixture(scope="module")
def dead_address():
    """An address nothing listens on: a seed that can only be walked past."""
    return f"127.0.0.1:{allocate_port(span=1)}"


@pytest.fixture
def open_client():
    """Open group clients, and close every one of them when the test is over.

    A client that nobody closes leaves a channel behind, which is a socket and a thread
    pool per test - and on Windows a leaked socket is a thing the next test can trip over.
    """
    opened = []

    def open_one(factory, *args, **kwargs):
        client = factory(*args, **kwargs)
        opened.append(client)
        return client

    yield open_one
    for client in opened:
        client.close()


def _asking(call):
    """``call`` as a ``wait_until`` predicate: a group that cannot answer has not answered.

    A group with no leader yet refuses, and a client with nothing to ask raises - neither is
    a failure of the thing under test, and both are what waiting is for.  Anything else is a
    real error and is left to come out of the test.
    """
    def asked():
        try:
            return call()
        except (NodeUnreachable, RuntimeError):
            return None

    return asked


def _published_table(client, num_shards):
    """The table, once the group has published the ranges this cluster routes by.

    Placement arrives a command at a time, so a table read before the publisher's first
    pass holds no ranges at all, and one read a moment later holds the ranges without a
    replica set.  Both are answers rather than failures, and a client waits them out by
    reading again - which is what this does, so that the assertions below are about a
    published table rather than about a moment in the middle of publishing one.

    The replica sets are waited for as well as the ranges: the publisher sends them as
    commands of their own, so a table that has the ranges and no nodes is one it has not
    finished with, and the assertions below are about a finished one.
    """
    table = client.table(refresh=True)
    if table.routes() != default_range_map(num_shards):
        return None
    if any(not placement.nodes or not placement.addresses
           for placement in table.shards.values()):
        return None
    return table


def _follower_naming_the_leader(cluster, pool):
    """A member that refused and named where the leader is, with the refusal it gave.

    Asked with the raw stub, because a client is what hides this: it consumes the hint, and
    a test that only saw the table arrive could not tell whether the hint was followed or
    whether the seed it was given happened to work.  The refusal is the evidence that the
    client had somewhere to go.
    """
    for node in cluster.nodes:
        address = cluster.metadata_address(node.node_id)
        stub = MetadataServiceStub(pool.channel(address))
        try:
            response = stub.ListShards(groups_pb2.ListShardsRequest(), timeout=2.0)
        except grpc.RpcError:
            continue
        if response.error_code != groups_pb2.NOT_LEADER:
            continue
        if not (response.HasField("leader_address") and response.leader_address):
            continue
        return address, response
    return None


def _get_one_timestamp(pool, cluster):
    """One answer from ``GetTimestamp``, asked of each member until one of them leads.

    A raw stub does not follow the leader a refusal names the way a client does, so a test
    that wants to see this call itself asks the members in turn: the ones that do not lead
    refuse, and the one that does answers with a run of one.  None means the group had
    nothing to say this time round, which is what waiting is for.
    """
    for address in cluster.tso_seeds:
        try:
            stub = TSOServiceStub(pool.channel(address))
            response = stub.GetTimestamp(groups_pb2.GetTimestampRequest(), timeout=2.0)
        except grpc.RpcError:
            continue
        if response.error_code == groups_pb2.OK:
            return response
    return None


def _write_the_placement_back(client):
    """A proposal the group has to accept: the placement it is already holding.

    Written back rather than invented, because what is under test is the write path and not
    the table's geometry: a command the table would refuse could not tell "the proposal
    arrived" from "the proposal was wrong".  None while there is nothing to write back or a
    member refused it, which is what waiting is for.
    """
    placement = client.table(refresh=True).shard(0)
    if placement is None or not placement.nodes or not placement.addresses:
        return None
    if not client.set_shard_nodes(placement.shard_id, placement.nodes,
                                  placement.addresses).success:
        return None
    return placement


def _answer_to_an_empty_batch(stub):
    """The clock's last word on a batch of none: not leadership, but the batch itself."""
    try:
        response = stub.GetTimestampBatch(
            groups_pb2.GetTimestampBatchRequest(count=0), timeout=2.0)
    except grpc.RpcError:
        return None
    return None if response.error_code == groups_pb2.NOT_LEADER else response


class TestTheRoutingTableOverTheWire:
    """The whole table, read by a process that is not one of the nodes."""

    def test_the_whole_table_arrives_over_a_socket(self, cluster, open_client):
        """The ranges, the replica sets and the addresses, as the nodes themselves hold them.

        The addresses are not left as strings that look right: each one is opened, because
        an address in a table is worth something only when something is behind it, and that
        hop is the one a client takes to send a shard its request.
        """
        client = open_client(RemoteMetadataClient, cluster.metadata_seeds)

        table = wait_until(_asking(lambda: _published_table(client, cluster.num_shards)),
                           message="the table was never read over the wire")

        # The ranges are the ones the nodes route by, byte for byte - a boundary that
        # changed on the way through a message would send a client to the wrong shard,
        # and the two halves of the keyspace have to stay two shards.
        assert table.routes() == default_range_map(cluster.num_shards)
        low = table.route_for(b"alpha")
        high = table.route_for(bytes([0x90]) + b"key")
        assert low != high, "one shard answers for the whole keyspace"

        every_node = [node.node_id for node in cluster.nodes]
        for placement in table.shards.values():
            assert sorted(placement.nodes) == every_node
            assert sorted(placement.addresses) == every_node
            for node_id, address in sorted(placement.addresses.items()):
                assert address == cluster.shard_address(placement.shard_id, node_id)
                host, port = address.split(":")
                with socket.create_connection((host, int(port)), timeout=2.0):
                    pass

    def test_a_leader_the_table_names_is_a_node_that_serves_that_shard(
            self, cluster, open_client):
        """Whatever is said about a leader has to be a node that can answer for the shard.

        What this does not require is that every shard has a leader named in the table, for
        the reason the module docstring gives: only the node that leads the metadata group
        can propose a leader report to it, so a shard led elsewhere is published without
        one.  That is a gap in the cluster's publishing rather than in the table, and the
        README's Known gaps says what it costs and what closing it would take.  Here the
        assertion is that a leader which *is* named is real and reachable.
        """
        client = open_client(RemoteMetadataClient, cluster.metadata_seeds)
        table = wait_until(_asking(lambda: _published_table(client, cluster.num_shards)),
                           message="the table was never read over the wire")

        for placement in table.shards.values():
            if placement.leader_id is None:
                continue
            assert placement.leader_id in placement.nodes
            assert placement.leader_address() == cluster.shard_address(
                placement.shard_id, placement.leader_id)
            host, port = placement.leader_address().split(":")
            with socket.create_connection((host, int(port)), timeout=2.0):
                pass

    def test_a_member_that_does_not_lead_sends_a_client_to_the_one_that_does(
            self, cluster, open_client):
        """Seeded with one member that is not the leader, a client still reads the table.

        The only way it can: the member refuses and names the leader.  The refusal is read
        here first, so that what the client does with it is not taken on faith - a client
        seeded with one address it cannot use has no other seed to fall back on, so a table
        it manages to read can only have come from following the name.
        """
        pool = ChannelPool()
        try:
            found = wait_until(
                lambda: _follower_naming_the_leader(cluster, pool),
                message="no member refused with a leader to name")
        finally:
            pool.close()
        address, refusal = found

        assert refusal.leader_address in cluster.metadata_seeds
        assert refusal.leader_address != address

        client = open_client(RemoteMetadataClient, [address])
        table = wait_until(
            _asking(lambda: _published_table(client, cluster.num_shards)),
            message=f"a client seeded only with {address} never read the table")

        assert table.routes() == default_range_map(cluster.num_shards)

    def test_a_single_node_names_itself_as_the_leader_of_every_shard(
            self, one_node, open_client):
        """The leader column, where it can be decided instead of waited for.

        One node leads the metadata group, both shards and the clock, so the reports have
        nowhere else to come from and nothing to race with: the table has to end up naming
        node 1 as the leader of each shard, at the term it claimed, at the address the
        shard really listens on.  That is the half of the table a client cannot work out
        for itself - the replica set says where the shard is served, and only the cluster
        knows which of those nodes won the election.
        """
        client = open_client(RemoteMetadataClient, one_node.metadata_seeds)

        def settled():
            table = _published_table(client, one_node.num_shards)
            if table is None:
                return None
            if any(placement.leader_id is None for placement in table.shards.values()):
                return None
            return table

        table = wait_until(_asking(settled),
                           message="one node never named itself as a shard's leader")

        assert table.routes() == default_range_map(one_node.num_shards)
        for placement in table.shards.values():
            assert placement.nodes == [1]
            assert placement.leader_id == 1
            assert placement.leader_term >= 1
            assert placement.leader_address() == one_node.shard_address(
                placement.shard_id, 1)

    def test_a_seed_that_answers_nothing_is_walked_past(
            self, cluster, dead_address, open_client):
        """A client is told where the nodes are, and one of them may be wrong or gone.

        The walk is the whole of what a client has here: there is no directory to ask which
        of its seeds is the live one, so a seed that does not answer must cost a hop and not
        the call.
        """
        client = open_client(RemoteMetadataClient, [dead_address] + cluster.metadata_seeds)

        table = wait_until(_asking(lambda: _published_table(client, cluster.num_shards)),
                           message="the walk never reached a member of the group")

        assert table.routes() == default_range_map(cluster.num_shards)

    def test_nothing_answering_at_all_is_unreachable_and_not_a_refusal(
            self, dead_address, open_client):
        """A wire failure is not an answer, and must not be reported as one.

        A shorter deadline than the default, because what is under test is the
        classification and not how long a socket takes to say nothing is there.
        """
        client = open_client(RemoteMetadataClient, [dead_address], timeout=1.0)

        with pytest.raises(NodeUnreachable) as raised:
            client.refresh()

        assert dead_address in str(raised.value)


class TestTheClockOverTheWire:
    """Timestamps, taken one at a time and in runs, by a process outside the cluster."""

    def test_timestamps_come_back_increasing(self, cluster, open_client):
        client = open_client(RemoteTSOClient, cluster.tso_seeds)

        stamps = wait_until(_asking(lambda: client.batch_get_timestamps(5)),
                            message="the clock never answered")

        assert stamps == sorted(set(stamps))
        assert all(later > earlier for earlier, later in zip(stamps, stamps[1:]))

    def test_a_run_that_is_used_up_is_replaced_by_one_above_it(self, cluster, open_client):
        """The run's far end is inclusive, and the next run starts past it.

        A client that read that end as exclusive would drop the last timestamp of every run
        and hand out the first one of the next; a client that read it as one past the end
        the other way would hand the same number out twice.  Three then one makes the
        boundary visible: the fourth number has to come from a new run and be above the
        third, and no number may repeat.
        """
        client = open_client(RemoteTSOClient, cluster.tso_seeds, batch_size=3)

        first = wait_until(_asking(lambda: client.batch_get_timestamps(3)),
                           message="the clock never answered")
        second = client.batch_get_timestamps(1)

        assert len(set(first)) == 3
        assert second[0] > first[-1]

    def test_a_timestamp_asked_for_on_its_own_is_one_timestamp(self, cluster):
        """``GetTimestamp`` allocates a run of one, so nothing is handed out behind it.

        Two calls in a row differ by exactly one.  A run of a thousand would show up here as
        a gap of a thousand, and it is the reason the single call is in the contract at all:
        a caller that reads one number at a time should not take a run it will not use.
        """
        pool = ChannelPool()
        try:
            first = wait_until(_asking(lambda: _get_one_timestamp(pool, cluster)),
                               message="no member of the clock answered GetTimestamp")
            second = _get_one_timestamp(pool, cluster)
        finally:
            pool.close()

        assert second is not None, "the clock stopped answering between two calls"
        assert second.timestamp == first.timestamp + 1

    def test_asking_for_no_timestamps_is_refused_and_not_rounded_up(self, cluster):
        """A run of none is not a range, and the group says so instead of guessing one.

        Rounded up to one, this call would answer with a timestamp the caller did not ask
        for and would not use; the honest answer is that the request has no meaning.
        """
        pool = ChannelPool()
        try:
            stub = TSOServiceStub(pool.channel(cluster.tso_address()))
            response = wait_until(lambda: _answer_to_an_empty_batch(stub),
                                  message="the clock never answered a batch of none")
        finally:
            pool.close()

        assert response.error_code == groups_pb2.REFUSED
        assert not response.HasField("start_ts")

    def test_a_seed_that_answers_nothing_is_walked_past(
            self, cluster, dead_address, open_client):
        """The clock is seeded the same way the table is, and walked the same way."""
        client = open_client(RemoteTSOClient, [dead_address] + cluster.tso_seeds)

        stamps = wait_until(_asking(lambda: client.batch_get_timestamps(1)),
                            message="the walk never reached a member of the group")

        assert len(stamps) == 1


class TestWritingTheTableOverTheWire:
    """The table changed by a process that is not one of the nodes.

    The cluster's publisher is a client of this group like any other, and what it does is
    what these tests do: build a command, send it to the member that leads, and act on the
    answer.  What is pinned here is that the port answers a proposal at all, that the walk
    carries one to the leader as it carries a question, and that a command the table will not
    take comes back as a refusal rather than as another address to ask.
    """

    def test_a_proposal_reaches_the_leader_through_a_member_that_does_not_lead(
            self, cluster, open_client):
        """A write walks the same path a read does, and for the same reason.

        The client is seeded with one member that is not the leader and nothing else, so a
        write that comes back successful can only have been applied by the member the refusal
        named.  The table is then read back by a second client, because the writer's own
        answer says what it asked for and only the group can say what it holds.
        """
        pool = ChannelPool()
        try:
            found = wait_until(lambda: _follower_naming_the_leader(cluster, pool),
                               message="no member refused with a leader to name")
        finally:
            pool.close()
        address, refusal = found

        assert refusal.leader_address in cluster.metadata_seeds
        assert refusal.leader_address != address

        client = open_client(RemoteMetadataClient, [address])
        written = wait_until(_asking(lambda: _write_the_placement_back(client)),
                             message=f"a client seeded only with {address} never wrote")

        reader = open_client(RemoteMetadataClient, cluster.metadata_seeds)
        table = wait_until(_asking(lambda: _published_table(reader, cluster.num_shards)),
                           message="the table was never read back")

        assert table.shard(written.shard_id).nodes == written.nodes
        assert table.shard(written.shard_id).addresses == written.addresses

    def test_a_command_the_table_will_not_take_is_answered_with_a_refusal(
            self, cluster, open_client):
        """The refusal of the member that leads outranks the others saying ask the leader.

        There is no shard 999 and there never will be, so the group refuses this report on its
        geometry.  What the client has to hear is that refusal: a walk that kept the last
        member's "ask the leader" would send the caller round the group again for an answer it
        had already been given, and a publisher that read it as a leaderless group would keep
        proposing a command that can never apply.
        """
        client = open_client(RemoteMetadataClient, cluster.metadata_seeds)
        wait_until(_asking(lambda: _published_table(client, cluster.num_shards)),
                   message="the table was never read over the wire")

        result = client.report_leader(999, 1, 1)

        assert not result.success
        assert result.error_code == ErrorCode.ERR_APPLY_ERROR
        assert "999" in result.error_msg

    def test_a_report_from_a_client_is_what_the_table_names_afterwards(
            self, cluster, open_client):
        """The leader column, moved by a client - which is the whole of what the publisher does.

        The term is one no election in this test will reach, so the claim cannot be moved out
        from under the assertion by the publisher's next poll: what the table names afterwards
        is the report this test proposed.  The node named is a real replica of that shard, so
        the table still sends callers somewhere that can answer for it.
        """
        client = open_client(RemoteMetadataClient, cluster.metadata_seeds)
        table = wait_until(_asking(lambda: _published_table(client, cluster.num_shards)),
                           message="the table was never read over the wire")

        placement = table.shard(0)
        node_id = placement.nodes[0]
        term = placement.leader_term + 10 ** 6

        assert client.report_leader(placement.shard_id, node_id, term).success

        named = client.table(refresh=True).shard(placement.shard_id)
        assert named.leader_id == node_id
        assert named.leader_term == term
        assert named.leader_address() == cluster.shard_address(placement.shard_id, node_id)

