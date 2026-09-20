"""``ensure_serving``: what a node or a cluster holds for a shard, and what it lets go.

``docs/recovery.md`` gives the recovery one call for both directions of a shard's
membership - build my member of the group, or close it - so that a node coming back can
agree with the routing table about who serves a shard.  Both implementations of that call
are here: the view one node of the launcher keeps, and the cluster object the in-process
tests and the recovery run against.  What the two share is the decision - built, left
alone, closed, built again - and the answer both give for it: the nodes whose group the
call closed, which is how a caller that ran twice tells a call that did something from one
that found the work already done.

The view's tests are view-level on purpose: what is pinned there is the decision, and the
decision is the same one a process makes.  What a process adds is that a group is also a
bound port and a storage directory, and there is one test below of the half of that a
rebuild depends on: the port coming back after the close.  The storage of a shard this
node stops serving is not covered - nothing sets a directory aside yet, where the cluster
side does.

Two of the tests are about the other half of a close, which is not a decision but a
reading: what the view says it holds, for the publisher that asks it.  They are here
because that is where a closed group went wrong - the range map still named the shard
and the shard server still named this node, so a node that had just stopped serving a
shard would have been written into the table as a replica of it.

Two of the tests are about the boundaries of this call rather than cases of it.  What
a cluster answers to "who serves this shard" is the routing table's placement, so
`ensure_serving` must not write it, and the call that does is `apply_move_locally` - the
two disagree for the length of a move's drain window on purpose.  The other boundary is
`close_group_on`, which is this call's closing half with the set named by the caller: a
refused move takes down the group it built and mends to nothing, which is not something
`ensure_serving` can be asked for, because it answers to the set the table names.

The last two are about the answer itself rather than about this call: what a cluster says
to "who serves this shard" when it has a table to ask, and what it says when the table will
not answer.  That answer is what a recovery reads - it is how a side that comes back learns
how far a move got - so where it comes from is worth pinning here, next to the placement
those two tests keep apart from it.
"""

import os

import pytest

from _ports import allocate_port
from oxidedb.launcher import ClusterConfig, NodeClusterView, Peer, block_width
from oxidedb.metadata.service import RoutingTable, ShardPlacement
from oxidedb.raft.shard_server import ShardedRaftCluster, ShardServer
from oxidedb.raft.state_machine import MVCCStateMachine
from oxidedb.raft.storage import EngineRaftStorage
from oxidedb.shard.router import default_range_map

#: The first node's base port, for the tests that bind nothing: a group in process is a
#: log and a state machine, and its port is a number nobody listens on.
BASE = 40000

SHARD = 0
A_SHARD_NOBODY_STARTED_WITH = 7


def _config(node_id: int, bases) -> ClusterConfig:
    """``node_id`` as one node of a cluster whose nodes' shard 0 is at ``bases``."""
    peers = tuple(Peer(index + 1, "127.0.0.1", base)
                  for index, base in enumerate(bases) if index + 1 != node_id)
    return ClusterConfig(node_id=node_id, port=bases[node_id - 1], peers=peers, num_shards=1)


def _nodes_a_block_apart(total_nodes: int = 3):
    """Ports worked out the way ``tests/_ports.py`` spaces them, and bound by nobody."""
    return [BASE + index * block_width() for index in range(total_nodes)]


def _serve(view_config: ClusterConfig, network) -> ShardServer:
    """The server this node would run: bound to ports if ``network``, else in process."""
    server = ShardServer(view_config.node_id, view_config.num_shards,
                         len(view_config.node_ids()))
    server.set_range_map(default_range_map(view_config.num_shards))
    # Started the way the launcher starts a node: no placement, so every node serves
    # every shard.  That is the state a process comes back in, and the state the
    # tables these tests are told about disagree with.
    if network:
        server.start_shards_network(state_machine_factory=MVCCStateMachine,
                                    peer_addresses=view_config.base_addresses())
    else:
        server.start_shards(state_machine_factory=MVCCStateMachine)
    return server


@pytest.fixture
def a_node():
    """A node built in process, closed when the test is over."""
    yield from _nodes(lambda: (_nodes_a_block_apart(), False))


@pytest.fixture
def a_node_that_binds_ports():
    """A node whose groups are bound to ports, as they are when the node is a process."""
    def ports():
        return [allocate_port(span=block_width()) for _ in range(3)], True

    yield from _nodes(ports)


def _nodes(bases_and_network):
    """Yield a builder for one node's view and server, and close what it built after."""
    built = []

    def build(node_id: int = 1):
        bases, network = bases_and_network()
        config = _config(node_id, bases)
        view = NodeClusterView(config, default_range_map(config.num_shards))
        server = _serve(config, network)
        view.serve(server)
        built.append(server)
        return view, server

    yield build
    for server in built:
        server.shutdown()


def test_a_group_with_the_wrong_members_is_closed_and_built_again(a_node):
    """The case a process has that the cluster object does not: a group that is already
    here, whose members are not the set the table now names.

    It is built again rather than re-pointed, so the object is a new one - and a
    ``MemoryRaftNode`` is why: it takes its peers once and keeps them.
    """
    view, server = a_node()
    started_with = server.get_shard_node(SHARD)
    assert server.shard_replica_ids(SHARD) == [1, 2, 3]

    view.ensure_serving(SHARD, [1, 2])

    assert server.get_shard_node(SHARD) is not started_with
    assert server.shard_replica_ids(SHARD) == [1, 2]


def test_a_group_that_already_has_those_members_is_left_alone(a_node):
    """The publisher's own answer, which this must not disturb: a node that serves a
    shard with every node of the cluster is asked for that same set and keeps its group."""
    view, server = a_node()
    started_with = server.get_shard_node(SHARD)

    view.ensure_serving(SHARD, [1, 2, 3])

    assert server.get_shard_node(SHARD) is started_with


def test_calling_it_twice_leaves_what_calling_it_once_left(a_node):
    """Idempotence, which is not decoration: a recovery that came back twice calls this
    twice, and the second call must not close and rebuild what the first one built."""
    view, server = a_node()
    assert view.ensure_serving(SHARD, [1, 2]) == [1], "the group it had was closed"
    built = server.get_shard_node(SHARD)

    assert view.ensure_serving(SHARD, [1, 2]) == []

    assert server.get_shard_node(SHARD) is built
    assert server.shard_replica_ids(SHARD) == [1, 2]


def test_a_shard_this_node_is_not_one_of_is_closed(a_node):
    """The other direction: a move that took the shard elsewhere leaves this node holding
    a group it is no longer a member of, and a group that no longer owns the range has to
    stop answering clients that still hold the old table."""
    view, server = a_node(node_id=3)
    assert server.get_shard_node(SHARD) is not None

    assert view.ensure_serving(SHARD, [1, 2]) == [3], "this node closed its group"

    assert server.get_shard_node(SHARD) is None

    assert view.ensure_serving(SHARD, [1, 2]) == [], "and there is nothing left to close"
    assert server.get_shard_node(SHARD) is None


def test_a_shard_this_node_was_not_started_with_is_built(a_node):
    """A split's new shard, which every node has to build before the split can be
    finished: this node was started with one shard, and the shard it is asked for is the
    seventh - with members this node was not started with either."""
    view, server = a_node()
    assert server.get_shard_node(A_SHARD_NOBODY_STARTED_WITH) is None

    view.ensure_serving(A_SHARD_NOBODY_STARTED_WITH, [1, 3])

    assert server.shard_replica_ids(A_SHARD_NOBODY_STARTED_WITH) == [1, 3]
    # In process there is no port to bind, so there is no address to publish either.
    assert server.shard_address(A_SHARD_NOBODY_STARTED_WITH) is None


def test_a_group_built_again_comes_back_on_the_same_port(a_node_that_binds_ports):
    """What the in-process tests cannot say, and the reason the close comes first.

    In a process a group is a port: ``add_shard`` binds it again, and grpc raises out of
    ``add_insecure_port`` for a port that is still held rather than reporting it.  So the
    address the routing table will be handed has to be the one the rebuilt group bound.
    """
    view, server = a_node_that_binds_ports()
    bound = server.shard_address(SHARD)
    assert bound is not None

    view.ensure_serving(SHARD, [1, 2])

    assert server.shard_address(SHARD) == bound
    assert server.shard_replica_ids(SHARD) == [1, 2]


def test_a_group_this_node_closed_is_no_longer_answered_for(a_node):
    """What the view says after a close, for the publisher that asks it.

    ``shard_ids`` still names the shard and that is not a bug: the range map is what
    this node routes by and what it reads its pending-split notes by, so it is the
    shard's own note that a restart finds.  The other two answers are the ones the
    table is written from, and after the close there is no group here to serve the
    shard and no address here to send a client to.
    """
    view, server = a_node(node_id=3)
    assert view.shard_replica_ids(SHARD) == [1, 2, 3]
    assert view.shard_addresses(SHARD)

    view.ensure_serving(SHARD, [1, 2])

    assert view.shard_ids() == [SHARD]
    assert view.shard_replica_ids(SHARD) == []
    assert view.shard_addresses(SHARD) == {}


def test_a_shard_this_node_was_not_started_with_is_claimed_only_once_it_is_built(a_node):
    """The same two answers before a group exists, which is what keeps a split's new
    shard from being published before anybody has built it: the range map has it - it
    came from the table - and this node does not, so there is nothing to say about it
    until ``ensure_serving`` builds one.
    """
    view, server = a_node()
    assert view.shard_ids() == [SHARD]

    assert view.shard_replica_ids(A_SHARD_NOBODY_STARTED_WITH) == []
    assert view.shard_addresses(A_SHARD_NOBODY_STARTED_WITH) == {}

    view.ensure_serving(A_SHARD_NOBODY_STARTED_WITH, [1, 2])

    assert view.shard_replica_ids(A_SHARD_NOBODY_STARTED_WITH) == [1, 2]

# -- the cluster's own implementation --------------------------------------------


def _in_process_cluster(tmp_path, num_nodes=3, num_shards=1):
    """A cluster whose nodes are all started serving every shard, with durable storage.

    Storage because half of what the cluster's close does is a rename: the directory a
    retired group's rows are in is put aside under an orphan name rather than deleted,
    and that is a fact about a file rather than about a group.
    """
    cluster = ShardedRaftCluster(num_nodes=num_nodes, num_shards=num_shards)
    cluster.start(
        state_machine_factory=lambda: MVCCStateMachine(),
        storage_factory=lambda node_id, shard_id: EngineRaftStorage(
            data_dir=str(tmp_path / f"shard{shard_id}_node{node_id}")),
        lock_cleaner_interval=None,
    )
    return cluster


def _holders(cluster, shard_id):
    """The nodes of ``cluster`` holding a group for ``shard_id`` right now."""
    return [node_id for node_id in sorted(cluster._shard_servers)
            if cluster.get_shard_server(node_id).get_shard_node(shard_id) is not None]


def test_the_cluster_closes_the_groups_of_the_nodes_a_placement_leaves_out(tmp_path):
    """The other direction on the cluster side, which is how a move ends.

    The nodes in the set keep the groups they had - the same objects, not built again -
    and the ones outside it are closed, which on this side is a group, its port and its
    storage going together: the directory is put aside under a name nothing reads, for
    the reason the whole of that path exists - a leaked directory costs disk, and a lost
    range costs the data.
    """
    cluster = _in_process_cluster(tmp_path)
    try:
        started_with = {node_id: cluster.get_shard_server(node_id).get_shard_node(0)
                        for node_id in (1, 2, 3)}
        assert all(started_with.values()), "every node was started with the shard"

        assert cluster.ensure_serving(0, [1, 2]) == [3]

        assert cluster.get_shard_server(3).get_shard_node(0) is None
        for node_id in (1, 2):
            assert cluster.get_shard_server(node_id).get_shard_node(0) is started_with[node_id], \
                "a node that was already serving it keeps the group it had"
        orphans = cluster.orphan_dirs()
        assert len(orphans) == 1, "one directory aside"
        assert os.path.basename(orphans[0]).startswith("orphan-shard-0-")
        assert not os.path.isdir(str(tmp_path / "shard0_node3")), \
            "the name the shard was stored under is free"
    finally:
        cluster.shutdown()


def test_the_cluster_builds_a_shard_it_was_not_started_with_on_every_node(tmp_path):
    """A split's new shard, which is the reason this call exists at all.

    A restarted cluster builds the shards it was told to serve and nothing else, so the
    shard the note names is one nobody holds until this builds it - and the whole cluster
    is the set, which is what a shard created by a split gets.
    """
    cluster = _in_process_cluster(tmp_path)
    try:
        assert _holders(cluster, A_SHARD_NOBODY_STARTED_WITH) == []

        assert cluster.ensure_serving(A_SHARD_NOBODY_STARTED_WITH, [1, 2, 3]) == []

        assert _holders(cluster, A_SHARD_NOBODY_STARTED_WITH) == [1, 2, 3]
        for node_id in (1, 2, 3):
            server = cluster.get_shard_server(node_id)
            assert server.shard_replica_ids(A_SHARD_NOBODY_STARTED_WITH) == [1, 2, 3]
        assert cluster._num_shards == A_SHARD_NOBODY_STARTED_WITH + 1, \
            "the cluster counts a shard it now holds"
    finally:
        cluster.shutdown()


def test_the_cluster_builds_a_group_with_the_members_it_is_given(tmp_path):
    """A move's target set: the nodes are the caller's, and the group built on them has
    them and no others as its members, which is what lets the new group elect and
    replicate on its own before anything is copied into it."""
    cluster = _in_process_cluster(tmp_path, num_nodes=5)
    try:
        assert _holders(cluster, A_SHARD_NOBODY_STARTED_WITH) == []

        assert cluster.ensure_serving(A_SHARD_NOBODY_STARTED_WITH, [4, 5]) == []

        assert _holders(cluster, A_SHARD_NOBODY_STARTED_WITH) == [4, 5]
        server = cluster.get_shard_server(4)
        assert server.shard_replica_ids(A_SHARD_NOBODY_STARTED_WITH) == [4, 5]
    finally:
        cluster.shutdown()


def test_the_cluster_closing_twice_is_closing_once(tmp_path):
    """Idempotence on the cluster side, and the same answer the view gives: a recovery
    that came back twice closes nothing the second time, and the empty list is how it
    knows the first call was the one that did it."""
    cluster = _in_process_cluster(tmp_path)
    try:
        assert cluster.ensure_serving(0, [1, 2]) == [3]
        orphans = cluster.orphan_dirs()

        assert cluster.ensure_serving(0, [1, 2]) == []
        assert cluster.orphan_dirs() == orphans, "and nothing else was put aside"
    finally:
        cluster.shutdown()


def test_closing_a_set_takes_those_groups_and_mends_to_nothing(tmp_path):
    """A move the table refused, which is the caller this call was drawn for.

    The group built to receive the rows has to go, and nothing else may: a refusal does not
    say what the routing table names instead, so mending to a set - which is what
    ``ensure_serving`` does, over the set the table names - would take down the groups of a
    set the table does name, on behalf of a move that has just failed.  What pins the
    difference is one set given to the two calls: this one closes exactly the nodes it was
    handed and leaves the rest holding the groups they had, where ``ensure_serving`` would
    have closed every holder outside its set.
    """
    cluster = _in_process_cluster(tmp_path)
    try:
        cluster.ensure_serving(A_SHARD_NOBODY_STARTED_WITH, [1, 2, 3])
        started_with = {node_id: cluster.get_shard_server(node_id).get_shard_node(
                        A_SHARD_NOBODY_STARTED_WITH) for node_id in (1, 2, 3)}
        placement = cluster.serving_nodes(A_SHARD_NOBODY_STARTED_WITH)

        assert cluster.close_group_on(A_SHARD_NOBODY_STARTED_WITH, [2]) == [2]

        assert _holders(cluster, A_SHARD_NOBODY_STARTED_WITH) == [1, 3], \
            "exactly the nodes named, and no others"
        for node_id in (1, 3):
            assert cluster.get_shard_server(node_id).get_shard_node(
                A_SHARD_NOBODY_STARTED_WITH) is started_with[node_id], \
                "a node outside the set keeps the group it had"
        assert cluster.serving_nodes(A_SHARD_NOBODY_STARTED_WITH) == placement, \
            "the placement is the routing table's, and this call does not write it"
        assert cluster.close_group_on(A_SHARD_NOBODY_STARTED_WITH, [2]) == [], \
            "a group that is already closed is an answer rather than an error"
        orphans = cluster.orphan_dirs()
        assert len(orphans) == 1, "one directory aside, for the group that went"
        assert os.path.basename(orphans[0]).startswith(
            f"orphan-shard-{A_SHARD_NOBODY_STARTED_WITH}-")
    finally:
        cluster.shutdown()

def test_the_placement_moves_on_its_own_call_and_not_on_this_one(tmp_path):
    """Where a committed move's two halves live, and why they are two calls.

    ``ensure_serving`` changes the groups this cluster holds; it must not change what this
    cluster answers to "who serves this shard", because the routing table moves a step
    earlier: between the two, the table names the new set while the group it left is still
    up and still answering, which is the window a client that cached the old table finishes
    its read in.  So the two are made to disagree here - a placement written by hand, then
    a call that builds and closes groups - and the placement is asserted to be untouched.

    ``_placed_shards`` is written by hand because the only other writer is a committed
    move, and a test that drove one would be testing the move rather than this boundary.
    """
    cluster = _in_process_cluster(tmp_path)
    try:
        cluster._placed_shards[0] = [1]

        assert cluster.ensure_serving(0, [1, 2, 3]) == [], "every node already held it"
        assert cluster.serving_nodes(0) == [1], \
            "building and closing groups is not the routing table's placement"

        cluster.apply_move_locally(0, [2, 3])
        assert cluster.serving_nodes(0) == [2, 3]
        assert _holders(cluster, 0) == [1, 2, 3], \
            "the placement moved and the groups did not, which is the order a move needs"

        cluster.apply_move_locally(0, [2, 3])
        assert cluster.serving_nodes(0) == [2, 3], "writing it twice is writing it once"
    finally:
        cluster.shutdown()


class _ATable:
    """A routing table that answers with what it was handed.

    What a cluster reads when it has one: the two tests below are about the two answers a
    table gives that its own placement cannot - a set that is not this cluster's, and the
    refusal of a group with no leader to ask.
    """

    def __init__(self, placements=None, complaint=None):
        self._placements = dict(placements or {})
        self._complaint = complaint

    def table(self, refresh=False):
        if self._complaint is not None:
            raise RuntimeError(self._complaint)
        return RoutingTable(version=1, shards=dict(self._placements))


def test_the_table_is_what_this_cluster_answers_for_a_shard(tmp_path):
    """``serving_nodes`` is the table's answer, not the placement this cluster holds.

    A cluster keeps a placement of its own because it is the thing that publishes one, and
    the two are meant to disagree: while a move is in flight, and for as long as a client
    can be routed by the copy of the table it cached.  What a recovery reads has to be the
    table's, because the question is how far somebody else's move got - and this cluster's
    own placement is one step behind that by construction.
    """
    cluster = _in_process_cluster(tmp_path)
    try:
        cluster._placed_shards[0] = [1]
        cluster._metadata_client = _ATable(
            {0: ShardPlacement(0, b"", b"\xff", nodes=[2, 3])})

        assert cluster.serving_nodes(0) == [2, 3], \
            "the table's set, not the placement this cluster would publish"
        assert cluster.serving_nodes(A_SHARD_NOBODY_STARTED_WITH) is None, \
            "a shard the table does not name is None, not every node"
    finally:
        cluster.shutdown()


def test_a_table_that_cannot_be_read_is_not_an_answer(tmp_path):
    """The third thing: not a set and not None - nothing was read, so nothing is known.

    A recovery reads this answer to find out which half of a move is left, and a read that
    did not happen is not an answer about the shard at all.  So it raises, and the recovery
    is what decides to wait for it or to leave the shard frozen.
    """
    cluster = _in_process_cluster(tmp_path)
    try:
        cluster._metadata_client = _ATable(complaint="the metadata group has no leader")

        with pytest.raises(RuntimeError, match="no leader"):
            cluster.serving_nodes(0)
    finally:
        cluster.shutdown()
