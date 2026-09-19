"""``NodeClusterView.ensure_serving``: what one node holds, with no cluster around it.

``docs/recovery.md`` gives the recovery one call for both directions of a shard's
membership - build my member of the group, or close it - so that a node coming back can
agree with the routing table about who serves a shard.  These are that call's own tests:
which case does what, and that calling it twice is calling it once.

They are view-level on purpose: what is pinned here is the decision - built, left alone,
closed, built again - and the decision is the same one a process makes.  What a process
adds is that a group is also a bound port and a storage directory, and there is one test
below of the half of that a rebuild depends on: the port coming back after the close.
The storage of a shard this node stops serving is not covered - nothing sets a directory
aside yet.

Two of the tests are about the other half of a close, which is not a decision but a
reading: what the view says it holds, for the publisher that asks it.  They are here
because that is where a closed group went wrong - the range map still named the shard
and the shard server still named this node, so a node that had just stopped serving a
shard would have been written into the table as a replica of it.
"""

import pytest

from _ports import allocate_port
from oxidedb.launcher import ClusterConfig, NodeClusterView, Peer, block_width
from oxidedb.raft.shard_server import ShardServer
from oxidedb.raft.state_machine import MVCCStateMachine
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
    view.ensure_serving(SHARD, [1, 2])
    built = server.get_shard_node(SHARD)

    view.ensure_serving(SHARD, [1, 2])

    assert server.get_shard_node(SHARD) is built
    assert server.shard_replica_ids(SHARD) == [1, 2]


def test_a_shard_this_node_is_not_one_of_is_closed(a_node):
    """The other direction: a move that took the shard elsewhere leaves this node holding
    a group it is no longer a member of, and a group that no longer owns the range has to
    stop answering clients that still hold the old table."""
    view, server = a_node(node_id=3)
    assert server.get_shard_node(SHARD) is not None

    view.ensure_serving(SHARD, [1, 2])

    assert server.get_shard_node(SHARD) is None

    view.ensure_serving(SHARD, [1, 2])
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
