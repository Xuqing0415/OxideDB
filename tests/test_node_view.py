"""A node in a process is a recovery view: every call the protocol names, taking what it says.

``NodeClusterView`` is what a recovery is handed on the process side of the wire, and it is
the side where the gate is worth the most: a cluster object is built by the tests
themselves, while a node is a process that comes back from a crash and runs the one code
path that only runs after one.  So the same two questions ``tests/test_cluster_view.py``
asks of a cluster are asked here of a node, through the same helpers.

What is built here has opened no ports and started no groups, and it opens no channel
either: how a node reaches the rest of the cluster is built when something first asks for
it, so a view that answers only about itself - which is what a recovery that finds no notes
does - never dials anybody.
"""

from _views import calls, mismatches
from oxidedb.launcher import ClusterConfig, NodeClusterView, Peer, block_width
from oxidedb.raft.recovery_view import RecoveryView
from oxidedb.shard.router import default_range_map

#: The first node's base port, for views that bind nothing: what these tests ask is how the
#: calls are made and what the arithmetic answers, and neither needs a socket.
BASE = 41000


def _config(node_id: int = 1, peers: int = 0) -> ClusterConfig:
    """One node of a cluster whose nodes' base ports are a block apart."""
    others = tuple(Peer(other, "127.0.0.1", BASE + (other - 1) * block_width())
                   for other in range(2, peers + 2))
    return ClusterConfig(node_id=node_id, port=BASE + (node_id - 1) * block_width(),
                         num_shards=2, peers=others)


def _view() -> NodeClusterView:
    config = _config()
    return NodeClusterView(config, default_range_map(config.num_shards))


def test_a_node_answers_every_call_a_view_names():
    assert calls(), "the protocol names no calls at all"
    assert isinstance(_view(), RecoveryView)


def test_a_node_takes_each_call_the_way_the_protocol_declares_it():
    """The half ``isinstance`` cannot see, asked of the side that crosses a process."""
    wrong = mismatches(NodeClusterView)
    assert not wrong, (f"these calls take something other than what the protocol declares, "
                       f"protocol first: {wrong}")


def test_a_peer_is_addressed_by_the_arithmetic_both_sides_share():
    """A peer's address is worked out, and a node the configuration does not place is not.

    The one thing this side cannot do the way a cluster does is ask a peer where it
    listens, so the answer has to be the arithmetic both sides derive a shard port with -
    and a node id the configuration names nowhere has no address to answer with at all.
    """
    view = NodeClusterView(_config(peers=1), default_range_map(2))

    assert view.addresses_on(1, [1, 2, 9]) == {1: f"127.0.0.1:{BASE + 1}",
                                               2: f"127.0.0.1:{BASE + block_width() + 1}"}
