"""The ports of one node: three fixed segments, and what depends on that.

A node binds its shards at ``base + shard_id``, the routing table's group at
``base + SHARD_SEGMENT``, and the timestamp group at the port above that.  Three segments
rather than "the groups above the last shard", because the last shard is not a number a
client knows: a split makes one, and a group port has to be a port no shard will want.

Three things rest on the layout, and each is a test here rather than a comment:

* the group ports follow from one address, so a client finds them without being told how
  the cluster was built - which is what makes the CLI's old ``--shards`` unnecessary;
* a split's new shard has a port that is not a group's, which is what the segments are
  for: with the old arithmetic the first shard a node's split created wanted the port the
  routing table's group was holding, and binding a port twice raises;
* a node's block is exactly the ports it can bind, so two nodes a block apart share
  nothing - which is how ``tests/_ports.py`` and ``tests/_cluster.py`` space them.
"""

import pytest

from oxidedb.launcher import DEFAULT_NUM_SHARDS, ClusterConfig, block_width, ports_for
from oxidedb.raft.shard_server import SHARD_SEGMENT

#: A base port the tests' allocator can hand out, used here because nothing is bound:
#: the arithmetic is what is under test, not a listener.
BASE = 40000


def test_a_shard_a_split_creates_has_a_port_of_its_own():
    """The collision the three segments exist to remove.

    A split's first shard is numbered for the node that serves it, so for a node serving
    two shards it is shard 2 - and shard 2 at a stride of a hundred ports used to be
    exactly where the routing table's group listens.
    """
    ports = ports_for(BASE)

    assert ports.shard(DEFAULT_NUM_SHARDS) == BASE + DEFAULT_NUM_SHARDS
    assert ports.shard(DEFAULT_NUM_SHARDS) < ports.metadata


def test_the_group_ports_sit_above_every_shard_a_node_can_serve():
    """``SHARD_SEGMENT`` is the bound, and the two groups are the two ports above it."""
    ports = ports_for(BASE)

    assert ports.shard(SHARD_SEGMENT - 1) == BASE + SHARD_SEGMENT - 1
    assert ports.metadata == BASE + SHARD_SEGMENT
    assert ports.tso == ports.metadata + 1


def test_a_node_is_refused_more_shards_than_it_has_ports_for():
    """Refused where a node is configured, and not met as a bind that raised."""
    ClusterConfig(node_id=1, port=BASE, num_shards=SHARD_SEGMENT).validate()

    with pytest.raises(ValueError, match="at most"):
        ClusterConfig(node_id=1, port=BASE, num_shards=SHARD_SEGMENT + 1).validate()


def test_a_nodes_block_is_exactly_the_ports_it_binds():
    """What the test helpers' spacing is for: the next node starts where this one ends."""
    ports = ports_for(BASE)
    next_node = ports_for(BASE + block_width())

    assert ports.highest - ports.base + 1 == ports.width == block_width()
    assert next_node.base == ports.highest + 1
