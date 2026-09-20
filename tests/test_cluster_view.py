"""A cluster is a recovery view: every call the protocol names, taking what it says.

``isinstance`` against a ``runtime_checkable`` protocol asks whether the names are there
and nothing else.  A class whose methods have the right names and the wrong arguments
passes it, and fails the first time a recovery calls one - in the one code path
that only runs after a crash.  So the gate is asked here twice: once as ``isinstance``,
and once by comparing what each call takes with what the protocol says it takes - see
``tests/_views`` for what that comparison is and what it leaves out.

That second half is not decoration: on its first run it found one disagreement, at
``leader_client_for_nodes``, whose set of nodes was called ``node_ids`` on this side and
``nodes`` in the protocol.  Nothing calls that method by keyword today, so nothing was
broken; a caller that did would have been, and the naming of a parameter is exactly the
part of a signature that ``isinstance`` cannot see.

What is built here has opened no ports and started no groups.  Which calls exist, and what
each takes, is a fact about the class, and a cluster that has not been started is still the
object a recovery is handed.
"""

from _views import calls, mismatches
from oxidedb.raft.recovery_view import RecoveryView
from oxidedb.raft.shard_server import ShardedRaftCluster


def _cluster() -> ShardedRaftCluster:
    return ShardedRaftCluster(num_nodes=3, num_shards=1)


def test_a_cluster_answers_every_call_a_view_names():
    assert calls(), "the protocol names no calls at all"
    assert isinstance(_cluster(), RecoveryView)


def test_a_cluster_takes_each_call_the_way_the_protocol_declares_it():
    """The half ``isinstance`` cannot see, and the half a keyword caller trips on."""
    wrong = mismatches(ShardedRaftCluster)
    assert not wrong, (f"these calls take something other than what the protocol declares, "
                       f"protocol first: {wrong}")
