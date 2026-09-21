"""Bounded waits shared by the network tests.

Those tests used to ``time.sleep(N)`` and then assert that an election had
finished.  That is a race: the full suite starts dozens of local gRPC servers,
and on a loaded machine settling takes longer than the sleep, so the test fails
for a reason that has nothing to do with the code under test.  Waiting for the
observable condition is both faster in the common case and honest about what the
test actually depends on.
"""

import time
from typing import Any, Callable

from oxidedb.raft.node import NodeState

DEFAULT_TIMEOUT = 20.0


def wait_until(predicate: Callable[[], Any], timeout: float = DEFAULT_TIMEOUT,
               interval: float = 0.05, message: str = "condition never became true"):
    """Return the first truthy result of ``predicate``, or fail after ``timeout``."""
    deadline = time.time() + timeout
    while True:
        result = predicate()
        if result:
            return result
        if time.time() >= deadline:
            raise AssertionError(f"{message} (gave up after {timeout}s)")
        time.sleep(interval)


def wait_for_leader(cluster, timeout: float = DEFAULT_TIMEOUT) -> int:
    """Wait until some node claims leadership and return its node id."""
    return wait_until(cluster.get_leader, timeout=timeout,
                      message="no leader was elected")


def wait_for_single_leader(cluster, timeout: float = DEFAULT_TIMEOUT) -> int:
    """Wait until one node leads, everyone else follows, and the leader knows its log.

    A node that started an election just before the winner did stays a CANDIDATE
    until the winner's first heartbeat reaches it, so asserting on a single
    instant races with that handover.  A cluster that never converges still
    fails here.

    Winning is not the same as being able to read, which is the second half of
    this wait: the entries a new leader finds in its log were appended by leaders
    of earlier terms, and which of them committed is something only a majority
    acknowledging an entry of *this* term reveals - its own no-op, appended on
    election.  Until that lands the state machine can be missing rows the group
    has long since committed, and a caller that reads it rather than merely
    appending to it is asking about a leader this wait has not reached yet.  A
    caller that only needs somebody to append to has ``wait_for_leader``.
    """
    def settled():
        leaders = [node_id for node_id, node in cluster._nodes.items()
                   if node.state == NodeState.LEADER]
        if len(leaders) != 1:
            return None
        stragglers = [node for node_id, node in cluster._nodes.items()
                      if node_id != leaders[0] and node.state != NodeState.FOLLOWER]
        if stragglers:
            return None
        leader = cluster._nodes[leaders[0]]
        return leaders[0] if leader.has_committed_in_its_own_term() else None

    return wait_until(settled, timeout=timeout,
                      message="cluster did not settle on a single leader")


def wait_for_keys_leader(cluster, keys, timeout: float = DEFAULT_TIMEOUT):
    """Wait until every key in ``keys`` has a shard whose leader is up."""
    keys = list(keys)
    return wait_until(lambda: all(cluster.get_leader_for_key(key) for key in keys),
                      timeout=timeout,
                      message=f"no shard leader for every key in {keys!r}")


def wait_for_leader_of_key(cluster, key, timeout: float = DEFAULT_TIMEOUT):
    """Wait until some node leads the shard that owns ``key``, and return it.

    The leader read is a snapshot: :meth:`get_leader_for_key` answers ``None``
    whenever no node's state is ``LEADER`` for that shard, which is true before the
    first election and again while a leadership change is in flight.  A caller that
    wants the leader itself waits for it here rather than reading a value some
    earlier wait happened to establish - the two are not the same, and the run that
    put this function here is the one that read ``None`` after such a wait.
    """
    return wait_until(lambda: cluster.get_leader_for_key(key), timeout=timeout,
                      message=f"no shard leader for {key!r}")


def wait_for_shard_leaders(cluster, shard_ids, timeout: float = DEFAULT_TIMEOUT):
    """Wait until every shard in ``shard_ids`` has a leader.

    A key-based wait cannot say this: it is the groups themselves a test is asking
    about when it has no key in mind, and a sharded cluster elects once per shard.
    """
    shard_ids = list(shard_ids)
    return wait_until(lambda: all(cluster.shard_leader(shard_id) for shard_id in shard_ids),
                      timeout=timeout,
                      message=f"no leader for every shard in {shard_ids!r}")


def wait_for_replication(leader, followers, timeout: float = DEFAULT_TIMEOUT):
    """Wait until every node in ``followers`` has applied what ``leader`` committed.

    The condition is ``last_applied`` against the leader commit index, not log length: a
    follower that holds an entry it has not applied cannot answer a read with it, so a test
    that reads a replica is waiting for the apply and not for the append.
    """
    def applied():
        commit_index = leader.commit_index
        return commit_index if all(node.last_applied >= commit_index
                                   for node in followers) else None

    return wait_until(applied, timeout=timeout,
                      message="a follower did not apply what the leader committed")


def wait_for_tso_client(tso_cluster, timeout: float = DEFAULT_TIMEOUT):
    """Wait until the TSO Raft group has a leader, then return a client for it."""
    return wait_until(tso_cluster.get_client, timeout=timeout,
                      message="TSO cluster elected no leader")


def wait_for_metadata_client(metadata_cluster, timeout: float = DEFAULT_TIMEOUT):
    """Wait until the metadata Raft group has a leader, then return a client."""
    wait_until(metadata_cluster.get_leader_node, timeout=timeout,
               message="metadata cluster elected no leader")
    return metadata_cluster.get_client()
