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
    """Wait until exactly one node leads and every other node follows it.

    A node that started an election just before the winner did stays a CANDIDATE
    until the winner's first heartbeat reaches it, so asserting on a single
    instant races with that handover.  A cluster that never converges still
    fails here.
    """
    def settled():
        leaders = [node_id for node_id, node in cluster._nodes.items()
                   if node.state == NodeState.LEADER]
        if len(leaders) != 1:
            return None
        stragglers = [node for node_id, node in cluster._nodes.items()
                      if node_id != leaders[0] and node.state != NodeState.FOLLOWER]
        return None if stragglers else leaders[0]

    return wait_until(settled, timeout=timeout,
                      message="cluster did not settle on a single leader")


def wait_for_keys_leader(cluster, keys, timeout: float = DEFAULT_TIMEOUT):
    """Wait until every key in ``keys`` has a shard whose leader is up."""
    keys = list(keys)
    return wait_until(lambda: all(cluster.get_leader_for_key(key) for key in keys),
                      timeout=timeout,
                      message=f"no shard leader for every key in {keys!r}")


def wait_for_tso_client(tso_cluster, timeout: float = DEFAULT_TIMEOUT):
    """Wait until the TSO Raft group has a leader, then return a client for it."""
    return wait_until(tso_cluster.get_client, timeout=timeout,
                      message="TSO cluster elected no leader")
