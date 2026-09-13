"""The cluster has to start a lock cleaner; the tests used to be the only caller.

A coordinator that dies between prewrite and commit leaves locks behind, and the read
path can only report them as locked.  Resolving one means asking the primary key's
write record what happened to the transaction and then rolling the lock forward or
back - which is what LockCleaner does, and what nothing in the library was running:
only this test module's predecessor ever constructed one, so every user of
ShardedRaftCluster ran without it.

These tests drive the cluster's own cleaner rather than constructing one, and the
second one turns it off to show the difference.  A lock that outlives its coordinator
is only resolved because the cluster started the cleaner.
"""

import time

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_until
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType, ErrorCode

ABANDONED_KEY = b"abandoned_lock"


def _lock_status(cluster, key):
    leader_info = cluster.get_leader_for_key(key)
    if leader_info is None:
        return None
    return leader_info[1]._state_machine.get_lock_status(key)


def _abandon_a_lock(cluster, key, start_ts=100):
    """Write a value, then prewrite over it and walk away: a dead coordinator's lock."""
    wait_for_keys_leader(cluster, [key])
    leader = cluster.get_leader_for_key(key)[1]

    write = leader._state_machine.serialize_command(
        CommandType.SET, key=key, value=b"original", timestamp=50)
    assert leader.propose(write).success

    prewrite = leader._state_machine.serialize_command(
        CommandType.PREWRITE, key=key, value=b"locked", start_ts=start_ts,
        primary_key=key)
    assert leader.propose(prewrite).success
    return leader


def test_the_cluster_resolves_an_abandoned_lock_by_itself():
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(),
        lock_cleaner_interval=0.05,
        lock_cleaner_ttl=0.25,
    )
    assert cluster._lock_cleaner is not None, "start_network has to start one"

    leader = _abandon_a_lock(cluster, ABANDONED_KEY)
    assert _lock_status(cluster, ABANDONED_KEY) is not None, "the prewrite left no lock"
    assert not leader.get(ABANDONED_KEY).success, "the lock is in the way until it is resolved"

    wait_until(lambda: _lock_status(cluster, ABANDONED_KEY) is None,
               message="the cluster's own lock cleaner never resolved the lock")

    result = cluster.get_leader_for_key(ABANDONED_KEY)[1].get(ABANDONED_KEY)
    assert result.success, f"the rollback should leave a readable key: {result.error_msg}"
    assert result.value == b"original", "the abandoned write must not be visible"

    cluster.shutdown()
    assert cluster._lock_cleaner is None, "shutdown has to stop the cleaner"
    print("Cluster lock cleaner test passed!")


def test_without_a_cleaner_the_lock_is_left_alone():
    """interval=None is the knob that shows the cleaner is what resolves the lock."""
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(),
        lock_cleaner_interval=None,
        lock_cleaner_ttl=0.05,
    )
    assert cluster._lock_cleaner is None, "interval=None has to mean no cleaner"

    _abandon_a_lock(cluster, ABANDONED_KEY)

    # The other test sits on the same TTL and clears the lock well inside this
    # window, so letting time pass is the assertion here, not a way to wait for a
    # race.
    time.sleep(0.3)

    assert _lock_status(cluster, ABANDONED_KEY) is not None, "nothing should have resolved it"
    result = cluster.get_leader_for_key(ABANDONED_KEY)[1].get(ABANDONED_KEY)
    assert not result.success, "a read of a locked key is still refused"
    assert result.error_code == ErrorCode.ERR_LOCKED

    cluster.shutdown()
    print("No-cleaner test passed!")
