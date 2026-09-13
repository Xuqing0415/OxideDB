"""A read that trips over a lock has to be able to finish.

A snapshot read used to raise when a lock older than the snapshot was in the way: the
lock may belong to a transaction that already committed, and its version is part of
the snapshot the reader is entitled to, but the lock itself does not say which way it
went.  Percolator's answer is that the lock is not the decision - the primary key's
write record is - so a reader can ask that question and finish the read: roll the lock
forward if the transaction committed, clear it if it did not, and only give up while
the transaction is still live.

The stranded lock these tests use is the one a coordinator leaves behind when it dies
between the primary commit and the secondary commits, which is the case the lock
protocol exists for.
"""

import time

import pytest

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_tso_client, wait_until
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType, ErrorCode
from oxidedb.shard.router import locate
from oxidedb.storage.mvcc import LockStatus
from oxidedb.tso.tso import TSOCluster
from oxidedb.transaction.coordinator import TransactionCoordinator
from oxidedb.transaction.smart_client import SmartClient

KEY_A = b"key0"      # first byte 0x6b -> shard 0
KEY_B = b"\x80key1"  # first byte 0x80 -> shard 1


def _two_shard_cluster():
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())

    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(num_shards=2),
        # The cleaner resolves these locks on its own after its TTL; these tests are
        # about the read path doing it, so it is off.  test_lock_cleaner_wiring
        # covers the cleaner.
        lock_cleaner_interval=None,
    )
    return tso_cluster, shard_cluster


def _die_after_the_primary_commit(shard_cluster, tso_client, coordinator, primary, secondary):
    """Prewrite both keys, commit only the primary, and walk away.

    Returns the timestamp the primary committed at.  This is the state a coordinator
    leaves behind when it dies between the primary commit and the secondary commits:
    the transaction is decided, and the locks on the secondary keys are stranded.
    """
    writer, start_ts = coordinator.begin()
    coordinator.add_write(writer, primary, b"a1")
    coordinator.add_write(writer, secondary, b"b1")
    assert coordinator.prewrite(writer), "the prewrite has to lock both keys"

    commit_ts = tso_client.get_timestamp()
    leader = shard_cluster.get_leader_for_key(primary)[1]
    command = leader._state_machine.serialize_command(
        CommandType.COMMIT, key=primary, start_ts=start_ts, commit_ts=commit_ts)
    assert leader.propose(command).success, "the primary commit has to succeed"

    stranded = shard_cluster.get_leader_for_key(secondary)[1]._state_machine.get_lock_status(secondary)
    assert stranded is not None, "the secondary key has to still be locked"
    return commit_ts


def _lock_status(cluster, key):
    return cluster.get_leader_for_key(key)[1]._state_machine.get_lock_status(key)


def test_a_snapshot_read_rolls_a_committed_lock_forward():
    tso_cluster, shard_cluster = _two_shard_cluster()
    assert locate(shard_cluster._range_map, KEY_A) != locate(shard_cluster._range_map, KEY_B)

    wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
    tso_client = wait_for_tso_client(tso_cluster)
    coordinator = TransactionCoordinator(tso_client, shard_cluster)

    commit_ts = _die_after_the_primary_commit(
        shard_cluster, tso_client, coordinator, KEY_A, KEY_B)

    reader, reader_start = coordinator.begin()
    assert commit_ts < reader_start, "the commit has to be inside this snapshot"
    assert coordinator.read(reader, KEY_A) == b"a1"

    # This is the read that used to raise: the lock is older than the snapshot, so
    # the version it holds belongs in the answer.  The primary says the transaction
    # committed, so the lock is rolled forward and the read is retried.
    assert coordinator.read(reader, KEY_B) == b"b1"
    assert _lock_status(shard_cluster, KEY_B) is None, "the lock has to be gone"
    assert shard_cluster.get_leader_for_key(KEY_B)[1].get(KEY_B).value == b"b1"

    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Committed lock roll-forward test passed!")


def test_a_snapshot_read_clears_a_lock_that_never_committed():
    tso_cluster, shard_cluster = _two_shard_cluster()
    wait_for_keys_leader(shard_cluster, [KEY_A])
    tso_client = wait_for_tso_client(tso_cluster)

    # A short TTL: the point is the boundary between "may yet commit" and "nobody is
    # coming", and five seconds of sleeping would only make the suite slower.
    coordinator = TransactionCoordinator(tso_client, shard_cluster, lock_ttl=0.1)

    put_ts = tso_client.get_timestamp()
    leader = shard_cluster.get_leader_for_key(KEY_A)[1]
    command = leader._state_machine.serialize_command(
        CommandType.SET, key=KEY_A, value=b"original", timestamp=put_ts)
    assert leader.propose(command).success

    dead, dead_start = coordinator.begin()
    coordinator.add_write(dead, KEY_A, b"never")
    assert coordinator.prewrite(dead), "the prewrite has to lock the key"

    reader, reader_start = coordinator.begin()
    assert dead_start < reader_start, "the lock has to be older than this snapshot"

    # Inside the TTL the transaction may still commit, so there is no answer to be
    # had and the read says so instead of blocking.
    with pytest.raises(RuntimeError, match="live transaction"):
        coordinator.read(reader, KEY_A)

    # Past it, the lock is cleared and the snapshot is answered from the version
    # that was there before it.
    def readable():
        try:
            return coordinator.read(reader, KEY_A)
        except RuntimeError:
            return None

    assert wait_until(readable, message="the abandoned lock was never resolved") == b"original"
    assert _lock_status(shard_cluster, KEY_A) is None, "the lock has to be gone"

    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Abandoned lock rollback test passed!")


def test_the_client_read_resolves_a_stranded_lock():
    """The same recovery, one layer up: this is the call a user makes."""
    tso_cluster, shard_cluster = _two_shard_cluster()
    wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
    tso_client = wait_for_tso_client(tso_cluster)
    coordinator = TransactionCoordinator(tso_client, shard_cluster)

    _die_after_the_primary_commit(shard_cluster, tso_client, coordinator, KEY_A, KEY_B)

    client = SmartClient(tso_client, shard_cluster)
    assert client.get(KEY_B) == b"b1", "a stranded lock must not surface as an error"
    assert _lock_status(shard_cluster, KEY_B) is None

    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Client lock resolution test passed!")


def test_an_expired_lock_is_still_reported_and_not_skipped():
    """The TTL is the resolver's policy; this layer only reports the obstacle.

    `MVCCStateMachine.get` used to stop treating a lock as an obstacle once it was
    five seconds old and answer with the newest applied version.  That is right when
    the transaction died, and wrong when it committed and only the secondary commit is
    missing - the newest applied version is then older than one that is already
    committed, which is exactly the state the tests above resolve.  Expiry is not a
    fact this layer can know: nothing here decided that the transaction is dead.
    """
    state_machine = MVCCStateMachine()
    command = state_machine.serialize_command(
        CommandType.SET, key=KEY_A, value=b"before", timestamp=10)
    assert state_machine.apply(command).success

    # A lock nobody resolved, long past any TTL.
    state_machine._storage.put_lock(
        KEY_A, 20, LockStatus.LOCKED, KEY_A, time.time() - 600, b"stranded")

    untimestamped = state_machine.get(KEY_A)
    assert not untimestamped.success, "an expired lock is still an obstacle"
    assert untimestamped.error_code == ErrorCode.ERR_LOCKED

    snapshot = state_machine.get(KEY_A, 30)
    assert not snapshot.success, "and it is an obstacle to an older snapshot too"
    assert snapshot.error_code == ErrorCode.ERR_LOCKED
