"""Write skew: two transactions that each read what the other is about to overwrite.

Snapshot isolation gives a transaction a fixed snapshot, but it does not stop two
transactions from basing opposite decisions on that same snapshot and then writing
disjoint keys.  The standard shape is two doctors who are both on call: each checks
that the other is there, each concludes it can go home, and each writes its own key.
Neither write conflicts with the other, so Percolator's prewrite check - which only
looks at the key being written - has nothing to say, and both commit.  The rule that
somebody stays on call is broken by a commit that no serial order can explain.

The read set is what gives the second one a reason to be refused: a transaction
remembers every key it read, and at commit it checks that none of them has been
committed over since the snapshot it read them at.  That check and the primary commit
happen under one lock, because two transactions validating a moment apart could
otherwise both pass - which is the anomaly, one layer up.

Honest about what this is: it is optimistic validation, not the conflict-graph SSI
(a transaction is refused whenever its snapshot was superseded, even when no cycle
existed, so it aborts more than SSI would).  It covers the keys this coordinator
reads, not range scans, and it orders the transactions that commit through one
coordinator; two coordinators committing at the same time would need the graph.
"""

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_tso_client
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine
from oxidedb.shard.router import locate
from oxidedb.tso.tso import TSOCluster
from oxidedb.transaction.coordinator import TransactionCoordinator
from oxidedb.transaction.smart_client import SmartClient

KEY_A = b"key0"        # first byte 0x6b -> shard 0
KEY_B = b"\x80key1"   # first byte 0x80 -> shard 1
KEY_C = b"key2"        # shard 0


def _cluster():
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())

    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(num_shards=2),
        # Nothing here is abandoned, and these tests are about the commit path, so
        # the cleaner stays off.
        lock_cleaner_interval=None,
    )
    return tso_cluster, shard_cluster


def _value(cluster, key):
    return cluster.get_leader_for_key(key)[1].get(key).value


def _lock_status(cluster, key):
    return cluster.get_leader_for_key(key)[1]._state_machine.get_lock_status(key)


def _seed(cluster, tso_client, coordinator, value=b"on_call"):
    writer, _ = coordinator.begin()
    for key in (KEY_A, KEY_B):
        coordinator.add_write(writer, key, value)
    committed, _ = coordinator.commit(writer)
    assert committed, "the keys have to start out written"


def _both_doctors_go_off_call(coordinator):
    """The scenario: two transactions, one snapshot, disjoint writes.

    Returns the two commit results, so a caller can assert on either.
    """
    doctor_1, start_1 = coordinator.begin()
    doctor_2, start_2 = coordinator.begin()
    assert start_1 < start_2

    for doctor in (doctor_1, doctor_2):
        assert coordinator.read(doctor, KEY_A) == b"on_call"
        assert coordinator.read(doctor, KEY_B) == b"on_call"

    coordinator.add_write(doctor_1, KEY_A, b"off_call")
    coordinator.add_write(doctor_2, KEY_B, b"off_call")

    return doctor_1, doctor_2, coordinator.commit(doctor_1), coordinator.commit(doctor_2)


def test_the_second_doctor_cannot_go_off_call():
    tso_cluster, shard_cluster = _cluster()
    assert locate(shard_cluster._range_map, KEY_A) != locate(shard_cluster._range_map, KEY_B)

    wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
    tso_client = wait_for_tso_client(tso_cluster)
    coordinator = TransactionCoordinator(tso_client, shard_cluster)
    _seed(shard_cluster, tso_client, coordinator)

    doctor_1, doctor_2, commit_1, commit_2 = _both_doctors_go_off_call(coordinator)

    assert commit_1[0], "the first doctor's commit should succeed"
    assert not commit_2[0], "the second doctor read a snapshot the first one deleted"

    refused = coordinator.get_transaction(doctor_2)
    assert refused.read_set == {KEY_A, KEY_B}, "both reads have to be remembered"
    assert refused.abort_reason is not None
    assert repr(KEY_A) in refused.abort_reason, refused.abort_reason
    assert _lock_status(shard_cluster, KEY_B) is None, "the refused write must leave no lock"

    # The rule survives: exactly one of them is still on call.
    assert _value(shard_cluster, KEY_A) == b"off_call"
    assert _value(shard_cluster, KEY_B) == b"on_call"

    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Write skew test passed!")


def test_without_validation_both_doctors_walk_out():
    """The control: what the same script does with snapshot isolation alone.

    Nothing about the two transactions changed - same reads, same writes, same order.
    Without the read-set check both commits land, which is the anomaly the validation
    above exists to prevent: the nightly rule is broken by two successful commits.
    """
    tso_cluster, shard_cluster = _cluster()
    wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
    tso_client = wait_for_tso_client(tso_cluster)
    coordinator = TransactionCoordinator(tso_client, shard_cluster, validate_reads=False)
    _seed(shard_cluster, tso_client, coordinator)

    _, _, commit_1, commit_2 = _both_doctors_go_off_call(coordinator)

    assert commit_1[0] and commit_2[0], "snapshot isolation lets both through"
    assert _value(shard_cluster, KEY_A) == b"off_call"
    assert _value(shard_cluster, KEY_B) == b"off_call", "nobody is on call any more"

    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Snapshot isolation control test passed!")


def test_a_read_that_nobody_superseded_does_not_abort():
    """Validation refuses stale snapshots, not concurrent ones.

    The writer here commits a key the reader never read, and the reader writes a key
    nobody else touched - the ordinary case, where the two transactions are correctly
    ordered one after the other.  Refusing that would make every transaction on a
    busy keyspace conflict with every other.
    """
    tso_cluster, shard_cluster = _cluster()
    wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
    tso_client = wait_for_tso_client(tso_cluster)
    coordinator = TransactionCoordinator(tso_client, shard_cluster)
    _seed(shard_cluster, tso_client, coordinator)

    reader, reader_start = coordinator.begin()
    assert coordinator.read(reader, KEY_A) == b"on_call"

    writer, _ = coordinator.begin()
    coordinator.add_write(writer, KEY_B, b"off_call")
    committed, commit_ts = coordinator.commit(writer)
    assert committed
    assert reader_start < commit_ts, "the writer's commit has to be newer than the read"

    coordinator.add_write(reader, KEY_C, b"written")
    committed, _ = coordinator.commit(reader)
    assert committed, "a read the writer did not touch cannot invalidate the snapshot"
    assert _value(shard_cluster, KEY_C) == b"written"

    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Unrelated read test passed!")


def test_the_client_runs_the_work_again_after_a_refusal():
    """The retry is the only thing that can fix a refused commit.

    Attempt one decides from a snapshot that a concurrent commit then invalidates, so
    its commit is refused.  `run` starts a fresh transaction, the work sees the commit
    that invalidated it, and this time it decides not to go off call at all.
    """
    tso_cluster, shard_cluster = _cluster()
    wait_for_keys_leader(shard_cluster, [KEY_A, KEY_B])
    tso_client = wait_for_tso_client(tso_cluster)
    coordinator = TransactionCoordinator(tso_client, shard_cluster)
    _seed(shard_cluster, tso_client, coordinator)

    client = SmartClient(tso_client, shard_cluster)
    attempts = []

    def go_off_call(txn_id):
        attempts.append(txn_id)
        on_call = (client.read(txn_id, KEY_A), client.read(txn_id, KEY_B))

        if len(attempts) == 1:
            # Somebody else finishes while this attempt is deciding.  This is the
            # concurrent commit that makes the attempt's snapshot stale.
            other, _ = coordinator.begin()
            coordinator.add_write(other, KEY_A, b"off_call")
            assert coordinator.commit(other)[0]

        if on_call == (b"on_call", b"on_call"):
            client.add_write(txn_id, KEY_B, b"off_call")

    assert client.run(go_off_call), "the work has to end up committed"
    assert len(attempts) == 2, "the first attempt should have been refused and rerun"
    assert _value(shard_cluster, KEY_A) == b"off_call"
    assert _value(shard_cluster, KEY_B) == b"on_call", "the retry saw the new state"

    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Client retry test passed!")
