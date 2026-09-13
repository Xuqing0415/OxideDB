"""A read has to be able to ask for an older version than the newest one.

Every read used to answer with the newest version the replica had applied, so two
reads in one transaction could straddle somebody else's commit and a transaction
could not see its own prewrite.  The read path now takes the timestamp to read at:
the state machine picks the version by it and decides which locks are the reader's
business, and a node serving the read still does the ReadIndex handshake first -
which is what makes the timestamp safe, because that handshake puts the replica at
or past everything committed before the read began, and the TSO issued the
timestamp before that.
"""

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_single_leader, wait_for_tso_client
from oxidedb.raft.node import RaftCluster
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType, ErrorCode
from oxidedb.tso.tso import TSOCluster
from oxidedb.transaction.coordinator import TransactionCoordinator


def test_a_read_can_ask_for_an_older_version():
    """The read path itself: two versions, three timestamps, three answers."""
    cluster = RaftCluster(num_nodes=3)
    cluster.start(state_machine_factory=lambda: MVCCStateMachine())
    leader = cluster.get_node(wait_for_single_leader(cluster))

    key = b"snapshot_key"
    for value, timestamp in ((b"v1", 100), (b"v2", 200)):
        command = leader._state_machine.serialize_command(
            CommandType.SET, key=key, value=value, timestamp=timestamp)
        assert leader.propose(command).success

    assert leader.get(key).value == b"v2"       # no timestamp: the newest version
    assert leader.get(key, 200).value == b"v2"  # at the second version's timestamp
    assert leader.get(key, 199).value == b"v1"  # one tick earlier: the first
    assert leader.get(key, 100).value == b"v1"
    assert leader.get(key, 99).value is None    # nothing had been committed yet

    cluster.shutdown()
    print("Snapshot read test passed!")


def test_transaction_reads_the_snapshot_it_started_with():
    """The same thing through the coordinator, which is where a client meets it."""
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())

    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(num_shards=2),
    )

    key = b"snapshot_key"  # first byte 0x73 -> shard 0
    wait_for_keys_leader(shard_cluster, [key])
    tso_client = wait_for_tso_client(tso_cluster)
    coordinator = TransactionCoordinator(tso_client, shard_cluster)

    def newest(key):
        return shard_cluster.get_leader_for_key(key)[1].get(key)

    writer, _ = coordinator.begin()
    coordinator.add_write(writer, key, b"v1")
    committed, commit_1 = coordinator.commit(writer)
    assert committed, "the first commit should succeed"

    # A transaction that starts after v1 committed, and reads it.
    reader, reader_start = coordinator.begin()
    assert commit_1 <= reader_start, "v1 has to be inside this snapshot"
    assert coordinator.read(reader, key) == b"v1"

    # v2 commits after that snapshot was taken.
    writer2, _ = coordinator.begin()
    coordinator.add_write(writer2, key, b"v2")
    committed, commit_2 = coordinator.commit(writer2)
    assert committed, "the second commit should succeed"
    assert reader_start < commit_2, "v2 has to fall outside this snapshot"

    # So the reader keeps its answer while a fresh read moves on.
    assert coordinator.read(reader, key) == b"v1"
    assert newest(key).value == b"v2"

    # An uncommitted prewrite is the writer's own write, and nobody else's.
    pending, _ = coordinator.begin()
    coordinator.add_write(pending, key, b"v3")
    assert coordinator.prewrite(pending), "the prewrite should succeed"

    assert coordinator.read(pending, key) == b"v3", "a transaction sees its own prewrite"
    assert coordinator.read(reader, key) == b"v1", "a newer lock cannot hide the snapshot"

    # A read with no timestamp is unchanged: the lock is still in the way.
    locked = newest(key)
    assert not locked.success and locked.error_code == ErrorCode.ERR_LOCKED

    assert coordinator.rollback(pending)
    assert newest(key).value == b"v2"
    assert coordinator.read(reader, key) == b"v1"

    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Transaction snapshot read test passed!")