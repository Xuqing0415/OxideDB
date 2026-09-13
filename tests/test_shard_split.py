import time

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_tso_client, wait_until
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType
from oxidedb.shard.router import locate
from oxidedb.transaction.coordinator import TransactionCoordinator
from oxidedb.tso.tso import TSOCluster


def test_shard_split():
    # Room for the shard the split creates and binds a port for; see _ports.
    peer_addresses = free_addresses(num_shards=2)
    
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    print(f"Initial range map: {cluster._range_map}")
    
    keys = [b"a_key", b"m_key", b"z_key"]
    wait_for_keys_leader(cluster, keys)
    for key in keys:
        leader_info = cluster.get_leader_for_key(key)
        assert leader_info is not None
        _, leader = leader_info
        
        command = leader._state_machine.serialize_command(
            CommandType.SET,
            key=key,
            value=key + b"_value",
            timestamp=100,
        )
        leader.propose(command)
    
    time.sleep(0.5)
    
    server = cluster.get_shard_server(1)
    node = server.get_shard_node(0)
    if node:
        scan_result = node._state_machine.scan(b"", b"\xff")
        print(f"Before split - Shard 0 scan: {scan_result}")
    
    success = cluster.split_shard(0, b"n")
    assert success, "Split should succeed"
    
    print(f"After split range map: {cluster._range_map}")
    
    wait_for_keys_leader(cluster, keys)
    for key in keys:
        leader_info = cluster.get_leader_for_key(key)
        assert leader_info is not None
        server_id, leader = leader_info
        print(f"Key {key} now in server {server_id}, shard {server._get_shard_id(key)}")
    
    cluster.shutdown()
    print("Shard split test passed!")


MOVED_KEY = b"z_key"   # >= the split point, so the split moves it
KEPT_KEY = b"a_key"    # < the split point, so it stays where it is


def test_a_split_moves_a_row_as_the_version_it_already_is():
    """The copy is not a write to the key, and must not look like one.

    A row stamped with the moment of the move is newer than any timestamp the TSO will
    ever hand out, so the shard that now owns it answers every snapshot read with
    nothing and refuses every prewrite as a write conflict - the row would be there and
    unusable at the same time.  What the move has to preserve is the version: the
    timestamp it was committed at, and the write record that says which transaction
    committed it.
    """
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())
    tso_client = wait_for_tso_client(tso_cluster)

    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        # Room for the shard the split creates: shard ``s`` of a node lives at
        # ``base + 100 * s``, so a one-shard allocation collides with itself.
        peer_addresses=free_addresses(num_shards=2),
        lock_cleaner_interval=None,
    )
    try:
        wait_for_keys_leader(cluster, [MOVED_KEY, KEPT_KEY])

        # Written the way a client writes: prewrite, then commit at a TSO timestamp,
        # which leaves both a version and a write record behind.
        coordinator = TransactionCoordinator(tso_client, cluster)
        txn_id, _ = coordinator.begin()
        coordinator.add_write(txn_id, MOVED_KEY, b"v1")
        coordinator.add_write(txn_id, KEPT_KEY, b"kept")
        assert coordinator.commit(txn_id)[0]
        commit_ts = coordinator.get_transaction(txn_id).commit_ts

        # A split refuses while a transaction is in flight over the range, so let
        # the coordinator finish the commits it is still making first.
        lead = cluster.get_leader_for_key(MOVED_KEY)[1]
        wait_until(lambda: not list(lead._state_machine._storage.iter_locks()),
                   message="the transaction never finished committing")

        assert cluster.split_shard(0, b"n"), "the split has to happen"
        wait_for_keys_leader(cluster, [MOVED_KEY, KEPT_KEY])
        assert locate(cluster.range_map(), MOVED_KEY) == 1, "the key really moved"
        assert locate(cluster.range_map(), KEPT_KEY) == 0, "and this one did not"

        leader = cluster.get_leader_for_key(MOVED_KEY)[1]
        storage = leader._state_machine._storage
        moved = storage.get_latest_version(MOVED_KEY)
        assert moved is not None and moved.timestamp == commit_ts
        assert moved.value == b"v1"
        assert storage.get_latest_write(MOVED_KEY)["commit_ts"] == commit_ts

        # Every read a client can make at or after that timestamp sees the row,
        # including one at exactly the timestamp it was committed at.
        assert leader.get(MOVED_KEY, commit_ts).value == b"v1"
        assert leader.get(MOVED_KEY).value == b"v1"

        # And the row is not a write conflict: a transaction that starts now - long
        # after the commit, at a timestamp the TSO hands out - can still write it.
        txn_id, start_ts = coordinator.begin()
        assert start_ts > commit_ts
        coordinator.add_write(txn_id, MOVED_KEY, b"v2")
        assert coordinator.commit(txn_id)[0], "a moved row is not a write conflict"
        assert leader.get(MOVED_KEY).value == b"v2"
    finally:
        cluster.shutdown()
        tso_cluster.shutdown()


def test_a_split_refuses_while_a_transaction_holds_a_lock_in_the_range():
    """A lock in the range may be a commit that has not been applied yet.

    A row copied without it would be a write dropped at the moment the row moved, so the
    split answers no and the caller comes back - the same answer the read path gives when
    it cannot justify one.  Once the transaction has settled there is nothing in flight
    to lose, and the split goes through.
    """
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())
    tso_client = wait_for_tso_client(tso_cluster)

    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        # Room for the shard the split creates: shard ``s`` of a node lives at
        # ``base + 100 * s``, so a one-shard allocation collides with itself.
        peer_addresses=free_addresses(num_shards=2),
        lock_cleaner_interval=None,
    )
    try:
        wait_for_keys_leader(cluster, [MOVED_KEY, KEPT_KEY])
        coordinator = TransactionCoordinator(tso_client, cluster)
        txn_id, _ = coordinator.begin()
        coordinator.add_write(txn_id, MOVED_KEY, b"v1")
        assert coordinator.prewrite(txn_id), "the transaction has to hold the lock"

        assert not cluster.split_shard(0, b"n"), "a lock in the range stops the split"
        assert locate(cluster.range_map(), MOVED_KEY) == 0, "and nothing moved"

        assert coordinator.rollback(txn_id)
        assert cluster.split_shard(0, b"n"), "with nothing in flight, the split happens"
        wait_for_keys_leader(cluster, [MOVED_KEY, KEPT_KEY])
        assert locate(cluster.range_map(), MOVED_KEY) == 1
    finally:
        cluster.shutdown()
        tso_cluster.shutdown()


if __name__ == "__main__":
    test_shard_split()
