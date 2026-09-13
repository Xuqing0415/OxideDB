import time
from _ports import allocate_port, free_addresses
from _wait import wait_for_keys_leader, wait_for_tso_client, wait_until
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType, ErrorCode
from oxidedb.shard.router import locate
from oxidedb.tso.tso import TSOCluster, TSOClient
from oxidedb.transaction.coordinator import TransactionCoordinator, TxnStatus


def get_free_port():
    return allocate_port()


def shard_ids(cluster, *keys):
    '''Which shard the cluster's own router puts each key in.'''
    return [locate(cluster._range_map, key) for key in keys]


def assert_spans_two_shards(coordinator, txn_id, shard_id_1, shard_id_2):
    '''Fail loudly unless this transaction really touches two shards.

    Two separate checks on purpose.  The first pins the keys to different
    ranges; the second pins the commit path's own grouping.  A change to either
    one lands here instead of quietly turning this back into a single-shard
    test - which is how the md5 assertion this replaced stayed green.
    '''
    assert shard_id_1 != shard_id_2, "Keys should be in different shards"

    groups = coordinator._group_keys_by_shard(coordinator.get_transaction(txn_id))
    assert len(groups) == 2, (
        "Commit path grouped the keys into shard(s) %s; it would touch one Raft group"
        % sorted(groups)
    )


def lock_on(cluster, key):
    """The newest lock a shard leader holds for ``key``, or None."""
    leader_info = cluster.get_leader_for_key(key)
    assert leader_info is not None, f"no shard leader for {key!r}"
    _, leader = leader_info
    return leader._state_machine._storage.get_newest_lock(key)


def two_shard_cluster():
    """A TSO group and a two-shard cluster, both started but not yet settled."""
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())

    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(num_shards=2),
    )
    return tso_cluster, shard_cluster


def test_cross_shard_prewrite():
    tso_peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    shard_peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(peer_addresses=tso_peer_addresses)
    
    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=shard_peer_addresses)
    
    time.sleep(5)
    
    tso_client = tso_cluster.get_client()
    assert tso_client is not None
    
    coordinator = TransactionCoordinator(tso_client, shard_cluster)
    
    txn_id, start_ts = coordinator.begin()
    
    key1 = b"key_shard_a"       # first byte 0x6b -> shard 0
    key2 = b"\x80key_shard_b"   # first byte 0x80 -> shard 1

    coordinator.add_write(txn_id, key1, b"value_a")
    coordinator.add_write(txn_id, key2, b"value_b")

    shard_id_1, shard_id_2 = shard_ids(shard_cluster, key1, key2)
    assert_spans_two_shards(coordinator, txn_id, shard_id_1, shard_id_2)

    print(f"{key1!r} -> shard {shard_id_1}, {key2!r} -> shard {shard_id_2}")

    leader_1 = shard_cluster.get_leader_for_key(key1)
    leader_2 = shard_cluster.get_leader_for_key(key2)
    assert leader_1 is not None and leader_2 is not None
    assert leader_1[1] is not leader_2[1], "Both keys are served by the same Raft group"
    
    commit_success, commit_ts = coordinator.commit(txn_id)
    assert commit_success, "Cross-shard commit should succeed"
    
    time.sleep(1)
    
    for key in [key1, key2]:
        leader_info = shard_cluster.get_leader_for_key(key)
        assert leader_info is not None
        
        _, node = leader_info
        result = node.get(key)
        assert result.success, f"Read should succeed: {result.error_msg}"
        value = result.value
        expected = b"value_a" if key == key1 else b"value_b"
        assert value == expected, f"Key {key} should have value {expected}, got {value}"
    
    print(f"Cross-shard transaction: {key1!r} in shard {shard_id_1}, {key2!r} in shard {shard_id_2}")
    print("Both keys committed successfully!")
    
    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Cross-shard prewrite/commit test passed!")


def test_cross_shard_rollback():
    tso_peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    shard_peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(peer_addresses=tso_peer_addresses)
    
    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=shard_peer_addresses)
    
    time.sleep(5)
    
    tso_client = tso_cluster.get_client()
    assert tso_client is not None
    
    coordinator = TransactionCoordinator(tso_client, shard_cluster)
    
    key1 = b"key0"              # first byte 0x6b -> shard 0
    key2 = b"\x80key1"          # first byte 0x80 -> shard 1
    
    leader_info = shard_cluster.get_leader_for_key(key1)
    assert leader_info is not None
    _, leader = leader_info
    
    set_cmd = leader._state_machine.serialize_command(CommandType.SET, key=key1, value=b"original", timestamp=50)
    leader.propose(set_cmd)
    
    leader_info2 = shard_cluster.get_leader_for_key(key2)
    assert leader_info2 is not None
    _, leader2 = leader_info2
    
    set_cmd2 = leader2._state_machine.serialize_command(CommandType.SET, key=key2, value=b"original_b", timestamp=50)
    leader2.propose(set_cmd2)
    
    time.sleep(0.5)
    
    txn_id, start_ts = coordinator.begin()
    coordinator.add_write(txn_id, key1, b"new_value_a")
    coordinator.add_write(txn_id, key2, b"new_value_b")
    
    shard_id_1, shard_id_2 = shard_ids(shard_cluster, key1, key2)
    assert_spans_two_shards(coordinator, txn_id, shard_id_1, shard_id_2)

    print(f"{key1!r} -> shard {shard_id_1}, {key2!r} -> shard {shard_id_2}")

    rollback_success = coordinator.rollback(txn_id)
    assert rollback_success, "Rollback should succeed"
    
    time.sleep(0.5)
    
    leader_info = shard_cluster.get_leader_for_key(key1)
    _, leader = leader_info
    result1 = leader.get(key1)
    assert result1.success, f"Read should succeed: {result1.error_msg}"
    value1 = result1.value
    assert value1 == b"original", f"Key1 should rollback to original, got {value1}"
    
    leader_info2 = shard_cluster.get_leader_for_key(key2)
    _, leader2 = leader_info2
    result2 = leader2.get(key2)
    assert result2.success, f"Read should succeed: {result2.error_msg}"
    value2 = result2.value
    assert value2 == b"original_b", f"Key2 should rollback to original, got {value2}"
    
    print("Cross-shard rollback test: Both keys restored to original values")
    
    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Cross-shard rollback test passed!")


def test_coordinator_begin():
    tso_peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    shard_peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(peer_addresses=tso_peer_addresses)
    
    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=shard_peer_addresses)
    
    time.sleep(5)
    
    tso_client = tso_cluster.get_client()
    assert tso_client is not None
    
    coordinator = TransactionCoordinator(tso_client, shard_cluster)
    
    txn_ids = []
    start_ts_list = []
    
    for i in range(10):
        txn_id, start_ts = coordinator.begin()
        txn_ids.append(txn_id)
        start_ts_list.append(start_ts)
    
    for i in range(1, len(txn_ids)):
        assert txn_ids[i] == txn_ids[i-1] + 1, "Txn IDs should be consecutive"
    
    for i in range(1, len(start_ts_list)):
        assert start_ts_list[i] > start_ts_list[i-1], "Start timestamps should be strictly increasing"
    
    print(f"Coordinator begin test: got {len(txn_ids)} transactions with consecutive IDs and increasing timestamps")
    
    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Coordinator begin test passed!")


def test_prewrite_without_a_leader_for_one_shard_leaves_no_lock():
    """If one shard has no leader, no shard may be written to at all.

    This is the leak the coordinator used to have: it locked the shards whose
    leader it could find, then met a shard without one and returned False without
    telling the first shard to undo anything.  The lock left behind is
    indistinguishable from a live transaction, so the key stops being readable -
    there is nothing for a reader to wait for and nothing to roll forward.
    """
    tso_cluster, shard_cluster = two_shard_cluster()

    key1 = b"key0"      # first byte 0x6b -> shard 0
    key2 = b"\x80key1"  # first byte 0x80 -> shard 1
    wait_for_keys_leader(shard_cluster, [key1, key2])
    tso_client = wait_for_tso_client(tso_cluster)

    coordinator = TransactionCoordinator(tso_client, shard_cluster)

    # Stop every replica of shard 1, so the group cannot elect a leader.
    for server in shard_cluster._shard_servers.values():
        node = server.get_shard_node(1)
        if node is not None:
            node.shutdown()
    assert shard_cluster.get_leader_for_key(key2) is None, "shard 1 still has a leader"
    assert coordinator._get_shard_leader(1) is None

    txn_id, start_ts = coordinator.begin()
    # key1 first on purpose: shard 0 is the shard that used to be written to
    # before the coordinator noticed that shard 1 had no leader.
    coordinator.add_write(txn_id, key1, b"value_a")
    coordinator.add_write(txn_id, key2, b"value_b")

    assert not coordinator.prewrite(txn_id), "prewrite should fail: shard 1 has no leader"

    assert lock_on(shard_cluster, key1) is None, (
        "shard 0 was written to even though the prewrite could not reach shard 1"
    )

    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Cross-shard prewrite without a leader test passed!")


if __name__ == "__main__":
    test_coordinator_begin()
    print("\n" + "="*60 + "\n")
    test_cross_shard_prewrite()
    print("\n" + "="*60 + "\n")
    test_cross_shard_rollback()
    print("\n" + "="*60 + "\n")
    test_prewrite_without_a_leader_for_one_shard_leaves_no_lock()
