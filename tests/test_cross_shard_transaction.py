import time
import socket
import hashlib
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType
from oxidedb.tso.tso import TSOCluster, TSOClient
from oxidedb.transaction.coordinator import TransactionCoordinator


def get_free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


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
    
    key1 = b"key_shard_a"
    key2 = b"key_shard_b"
    
    coordinator.add_write(txn_id, key1, b"value_a")
    coordinator.add_write(txn_id, key2, b"value_b")
    
    shard_id_1 = int(hashlib.md5(key1).hexdigest(), 16) % 2
    shard_id_2 = int(hashlib.md5(key2).hexdigest(), 16) % 2
    
    assert shard_id_1 != shard_id_2, "Keys should be in different shards"
    
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
    
    print(f"Cross-shard transaction: key1={key1.decode()} in shard {shard_id_1}, key2={key2.decode()} in shard {shard_id_2}")
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
    
    key1 = b"key0"
    key2 = b"key1"
    
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
    
    shard_id_1 = int(hashlib.md5(key1).hexdigest(), 16) % 2
    shard_id_2 = int(hashlib.md5(key2).hexdigest(), 16) % 2
    
    assert shard_id_1 != shard_id_2, "Keys should be in different shards"
    
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


if __name__ == "__main__":
    test_coordinator_begin()
    print("\n" + "="*60 + "\n")
    test_cross_shard_prewrite()
    print("\n" + "="*60 + "\n")
    test_cross_shard_rollback()