import time
from oxidedb.raft.node import RaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType


def test_scan_basic():
    cluster = RaftCluster(num_nodes=3)
    cluster.start(lambda: MVCCStateMachine())
    time.sleep(2)
    
    leader = cluster.get_node(cluster.get_leader())
    
    keys = [b"key_a", b"key_b", b"key_c", b"key_d", b"key_e"]
    values = [b"value_a", b"value_b", b"value_c", b"value_d", b"value_e"]
    
    for i, (key, value) in enumerate(zip(keys, values)):
        command = leader._state_machine.serialize_command(
            CommandType.SET,
            key=key,
            value=value,
            timestamp=100 + i,
        )
        leader.propose(command)
    
    time.sleep(0.5)
    
    result = leader._state_machine.scan(b"key_b", b"key_d")
    expected = [(b"key_b", b"value_b"), (b"key_c", b"value_c")]
    
    assert result == expected, f"Scan result should be {expected}, got {result}"
    
    result_all = leader._state_machine.scan(b"", b"\xff")
    assert len(result_all) == 5, f"Should return all 5 keys, got {len(result_all)}"
    
    cluster.shutdown()
    print("Basic scan test passed!")


def test_scan_empty_range():
    cluster = RaftCluster(num_nodes=3)
    cluster.start(lambda: MVCCStateMachine())
    time.sleep(2)
    
    leader = cluster.get_node(cluster.get_leader())
    
    command = leader._state_machine.serialize_command(
        CommandType.SET,
        key=b"key_a",
        value=b"value_a",
        timestamp=100,
    )
    leader.propose(command)
    time.sleep(0.5)
    
    result = leader._state_machine.scan(b"key_z", b"\xff")
    assert result == [], f"Empty range should return empty, got {result}"
    
    cluster.shutdown()
    print("Empty range scan test passed!")


def test_scan_across_shards():
    from oxidedb.raft.shard_server import ShardedRaftCluster
    
    peer_addresses = {
        1: '127.0.0.1:20001',
        2: '127.0.0.1:20002',
        3: '127.0.0.1:20003',
    }
    
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    time.sleep(3)
    
    range_map = cluster._range_map
    print(f"Range map: {range_map}")
    
    keys = [b"a_key", b"m_key", b"z_key"]
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
    
    for shard_id in range(2):
        server = cluster.get_shard_server(1)
        node = server.get_shard_node(shard_id)
        if node:
            scan_result = node._state_machine.scan(b"", b"\xff")
            print(f"Shard {shard_id} scan result: {scan_result}")
    
    cluster.shutdown()
    print("Cross-shard scan test passed!")


if __name__ == "__main__":
    test_scan_basic()
    print("\n" + "="*60 + "\n")
    test_scan_empty_range()
    print("\n" + "="*60 + "\n")
    test_scan_across_shards()