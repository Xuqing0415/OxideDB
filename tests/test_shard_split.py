import time
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType


def test_shard_split():
    peer_addresses = {
        1: '127.0.0.1:21001',
        2: '127.0.0.1:21002',
        3: '127.0.0.1:21003',
    }
    
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    time.sleep(3)
    
    print(f"Initial range map: {cluster._range_map}")
    
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
    
    server = cluster.get_shard_server(1)
    node = server.get_shard_node(0)
    if node:
        scan_result = node._state_machine.scan(b"", b"\xff")
        print(f"Before split - Shard 0 scan: {scan_result}")
    
    success = cluster.split_shard(0, b"n")
    assert success, "Split should succeed"
    
    print(f"After split range map: {cluster._range_map}")
    
    for key in keys:
        leader_info = cluster.get_leader_for_key(key)
        assert leader_info is not None
        server_id, leader = leader_info
        print(f"Key {key} now in server {server_id}, shard {server._get_shard_id(key)}")
    
    cluster.shutdown()
    print("Shard split test passed!")


if __name__ == "__main__":
    test_shard_split()