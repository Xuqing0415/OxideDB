import time
import tempfile
import shutil
from _ports import allocate_port
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType
from oxidedb.raft.node import NodeState


def get_free_port():
    return allocate_port()


def test_sharded_election():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    for shard_id in range(2):
        leader_info = None
        for server_id, server in cluster._shard_servers.items():
            node = server.get_shard_node(shard_id)
            if node and node.state == NodeState.LEADER:
                leader_info = server_id
                break
        
        assert leader_info is not None, f"Shard {shard_id} should have a leader"
        print(f"Shard {shard_id} leader: Node {leader_info}")
    
    cluster.shutdown()
    print("Sharded election test passed!")


def test_sharded_routing():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    key1 = b"key_shard_0"
    key2 = b"key_shard_1"
    
    leader1 = cluster.get_leader_for_key(key1)
    leader2 = cluster.get_leader_for_key(key2)
    
    assert leader1 is not None, f"Key {key1} should have a leader"
    assert leader2 is not None, f"Key {key2} should have a leader"
    
    print(f"Key '{key1.decode()}' routes to shard leader: Node {leader1[0]}")
    print(f"Key '{key2.decode()}' routes to shard leader: Node {leader2[0]}")
    
    cluster.shutdown()
    print("Sharded routing test passed!")


def test_sharded_proposal():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    keys = [b"key0", b"key1", b"key2", b"key3"]
    
    for key in keys:
        leader_info = cluster.get_leader_for_key(key)
        assert leader_info is not None, f"Key {key} should have a leader"
        
        server_id, node = leader_info
        command = node._state_machine.serialize_command(CommandType.SET, key=key, value=f"value_{key.decode()}".encode())
        result = node.propose(command)
        
        assert result.success, f"Proposal for key {key} should succeed: {result.error_msg}"
        print(f"Proposed key '{key.decode()}' to leader Node {server_id}")
        
        time.sleep(0.2)
    
    time.sleep(1)
    
    for key in keys:
        leader_info = cluster.get_leader_for_key(key)
        assert leader_info is not None

        _, node = leader_info
        read_result = node.get(key)
        expected = f"value_{key.decode()}".encode()
        assert read_result.success, f"Read for key {key} should succeed: {read_result.error_msg}"
        assert read_result.value == expected, (
            f"Key {key} should have value {expected}, got {read_result.value}"
        )
        print(f"Verified key '{key.decode()}' = '{read_result.value.decode()}'")
    
    cluster.shutdown()
    print("Sharded proposal test passed!")


if __name__ == "__main__":
    test_sharded_election()
    print("\n" + "="*60 + "\n")
    test_sharded_routing()
    print("\n" + "="*60 + "\n")
    test_sharded_proposal()
