import tempfile
import shutil
from _ports import allocate_port
from _wait import wait_for_keys_leader, wait_for_shard_leaders, wait_until
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType
from oxidedb.raft.node import NodeState


def get_free_port():
    return allocate_port()


def _read_value(cluster, key):
    """What ``key``'s shard leader holds for it, or None while it holds nothing."""
    leader_info = cluster.get_leader_for_key(key)
    if leader_info is None:
        return None
    _, node = leader_info
    result = node.get(key)
    return result.value if result.success else None


def test_sharded_election():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    # Each shard elects on its own, so the condition is one leader per shard - and a
    # shard that never elects one is what the assertion below is for, which is why the
    # wait is for the condition rather than for a number of seconds that suits this
    # machine.
    wait_for_shard_leaders(cluster, range(2))
    
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
    
    key1 = b"key_shard_0"
    key2 = b"key_shard_1"
    wait_for_keys_leader(cluster, [key1, key2])
    
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
    
    keys = [b"key0", b"key1", b"key2", b"key3"]
    wait_for_keys_leader(cluster, keys)
    
    for key in keys:
        leader_info = cluster.get_leader_for_key(key)
        assert leader_info is not None, f"Key {key} should have a leader"
        
        server_id, node = leader_info
        command = node._state_machine.serialize_command(CommandType.SET, key=key, value=f"value_{key.decode()}".encode())
        result = node.propose(command)
        
        assert result.success, f"Proposal for key {key} should succeed: {result.error_msg}"
        print(f"Proposed key '{key.decode()}' to leader Node {server_id}")
    
    # A proposal returns once its own entry has applied on the leader it went to, so the
    # reads below are of a state that is already there; what they wait for is every one of
    # them being readable, which is the thing the loop is about and a second is a guess at.
    wait_until(lambda: all(_read_value(cluster, key) == f"value_{key.decode()}".encode()
                           for key in keys),
               message="the proposed rows were never all readable")
    
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
