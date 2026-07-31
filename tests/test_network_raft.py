import time
import tempfile
import shutil
from oxidedb.raft.node import RaftCluster, NodeState
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType
from oxidedb.raft.storage import JSONFileStorage


def test_network_election():
    peer_addresses = {
        1: "127.0.0.1:51001",
        2: "127.0.0.1:51002",
        3: "127.0.0.1:51003",
    }
    
    cluster = RaftCluster(num_nodes=3)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None, "Should have a leader"
    print(f"Elected leader: Node {leader_id}")
    
    for node_id, node in cluster._nodes.items():
        print(f"Node {node_id}: state={node.state}, term={node.current_term}, log_len={node.log_length}")
    
    leader = cluster.get_node(leader_id)
    assert leader is not None
    assert leader.state == NodeState.LEADER
    
    for node_id, node in cluster._nodes.items():
        if node_id != leader_id:
            assert node.state == NodeState.FOLLOWER
    
    print("Network election test passed!")
    
    cluster.shutdown()
    time.sleep(1)


def test_network_log_replication():
    peer_addresses = {
        1: "127.0.0.1:52001",
        2: "127.0.0.1:52002",
        3: "127.0.0.1:52003",
    }
    
    cluster = RaftCluster(num_nodes=3)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None
    print(f"Leader for replication test: Node {leader_id}")
    
    leader = cluster.get_node(leader_id)
    assert leader is not None
    
    for i in range(5):
        command = leader._state_machine.serialize_command(CommandType.SET, key=f"key{i}".encode(), value=f"value{i}".encode())
        result = leader.propose(command)
        assert result.success, f"Proposal {i} should succeed: {result.error_msg}"
        time.sleep(0.1)
    
    time.sleep(1)
    
    for node_id, node in cluster._nodes.items():
        print(f"Node {node_id}: log_len={node.log_length}, commit_index={node.commit_index}")
        assert node.log_length == 5, f"Node {node_id} should have 5 log entries"
    
    for node_id, node in cluster._nodes.items():
        node_sm = node._state_machine
        for i in range(5):
            key = f"key{i}".encode()
            value = f"value{i}".encode()
            assert node_sm.get(key) == value, f"Node {node_id} should have {key}={value}"
    
    print("Network log replication test passed!")
    
    cluster.shutdown()
    time.sleep(1)


def test_network_persistence():
    data_dirs = {i: tempfile.mkdtemp() for i in range(1, 4)}
    peer_addresses = {
        1: "127.0.0.1:53001",
        2: "127.0.0.1:53002",
        3: "127.0.0.1:53003",
    }
    
    try:
        def storage_factory(node_id):
            return JSONFileStorage(data_dirs[node_id])
        
        cluster = RaftCluster(num_nodes=3)
        cluster.start_network(
            state_machine_factory=lambda: MVCCStateMachine(),
            peer_addresses=peer_addresses,
            storage_factory=storage_factory,
        )
        
        time.sleep(5)
        
        leader_id = cluster.get_leader()
        assert leader_id is not None
        leader = cluster.get_node(leader_id)
        
        for i in range(5):
            command = leader._state_machine.serialize_command(CommandType.SET, key=f"key{i}".encode(), value=f"value{i}".encode())
            leader.propose(command)
            time.sleep(0.1)
        
        cluster.shutdown()
        time.sleep(3)
        
        new_cluster = RaftCluster(num_nodes=3)
        new_cluster.start_network(
            state_machine_factory=lambda: MVCCStateMachine(),
            peer_addresses=peer_addresses,
            storage_factory=storage_factory,
        )
        
        time.sleep(5)
        
        new_leader_id = new_cluster.get_leader()
        assert new_leader_id is not None
        
        for node_id, node in new_cluster._nodes.items():
            assert node.log_length == 5, f"Node {node_id} should have 5 log entries"
        
        print("Network persistence test passed!")
        
        new_cluster.shutdown()
    finally:
        for d in data_dirs.values():
            shutil.rmtree(d)


if __name__ == "__main__":
    test_network_election()
    time.sleep(2)
    print("\n" + "="*60 + "\n")
    test_network_log_replication()
    time.sleep(2)
    print("\n" + "="*60 + "\n")
    test_network_persistence()