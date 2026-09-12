import time
from _ports import free_addresses
from _wait import wait_for_single_leader
import tempfile
import shutil
from oxidedb.raft.node import NOOP_COMMAND, RaftCluster, NodeState
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType
from oxidedb.raft.storage import JSONFileStorage


def test_network_election():
    peer_addresses = free_addresses()
    
    cluster = RaftCluster(num_nodes=3)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    leader_id = wait_for_single_leader(cluster)
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
    peer_addresses = free_addresses()
    
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
        # One no-op per election sits in between, so count the writes instead.
        writes = [entry for entry in node._log if entry.command != NOOP_COMMAND]
        assert len(writes) == 5, f"Node {node_id} should have 5 write entries"
    
    for node_id, node in cluster._nodes.items():
        node_sm = node._state_machine
        for i in range(5):
            key = f"key{i}".encode()
            value = f"value{i}".encode()
            read_result = node_sm.get(key)
            assert read_result.success, f"Node {node_id} read {key} failed: {read_result.error_msg}"
            assert read_result.value == value, (
                f"Node {node_id} should have {key}={value}, got {read_result.value}"
            )
    
    print("Network log replication test passed!")
    
    cluster.shutdown()
    time.sleep(1)


def test_network_persistence():
    data_dirs = {i: tempfile.mkdtemp() for i in range(1, 4)}
    peer_addresses = free_addresses()
    
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
        
        # The restarted cluster listens on fresh ports.  Re-binding the same
        # ports immediately is not reliable on Windows: once the listening socket
        # closes, the port can sit in TIME_WAIT and gRPC's bind then fails.  The
        # node identities and data directories are unchanged, which is what this
        # test is actually about.
        restarted_addresses = free_addresses()
        
        new_cluster = RaftCluster(num_nodes=3)
        new_cluster.start_network(
            state_machine_factory=lambda: MVCCStateMachine(),
            peer_addresses=restarted_addresses,
            storage_factory=storage_factory,
        )
        
        time.sleep(5)
        
        new_leader_id = new_cluster.get_leader()
        assert new_leader_id is not None
        
        for node_id, node in new_cluster._nodes.items():
            writes = [entry for entry in node._log if entry.command != NOOP_COMMAND]
            print(f"Node {node_id}: log_len={node.log_length}, writes={len(writes)}")
            assert len(writes) == 5, f"Node {node_id} should still have its 5 writes"
        
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
