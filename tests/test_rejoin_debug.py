import time
from oxidedb.raft import RaftCluster, MVCCStateMachine, CommandType, NodeState, MemoryRaftNode


def test_rejoin_debug():
    cluster = RaftCluster(num_nodes=3)
    cluster.start(lambda: MVCCStateMachine())
    
    time.sleep(2)
    
    leader_id = cluster.get_leader()
    print(f"Initial leader: Node {leader_id}")
    
    leader = cluster.get_node(leader_id)
    state_machine = leader._state_machine
    
    for i in range(5):
        command = state_machine.serialize_command(CommandType.SET, key=f"key{i}".encode(), value=f"value{i}".encode())
        leader.propose(command)
    
    time.sleep(0.5)
    
    for node_id, node in cluster._nodes.items():
        print(f"Node {node_id}: term={node._current_term}, voted_for={node._voted_for}, log_len={len(node._log)}")
    
    peer_id = leader._peers[0]
    print(f"\nShutting down node {peer_id}")
    peer = cluster.get_node(peer_id)
    peer.shutdown()
    
    for i in range(5, 10):
        command = state_machine.serialize_command(CommandType.SET, key=f"key{i}".encode(), value=f"value{i}".encode())
        leader.propose(command)
    
    time.sleep(0.5)
    
    print(f"\nAfter more entries:")
    for node_id, node in cluster._nodes.items():
        if node_id != peer_id:
            print(f"Node {node_id}: term={node._current_term}, voted_for={node._voted_for}, log_len={len(node._log)}")
    
    new_state_machine = MVCCStateMachine()
    new_peers = [nid for nid in cluster._nodes.keys() if nid != peer_id]
    new_peer = MemoryRaftNode(
        node_id=peer_id,
        peers=new_peers,
        state_machine=new_state_machine,
        get_peer_node=cluster._get_node,
        election_timeout_min=500,
        election_timeout_max=1000,
    )
    cluster._nodes[peer_id] = new_peer
    
    print(f"\nNew node {peer_id} joined: term={new_peer._current_term}, voted_for={new_peer._voted_for}, log_len={len(new_peer._log)}")
    
    for _ in range(3):
        leader._send_heartbeats()
        time.sleep(0.1)
    
    time.sleep(2)
    
    final_leader_id = cluster.get_leader()
    print(f"\nFinal leader: Node {final_leader_id}")
    
    for node_id, node in cluster._nodes.items():
        print(f"Node {node_id}: state={node.state}, term={node._current_term}, voted_for={node._voted_for}, log_len={len(node._log)}")
    
    cluster.shutdown()


if __name__ == "__main__":
    test_rejoin_debug()