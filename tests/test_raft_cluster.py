import pytest
import threading
import tempfile
import shutil
from _wait import (wait_for_leader, wait_for_replication, wait_for_single_leader,
                   wait_until)
from oxidedb.raft import (
    RaftCluster, MVCCStateMachine, CommandType, NodeState, MemoryRaftNode,
    JSONFileStorage, LogEntry, NOOP_COMMAND,
)


def _write_entries(node):
    """The log entries that carry writes.

    Each election appends a no-op entry, so the raw log length depends on how
    many elections the cluster went through; the write count does not.
    """
    return [entry for entry in node._log if entry.command != NOOP_COMMAND]


class TestRaftCluster:
    def test_election(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        
        try:
            leader = wait_for_single_leader(cluster)
            print(f"Leader elected: Node {leader}")
            
            nodes = cluster._nodes
            leader_node = nodes[leader]
            assert leader_node.state == NodeState.LEADER
            
            for node_id, node in nodes.items():
                if node_id != leader:
                    assert node.state == NodeState.FOLLOWER
        finally:
            cluster.shutdown()

    def test_a_new_leader_does_not_know_its_commit_index_until_it_commits_one(self):
        """Raft 5.4.2, as a split's copy check has to see it.

        The entries a node finds in its log after a restart were appended by leaders of
        earlier terms, and whether they ever committed is something only a majority
        acknowledging an entry of *this* term reveals - which is why a new leader
        appends a no-op.  Until that lands, its state machine can be missing rows the
        group has long since committed, and a caller that reads that state machine
        rather than merely appending to it has to be able to tell the difference.
        """
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())

        try:
            leader = wait_for_single_leader(cluster)
            node = cluster._nodes[leader]

            # An elected leader has committed the no-op of its own term, so it knows
            # which of the entries before it are committed.
            assert node.has_committed_in_its_own_term()

            # A node that has just come back is in neither state.  The commit index is
            # zeroed under the node's own lock, which the heartbeat acknowledgements
            # also take, so nothing can put it back between the two lines.
            with node._lock:
                node._commit_index = 0
                assert not node.has_committed_in_its_own_term()
        finally:
            cluster.shutdown()

    def test_leader_failure(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        
        original_leader = wait_for_single_leader(cluster)
        print(f"Original leader: Node {original_leader}")
        
        original_leader_node = cluster.get_node(original_leader)
        original_leader_node.shutdown()
        
        new_leader = wait_for_single_leader(cluster)
        assert new_leader != original_leader, "New leader should be different from original"
        print(f"New leader after failure: Node {new_leader}")
        
        cluster.shutdown()

    def test_log_replication(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        
        leader_id = wait_for_single_leader(cluster)
        
        leader = cluster.get_node(leader_id)
        state_machine = leader._state_machine
        
        command = state_machine.serialize_command(CommandType.SET, key=b"test_key", value=b"test_value")
        result = leader.propose(command)
        assert result.success, f"Propose should succeed on leader: {result.error_msg}"
        
        wait_for_replication(leader, [node for node_id, node in cluster._nodes.items()
                                      if node_id != leader_id])
        
        for node_id, node in cluster._nodes.items():
            node_sm = node._state_machine
            read_result = node_sm.get(b"test_key")
            assert read_result.success, f"Read should succeed on node {node_id}: {read_result.error_msg}"
            value = read_result.value
            assert value == b"test_value", f"Node {node_id} should have test_key=test_value"
        
        print("Log replication successful - all nodes have the same data")
        
        cluster.shutdown()

    def test_append_entries_heartbeat(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        
        leader_id = wait_for_single_leader(cluster)
        
        leader = cluster.get_node(leader_id)
        
        wait_until(lambda: all(cluster.get_node(peer_id).current_term == leader.current_term
                               for peer_id in leader._peers),
                   message="a peer never heard the term the leader was elected at")
        
        for peer_id in leader._peers:
            peer = cluster.get_node(peer_id)
            assert peer.current_term == leader.current_term
        
        cluster.shutdown()

    def test_concurrent_proposals(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        
        leader_id = wait_for_single_leader(cluster)
        
        leader = cluster.get_node(leader_id)
        state_machine = leader._state_machine
        
        results = []
        def do_propose(key, value):
            command = state_machine.serialize_command(CommandType.SET, key=key, value=value)
            result = leader.propose(command)
            results.append((key, value, result.success))
        
        threads = []
        for i in range(5):
            t = threading.Thread(target=do_propose, args=(f"key{i}".encode(), f"value{i}".encode()), daemon=True)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        for key, value, success in results:
            assert success, f"Proposal for {key} should succeed"
        
        wait_for_replication(leader, [node for node_id, node in cluster._nodes.items()
                                      if node_id != leader_id])
        
        for i in range(5):
            key = f"key{i}".encode()
            value = f"value{i}".encode()
            for node_id, node in cluster._nodes.items():
                node_sm = node._state_machine
                read_result = node_sm.get(key)
                assert read_result.success, f"Read should succeed on node {node_id}: {read_result.error_msg}"
                assert read_result.value == value, f"Node {node_id} should have {key}={value}"
        
        print("Concurrent proposals successful")
        
        cluster.shutdown()

    def test_stale_leader_rejection(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        
        original_leader_id = wait_for_single_leader(cluster)
        original_leader = cluster.get_node(original_leader_id)
        
        original_leader.shutdown()
        
        new_leader_id = wait_for_single_leader(cluster)
        assert new_leader_id != original_leader_id
        
        cluster.shutdown()

    def test_stale_leader_propose_returns_false(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        
        original_leader_id = wait_for_single_leader(cluster)
        original_leader = cluster.get_node(original_leader_id)
        state_machine = original_leader._state_machine
        
        original_leader.shutdown()
        
        new_leader_id = wait_for_single_leader(cluster)
        assert new_leader_id != original_leader_id
        
        command = state_machine.serialize_command(CommandType.SET, key=b"test_key", value=b"test_value")
        result = original_leader.propose(command, timeout=1.0)
        assert not result.success, "Propose on stale leader should return failure"
        
        cluster.shutdown()

    def test_log_catchup_after_rejoin(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        
        leader_id = wait_for_single_leader(cluster)
        
        leader = cluster.get_node(leader_id)
        state_machine = leader._state_machine
        
        for i in range(5):
            command = state_machine.serialize_command(CommandType.SET, key=f"key{i}".encode(), value=f"value{i}".encode())
            leader.propose(command)
        
        wait_for_replication(leader, [node for node_id, node in cluster._nodes.items()
                                      if node_id != leader_id])
        
        for node_id, node in cluster._nodes.items():
            assert len(_write_entries(node)) == 5, f"Node {node_id} should have 5 writes"
        
        print("Log replication across all nodes successful")
        
        cluster.shutdown()

    def test_rejoin_empty_log_node(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        
        leader_id = wait_for_single_leader(cluster)
        
        leader = cluster.get_node(leader_id)
        state_machine = leader._state_machine
        
        for i in range(5):
            command = state_machine.serialize_command(CommandType.SET, key=f"key{i}".encode(), value=f"value{i}".encode())
            leader.propose(command)
        
        peer_id = leader._peers[0]
        peer = cluster.get_node(peer_id)
        wait_for_replication(leader, [peer])
        peer.shutdown()
        
        for i in range(5, 10):
            command = state_machine.serialize_command(CommandType.SET, key=f"key{i}".encode(), value=f"value{i}".encode())
            leader.propose(command)
        
        survivors = [node for node_id, node in cluster._nodes.items()
                     if node_id != peer_id]
        wait_for_replication(leader, survivors)
        
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
        
        final_leader_id = wait_for_leader(cluster)
        
        final_leader = cluster.get_node(final_leader_id)
        wait_for_replication(final_leader, cluster._nodes.values())
        
        for i in range(10):
            key = f"key{i}".encode()
            value = f"value{i}".encode()
            leader_sm = final_leader._state_machine
            read_result = leader_sm.get(key)
            assert read_result.success, f"Read should succeed on leader: {read_result.error_msg}"
            assert read_result.value == value, f"Leader should have {key}={value}"
        
        for node_id, node in cluster._nodes.items():
            node_sm = node._state_machine
            for i in range(10):
                key = f"key{i}".encode()
                value = f"value{i}".encode()
                read_result = node_sm.get(key)
                assert read_result.success, f"Read should succeed on node {node_id}: {read_result.error_msg}"
                assert read_result.value == value, f"Node {node_id} should have {key}={value}"
        
        print("Rejoin empty log node test passed")
        
        cluster.shutdown()

    def test_persistence_single_node(self):
        data_dir = tempfile.mkdtemp()
        
        try:
            storage = JSONFileStorage(data_dir)
            
            state_machine = MVCCStateMachine()
            node = MemoryRaftNode(
                node_id=1,
                peers=[],
                state_machine=state_machine,
                get_peer_node=lambda x: None,
                storage=storage,
            )
            
            command = state_machine.serialize_command(CommandType.SET, key=b"test_key", value=b"test_value")
            node._current_term = 5
            node._voted_for = 1
            node._log.append(LogEntry(term=5, index=1, command=command))
            node._save_meta()
            node._save_log_entry(node._log[0])

            # A restarted node only replays *committed* entries, so the commit
            # index is part of the persisted state (see the state-machine safety
            # rule in the Raft paper).
            storage.save_commit_index(1)

            # Stop the node's timers before dropping the reference: with `del`
            # alone the election timer keeps running against a directory the
            # test is about to remove.
            node.shutdown()
            del node
            del state_machine
            
            new_state_machine = MVCCStateMachine()
            new_node = MemoryRaftNode(
                node_id=1,
                peers=[],
                state_machine=new_state_machine,
                get_peer_node=lambda x: None,
                storage=storage,
            )
            
            assert new_node._current_term == 5
            assert new_node._voted_for == 1
            assert len(new_node._log) == 1
            assert new_node._log[0].term == 5
            assert new_node._log[0].index == 1
            read_result = new_state_machine.get(b"test_key")
            assert read_result.success, f"Read should succeed: {read_result.error_msg}"
            assert read_result.value == b"test_value"
            
            print("Single node persistence test passed")
            
            new_node.shutdown()
        finally:
            shutil.rmtree(data_dir)

    def test_restart_recovery(self):
        data_dirs = {i: tempfile.mkdtemp() for i in range(1, 4)}
        
        try:
            def storage_factory(node_id):
                return JSONFileStorage(data_dirs[node_id])
            
            cluster = RaftCluster(num_nodes=3)
            cluster.start(lambda: MVCCStateMachine(), storage_factory)
            
            leader_id = wait_for_single_leader(cluster)
            leader = cluster.get_node(leader_id)
            state_machine = leader._state_machine
            
            for i in range(5):
                command = state_machine.serialize_command(CommandType.SET, key=f"key{i}".encode(), value=f"value{i}".encode())
                leader.propose(command)
            
            wait_for_replication(leader, [node for node_id, node in cluster._nodes.items()
                                          if node_id != leader_id])
            
            cluster.shutdown()
            
            new_cluster = RaftCluster(num_nodes=3)
            new_cluster.start(lambda: MVCCStateMachine(), storage_factory)
            
            wait_for_single_leader(new_cluster)
            
            for node_id, node in new_cluster._nodes.items():
                assert len(_write_entries(node)) == 5, f"Node {node_id} should have 5 writes"
                node_sm = node._state_machine
                for i in range(5):
                    key = f"key{i}".encode()
                    value = f"value{i}".encode()
                    read_result = node_sm.get(key)
                    assert read_result.success, f"Read should succeed on node {node_id}: {read_result.error_msg}"
                    assert read_result.value == value, f"Node {node_id} should have {key}={value}"
            
            print("Restart recovery test passed")
            
            new_cluster.shutdown()
        finally:
            for d in data_dirs.values():
                shutil.rmtree(d)
