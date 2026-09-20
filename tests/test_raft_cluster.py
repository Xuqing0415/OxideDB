import pytest
import threading
import tempfile
import time
import shutil
from _wait import (wait_for_leader, wait_for_replication, wait_for_single_leader,
                   wait_until)
from oxidedb.raft import (
    RaftCluster, MVCCStateMachine, CommandType, NodeState, MemoryRaftNode,
    JSONFileStorage, LogEntry, NOOP_COMMAND,
)
from oxidedb.raft.state_machine import ErrorCode, ScanRefused


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

    def test_a_read_that_cannot_catch_up_is_refused_rather_than_held(self):
        """A replica whose apply is stuck answers, and the answer is a refusal.

        A read waits for the state machine to reach the commit point it is answered
        at, and nothing bounded that wait: an apply loop that has stopped - a machine
        stuck on a command - left the reader in it for as long as the node lived.
        That is worse than a refusal, because a caller cannot tell a read that will
        never be answered from one that is merely slow.

        The node here leads itself with its apply loop taken away, so entries commit
        and are never applied: that shape and not a slow one.  The wait it is given is
        short, so that what is asserted is the bound rather than five seconds passing.
        """
        node = MemoryRaftNode(node_id=1, peers=[], state_machine=MVCCStateMachine(),
                              election_timeout_min=20, election_timeout_max=40,
                              apply_timeout=0.2)
        try:
            def never_applies():
                return None

            node._apply_committed_entries = never_applies
            wait_until(lambda: node.state == NodeState.LEADER, timeout=10,
                       message="the single node never became the leader")
            wait_until(lambda: node.commit_index > node.last_applied, timeout=10,
                       message="nothing committed, so nothing is waiting to be applied")

            started = time.time()
            result = node.get(b"key")
            elapsed = time.time() - started

            assert not result.success, "a read was answered from a machine behind its log"
            assert result.error_code == ErrorCode.ERR_TIMEOUT, result.error_msg
            assert "applied" in result.error_msg, result.error_msg
            assert elapsed < 4, (
                f"the read took {elapsed:.1f}s to be refused: the wait is bounded by "
                f"this node's own apply_timeout and not by a constant of its own")

            with pytest.raises(ScanRefused) as refused:
                node.scan(b"a", b"z")
            assert refused.value.error_code == ErrorCode.ERR_TIMEOUT, refused.value.error_msg
        finally:
            node.shutdown()

    def test_a_read_named_at_an_index_sees_the_write_that_made_it(self):
        """An index from the caller is read at, and the write it is the index of is in.

        A caller that has an index names it instead of asking for a handshake.  The
        index a proposal reports is where that write landed, so waiting for the apply to
        reach it is waiting for the write to be readable - which is what makes an index
        worth passing around: it is a promise that everything before it is visible.
        """
        node = MemoryRaftNode(node_id=1, peers=[], state_machine=MVCCStateMachine(),
                              election_timeout_min=20, election_timeout_max=40)
        try:
            wait_until(lambda: node.state == NodeState.LEADER, timeout=10,
                       message="the single node never became the leader")
            command = node._state_machine.serialize_command(
                CommandType.SET, key=b"named", value=b"value")

            written = node.propose(command)
            assert written.success, written.error_msg
            assert written.index is not None

            read = node.get(b"named", read_index=written.index)
            assert read.success, read.error_msg
            assert read.value == b"value"

            rows = node.scan(b"named", b"namee", read_index=written.index)
            assert rows == [(b"named", b"value")]
        finally:
            node.shutdown()

    def test_a_replica_that_does_not_lead_answers_a_read_named_at_an_index(self):
        """The point of a named index: a replica no longer has to lead to be read from.

        Three nodes, the write goes in through the leader, and a follower that has
        applied the index the leader reported answers the same value at it.  The same
        read on the same follower with no index is refused, so what changed the answer
        is the index and not the follower - nothing in this path asks whether the node
        holding the rows is the leader.
        """
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())

        try:
            leader = cluster.get_node(wait_for_single_leader(cluster))
            command = leader._state_machine.serialize_command(
                CommandType.SET, key=b"named", value=b"value")
            written = leader.propose(command)
            assert written.success, written.error_msg

            follower = next(node for node_id, node in cluster._nodes.items()
                            if node_id != leader.node_id)
            wait_for_replication(leader, [follower])

            read = follower.get(b"named", read_index=written.index)
            assert read.success, read.error_msg
            assert read.value == b"value"

            without = follower.get(b"named")
            assert not without.success, "a follower answered a read with no index"
            assert without.error_code == ErrorCode.ERR_NOT_LEADER, without.error_msg
        finally:
            cluster.shutdown()

    def test_a_read_named_at_an_index_this_replica_cannot_reach_is_refused(self):
        """Naming an index skips the handshake, and the wait that is left stays bounded.

        The replica here never led and applied nothing, so an index it cannot reach is
        the only kind there is.  What it answers is ``ERR_TIMEOUT`` - not the
        ``ERR_NOT_LEADER`` every other path out of this read would give - which is the
        two checks being told apart: the wait is what the read went through, and the
        leadership it did not ask about.  A replica behind the index is a thing to ask
        again rather than a bad request, which is why the refusal says how far it got.
        """
        node = MemoryRaftNode(node_id=1, peers=[], state_machine=MVCCStateMachine(),
                              election_timeout_min=2000, election_timeout_max=4000,
                              apply_timeout=0.2)
        try:
            assert node.state == NodeState.FOLLOWER, "the node is expected never to lead"

            started = time.time()
            result = node.get(b"key", read_index=10 ** 6)
            elapsed = time.time() - started

            assert not result.success, "a read was answered from a machine behind its log"
            assert result.error_code == ErrorCode.ERR_TIMEOUT, result.error_msg
            assert "applied" in result.error_msg, result.error_msg
            assert elapsed < 4, (
                f"the read took {elapsed:.1f}s to be refused: a named index does not "
                f"lift the bound on the wait")

            with pytest.raises(ScanRefused) as refused:
                node.scan(b"a", b"z", read_index=10 ** 6)
            assert refused.value.error_code == ErrorCode.ERR_TIMEOUT, refused.value.error_msg
        finally:
            node.shutdown()

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
