"""State-machine snapshots and Raft log compaction.

The log used to grow for ever: every election appends a no-op and every client
write an entry, and nothing ever removed them.  A snapshot is what makes it safe
to drop that prefix - the state can be rebuilt from the snapshot alone - while
``InstallSnapshot`` is what a replica that fell behind the compacted prefix
needs, because the entries it is missing no longer exist anywhere.

Like ``test_durability`` these tests avoid the gRPC layer and drive the protocol
directly, which keeps them deterministic.
"""

import time

from oxidedb.raft.node import (
    InstallSnapshotRequest,
    LogEntry,
    MemoryRaftNode,
    NOOP_COMMAND,
    NodeState,
    RequestVoteRequest,
)
from oxidedb.raft.state_machine import CommandType, MVCCStateMachine
from oxidedb.raft.storage import EngineRaftStorage, JSONFileStorage
from oxidedb.storage.engine import MemoryEngine
from oxidedb.storage.mvcc import MVCCStorage
from oxidedb.tso.tso import TSOSMStateMachine


def _set_command(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp
    )


def _wait_until(predicate, timeout=5.0, message="condition never became true"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(message)


def _quiet_node(node_id, state_machine, storage, peers=None, snapshot_interval=100):
    """A node whose election timer will not fire during the test."""
    return MemoryRaftNode(
        node_id=node_id,
        peers=peers or [],
        state_machine=state_machine,
        get_peer_node=lambda _peer_id: None,
        storage=storage,
        election_timeout_min=60000,
        election_timeout_max=60000,
        snapshot_interval=snapshot_interval,
    )


def _leader_node(data_dir, snapshot_interval):
    """A single-node cluster that elects itself, so writes can be committed."""
    return MemoryRaftNode(
        node_id=1,
        peers=[],
        state_machine=MVCCStateMachine(),
        get_peer_node=lambda _peer_id: None,
        storage=EngineRaftStorage(data_dir=data_dir),
        election_timeout_min=20,
        election_timeout_max=40,
        snapshot_interval=snapshot_interval,
    )


def _three_nodes(snapshot_interval=3):
    """Three in-process nodes plus a switch that cuts node 1 off from node 3.

    Only node 1 campaigns - its peers have long election timers - so the test can
    predict who leads, and node 3 is the replica that gets left behind.
    """
    storages = {node_id: EngineRaftStorage() for node_id in (1, 2, 3)}
    machines = {node_id: MVCCStateMachine() for node_id in (1, 2, 3)}
    link = {"up": True}
    nodes = {}

    def lookup_from_1(peer_id):
        if peer_id == 3 and not link["up"]:
            return None
        return nodes.get(peer_id)

    def lookup(peer_id):
        return nodes.get(peer_id)

    nodes[1] = MemoryRaftNode(
        1, [2, 3], machines[1], lookup_from_1,
        election_timeout_min=40, election_timeout_max=80,
        storage=storages[1], snapshot_interval=snapshot_interval,
    )
    nodes[2] = MemoryRaftNode(
        2, [1, 3], machines[2], lookup,
        election_timeout_min=60000, election_timeout_max=60000,
        storage=storages[2], snapshot_interval=snapshot_interval,
    )
    nodes[3] = MemoryRaftNode(
        3, [1, 2], machines[3], lookup,
        election_timeout_min=60000, election_timeout_max=60000,
        storage=storages[3], snapshot_interval=snapshot_interval,
    )
    return nodes, machines, storages, link


class TestRaftStorageSnapshots:
    def test_compact_log_drops_only_the_covered_prefix(self, tmp_path):
        storage = EngineRaftStorage(data_dir=str(tmp_path / "raft"))
        for index in (1, 2, 3, 4):
            storage.append_log_entry(LogEntry(term=1, index=index, command=b"c"))

        storage.compact_log(2)

        assert [e.index for e in storage.load_log()] == [3, 4], (
            "compaction must drop the prefix the snapshot covers and nothing else"
        )

    def test_snapshot_round_trips_through_the_engine(self, tmp_path):
        data_dir = str(tmp_path / "raft")
        storage = EngineRaftStorage(data_dir=data_dir)
        assert storage.load_snapshot() is None

        storage.save_snapshot(7, 3, b"payload")

        reopened = EngineRaftStorage(data_dir=data_dir)
        assert reopened.load_snapshot() == (7, 3, b"payload"), (
            "a restarted node reads the snapshot and its index back out of storage"
        )

        reopened.save_snapshot(9, 4, b"newer")
        assert EngineRaftStorage(data_dir=data_dir).load_snapshot() == (9, 4, b"newer"), (
            "only the newest snapshot is kept"
        )

    def test_json_storage_what_is_compacted_does_not_come_back(self, tmp_path):
        storage = JSONFileStorage(str(tmp_path / "json"))
        for index in (1, 2, 3):
            storage.append_log_entry(LogEntry(term=1, index=index, command=b"c"))
        storage.save_snapshot(2, 1, b"state")
        storage.compact_log(2)

        reopened = JSONFileStorage(str(tmp_path / "json"))
        assert [e.index for e in reopened.load_log()] == [3]
        assert reopened.load_snapshot() == (2, 1, b"state")

        reopened.clear()
        assert reopened.load_log() == [] and reopened.load_snapshot() is None


class TestStateMachineSnapshots:
    def test_mvcc_snapshot_carries_versions_locks_and_timestamp(self):
        state_machine = MVCCStateMachine(MVCCStorage(engine=MemoryEngine()))
        state_machine.apply(_set_command(state_machine, b"k", b"v1", 1))
        state_machine.apply(_set_command(state_machine, b"k", b"v2", 2))
        state_machine.apply(state_machine.serialize_command(
            CommandType.PREWRITE, key=b"locked", value=b"pending", start_ts=3, primary_key=b"locked",
        ))

        restored = MVCCStateMachine(MVCCStorage(engine=MemoryEngine()))
        restored.restore(state_machine.snapshot())

        assert restored.get(b"k").value == b"v2"
        assert restored.get(b"q").value is None, "the snapshot does not invent keys"
        assert restored.get_lock_status(b"locked")["value"] == b"pending", (
            "an unresolved lock is part of the state: dropping it would let the "
            "transaction's intent be overwritten after a restart"
        )
        assert restored._last_applied_timestamp == state_machine._last_applied_timestamp, (
            "the read timestamp travels with the snapshot, otherwise a restored "
            "node reads none of its own data back"
        )

    def test_mvcc_old_versions_stay_readable_after_restore(self):
        state_machine = MVCCStateMachine(MVCCStorage(engine=MemoryEngine()))
        state_machine.apply(_set_command(state_machine, b"k", b"v1", 1))
        state_machine.apply(_set_command(state_machine, b"k", b"v2", 2))

        restored = MVCCStateMachine(MVCCStorage(engine=MemoryEngine()))
        restored.restore(state_machine.snapshot())

        assert restored._storage.get(b"k", 1) == b"v1", (
            "MVCC history is state too: a snapshot that kept only the newest "
            "version would break a read at an older timestamp"
        )

    def test_restoring_empty_data_clears_the_state(self):
        state_machine = MVCCStateMachine(MVCCStorage(engine=MemoryEngine()))
        state_machine.apply(_set_command(state_machine, b"k", b"v", 1))

        state_machine.restore(b"")

        assert state_machine.get(b"k").value is None
        assert state_machine._last_applied_timestamp == 0

    def test_tso_snapshot_does_not_hand_out_a_timestamp_twice(self):
        state_machine = TSOSMStateMachine()
        state_machine.apply(state_machine.serialize_command(b"allocate", batch_size=100))
        assert state_machine.get_current_ts() == 100

        restored = TSOSMStateMachine()
        restored.restore(state_machine.snapshot())

        assert restored.get_current_ts() == 100, (
            "a timestamp oracle that restarted at 0 would re-issue timestamps "
            "that transactions already used"
        )


class TestNodeCompaction:
    def test_restart_rebuilds_state_from_snapshot_without_replaying(self, tmp_path):
        """Once the log is compacted the snapshot is the only copy of the state.

        A restart that replayed the log alone would come back empty, because the
        entries it would replay are precisely the ones compaction removed.
        """
        data_dir = str(tmp_path / "node")
        node = _leader_node(data_dir, snapshot_interval=2)
        try:
            _wait_until(lambda: node.state == NodeState.LEADER, message="no leader elected")
            for i in range(4):
                command = _set_command(node._state_machine, f"k{i}".encode(), f"v{i}".encode(), i + 1)
                assert node.propose(command).success

            _wait_until(lambda: node._last_included_index > 0, message="the log was never compacted")
            snapshot_index = node._last_included_index
        finally:
            node.shutdown()

        storage = EngineRaftStorage(data_dir=data_dir)
        stored = storage.load_snapshot()
        assert stored is not None and stored[0] == snapshot_index
        remaining = storage.load_log()
        assert all(e.index > snapshot_index for e in remaining), (
            "the compacted entries must be gone from storage, otherwise this test "
            "would pass even if nothing was truncated"
        )
        assert not [e for e in remaining if e.command == NOOP_COMMAND], (
            "the no-op entries are compacted away too - a log that grows by one "
            "entry per election is exactly what compaction is for"
        )

        revived = _quiet_node(1, MVCCStateMachine(), EngineRaftStorage(data_dir=data_dir))
        try:
            assert revived._last_included_index == snapshot_index, (
                "the restarted node adopts the snapshot's index as its log base"
            )
            for i in range(4):
                assert revived._state_machine.get(f"k{i}".encode()).value == f"v{i}".encode()
        finally:
            revived.shutdown()


class TestInstallSnapshot:
    def test_follower_behind_the_compacted_prefix_catches_up(self):
        """The entries a lagging replica is missing may not exist any more.

        This is the case ``AppendEntries`` cannot fix: once the leader compacted
        past the follower's ``next_index`` there is nothing left to append, so the
        follower has to be handed the snapshot instead.
        """
        nodes, machines, storages, link = _three_nodes()
        try:
            _wait_until(lambda: nodes[1].state == NodeState.LEADER, message="node 1 never led")
            _wait_until(lambda: nodes[3]._last_log_index() >= 1,
                        message="node 3 never got the first entry")

            for i in range(3):
                command = _set_command(nodes[1]._state_machine, f"early{i}".encode(), b"v", i + 1)
                assert nodes[1].propose(command).success
            _wait_until(lambda: nodes[1]._last_included_index > 0,
                        message="the leader never compacted")

            # Cut node 3 off, then write well past the entries it already has.
            link["up"] = False
            for i in range(5):
                command = _set_command(nodes[1]._state_machine, f"late{i}".encode(), b"v", 10 + i)
                assert nodes[1].propose(command).success

            leader_snapshot = nodes[1]._last_included_index
            assert leader_snapshot > nodes[3]._last_log_index(), (
                "the follower has to be behind the compacted prefix for this test "
                "to mean anything"
            )

            link["up"] = True
            _wait_until(
                lambda: nodes[3]._last_included_index >= leader_snapshot,
                timeout=10.0,
                message="the follower never installed the snapshot",
            )
            _wait_until(
                lambda: machines[3].get(b"late4").value == b"v",
                timeout=10.0,
                message="the follower never applied the writes it had missed",
            )
            assert machines[3].get(b"early0").value == b"v"
            assert storages[3].load_snapshot() is not None, (
                "what the follower installed must be durable, or a restart would "
                "put it back where it started"
            )
        finally:
            for node in nodes.values():
                node.shutdown()

    def test_snapshot_from_a_stale_term_is_refused(self, tmp_path):
        node = _quiet_node(1, MVCCStateMachine(), EngineRaftStorage(data_dir=str(tmp_path / "node")))
        try:
            # Take part in an election at term 5, so the node is genuinely there.
            vote = node.request_vote(RequestVoteRequest(
                term=5, candidate_id=2, last_log_index=0, last_log_term=0,
            ))
            assert vote.vote_granted

            response = node.install_snapshot(InstallSnapshotRequest(
                term=4, leader_id=2, last_included_index=3, last_included_term=1, data=b"stale",
            ))

            assert not response.success, "a snapshot from an older term must be ignored"
            assert response.term == 5
            assert node._state_machine.get(b"k").value is None
        finally:
            node.shutdown()

    def test_snapshot_older_than_the_applied_state_is_ignored(self, tmp_path):
        """A follower that already applied past the snapshot index must not be
        dragged backwards by a leader whose snapshot is stale."""
        machine = MVCCStateMachine()
        node = _quiet_node(1, machine, EngineRaftStorage(data_dir=str(tmp_path / "node")))
        try:
            entry = LogEntry(term=1, index=1, command=_set_command(machine, b"k", b"v", 1))
            node._log.append(entry)
            node._save_log_entry(entry)
            node._commit_index = 1
            node._apply_committed_entries()
            assert machine.get(b"k").value == b"v"

            response = node.install_snapshot(InstallSnapshotRequest(
                term=1, leader_id=2, last_included_index=1, last_included_term=1,
                data=MVCCStateMachine().snapshot(),
            ))

            assert response.success, (
                "reporting success is what lets the leader resume replication; the "
                "snapshot itself was simply not needed"
            )
            assert machine.get(b"k").value == b"v", "the applied state must not be rolled back"
        finally:
            node.shutdown()
