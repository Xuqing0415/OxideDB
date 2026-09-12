"""Regression tests for the durability and read-consistency fixes.

These deliberately avoid the gRPC layer so they stay deterministic: sockets and
hard-coded ports in the older tests are a recurring source of flakiness.
"""

import time

from oxidedb.raft.node import (
    AppendEntriesRequest,
    LogEntry,
    MemoryRaftNode,
    NOOP_COMMAND,
    NodeState,
    RaftCluster,
)
from oxidedb.raft.state_machine import CommandType, ErrorCode, MVCCStateMachine
from oxidedb.raft.storage import EngineRaftStorage
from oxidedb.storage.engine import MemoryEngine, SQLiteEngine
from oxidedb.storage.mvcc import LockStatus, MVCCStorage


def _quiet_node(node_id, state_machine, storage, peers=None):
    """A node whose election timer will not fire during the test."""
    return MemoryRaftNode(
        node_id=node_id,
        peers=peers or [],
        state_machine=state_machine,
        get_peer_node=lambda _peer_id: None,
        storage=storage,
        election_timeout_min=60000,
        election_timeout_max=60000,
    )


def _wait_for_leader(cluster, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        leader_id = cluster.get_leader()
        if leader_id is not None:
            return cluster.get_node(leader_id)
        time.sleep(0.05)
    raise AssertionError("no leader elected within timeout")


def _set_command(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp
    )


def _client_entries(node):
    """The node's log entries that carry work, i.e. everything but the no-ops.

    Every election appends one no-op, so the raw log length depends on how many
    elections happened; the number of real entries does not.
    """
    return [entry for entry in node._log if entry.command != NOOP_COMMAND]


class TestEngine:
    def test_memory_engine_scans_in_byte_order(self):
        engine = MemoryEngine()
        for key in (b"b", b"a", b"c", b"a\x01"):
            engine.put(key, key)
        assert [k for k, _ in engine.scan(b"a", b"c")] == [b"a", b"a\x01", b"b"]
        engine.delete_range(b"a", b"b")
        assert [k for k, _ in engine.scan(b"", b"\xff")] == [b"b", b"c"]

    def test_sqlite_engine_ordering_matches_memory_engine(self, tmp_path):
        keys = [b"", b"a", b"a\x00b", b"a\xff", b"b", b"user:1", b"\xfe"]
        memory, sqlite = MemoryEngine(), SQLiteEngine(str(tmp_path / "kv.sqlite3"))
        try:
            for key in keys:
                memory.put(key, b"v" + key)
                sqlite.put(key, b"v" + key)
            assert memory.scan(b"", b"\xff") == sqlite.scan(b"", b"\xff")

            sqlite.delete_range(b"a", b"b")
            memory.delete_range(b"a", b"b")
            assert memory.scan(b"", b"\xff") == sqlite.scan(b"", b"\xff")
        finally:
            sqlite.close()


class TestMVCCDurability:
    def test_versions_survive_engine_reopen(self, tmp_path):
        path = str(tmp_path / "data.sqlite3")

        store = MVCCStorage(SQLiteEngine(path))
        store.set(b"k", b"v1", 1)
        store.set(b"k", b"v2", 2)
        store.delete(b"k", 3)
        store.close()

        reopened = MVCCStorage(SQLiteEngine(path))
        try:
            assert reopened.get(b"k", 1) == b"v1"
            assert reopened.get(b"k", 2) == b"v2"
            assert reopened.get(b"k", 3) is None
            assert reopened.get(b"k", 0) is None
            assert reopened.scan(b"a", b"z", 2) == [(b"k", b"v2")]
            assert reopened.scan(b"a", b"z", 3) == []
        finally:
            reopened.close()

    def test_latest_version_and_write_record_survive_reopen(self, tmp_path):
        path = str(tmp_path / "data.sqlite3")

        store = MVCCStorage(SQLiteEngine(path))
        store.set(b"k", b"v1", 10)
        store.set(b"k", b"v2", 20)
        store.write_write_record(b"k", 10, 15)
        store.set_write_intent(b"k", b"pending", 30)
        store.close()

        reopened = MVCCStorage(SQLiteEngine(path))
        try:
            latest = reopened.get_latest_version(b"k")
            assert (latest.value, latest.timestamp, latest.deleted) == (b"v2", 20, False)
            assert reopened.get_latest_write(b"k") == {"commit_ts": 15, "start_ts": 10}
            assert reopened.get_write_intent(b"k", 30) == b"pending"

            reopened.remove_write_intent(b"k", 30)
            assert reopened.get_write_intent(b"k", 30) is None
        finally:
            reopened.close()

    def test_rejects_separator_byte_in_key(self):
        store = MVCCStorage()
        try:
            store.set(b"a\x00b", b"v", 1)
            raise AssertionError("expected ValueError for key containing 0x00")
        except ValueError:
            pass


class TestLockDurability:
    """A write intent has to outlive the process that created it.

    Locks used to live in a plain dict on the state machine, so a crash between
    prewrite and commit silently dropped the intent: nothing stopped a second
    transaction from prewriting the same key and committing over the first.
    """

    def test_lock_records_survive_engine_reopen(self, tmp_path):
        path = str(tmp_path / "data.sqlite3")

        store = MVCCStorage(SQLiteEngine(path))
        store.set(b"k", b"v1", 1)
        store.write_write_record(b"k", 1, 2)
        store.put_lock(b"k", 30, LockStatus.LOCKED, b"primary", 1234.5, b"pending")
        store.close()

        reopened = MVCCStorage(SQLiteEngine(path))
        try:
            assert reopened.get_lock(b"k", 30) == {
                "status": LockStatus.LOCKED,
                "primary_key": b"primary",
                "lock_time": 1234.5,
                "value": b"pending",
                "key": b"k",
                "start_ts": 30,
            }
            assert reopened.get_newest_lock(b"k")["start_ts"] == 30
            assert [key for key, _ in reopened.iter_locks()] == [b"k"]

            reopened.remove_lock(b"k", 30)
            assert reopened.get_lock(b"k", 30) is None
            assert reopened.iter_locks() == []
            # The other keyspaces must be untouched by the lock namespace.
            assert reopened.get(b"k", 1) == b"v1"
            assert reopened.get_latest_write(b"k") == {"commit_ts": 2, "start_ts": 1}
        finally:
            reopened.close()

    def test_prewrite_lock_survives_node_restart(self, tmp_path):
        data_dir = str(tmp_path / "node")
        state_path = str(tmp_path / "state.sqlite3")

        node = MemoryRaftNode(
            node_id=1,
            peers=[],
            state_machine=MVCCStateMachine(SQLiteEngine(state_path)),
            get_peer_node=lambda _peer_id: None,
            storage=EngineRaftStorage(data_dir=data_dir),
            election_timeout_min=20,
            election_timeout_max=40,
        )
        try:
            deadline = time.time() + 3
            while node.state != NodeState.LEADER and time.time() < deadline:
                time.sleep(0.02)
            assert node.state == NodeState.LEADER

            command = node._state_machine.serialize_command(
                CommandType.PREWRITE, key=b"k", value=b"v2", start_ts=10, primary_key=b"k"
            )
            assert node.propose(command).success
            assert node._state_machine.get_lock_status(b"k")["start_ts"] == 10
        finally:
            node.shutdown()

        revived = _quiet_node(
            1, MVCCStateMachine(SQLiteEngine(state_path)), EngineRaftStorage(data_dir=data_dir)
        )
        try:
            lock = revived._state_machine.get_lock_status(b"k")
            assert lock is not None, "the write intent must not vanish with the process"
            assert (lock["start_ts"], lock["value"], lock["primary_key"]) == (10, b"v2", b"k")
            assert not revived._state_machine.get(b"k").success, "a held lock must still block readers"

            # A commit arriving after the restart must still find its lock.
            commit = revived._state_machine.serialize_command(
                CommandType.COMMIT, key=b"k", start_ts=10, commit_ts=20
            )
            assert revived._state_machine.apply(commit).success
            assert revived._state_machine.get_lock_status(b"k") is None
            assert revived._state_machine.get(b"k").value == b"v2"
        finally:
            revived.shutdown()


class TestRaftLogPersistence:
    def test_restart_replays_only_committed_entries(self, tmp_path):
        storage = EngineRaftStorage(data_dir=str(tmp_path / "raft"))
        storage.save_meta(1, 1)
        storage.append_log_entry(LogEntry(term=1, index=1, command=_set_command(MVCCStateMachine(), b"a", b"1", 1)))
        storage.append_log_entry(LogEntry(term=1, index=2, command=_set_command(MVCCStateMachine(), b"b", b"2", 1)))
        storage.save_commit_index(1)

        state_machine = MVCCStateMachine()
        node = _quiet_node(1, state_machine, storage, peers=[2, 3])
        try:
            assert node.log_length == 2, "replicated entries stay in the log"
            assert node.last_applied == 1, "only the committed entry may be applied"
            assert state_machine.get(b"a").value == b"1"
            assert state_machine.get(b"b").value is None
        finally:
            node.shutdown()

    def test_conflicting_suffix_truncation_is_durable(self, tmp_path):
        data_dir = str(tmp_path / "raft")
        storage = EngineRaftStorage(data_dir=data_dir)
        storage.save_meta(1, None)
        storage.append_log_entry(LogEntry(term=1, index=1, command=b"original-1"))
        storage.append_log_entry(LogEntry(term=1, index=2, command=b"original-2"))
        storage.save_commit_index(1)

        node = _quiet_node(1, MVCCStateMachine(), storage)
        response = node.append_entries(AppendEntriesRequest(
            term=2,
            leader_id=2,
            prev_log_index=1,
            prev_log_term=1,
            entries=[LogEntry(term=2, index=2, command=b"replacement-2")],
            leader_commit=2,
        ))
        assert response.success
        node.shutdown()

        reopened = EngineRaftStorage(data_dir=data_dir)
        log = reopened.load_log()
        assert [(e.index, e.term) for e in log] == [(1, 1), (2, 2)]
        assert log[1].command == b"replacement-2", "the stale suffix must not come back"

    def test_truncate_log_drops_suffix(self, tmp_path):
        storage = EngineRaftStorage(data_dir=str(tmp_path / "raft"))
        for index in (1, 2, 3, 4):
            storage.append_log_entry(LogEntry(term=1, index=index, command=b"c"))
        storage.truncate_log(3)
        assert [e.index for e in storage.load_log()] == [1, 2]
        storage.truncate_log(1)
        assert storage.load_log() == []


class TestSingleNode:
    def test_single_node_commits_reads_and_survives_restart(self, tmp_path):
        data_dir = str(tmp_path / "node")

        node = MemoryRaftNode(
            node_id=1,
            peers=[],
            state_machine=MVCCStateMachine(),
            get_peer_node=lambda _peer_id: None,
            storage=EngineRaftStorage(data_dir=data_dir),
            election_timeout_min=20,
            election_timeout_max=40,
        )
        try:
            deadline = time.time() + 3
            while node.state != NodeState.LEADER and time.time() < deadline:
                time.sleep(0.02)
            assert node.state == NodeState.LEADER

            command = _set_command(node._state_machine, b"k", b"v", 1)
            assert node.propose(command).success, "a lone node must commit its own entry"

            read = node.get(b"k")
            assert read.success and read.value == b"v"
            # index 1 is the no-op a new leader appends, index 2 the write.
            assert node.commit_index == 2 and node.last_applied == 2
        finally:
            node.shutdown()

        revived = _quiet_node(1, MVCCStateMachine(), EngineRaftStorage(data_dir=data_dir))
        try:
            assert revived.log_length == 2
            assert revived.commit_index == 2
            assert revived.last_applied == 2
            assert revived._state_machine.get(b"k").value == b"v"
        finally:
            revived.shutdown()

    def test_election_commits_entries_inherited_from_the_previous_term(self, tmp_path):
        """The classic Raft 8 trap: entries from an older term are uncommittable
        until the new leader stores an entry of *its own* term.  The no-op the
        leader appends on election is what unsticks them."""
        data_dir = str(tmp_path / "node")
        storage = EngineRaftStorage(data_dir=data_dir)
        storage.save_meta(1, 1)
        storage.append_log_entry(LogEntry(
            term=1, index=1, command=_set_command(MVCCStateMachine(), b"k", b"v", 1)
        ))
        assert storage.load_commit_index() == 0, "the entry is replicated but not committed"

        node = MemoryRaftNode(
            node_id=1,
            peers=[],
            state_machine=MVCCStateMachine(),
            get_peer_node=lambda _peer_id: None,
            storage=storage,
            election_timeout_min=20,
            election_timeout_max=40,
        )
        try:
            deadline = time.time() + 3
            while node.state != NodeState.LEADER and time.time() < deadline:
                time.sleep(0.02)
            assert node.state == NodeState.LEADER

            # No client write happens here: the election alone must be enough.
            assert node.log_length == 2 and node._log[-1].command == b""
            assert node.commit_index == 2, "the no-op must carry the older entry with it"
            assert node._state_machine.get(b"k").value == b"v"
            # ...and the no-op itself must leave no data behind.
            assert node._state_machine.scan(b"", b"\xff") == [(b"k", b"v")]
        finally:
            node.shutdown()


class TestReadIndex:
    def test_read_succeeds_with_quorum(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        try:
            leader = _wait_for_leader(cluster)
            assert leader.propose(_set_command(leader._state_machine, b"k", b"v", 1)).success
            read = leader.get(b"k")
            assert read.success and read.value == b"v"
        finally:
            cluster.shutdown()

    def test_read_is_refused_when_leader_loses_quorum(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine())
        try:
            leader = _wait_for_leader(cluster)
            assert leader.propose(_set_command(leader._state_machine, b"k", b"v", 1)).success
            assert leader.get(b"k").value == b"v"

            # Cut the leader off from both peers.  It still believes it leads,
            # which is exactly when a read must be refused instead of served.
            leader._get_peer_node = lambda _peer_id: None

            read = leader.get(b"k")
            assert not read.success
            assert read.error_code == ErrorCode.ERR_NOT_LEADER
        finally:
            cluster.shutdown()


class TestClusterRestart:
    def test_committed_data_survives_a_full_restart(self, tmp_path):
        def storage_factory(node_id):
            return EngineRaftStorage(data_dir=str(tmp_path / f"node{node_id}"))

        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine(), storage_factory)
        try:
            leader = _wait_for_leader(cluster)
            for i in range(3):
                command = _set_command(leader._state_machine, f"k{i}".encode(), f"v{i}".encode(), i + 1)
                assert leader.propose(command).success
            # Let the followers catch up and persist their commit index.
            time.sleep(0.5)
        finally:
            cluster.shutdown()

        revived = RaftCluster(num_nodes=3)
        revived.start(lambda: MVCCStateMachine(), storage_factory)
        try:
            _wait_for_leader(revived)
            for node_id, node in revived._nodes.items():
                assert len(_client_entries(node)) == 3, f"node {node_id} lost log entries"
                for i in range(3):
                    read = node._state_machine.get(f"k{i}".encode())
                    assert read.value == f"v{i}".encode(), f"node {node_id} lost k{i}"
        finally:
            revived.shutdown()
