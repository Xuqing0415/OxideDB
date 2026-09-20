"""A split that died in the middle is finished when the cluster comes back.

Between the copy and the proposal a cluster is holding state nobody else has: rows in the
new shard's group, and a shard that has stopped taking writes.  A process that dies there
comes back with no memory of any of it, and the two halves of the damage look nothing
alike - the rows are in a group the table does not know about, and the shard that still
answers for the range has nothing left to copy them again from, unless it kept the rows,
which it did.

So the split is written down before it starts, in the source shard's own storage, and the
note is dropped only once the routing table has the range.  A cluster that finds one
freezes the shard again, copies whatever the new shard is missing - the rows come back out
of the source's state, not out of the note - and makes the proposal, which the group
recognises if it already applied it.

What these tests pin: a split killed between the copy and the proposal is finished on the
next start, with the table showing two ranges and the rows where the table says they are;
a split killed in the middle of the copy copies only what is missing and loses nothing; a
split that finished leaves no note behind, so the next start has nothing to pick up; and a
split that finds a lock in the range it would copy refuses, keeping its note and leaving the
shard frozen until the lock clears.
"""

import pytest

from _ports import free_addresses
from _wait import wait_for_metadata_client, wait_until
from oxidedb.metadata.service import MetadataCluster
from oxidedb.raft.recovery_notes import PendingNote, write_note
from oxidedb.raft.recovery_runner import RecoveryRunner
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import CommandType, MVCCStateMachine
from oxidedb.raft.storage import EngineRaftStorage
from oxidedb.shard.router import locate

KEPT_KEY = b"a_key"                       # below the split point: stays in shard 0
MOVED_KEYS = [b"z_key1", b"z_key2", b"z_key3"]   # above it: the new shard's rows
NEW_SHARD = 1


def _set(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp)


def _engine_storage(path):
    return EngineRaftStorage(data_dir=str(path))


def _start_metadata(tmp_path):
    metadata = MetadataCluster(num_nodes=3)
    metadata.start(storage_factory=lambda node_id: _engine_storage(
        tmp_path / f"metadata{node_id}"))
    return metadata


def _start_cluster(tmp_path, addresses, metadata):
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        # Room for the shard a split binds a port for; see _ports.free_addresses.
        peer_addresses=addresses,
        storage_factory=lambda node_id, shard_id: _engine_storage(
            tmp_path / f"shard{shard_id}_node{node_id}"),
        lock_cleaner_interval=None,
        metadata=metadata,
    )
    return cluster


def _write_rows(cluster):
    """One row below the split point and three above it, committed through Raft."""
    wait_until(lambda: cluster.get_leader_for_key(MOVED_KEYS[0]),
               message="the shard never elected a leader")
    leader = cluster.get_leader_for_key(KEPT_KEY)[1]
    assert leader.propose(_set(leader._state_machine, KEPT_KEY, b"kept", 1)).success
    for index, key in enumerate(MOVED_KEYS):
        assert leader.propose(_set(leader._state_machine, key, b"v%d" % index, 2 + index)).success
    return leader


def _wait_for_two_ranges(client):
    def ready():
        table = client.table(refresh=True)
        if sorted(table.routes()) != [0, NEW_SHARD]:
            return None
        return table

    return wait_until(ready, message="the table never got the second range")


def _wait_for_the_table_to_know_the_shard(client):
    """Wait until the table has the range the split is going to be proposed against.

    The proposal is checked by the group against its own table, which refuses a split of
    a shard it has never heard of.  On a cluster that has only just started, that is the
    publisher's first pass not having happened yet rather than anything about the split.
    """
    return wait_until(lambda: client.table(refresh=True).shard(0),
                      message="the table never got the first range")


def _assert_the_rows_are_where_the_table_says(cluster, client):
    """Every row is readable from the shard the table names, and nowhere else."""
    from oxidedb.metadata.cache import RoutingCache

    cache = RoutingCache(cluster, client)
    leader = wait_until(lambda: cache.leader_for_key(MOVED_KEYS[0]),
                        message="the new shard never got a leader in the table")
    for index, key in enumerate(MOVED_KEYS):
        assert leader.get(key).value == b"v%d" % index, key

    kept = cluster.get_leader_for_key(KEPT_KEY)[1]
    assert kept.get(KEPT_KEY).value == b"kept"
    for key in MOVED_KEYS:
        assert locate(cluster.range_map(), key) == NEW_SHARD


def test_a_split_that_died_before_the_proposal_is_finished_on_the_next_start(
        tmp_path, monkeypatch):
    addresses = free_addresses()
    metadata = _start_metadata(tmp_path)
    cluster = _start_cluster(tmp_path, addresses, metadata)
    try:
        _write_rows(cluster)
        client = wait_for_metadata_client(metadata)
        # The assertion below is that the table has not been told about the *split*, so
        # it has to have been told about the range the split starts from first: the
        # publisher's first pass is its own round trip, and on a loaded machine it has
        # not always happened by the time the rows are in.
        _wait_for_the_table_to_know_the_shard(client)

        def died_before_the_table_was_told(self, pending):
            raise RuntimeError("the process died with the rows copied and nowhere told")

        with monkeypatch.context() as patch:
            patch.setattr(RecoveryRunner, "_publish_split", died_before_the_table_was_told)
            with pytest.raises(RuntimeError):
                cluster.split_shard(0, b"n")

        # The rows really are in the new shard's group, and the table really has not
        # heard: this is the state the restart has to pick up from.
        new_leader = cluster.get_shard_server(1).get_shard_node(NEW_SHARD)
        assert new_leader is not None
        assert new_leader._state_machine._storage.get_latest_version(MOVED_KEYS[0]) is not None
        assert sorted(client.table(refresh=True).routes()) == [0]
    finally:
        cluster.shutdown()
        metadata.shutdown()

    # The whole process goes away, the routing table included.
    metadata = _start_metadata(tmp_path)
    revived = _start_cluster(tmp_path, addresses, metadata)
    try:
        client = wait_for_metadata_client(metadata)
        table = _wait_for_two_ranges(client)

        assert locate(revived.range_map(), MOVED_KEYS[0]) == NEW_SHARD
        assert not revived.pending_splits(), "the split is done, so nothing is pending"
        assert (table.shard(0).start, table.shard(0).end) == (b"", b"n")
        assert (table.shard(NEW_SHARD).start, table.shard(NEW_SHARD).end) == (b"n", b"\xff")
        _assert_the_rows_are_where_the_table_says(revived, client)
    finally:
        revived.shutdown()
        metadata.shutdown()


def test_a_split_that_died_mid_copy_copies_only_what_is_missing(tmp_path, monkeypatch):
    addresses = free_addresses()
    metadata = _start_metadata(tmp_path)
    cluster = _start_cluster(tmp_path, addresses, metadata)
    copied = []
    original = RecoveryRunner._move_row

    def died_after_one_row(self, source, target, key, value, version):
        original(self, source, target, key, value, version)
        copied.append(key)
        if len(copied) == 1:
            raise RuntimeError("the process died in the middle of the copy")

    try:
        _write_rows(cluster)
        client = wait_for_metadata_client(metadata)
        with monkeypatch.context() as patch:
            patch.setattr(RecoveryRunner, "_move_row", died_after_one_row)
            with pytest.raises(RuntimeError):
                cluster.split_shard(0, b"n")
        assert copied == [MOVED_KEYS[0]], "one row was copied before the process died"
    finally:
        cluster.shutdown()
        metadata.shutdown()

    metadata = _start_metadata(tmp_path)
    again = []
    original = RecoveryRunner._move_row

    def counting_move_row(self, source, target, key, value, version):
        again.append(key)
        # The freeze is state in the process, and the process is what died, so a
        # restarted cluster has to put it back on before it copies anything: these rows
        # are about to live in a shard the table does not name yet.  The copy is handed
        # clients now, and the freeze is on the node a client wraps - which is a fact a
        # test in this process can read and a recovery in another process could not.
        assert source._node.writes_frozen, "the source shard is not frozen as its rows move"
        return original(self, source, target, key, value, version)

    # The pick-up happens while the cluster starts, because that is when a process that
    # has just come back finds the note - so the count has to be in place before the
    # cluster is built, not after it has already finished the split.
    with monkeypatch.context() as patch:
        patch.setattr(RecoveryRunner, "_move_row", counting_move_row)
        revived = _start_cluster(tmp_path, addresses, metadata)
        try:
            client = wait_for_metadata_client(metadata)
            _wait_for_two_ranges(client)

            assert again == MOVED_KEYS[1:], (
                "the row that was already copied was copied again")
            _assert_the_rows_are_where_the_table_says(revived, client)
        finally:
            revived.shutdown()
            metadata.shutdown()


def test_a_split_that_finished_leaves_nothing_to_pick_up(tmp_path):
    addresses = free_addresses()
    metadata = _start_metadata(tmp_path)
    cluster = _start_cluster(tmp_path, addresses, metadata)
    try:
        _write_rows(cluster)
        _wait_for_the_table_to_know_the_shard(wait_for_metadata_client(metadata))
        assert cluster.split_shard(0, b"n"), cluster.split_error()
        assert not cluster.pending_splits()
    finally:
        cluster.shutdown()
        metadata.shutdown()

    # The note is what a restart picks up from, so a finished split must not leave one.
    assert _engine_storage(tmp_path / "shard0_node1").load_admin("split/0") is None

    metadata = _start_metadata(tmp_path)
    revived = _start_cluster(tmp_path, addresses, metadata)
    try:
        assert revived.recover_splits() == [], "there is nothing to recover"
        assert not revived.pending_splits()
    finally:
        revived.shutdown()
        metadata.shutdown()


def test_a_split_that_finds_a_lock_keeps_its_note_and_waits(tmp_path):
    """A lock in the range is a copy that must not be taken, and a note that must not go.

    The call that begins a split refuses over one because the row behind that lock is a row
    a transaction is in the middle of changing, and the version space the copy is read out of
    holds the old version of it.  The call that finishes a split out of a note reads the same
    frozen range, so it refuses for the same reason - and that is not a failure: the note is
    what the next call picks up, once the lock has cleared on its own.

    The note is written by hand, because no call leaves a note and a lock behind together:
    both of them check for a lock before they write one, and a frozen shard refuses the
    prewrite that would make one.  It is still the state a reader owes an answer about - the
    check costs one walk of the locks, and a copy taken over one cannot be undone.
    """
    addresses = free_addresses()
    metadata = _start_metadata(tmp_path)
    cluster = _start_cluster(tmp_path, addresses, metadata)
    try:
        leader = _write_rows(cluster)
        client = wait_for_metadata_client(metadata)
        _wait_for_the_table_to_know_the_shard(client)

        # A transaction that prewrote above the split point and never committed: the row is
        # there, and the lock on it is what makes copying that row unsafe right now.
        assert leader.propose(leader._state_machine.serialize_command(
            CommandType.PREWRITE, key=MOVED_KEYS[0], value=b"v0", start_ts=100,
            primary_key=MOVED_KEYS[0])).success

        # The note a split leaves when it dies before the table is told, written the way the
        # split writes it - into every replica of the shard it is about.  Nothing else of the
        # split is here: the rows are still in the source, which is where a recovery reads
        # them from, and no new shard exists.
        write_note(cluster._storages_of(0),
                   PendingNote.split(shard_id=0, split_key=b"n", new_shard_id=NEW_SHARD))
    finally:
        cluster.shutdown()
        metadata.shutdown()

    metadata = _start_metadata(tmp_path)
    revived = _start_cluster(tmp_path, addresses, metadata)
    try:
        assert list(revived.pending_splits()) == [0], "the note is there for the start to read"
        assert revived.recover_splits() == [], "a split that would copy across a lock refuses"
        assert list(revived.pending_splits()) == [0], "and the note is kept for the next call"
        assert "lock" in (revived.split_error() or ""), (
            "refused over the lock, and not for some other reason")
        assert revived.pending_notes(0), "the note is still on disk"
        assert revived.get_shard_server(1).get_shard_node(0).writes_frozen, (
            "and the shard stays frozen while that is true")
        new_shard = revived.get_shard_server(1).get_shard_node(NEW_SHARD)
        assert new_shard is not None, "the group the split would fill is built, and left empty"
        assert new_shard._state_machine._storage.get_latest_version(MOVED_KEYS[0]) is None, (
            "nothing was copied into it")
    finally:
        revived.shutdown()
        metadata.shutdown()
