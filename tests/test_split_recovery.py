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
a split killed in the middle of the copy copies only what is missing and loses nothing;
and a split that finished leaves no note behind, so the next start has nothing to pick up.
"""

import pytest

from _ports import free_addresses
from _wait import wait_for_metadata_client, wait_until
from oxidedb.metadata.service import MetadataCluster
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
    addresses = free_addresses(num_shards=2)
    metadata = _start_metadata(tmp_path)
    cluster = _start_cluster(tmp_path, addresses, metadata)
    try:
        _write_rows(cluster)
        client = wait_for_metadata_client(metadata)

        def died_before_the_table_was_told(self, pending):
            raise RuntimeError("the process died with the rows copied and nowhere told")

        with monkeypatch.context() as patch:
            patch.setattr(ShardedRaftCluster, "_publish_split", died_before_the_table_was_told)
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
    addresses = free_addresses(num_shards=2)
    metadata = _start_metadata(tmp_path)
    cluster = _start_cluster(tmp_path, addresses, metadata)
    copied = []
    original = ShardedRaftCluster._move_row

    def died_after_one_row(self, source_leader, target_leader, key, value):
        original(self, source_leader, target_leader, key, value)
        copied.append(key)
        if len(copied) == 1:
            raise RuntimeError("the process died in the middle of the copy")

    try:
        _write_rows(cluster)
        client = wait_for_metadata_client(metadata)
        with monkeypatch.context() as patch:
            patch.setattr(ShardedRaftCluster, "_move_row", died_after_one_row)
            with pytest.raises(RuntimeError):
                cluster.split_shard(0, b"n")
        assert copied == [MOVED_KEYS[0]], "one row was copied before the process died"
    finally:
        cluster.shutdown()
        metadata.shutdown()

    metadata = _start_metadata(tmp_path)
    again = []
    original = ShardedRaftCluster._move_row

    def counting_move_row(self, source_leader, target_leader, key, value):
        again.append(key)
        # The freeze is state in the process, and the process is what died, so a
        # restarted cluster has to put it back on before it copies anything: these rows
        # are about to live in a shard the table does not name yet.
        assert source_leader.writes_frozen, "the source shard is not frozen as its rows move"
        return original(self, source_leader, target_leader, key, value)

    # The pick-up happens while the cluster starts, because that is when a process that
    # has just come back finds the note - so the count has to be in place before the
    # cluster is built, not after it has already finished the split.
    with monkeypatch.context() as patch:
        patch.setattr(ShardedRaftCluster, "_move_row", counting_move_row)
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
    addresses = free_addresses(num_shards=2)
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
