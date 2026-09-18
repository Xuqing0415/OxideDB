"""A move that died in the middle is finished when the cluster comes back.

Between the copy and the proposal the rows are in a group the routing table has never heard
of, and the shard that still answers for the range has stopped taking writes.  That is the
window a split has, one size larger, because the group the copy went into is a whole replica
set rather than one new shard.  A process that dies there comes back with no memory of any of
it, which is why the move is written down before it starts - in the source shard's own
storage, before the first row moves - and the note is dropped only once the table names the
new group and the one it left has gone.

The note says which shard was moving and where to; it deliberately does not say how far the
move got, because anything it said about that would be a lie the moment the process writing
it died.  What a cluster that comes back reads instead is the routing table, the one place
that knows whether the proposal landed.  These tests walk the three answers it can give and
the half of the move each of them leaves to be done:

* it names the set the move was going to, so the proposal landed and the rows are already
  where the table says.  What is left is the half after it - the note goes, the group the
  shard left is let go - and nothing is copied or proposed a second time;
* it still names the set the move was leaving, so nothing was proposed.  The source is frozen
  again, whatever the new group is missing is copied out of it, and the proposal is made,
  which is what a caller retrying the move would have done;
* it names neither, which is somebody else's move or an operator's.  Nothing is guessed at:
  the shard stays frozen and the caller is told what the table says.

What these tests pin: a move killed before the proposal is finished on the next start, with
the rows readable out of the group the table now names; a move whose proposal landed before
the process died is only cleaned up, and is neither copied nor proposed again; a move that
finished leaves no note to pick up; and a table that names a third set, or cannot be read at
all, leaves the shard frozen and still remembered.
"""

import os

import pytest

from _ports import free_addresses
from _wait import wait_for_metadata_client, wait_until
from oxidedb.client import LocalNodeClientFactory
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import MetadataCluster, RoutingTable, ShardPlacement
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import CommandType, ErrorCode, MVCCStateMachine
from oxidedb.raft.storage import EngineRaftStorage

COUNT = 20
KEYS = [b"key%03d" % index for index in range(COUNT)]
VALUES = [b"value%03d" % index for index in range(COUNT)]

#: Five nodes, three of them holding the shard: a move needs somewhere to go that is not
#: where the shard is, and one node is not a replica set.
SERVING = [1, 2, 3]
MOVE_TO = [4, 5]

#: A set a shard can be moved to that is neither of the two above, which is all ordinary
#: routing-table traffic looks like from a cluster that was moving the shard itself.
NOBODY_ELSE = [5]

NOTE = "migrate/0"


def _set(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp)


def _start_metadata():
    metadata = MetadataCluster(num_nodes=3)
    metadata.start()
    return metadata


def _start_cluster(tmp_path, addresses, metadata, shard_nodes=None, storages=None):
    """A five-node cluster over ``addresses``, with shard 0 on ``shard_nodes``.

    ``storages`` collects the storages the cluster was built with.  ``shutdown`` deliberately
    leaves those open - a whole cluster closing hands them to whoever built the factory - and
    a test that starts a second cluster over the same directories has to release them itself:
    on Windows a directory cannot be renamed while a file inside it is open, and renaming one
    is how a move puts the group it left aside.
    """
    def storage(node_id, shard_id):
        opened = EngineRaftStorage(data_dir=str(tmp_path / f"shard{shard_id}_node{node_id}"))
        if storages is not None:
            storages.append(opened)
        return opened

    cluster = ShardedRaftCluster(num_nodes=5, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=addresses,
        storage_factory=storage,
        shard_nodes=shard_nodes,
        lock_cleaner_interval=None,
        metadata=metadata,
    )
    return cluster


def _close(storages):
    """Release the storages a cluster was built with, so their directories can be renamed."""
    for storage in storages:
        storage.close()


def _write_rows(cluster, count=COUNT):
    """``count`` committed rows in the shard about to move, and the leader holding them."""
    wait_until(lambda: cluster.get_leader_for_key(KEYS[0]),
               message="shard 0 never elected a leader")
    leader = cluster.get_leader_for_key(KEYS[0])[1]
    for index in range(count):
        assert leader.propose(
            _set(leader._state_machine, KEYS[index], VALUES[index], index + 1)).success
    return leader


def _published(client, shard_ids):
    """The table, once the publisher has filled in a leader for every one of ``shard_ids``."""
    def ready():
        table = client.table(refresh=True)
        for shard_id in shard_ids:
            placement = table.shard(shard_id)
            if placement is None or placement.leader_id is None:
                return None
        return table

    return wait_until(ready, message="the cluster never published a leader for the shard")


def _the_rows_are_where_the_table_says(cluster, client):
    """Every row readable out of the shard the table names, and out of no other."""
    cache = RoutingCache(cluster, client, factory=LocalNodeClientFactory(cluster))
    leader = wait_until(lambda: cache.leader_for_key(KEYS[0]),
                        message="the moved shard never got a leader in the table")
    for index, key in enumerate(KEYS):
        assert leader.get(key).value == VALUES[index], key


class _TableThatSays:
    """A table whose answer is the caller's, for the states only a stranger can make.

    A table that names a set this cluster never heard of, and one that answers nothing at
    all, are both states a real table can be in - an operator's move, a group mid-election,
    a network that dropped the read - and neither can be arranged by moving a shard here.
    """

    def __init__(self, table=None, complaint=None):
        self._table = table
        self._complaint = complaint

    def table(self, refresh=False):
        if self._complaint is not None:
            raise RuntimeError(self._complaint)
        return self._table


def _a_move_that_died_before_the_proposal(tmp_path, monkeypatch, addresses, metadata,
                                         storages=None):
    """A cluster stopped with the rows copied, the note written and nothing told."""
    cluster = _start_cluster(tmp_path, addresses, metadata, shard_nodes={0: SERVING},
                             storages=storages)
    client = wait_for_metadata_client(metadata)
    _write_rows(cluster)
    _published(client, (0,))

    def died_before_the_table_was_told(self, shard_id, target_nodes):
        raise RuntimeError("the process died with the rows copied and nowhere told")

    with monkeypatch.context() as patch:
        patch.setattr(ShardedRaftCluster, "_propose_move", died_before_the_table_was_told)
        with pytest.raises(RuntimeError):
            cluster.move_shard(0, MOVE_TO)
    return cluster, client


def test_a_move_that_died_before_the_proposal_is_finished_on_the_next_start(
        tmp_path, monkeypatch):
    addresses = free_addresses(num_nodes=5)
    metadata = _start_metadata()
    try:
        opened = []
        cluster, client = _a_move_that_died_before_the_proposal(
            tmp_path, monkeypatch, addresses, metadata, opened)
        try:
            # The state the restart has to pick up from: the rows are in the new group, the
            # move is written down, and the table has not been told any of it.
            assert cluster._load_migration_record(0) is not None
            assert client.table(refresh=True).shard(0).nodes == SERVING
            new_group = cluster.get_shard_server(MOVE_TO[0]).get_shard_node(0)
            assert new_group._state_machine._storage.get_latest_version(
                KEYS[0]) is not None
        finally:
            cluster.shutdown()
            _close(opened)

        copied = []
        original = ShardedRaftCluster._move_row

        def counting_move_row(self, source_leader, target_leader, key, value):
            copied.append(key)
            return original(self, source_leader, target_leader, key, value)

        with monkeypatch.context() as patch:
            patch.setattr(ShardedRaftCluster, "_move_row", counting_move_row)
            revived = _start_cluster(tmp_path, addresses, metadata,
                                     shard_nodes={0: SERVING})
            try:
                # A restarted cluster reads the note as it starts and acts on it in the
                # same start.  A table that has not elected its leader yet is the one case
                # where it cannot, and the answer to that is the same call again - which is
                # what a caller makes in any case.
                wait_until(lambda: not revived.migrations() or revived.recover_migrations(),
                           message="the move was never picked up")

                assert revived._load_migration_record(0) is None, "the note goes with the move"
                assert revived._serving_nodes(0) == MOVE_TO
                for node_id in SERVING:
                    assert revived.get_shard_server(node_id).get_shard_node(0) is None,                         "the group the shard left is gone"
                assert copied == [], "the rows were already in the new group"
                assert client.table(refresh=True).shard(0).nodes == MOVE_TO
                _the_rows_are_where_the_table_says(revived, client)
            finally:
                revived.shutdown()
    finally:
        metadata.shutdown()


def test_a_move_that_landed_and_died_before_the_cleanup_is_only_cleaned_up(
        tmp_path, monkeypatch):
    """The other half of a move, found half done: the table has moved and the shard has not.

    Nothing here is a retry.  The copy is in the group the table names, so copying again
    would be a second copy of rows that are already there, and proposing again would be a
    write the table has already applied - what is left is the part after the proposal, and
    it is done the way the proposal would have done it.
    """
    addresses = free_addresses(num_nodes=5)
    metadata = _start_metadata()
    opened = []
    try:
        cluster = _start_cluster(tmp_path, addresses, metadata, shard_nodes={0: SERVING},
                                 storages=opened)
        client = wait_for_metadata_client(metadata)
        try:
            _write_rows(cluster)
            _published(client, (0,))

            def died_before_the_old_group_was_let_go(self, shard_id, target_nodes,
                                                     drain=0.0):
                raise RuntimeError("the process died with the table told")

            with monkeypatch.context() as patch:
                patch.setattr(ShardedRaftCluster, "_commit_move",
                              died_before_the_old_group_was_let_go)
                with pytest.raises(RuntimeError):
                    cluster.move_shard(0, MOVE_TO)

            assert client.table(refresh=True).shard(0).nodes == MOVE_TO, "the table was told"
            assert cluster._load_migration_record(0) is not None, "and the note is still here"
            for node_id in SERVING:
                assert cluster.get_shard_server(node_id).get_shard_node(0) is not None
        finally:
            cluster.shutdown()
            _close(opened)

        told = []
        copied = []
        original_move_row = ShardedRaftCluster._move_row
        original_propose_move = ShardedRaftCluster._propose_move

        def counting_propose_move(self, shard_id, target_nodes):
            told.append(target_nodes)
            return original_propose_move(self, shard_id, target_nodes)

        def counting_move_row(self, source_leader, target_leader, key, value):
            copied.append(key)
            return original_move_row(self, source_leader, target_leader, key, value)

        with monkeypatch.context() as patch:
            patch.setattr(ShardedRaftCluster, "_propose_move", counting_propose_move)
            patch.setattr(ShardedRaftCluster, "_move_row", counting_move_row)
            revived = _start_cluster(tmp_path, addresses, metadata,
                                     shard_nodes={0: SERVING})
            try:
                wait_until(lambda: not revived.migrations() or revived.recover_migrations(),
                           message="the move was never picked up")

                assert told == [], "a move the table already names is not proposed again"
                assert copied == [], "and nothing is copied again"
                assert revived._load_migration_record(0) is None, "the note goes with the move"
                assert revived._serving_nodes(0) == MOVE_TO
                for node_id in SERVING:
                    assert revived.get_shard_server(node_id).get_shard_node(0) is None
                _the_rows_are_where_the_table_says(revived, client)
            finally:
                revived.shutdown()
    finally:
        metadata.shutdown()


def test_a_move_that_finished_leaves_nothing_to_pick_up(tmp_path):
    addresses = free_addresses(num_nodes=5)
    metadata = _start_metadata()
    cluster = _start_cluster(tmp_path, addresses, metadata, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        _write_rows(cluster)
        _published(client, (0,))
        assert cluster.move_shard(0, MOVE_TO, drain=0), cluster.migration_error()
        assert cluster.migrations() == {}, "the move is not in flight any more"
    finally:
        cluster.shutdown()

    # A caller coming back starts the node with the placement the table names, which is
    # where the shard is now: the note went with the move, so there is nothing to pick up.
    revived = _start_cluster(tmp_path, addresses, metadata, shard_nodes={0: MOVE_TO})
    try:
        assert revived.recover_migrations() == []
        assert revived.migrations() == {}
        assert revived._load_migration_record(0) is None
        assert not os.path.isdir(str(tmp_path / f"shard0_node{SERVING[0]}")),             "the group the shard left is under an orphan name, not the one it answered on"
        _the_rows_are_where_the_table_says(revived, client)
    finally:
        revived.shutdown()
        metadata.shutdown()


def test_a_table_that_names_a_third_set_leaves_the_move_to_a_person(tmp_path, monkeypatch):
    """Neither the set it was leaving nor the one it was going to: no answer but a guess.

    The table is the arbiter of which group holds a range, so a move found against a third
    set is a disagreement between its author and the table.  Choosing between two live groups
    from here would be choosing which of them to believe, so the shard stays frozen -
    answering reads of the rows it was copied from and no writes - until somebody who knows
    says which set it is.
    """
    addresses = free_addresses(num_nodes=5)
    metadata = _start_metadata()
    try:
        cluster, _ = _a_move_that_died_before_the_proposal(
            tmp_path, monkeypatch, addresses, metadata)
        try:
            cluster._metadata_client = _TableThatSays(RoutingTable(version=1, shards={
                0: ShardPlacement(0, b"", b"\xff", nodes=NOBODY_ELSE,
                                  addresses={5: "127.0.0.1:1"})}))

            assert cluster.recover_migrations() == []
            assert "routing table says [5]" in cluster.migration_error()
            assert cluster.migrations()[0].target_nodes == MOVE_TO, "the move stands"
            assert cluster._load_migration_record(0) is not None, "and so does its note"

            leader = cluster.get_leader_for_key(KEYS[0])[1]
            assert leader.writes_frozen and leader.freeze_reason == "migration"
            refusal = leader.propose(
                _set(leader._state_machine, b"a_later_row", b"v", 100_000))
            assert not refusal.success and refusal.error_code == ErrorCode.ERR_MIGRATING
        finally:
            cluster.shutdown()
    finally:
        metadata.shutdown()


def test_a_table_that_cannot_be_read_leaves_the_move_frozen_and_retryable(
        tmp_path, monkeypatch):
    """A table that did not answer is not a table that said no, so the move is not over.

    The shard stays frozen because the question the freeze asks - is this range mine, or is
    it the new group's - has not been answered at all, and it is finished by asking again
    with a table that answers.
    """
    addresses = free_addresses(num_nodes=5)
    metadata = _start_metadata()
    try:
        cluster, client = _a_move_that_died_before_the_proposal(
            tmp_path, monkeypatch, addresses, metadata)
        try:
            cluster._metadata_client = _TableThatSays(
                complaint="the metadata group has no leader")

            assert cluster.recover_migrations() == []
            assert cluster.migration_error() == "the metadata group has no leader"
            assert cluster.migrations()[0].target_nodes == MOVE_TO, "the move stands"
            leader = cluster.get_leader_for_key(KEYS[0])[1]
            assert leader.writes_frozen, "a table that said nothing is not a reason to thaw"

            # The answer this time is the set the move is leaving, which is the retry a
            # caller would have made: copy what is missing, propose, let the old group go.
            cluster._metadata_client = client
            assert cluster.recover_migrations() == [0]
            assert client.table(refresh=True).shard(0).nodes == MOVE_TO
            assert cluster._load_migration_record(0) is None
            assert cluster.migrations() == {}
            _the_rows_are_where_the_table_says(cluster, client)
        finally:
            cluster.shutdown()
    finally:
        metadata.shutdown()
