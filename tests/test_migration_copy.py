"""A shard moved to another group: frozen where it is, copied to where it is going.

A move is a split's protocol with the whole range as its subject - freeze the source, read
its rows at one moment, copy them into the group that will own them, and only then tell the
routing table.  What is here is the half that has to be right before that last step matters:
no row is copied out of a shard that can still take writes, no group the cluster would route
to is built before the table says so, and a move that cannot be made leaves the shard
exactly as it was.

These tests look at a move stopped after its copy, which is where a caller that could not
reach the table leaves it: they hand the cluster a table that can be read and cannot be
written to.  A move that finishes - the table told, the group it left let go - is
``tests/test_move_proposal.py``'s.

What these tests pin: a shard's rows land in a new group on the nodes it was given, at the
timestamps they already had, on every node of that group; while it is being moved the shard
refuses new rows - with the refusal a move gives, which is not a split's - and the cluster
still answers "who serves this shard" with the group it is leaving; a move onto a node that
already serves the shard is refused before anything is frozen; a transaction holding a lock
in the range stops a move and thaws the shard it had just frozen; and a move that died is
finished by the next start, out of the note the shard it was leaving left behind - which is
``tests/test_migration_recovery.py``'s subject, and the last test here is the in-process
half of it.
"""

from _wait import wait_until
from oxidedb.metadata.service import RoutingTable, ShardPlacement
from oxidedb.raft.shard_server import MigrationPhase, ShardedRaftCluster
from oxidedb.raft.state_machine import (ApplyResult, CommandType, ErrorCode, MVCCStateMachine)
from oxidedb.raft.storage import EngineRaftStorage

COUNT = 100
KEYS = [b"key%03d" % index for index in range(COUNT)]
VALUES = [b"value%03d" % index for index in range(COUNT)]

#: Shard 0 of a one-shard map owns the whole keyspace, so where a key is is not a
#: question these tests have to ask.
MOVE_TO = [2, 3]
NOTE = "migrate/0"


def _set(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp)


def _prewrite(state_machine, key, value, start_ts):
    return state_machine.serialize_command(
        CommandType.PREWRITE, key=key, value=value, start_ts=start_ts, primary_key=key)


def _started_cluster(tmp_path, shard_nodes=None, storages=None):
    """A three-node cluster, with shard 0 on ``shard_nodes``.

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

    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start(
        state_machine_factory=lambda: MVCCStateMachine(),
        storage_factory=storage,
        shard_nodes=shard_nodes,
        lock_cleaner_interval=None,
    )
    # A move is stopped where the rest of this file wants to look at it - after the copy and
    # before the table - by a table nothing can be written to.  An in-process cluster has no
    # metadata group of its own, and this is the shape of one that is out of reach.
    cluster._metadata_client = _TableThatAnswersNothing(cluster)
    return cluster


def _close(storages):
    """Release the storages a cluster was built with, so their directories can be renamed."""
    for storage in storages:
        storage.close()


def _write_rows(cluster, count=COUNT):
    leader = wait_until(lambda: cluster.get_leader_for_key(KEYS[0]),
                        message="shard 0 never elected a leader")[1]
    for index in range(count):
        assert leader.propose(
            _set(leader._state_machine, KEYS[index], VALUES[index], index + 1)).success
    return leader


def _group(cluster, node_id, shard_id):
    """The group that node holds for the shard, or None - peer set included."""
    return cluster.get_shard_server(node_id).get_shard_node(shard_id)


class _TableThatAnswersNothing:
    """A table that can be read and cannot be written to.

    The freeze and the copy are only visible while the move is still standing, so the move
    has to stop after them: every proposal here comes back as "no leader", which is what a
    table that cannot be reached says, and what every other write to an in-process cluster
    answers with too.
    """

    def __init__(self, cluster, shard_id=0):
        self._cluster = cluster
        self._shard_id = shard_id
        self.calls = 0

    def table(self, refresh=False):
        shard_id = self._shard_id
        start, end = self._cluster._range_map[shard_id]
        return RoutingTable(version=1, shards={shard_id: ShardPlacement(
            shard_id, start, end,
            nodes=self._cluster.shard_replica_ids(shard_id),
            addresses=self._cluster.shard_addresses(shard_id))})

    def move_shard(self, *args, **kwargs):
        self.calls += 1
        return ApplyResult.failure(ErrorCode.ERR_NOT_LEADER,
                                   "the metadata group has no leader")


def test_a_shards_rows_land_in_a_new_group_on_the_nodes_it_was_given(tmp_path):
    """The copy, and the two things about it that a table entry will depend on.

    Every row is in the new group at the timestamp it already had - a copy stamped with
    the moment of the move would be newer than any timestamp a client holds, and a
    snapshot read would not see it - and every node of the new group has it, not just the
    one the rows were sent to.
    """
    cluster = _started_cluster(tmp_path, shard_nodes={0: [1]})
    try:
        leader = _write_rows(cluster)
        source_storage = leader._state_machine._storage
        committed = {key: source_storage.get_latest_version(key).timestamp for key in KEYS}

        assert cluster.move_shard(0, MOVE_TO) is False, "the table was never reached"
        assert cluster.migration_error() == "the metadata group has no leader"

        state = cluster.migration_state(0)
        assert state.phase == MigrationPhase.PROPOSING, "the copy is done and the switch owed"
        assert state.target_nodes == MOVE_TO
        assert state.source_nodes == [1]
        assert sorted(state.copied_keys) == sorted(KEYS), "every row was copied"

        target_leader = wait_until(lambda: cluster._leader_on(MOVE_TO, 0),
                                   message="the new group never elected a leader")
        for index, key in enumerate(KEYS):
            assert target_leader.get(key).value == VALUES[index], key
            for node_id in MOVE_TO:
                version = _group(cluster, node_id, 0)._state_machine._storage.get_latest_version(key)
                assert version is not None, (node_id, key)
                assert version.timestamp == committed[key], (node_id, key)

        # The group it is leaving is still the shard: the table has not been told, so a
        # client has to keep being sent there and not to the group nothing routes to yet.
        assert cluster.shard_replica_ids(0) == [1]
        assert cluster.get_leader_for_key(KEYS[0])[0] == 1

        # ...and it is still frozen, because a shard in two places is a shard that must
        # not take a row only one of them would have.  A thaw is the proposal's to make.
        assert leader.writes_frozen
    finally:
        cluster.shutdown()


def test_the_shard_is_frozen_while_its_rows_are_copied(tmp_path, monkeypatch):
    """Observed from inside the copy, which is the only place that can see it.

    The row about to be copied is read from a shard that is refusing new rows at that
    moment, and the refusal says a move rather than a split: the caller's next move is not
    to wait for this shard to come back but to look the range up again, because this group
    will not answer for it.
    """
    cluster = _started_cluster(tmp_path, shard_nodes={0: [1]})
    seen = {}
    original = ShardedRaftCluster._move_row

    def spy_move_row(self, source_leader, target_leader, key, value):
        if not seen:
            seen["frozen"] = source_leader.writes_frozen
            seen["reason"] = source_leader.freeze_reason
            seen["refused"] = source_leader.propose(
                _set(source_leader._state_machine, b"x_new", b"v", 999))
        return original(self, source_leader, target_leader, key, value)

    monkeypatch.setattr(ShardedRaftCluster, "_move_row", spy_move_row)
    try:
        _write_rows(cluster, count=3)
        assert cluster.move_shard(0, MOVE_TO) is False, "the table was never reached"
        assert cluster.migration_error() == "the metadata group has no leader"
    finally:
        cluster.shutdown()

    assert seen, "the move copied no rows, so nothing was observed"
    assert seen["frozen"] is True, "the source was open while its rows were read"
    assert seen["reason"] == "migration"
    assert not seen["refused"].success
    assert seen["refused"].error_code == ErrorCode.ERR_MIGRATING
    assert "moving to another group" in seen["refused"].error_msg


def test_a_move_onto_a_node_that_already_serves_the_shard_is_refused(tmp_path):
    """A set that overlaps is not a move, and nothing is frozen to find that out.

    The refusal comes before the freeze, which is the point of it: a caller that named the
    wrong nodes has not asked for anything, and a shard left frozen by it would be a shard
    nobody meant to stop.
    """
    cluster = _started_cluster(tmp_path)
    try:
        leader = _write_rows(cluster, count=1)

        assert cluster.move_shard(0, MOVE_TO) is False
        assert "already serve shard 0" in cluster.migration_error()
        assert not leader.writes_frozen, "the caller was told no, so nothing was frozen"
        assert cluster.migrations() == {}
        assert _group(cluster, 2, 0).state is not None, "shard 0's group is untouched"
        assert cluster.get_shard_server(2).shard_replica_ids(0) == [1, 2, 3]
    finally:
        cluster.shutdown()


def test_a_lock_in_the_range_stops_the_move_and_thaws_the_shard(tmp_path):
    """A lock may be a commit on its way, and a copy taken with one in flight is a loss.

    The refusal is a reason to wait rather than a reason to stop answering: the shard is
    exactly as it was, and the next attempt - once the transaction has settled - goes
    through.
    """
    cluster = _started_cluster(tmp_path, shard_nodes={0: [1]})
    try:
        leader = _write_rows(cluster, count=2)
        assert leader.propose(_prewrite(leader._state_machine, KEYS[0], b"v", 10)).success

        assert cluster.move_shard(0, MOVE_TO) is False
        assert "lock" in cluster.migration_error()
        assert not leader.writes_frozen, "a refused move has to thaw the shard"
        assert cluster.migrations() == {}
        assert _group(cluster, 2, 0) is None, "no group was built for a move that stopped"
    finally:
        cluster.shutdown()


def test_a_move_that_died_is_finished_by_the_next_start(tmp_path):
    """The note outlives the process that wrote it, and the start after it is what acts.

    A cluster coming back cannot know how far the move got - the note deliberately does not
    say, because anything it said about that would be a lie the moment the process writing it
    died - so the first thing it does is the one that is safe either way: the source is frozen
    again, because its rows may already be in the new group and a shard that may be in two
    places cannot take rows.  Then the move is finished the way a retry of it is, and what is
    left is what a finished move always leaves - the new group serving the range, the group
    it left closed, and the rows readable out of the group the cluster now answers with.
    """
    opened = []
    cluster = _started_cluster(tmp_path, shard_nodes={0: [1]}, storages=opened)
    try:
        _write_rows(cluster, count=2)
        assert cluster.move_shard(0, MOVE_TO) is False, "the table was never reached"
        assert cluster.migration_error() == "the metadata group has no leader"

        note = _group(cluster, 1, 0)._storage.load_admin(NOTE)
        assert note is not None, "the move is written down before a row is moved"
    finally:
        cluster.shutdown()
        _close(opened)

    # The whole process goes away, and what comes back has the note and nothing else: the
    # freeze had to be put back on before the copy could be finished, and the rows are read
    # out of the source again - the note does not carry them.
    revived = _started_cluster(tmp_path, shard_nodes={0: [1]})
    try:
        assert revived.migrations() == {}, "the note was picked up, not left standing"
        assert revived._load_migration_record(0) is None, "and it goes with the move"
        assert revived._serving_nodes(0) == MOVE_TO, "the new group is the shard's"
        assert _group(revived, 1, 0) is None, "and the group it left is gone"
        for node_id in MOVE_TO:
            assert _group(revived, node_id, 0) is not None

        leader = wait_until(lambda: revived._shard_leader_node(0),
                            message="the moved shard never elected a leader")
        assert leader.get(KEYS[0]).value == VALUES[0]
        assert leader.get(KEYS[1]).value == VALUES[1]
    finally:
        revived.shutdown()


def test_a_shard_that_is_already_moving_refuses_a_different_move(tmp_path):
    """One shard, one move in flight: two of them would be two groups and one range.

    The refusal is about the target and not about the shard being busy, which is why it
    names the move that is standing: a caller that asked for the same move is served (see
    the test above), and a caller that asked for another one is told what is in the way.
    """
    cluster = _started_cluster(tmp_path, shard_nodes={0: [1]})
    try:
        _write_rows(cluster, count=2)
        assert cluster.move_shard(0, MOVE_TO) is False, "the table was never reached"
        assert cluster.migration_error() == "the metadata group has no leader"

        assert cluster.move_shard(0, [2]) is False
        assert "already moving to [2, 3]" in cluster.migration_error()
        assert cluster.migration_state(0).target_nodes == MOVE_TO, "the move stands"
        assert _group(cluster, 2, 0) is not None, "and its new group is still there"
    finally:
        cluster.shutdown()
