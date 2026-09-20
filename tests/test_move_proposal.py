"""A move, end to end and at every way it can stop: the table told, and the group let go.

``move_shard`` freezes the source, reads its rows at one moment, copies them into the group
that will own them, and then does the two things that make that group the shard's: it tells
the routing table, and it lets the group it left go.  The order is the split's - rows first,
table last - because the table is what clients route by and a range it hands out has to be a
range whose data is already there.

What a proposal can answer, and what each answer does to the freeze, is the whole of what
these tests are about:

* it lands - the table names the new group, this cluster's own answers follow it, the old
  group is closed on every node that held it and its storage is put aside under
  ``orphan-shard-<id>-<when>`` rather than deleted, and the move's note goes;
* it is refused, which is final, because the machine would answer the same command the same
  way - so the shard goes back to serving (it is the group the table names, and a refusal is
  not a reason to leave a range unserved) and what was copied is kept, closed, out of the
  way;
* nothing answers, which is not final, because the proposal may have landed - so the shard
  stays frozen with the move written down, and asking again with the same target set is how
  a caller finds out which it was.

The same three shapes decide what the *set being replaced* is: it is read out of the table
on every attempt and never taken from this process, because the machine refuses a move whose
expectation of the current set is wrong (code 14) and this process's own view is exactly the
stale thing that check exists to catch.  One of these tests points the table at a set the
cluster itself would not have said, which is the only way that decision is visible.
"""

import os
import socket
import threading
import time

import pytest

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_metadata_client, wait_until
from oxidedb.client import LocalNodeClientFactory
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import (PROPOSE_ATTEMPTS, MetadataCluster, RoutingTable,
                                      ShardPlacement)
from oxidedb.raft.shard_server import MigrationPhase, ProposalOutcome, ShardedRaftCluster
from oxidedb.raft.state_machine import (ApplyResult, CommandType, ErrorCode, MVCCStateMachine)
from oxidedb.raft.storage import EngineRaftStorage

COUNT = 100
KEYS = [b"key%03d" % index for index in range(COUNT)]
VALUES = [b"value%03d" % index for index in range(COUNT)]

#: Shard 0 of a one-shard map owns the whole keyspace, so where a key is is not a question
#: these tests have to ask.  Five nodes with the shard on three of them: a move needs
#: somewhere to go that is not where the shard is, and one node is not a replica set.
SERVING = [1, 2, 3]
MOVE_TO = [4, 5]
NOTE = "migrate/0"

#: The window a move is given in the tests that are not about the window: a move
#: commits and the group it left is gone either way, and what ``drain`` buys is the
#: time in between, so a test that jumps to the far side of it does not have to spend
#: the thirty seconds a cluster offers by default (``MIGRATION_DRAIN_SECONDS``).  What
#: the window itself is for is pinned in
#: ``test_the_group_the_shard_left_keeps_answering_until_the_window_closes``, which
#: asks for a window long enough to sample inside.
SHORT_WINDOW = 0.2


def _set(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp)


def _host_port(address):
    host, port = address.split(":")
    return (host, int(port))


def _cluster_with_metadata(tmp_path, shard_nodes=None, num_nodes=5):
    metadata = MetadataCluster(num_nodes=3)
    metadata.start()

    cluster = ShardedRaftCluster(num_nodes=num_nodes, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(num_nodes=num_nodes),
        storage_factory=lambda node_id, shard_id: EngineRaftStorage(
            data_dir=str(tmp_path / f"shard{shard_id}_node{node_id}")),
        shard_nodes=shard_nodes,
        lock_cleaner_interval=None,
        metadata=metadata,
    )
    return metadata, cluster


def _write_rows(cluster, count=COUNT):
    """``count`` committed rows in the shard about to move, and the leader holding them."""
    wait_for_keys_leader(cluster, KEYS[:1])
    leader = cluster.get_leader_for_key(KEYS[0])[1]
    for index in range(count):
        assert leader.propose(
            _set(leader._state_machine, KEYS[index], VALUES[index], index + 1)).success
    return leader


def _published(client, shard_ids):
    """The table, once the publisher has filled in every one of ``shard_ids``."""
    def ready():
        table = client.table(refresh=True)
        for shard_id in shard_ids:
            placement = table.shard(shard_id)
            if placement is None or placement.leader_id is None:
                return None
        return table

    return wait_until(ready, message="the cluster never published a leader for every shard")


def _what_was_put_aside(directory):
    """The group a move put aside, read back out of its orphan directory.

    Nothing else opens one of these - the name is the whole of why nobody finds it by
    accident - so a test that wants to know what is still in there has to.

    The rows are read the way a restart reads them - the snapshot first, then whatever
    entries the log still holds - rather than by counting log entries: a group folds its
    applied prefix into a snapshot every hundred entries, so the entries that carried
    these rows may already have been dropped from the log and be inside the snapshot
    instead.  Those two halves are one state, and that state is the promise: a group that
    came back to this directory answers with the rows.
    """
    storage = EngineRaftStorage(data_dir=directory)
    try:
        state_machine = MVCCStateMachine()
        snapshot = storage.load_snapshot()
        if snapshot is not None:
            state_machine.restore(snapshot[2])
        for entry in storage.load_log():
            state_machine.apply(entry.command)
        return state_machine, storage.load_admin(NOTE)
    finally:
        storage.close()


class _ToldClient:
    """The cluster's metadata client, with the table replaced and the call remembered.

    It answers the question the cluster cannot ask itself: which replica set ended up in
    the proposal, and whether it came from the table or from this process.
    """

    def __init__(self, table):
        self._table = table
        self.calls = []

    def table(self, refresh=False):
        return self._table

    def move_shard(self, shard_id, from_nodes, nodes, addresses):
        self.calls.append({"shard": shard_id, "from_nodes": list(from_nodes),
                           "nodes": list(nodes), "addresses": dict(addresses)})
        return ApplyResult.success()


class _RefusingClient:
    """A table that answers no, and would answer no again."""

    def __init__(self, inner, message):
        self._inner = inner
        self._message = message
        self.calls = 0

    def table(self, refresh=False):
        return self._inner.table(refresh)

    def move_shard(self, *args, **kwargs):
        self.calls += 1
        return ApplyResult.failure(ErrorCode.ERR_APPLY_ERROR, self._message)


class _UnreachableClient:
    """A table nothing answers for: every proposal comes back empty-handed.

    What the walk of a remote client looks like from the caller - no address answered at
    all - is the same answer as a group with no leader, which is the point: a caller cannot
    tell "the command never landed" from "the answer never came back".
    """

    def __init__(self, inner):
        self._inner = inner
        self.calls = 0

    def table(self, refresh=False):
        return self._inner.table(refresh)

    def move_shard(self, *args, **kwargs):
        self.calls += 1
        return ApplyResult.failure(ErrorCode.ERR_NOT_LEADER,
                                   "the metadata group has no leader")


class _LostAnswerClient:
    """A table that takes the move, whose answer never arrives.

    The proposal landed and the caller does not know it.  Proposing the same command again
    is the only thing that can tell the two apart, and it works because the machine
    recognises a move it has already applied.  Only the first answer is lost.
    """

    def __init__(self, inner, loses=1):
        self._inner = inner
        self._loses = loses
        self.calls = 0

    def table(self, refresh=False):
        return self._inner.table(refresh)

    def move_shard(self, *args, **kwargs):
        self.calls += 1
        self._inner.move_shard(*args, **kwargs)
        if self._loses > 0:
            self._loses -= 1
            return ApplyResult.failure(ErrorCode.ERR_NOT_LEADER, "the answer was lost")
        return ApplyResult.success()


class _BlindTableClient:
    """A table whose read fails before it answers: a group mid-election, a lost poll."""

    def __init__(self, inner, blind=1):
        self._inner = inner
        self._blind = blind
        self.reads = 0

    def table(self, refresh=False):
        self.reads += 1
        if self._blind > 0:
            self._blind -= 1
            raise RuntimeError("the routing table cannot be read: the group has no leader")
        return self._inner.table(refresh)

    def move_shard(self, *args, **kwargs):
        return self._inner.move_shard(*args, **kwargs)


def test_the_set_a_move_replaces_is_the_tables_and_not_this_processes(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path)
    try:
        # Every node serves the shard here, so this process's own answer for it is all
        # five.  The table is pointed at the three the move is really replacing.
        assert cluster._serving_nodes(0) == [1, 2, 3, 4, 5]
        table = RoutingTable(version=1, shards={
            0: ShardPlacement(0, b"", b"\xff", nodes=SERVING,
                              addresses=cluster._addresses_on(SERVING, 0)),
        })
        told = _ToldClient(table)
        cluster._metadata_client = told

        result = cluster._recovery_runner._propose_move(0, MOVE_TO)

        assert result.ok, result.message
        assert len(told.calls) == 1, "one proposal, and the table it was made against"
        call = told.calls[0]
        assert call["from_nodes"] == SERVING, "the set to replace is the table's"
        assert call["from_nodes"] != cluster._serving_nodes(0), "and not this process's"
        assert call["nodes"] == MOVE_TO
        assert call["addresses"] == cluster._addresses_on(MOVE_TO, 0), \
            "the addresses of the nodes it is going to, as those servers bound them"
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_table_that_does_not_hold_the_shard_refuses_rather_than_says_nothing(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path)
    try:
        told = _ToldClient(RoutingTable(version=1, shards={}))
        cluster._metadata_client = told

        result = cluster._recovery_runner._propose_move(0, MOVE_TO)

        assert result.outcome == ProposalOutcome.REJECTED, \
            "a table that answered is not a table that was silent"
        assert "not in the routing table" in result.message
        assert told.calls == [], "a shard the table does not have is never proposed"
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_cluster_with_no_table_has_nothing_to_propose():
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start(state_machine_factory=lambda: MVCCStateMachine(),
                  lock_cleaner_interval=None)
    try:
        assert cluster.metadata_client() is None

        result = cluster._recovery_runner._propose_move(0, [1, 2])

        assert result.ok, "an in-process cluster has no client to disagree with"
        assert "no routing table" in result.message
    finally:
        cluster.shutdown()


def test_a_move_ends_with_the_table_told_and_the_group_it_left_gone(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        source_addresses = dict(cluster.shard_addresses(0))
        assert sorted(source_addresses) == SERVING, "the shard is where it was put"

        _write_rows(cluster)
        before = _published(client, (0,))
        assert before.shard(0).nodes == SERVING

        assert cluster.move_shard(0, MOVE_TO, SHORT_WINDOW), cluster.migration_error()

        after = client.table(refresh=True)
        assert after.shard(0).nodes == MOVE_TO
        assert after.shard(0).addresses == cluster._addresses_on(MOVE_TO, 0)
        assert (after.shard(0).start, after.shard(0).end) == \
            (before.shard(0).start, before.shard(0).end), "a move is not a re-range"

        # Every question the cluster answers about the shard is about the new group now,
        # and the old one is gone from every node that held it - group, port and storage.
        assert cluster._serving_nodes(0) == MOVE_TO
        assert cluster.shard_replica_ids(0) == MOVE_TO
        assert cluster.migrations() == {}, "the move is not in flight any more"
        for node_id in SERVING:
            assert cluster.get_shard_server(node_id).get_shard_node(0) is None
            with pytest.raises(OSError):
                socket.create_connection(_host_port(source_addresses[node_id]), timeout=1)

        # Renamed and not deleted: the rows are still on disk under a name nothing reads,
        # the old name is free, and the note that said a move was in flight has gone.
        orphans = cluster.orphan_dirs()
        assert len(orphans) == len(SERVING), "one directory aside per node that held it"
        for orphan in orphans:
            assert os.path.basename(orphan).startswith("orphan-shard-0-")
            state, note = _what_was_put_aside(orphan)
            assert [state.get(key).value for key in KEYS] == VALUES, \
                "the rows are still there, in the state a group coming back would read"
            assert note is None, "the note goes with the move"
        for node_id in SERVING:
            assert not os.path.isdir(str(tmp_path / f"shard0_node{node_id}")), \
                "the name the shard was stored under is free"

        # A client that routes by the table reads the moved rows out of the group the
        # table names, which is the whole point of telling it.
        _published(client, (0,))
        leader = RoutingCache(cluster, client,
                              factory=LocalNodeClientFactory(cluster)).leader_for_key(KEYS[0])
        assert leader is not None
        assert leader.get(KEYS[0]).value == VALUES[0]
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_move_the_table_refuses_puts_the_shard_back_and_keeps_what_was_copied(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        _write_rows(cluster)
        _published(client, (0,))
        refuser = _RefusingClient(cluster.metadata_client(),
                                  "shard 0 is served by [1], not by [1, 2, 3]")
        cluster._metadata_client = refuser

        assert cluster.move_shard(0, MOVE_TO, SHORT_WINDOW) is False
        assert refuser.calls == 1, "a refusal is final: the same command is not asked twice"
        assert "served by [1]" in cluster.migration_error()

        # The shard is serving as it was.  The group the table names is the group that has
        # to answer for the range, and a refusal is not a reason to leave it unserved.
        source = cluster.get_shard_server(SERVING[0]).get_shard_node(0)
        assert source is not None and not source.writes_frozen
        assert cluster.migrations() == {}, "no move in flight"
        assert cluster._recovery_runner._load_migration_record(0) is None, \
            "and nothing written down"
        assert cluster._serving_nodes(0) == SERVING
        assert client.table(refresh=True).shard(0).nodes == SERVING, "the table did not move"

        # What was copied is closed and put aside: not serving, not routed to, and not
        # deleted either - an operator asking what was copied before the refusal finds it.
        for node_id in MOVE_TO:
            assert cluster.get_shard_server(node_id).get_shard_node(0) is None
        orphans = cluster.orphan_dirs()
        assert len(orphans) == len(MOVE_TO)
        for orphan in orphans:
            state, note = _what_was_put_aside(orphan)
            assert [state.get(key).value for key in KEYS] == VALUES, \
                "the rows that were copied are still there"
            assert note is None
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_proposal_whose_answer_never_arrives_is_proposed_again(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        _write_rows(cluster)
        _published(client, (0,))
        loser = _LostAnswerClient(cluster.metadata_client())
        cluster._metadata_client = loser

        assert cluster.move_shard(0, MOVE_TO, SHORT_WINDOW), cluster.migration_error()

        # The move landed on the attempt whose answer was lost, and the attempt that
        # followed it was answered as a success rather than applied a second time - which
        # is what makes a retry safe, and is pinned against the group directly in
        # tests/test_move_shard_proposal.py.
        assert loser.calls == 2
        assert client.table(refresh=True).shard(0).nodes == MOVE_TO
        assert cluster._serving_nodes(0) == MOVE_TO
        assert cluster.migrations() == {}
        for node_id in SERVING:
            assert cluster.get_shard_server(node_id).get_shard_node(0) is None
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_table_that_cannot_be_read_is_tried_again_before_the_move_gives_up(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        _write_rows(cluster)
        _published(client, (0,))
        blind = _BlindTableClient(cluster.metadata_client())
        cluster._metadata_client = blind

        assert cluster.move_shard(0, MOVE_TO, SHORT_WINDOW), cluster.migration_error()

        assert blind.reads == 2, "the read that failed was followed by another one"
        assert client.table(refresh=True).shard(0).nodes == MOVE_TO
        assert cluster._serving_nodes(0) == MOVE_TO
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_table_that_cannot_be_reached_leaves_the_shard_frozen_and_retryable(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        _write_rows(cluster)
        _published(client, (0,))
        original = cluster.metadata_client()
        unreachable = _UnreachableClient(original)
        cluster._metadata_client = unreachable

        assert cluster.move_shard(0, MOVE_TO, SHORT_WINDOW) is False
        assert unreachable.calls == PROPOSE_ATTEMPTS, "three attempts, then the caller is told"
        assert "no leader" in cluster.migration_error()

        # Frozen, written down, and still the group the table names.  The proposal may have
        # landed, so this shard taking rows would be rows the table has already given away.
        for node_id in SERVING:
            frozen = cluster.get_shard_server(node_id).get_shard_node(0)
            assert frozen.writes_frozen and frozen.freeze_reason == "migration"
        assert cluster.migration_state(0).phase == MigrationPhase.PROPOSING
        assert cluster._recovery_runner._load_migration_record(0) is not None
        assert cluster._serving_nodes(0) == SERVING
        assert client.table(refresh=True).shard(0).nodes == SERVING, "nothing was proposed"

        # The refusal is asked of the group that would take the row, which is the shard's
        # leader: a follower answers not-leader whatever else is going on with the shard.
        leader = cluster.get_leader_for_key(KEYS[0])[1]
        refusal = leader.propose(_set(leader._state_machine, b"a_later_row", b"v", 10_000))
        assert not refusal.success and refusal.error_code == ErrorCode.ERR_MIGRATING

        # Asking again is asking to finish that same move, and it does: the copy it had
        # already made is not made twice, and the table takes the proposal this time.
        cluster._metadata_client = original
        assert cluster.move_shard(0, MOVE_TO, SHORT_WINDOW), cluster.migration_error()
        assert client.table(refresh=True).shard(0).nodes == MOVE_TO
        assert cluster._recovery_runner._load_migration_record(0) is None
        assert cluster._serving_nodes(0) == MOVE_TO
        for node_id in SERVING:
            assert cluster.get_shard_server(node_id).get_shard_node(0) is None
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_the_group_the_shard_left_keeps_answering_until_the_window_closes(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        source_address = cluster.shard_addresses(0)[SERVING[0]]
        _write_rows(cluster)
        _published(client, (0,))
        original = cluster.metadata_client()
        cluster._metadata_client = _UnreachableClient(original)
        assert cluster.move_shard(0, MOVE_TO, SHORT_WINDOW) is False, "stopped with the copy made"
        cluster._metadata_client = original
        assert cluster._recovery_runner._propose_move(0, MOVE_TO).ok

        # A window long enough to be sampled inside, and a sample taken a tenth of the way
        # in, so that what is asserted is about the window rather than about how busy the
        # machine was.
        window = 3.0
        thread = threading.Thread(target=cluster._recovery_runner._commit_move,
                                  args=(0, MOVE_TO),
                                  kwargs={"drain": window}, daemon=True)
        thread.start()
        try:
            time.sleep(0.3)
            assert cluster.get_shard_server(SERVING[0]).get_shard_node(0) is not None, \
                "the group is still up while the window is open"
            with socket.create_connection(_host_port(source_address), timeout=2):
                pass
            assert cluster._recovery_runner._load_migration_record(0) is not None, \
                "and the note is still on disk until the move is finished"
            assert cluster._serving_nodes(0) == MOVE_TO, \
                "while the cluster already routes to the new group"
        finally:
            thread.join(timeout=15)

        assert not thread.is_alive(), "the window is a wait, not a hang"
        assert cluster.get_shard_server(SERVING[0]).get_shard_node(0) is None
        assert cluster._recovery_runner._load_migration_record(0) is None
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_finishing_a_move_twice_is_finishing_it_once(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        _write_rows(cluster)
        before = _published(client, (0,))
        assert cluster.move_shard(0, MOVE_TO, SHORT_WINDOW), cluster.migration_error()
        after = client.table(refresh=True)

        # The cluster that comes back to a finished move: the proposal is made again
        # because the answer may have been lost, and the machine recognises the move it
        # already applied rather than applying a second one.
        assert cluster._recovery_runner._propose_move(0, MOVE_TO).ok, \
            "a lost answer is proposed again"
        orphans = cluster.orphan_dirs()
        assert cluster._recovery_runner._commit_move(0, MOVE_TO, drain=0) == [], \
            "nothing left to close"
        assert cluster.orphan_dirs() == orphans, "and nothing left to move aside"

        assert client.table(refresh=True).version == after.version, "one move, one write"
        assert before.version < after.version, "and the table did move"
        assert cluster._serving_nodes(0) == MOVE_TO
        assert cluster.shard_replica_ids(0) == MOVE_TO
    finally:
        cluster.shutdown()
        metadata.shutdown()
