"""The copy a move or a split makes, written against the client seam and nothing else.

A recovery that finishes a move inside a process of its own has no ``MemoryRaftNode`` to
read: it holds ids and addresses, and the leader of a shard may be in another process
altogether.  So the copy is expressible through ``NodeClient`` - a range read that carries
versions, a write record, and a proposal - and there is one body for it rather than two,
which is what these tests pin.

What is pinned is what the copy did: the keys it copied, the rows the target ends up with
(the version included), and the command bytes it proposed - byte for byte, because a copy
that carried a row across as a *new* write would pass every other check here.  Both
branches of the write-record test are covered: a row a plain ``SET`` wrote has no record,
and a row a transaction committed has one, and only the second travels with a ``start_ts``.

The version read is what makes this possible at all: ``KeyValuePair`` carried no version
until recently, and without one a copy cannot know which moment to stamp a row with - it
would arrive in the new group as the newest thing that has ever happened to that key.

The expected commands are built here rather than taken from a recording of the copy, so
what is compared is the copy's output and not its behaviour.  The two implementations this
file used to hold side by side - one over node objects, the second written only to show
they proposed the same bytes - are down to the second one: it was shown to agree, and two
bodies for one copy is a change to the copy having to be made twice, and an equivalence
test whose two sides move together.

Which group the rows are going *into* is the other half of that.  The group a move builds
is not the group the routing table names yet, so ``leader_client(shard_id)`` cannot answer
for it - that lookup is the table's answer - and the first version of these tests reached
the target's leader with a factory and a node id instead, saying in a comment that the
interface could not name it.  It can name it now: ``leader_client_for_nodes(shard_id,
nodes)``, whose own tests are the two at the end of this file.
"""

from dataclasses import dataclass
from typing import List, Tuple

import msgpack

from _wait import wait_until
from oxidedb.client.node_client import LocalNodeClient
from oxidedb.raft.recovery_runner import RecoveryRunner
from oxidedb.raft.shard_server import MigrationState, ShardedRaftCluster
from oxidedb.raft.state_machine import CommandType, MVCCStateMachine, serialize_command

#: Shard 0 of a one-shard map owns the whole keyspace, so no test here has to ask where a
#: key is.  It is served by node 1 alone, which leaves nodes 2 and 3 free to be the group a
#: move builds beside it - and a move onto a node that already serves the shard is refused,
#: so this is also the only placement these tests could use.
WHOLE_KEYSPACE = (b"", b"\xff")
SOURCE_NODES = [1]
TARGET_NODES = [2, 3]

#: Two rows a plain ``SET`` wrote, and one a transaction committed.
PLAIN_KEY = b"k01"
PLAIN_VALUE = b"v01"
PLAIN_TS = 1
SECOND_KEY = b"k02"
SECOND_VALUE = b"v02"
SECOND_TS = 2
TXN_KEY = b"k03"
TXN_VALUE = b"v03"
TXN_START_TS = 10
TXN_COMMIT_TS = 11

#: The keys in the order the source answers a range read, which is the order the copy
#: visits them in.
EXPECTED_KEYS = [PLAIN_KEY, SECOND_KEY, TXN_KEY]
EXPECTED_ROWS = [
    (PLAIN_KEY, PLAIN_VALUE, PLAIN_TS),
    (SECOND_KEY, SECOND_VALUE, SECOND_TS),
    (TXN_KEY, TXN_VALUE, TXN_COMMIT_TS),
]
#: The commands, written out here rather than recorded off the copy: what the copy has to
#: be shown to do is hand these arguments to ``serialize_command`` and no others.  The
#: plain rows carry ``start_ts=None`` explicitly, which is a byte in the command.
EXPECTED_COMMANDS = [
    serialize_command(CommandType.SET, key=PLAIN_KEY, value=PLAIN_VALUE,
                      timestamp=PLAIN_TS, start_ts=None),
    serialize_command(CommandType.SET, key=SECOND_KEY, value=SECOND_VALUE,
                      timestamp=SECOND_TS, start_ts=None),
    serialize_command(CommandType.SET, key=TXN_KEY, value=TXN_VALUE,
                      timestamp=TXN_COMMIT_TS, start_ts=TXN_START_TS),
]

#: The split's own rows: the point it splits at, a key below it that stays in shard 0, and
#: two above it that the new shard takes.
SPLIT_KEY = b"n"
KEPT_KEY = b"a1"
SPLIT_KEYS = [b"z01", b"z02"]


@dataclass
class _Outcome:
    """What one copy did: the keys it took, the rows that landed, what it proposed."""

    copied_keys: List[bytes]
    rows: List[Tuple[bytes, bytes, int]]
    commands: List[bytes]


def _set(machine, key, value, timestamp):
    return machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp)


def _prewrite(machine, key, value, start_ts):
    return machine.serialize_command(
        CommandType.PREWRITE, key=key, value=value, start_ts=start_ts, primary_key=key)


def _commit(machine, key, start_ts, commit_ts):
    return machine.serialize_command(
        CommandType.COMMIT, key=key, start_ts=start_ts, commit_ts=commit_ts)


def _started_cluster():
    """A three-node cluster holding shard 0 on node 1 and nothing else."""
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start(
        state_machine_factory=lambda: MVCCStateMachine(),
        shard_nodes={0: SOURCE_NODES},
        lock_cleaner_interval=None,
    )
    return cluster


def _write_rows(cluster):
    """The source's rows, one of them through a transaction that commits."""
    leader = wait_until(lambda: cluster._shard_leader_node(0),
                        message="shard 0 never elected a leader")
    machine = leader._state_machine
    assert leader.propose(_set(machine, PLAIN_KEY, PLAIN_VALUE, PLAIN_TS)).success
    assert leader.propose(_set(machine, SECOND_KEY, SECOND_VALUE, SECOND_TS)).success
    assert leader.propose(_prewrite(machine, TXN_KEY, TXN_VALUE, TXN_START_TS)).success
    assert leader.propose(_commit(machine, TXN_KEY, TXN_START_TS, TXN_COMMIT_TS)).success
    return leader


def _copy_rows(cluster):
    """Run one copy over a freshly built target group and record what it did.

    The commands are collected by wrapping the target leader's own ``propose``: the copy
    reaches the target through the client that wraps that node, so the bytes recorded are
    the bytes it committed, and a copy that built its own command and never sent it would
    record nothing.
    """
    source = wait_until(lambda: cluster._shard_leader_node(0),
                        message="shard 0 never elected a leader")
    cluster._ensure_group_on(TARGET_NODES, 0)
    target_node = wait_until(lambda: cluster._leader_on(TARGET_NODES, 0),
                             message="the group a move built elected no leader")

    state = MigrationState(shard_id=0, target_nodes=list(TARGET_NODES))
    state.rows = cluster._committed_rows(source, *WHOLE_KEYSPACE)

    commands: List[bytes] = []
    real_propose = target_node.propose

    def recording_propose(command, *args, **kwargs):
        commands.append(command)
        return real_propose(command, *args, **kwargs)

    target_client = wait_until(lambda: cluster.leader_client_for_nodes(0, TARGET_NODES),
                               message="the group a move built has no leader to ask")

    target_node.propose = recording_propose
    try:
        assert cluster._copy_rows(cluster.leader_client(0), target_client, state)
    finally:
        del target_node.propose

    rows = target_client.scan_versions(*WHOLE_KEYSPACE)
    return _Outcome(list(state.copied_keys), rows, commands)


def test_a_copy_writes_the_rows_the_source_holds_at_the_version_they_are():
    """The copy against an answer it did not produce: the rows and bytes the data implies.

    The source is read once and the copies go in as the versions they already are.  A copy
    stamped with the moment of the move is the newest thing that has ever happened to that
    key, so this is also the test that fails if the version stops travelling.
    """
    cluster = _started_cluster()
    try:
        _write_rows(cluster)
        outcome = _copy_rows(cluster)

        assert outcome.copied_keys == EXPECTED_KEYS
        assert outcome.rows == EXPECTED_ROWS
        assert outcome.commands == EXPECTED_COMMANDS
    finally:
        cluster.shutdown()


def test_only_the_row_a_transaction_wrote_carries_a_start_ts():
    """A row a plain ``SET`` wrote has no record, and a row a transaction did.

    The command has to say which of the two it is carrying: the group the row lands in
    validates read sets against the write record, so a row that arrives without the
    transaction's ``start_ts`` is a row whose history says it was never in one.
    """
    cluster = _started_cluster()
    try:
        _write_rows(cluster)
        source = cluster.leader_client(0)
        assert source.get_write_record(PLAIN_KEY) is None
        assert source.get_write_record(SECOND_KEY) is None
        assert source.get_write_record(TXN_KEY) == {
            "start_ts": TXN_START_TS, "commit_ts": TXN_COMMIT_TS}

        outcome = _copy_rows(cluster)

        carried = {msgpack.unpackb(command)["key"]: msgpack.unpackb(command)["start_ts"]
                   for command in outcome.commands}
        assert carried == {PLAIN_KEY: None, SECOND_KEY: None, TXN_KEY: TXN_START_TS}
    finally:
        cluster.shutdown()


def test_a_second_copy_of_the_same_rows_writes_nothing():
    """What makes a retry of a copy safe: a row already at that version is that row.

    A copy that died half way is retried from the beginning, and the half it had already
    moved has to be recognised rather than proposed again - which for a row the move
    already carried would be the same value at the same timestamp, and for anything that
    had moved on would be a second timestamp on a row that already had one.
    """
    cluster = _started_cluster()
    try:
        _write_rows(cluster)
        first = _copy_rows(cluster)
        second = _copy_rows(cluster)

        assert first.commands == EXPECTED_COMMANDS
        assert second.copied_keys == []
        assert second.commands == []
        assert second.rows == first.rows
    finally:
        cluster.shutdown()


def test_the_copy_a_split_makes_reads_the_source_by_range(monkeypatch):
    """The split's copy reads versions over the seam, once, and writes into the new shard.

    The rows come out of the source shard, so the versions they travel at have to come out
    of it too - and out of it the way a process reaches it rather than the way this process
    happens to be able to: one range read that answers for every row at once, in place of a
    read per key, which is a quorum round per key.  A copy that reached into the source's
    storage instead would pass every other test here, because in this process it answers.

    The group the rows go into is the one the new shard has.  A copy that reached for the
    group the routing table names would write them back into the group they are leaving -
    which is, at this moment, still the group that answers for them.
    """
    cluster = _started_cluster()
    reads = []
    written = []
    real_scan_versions = LocalNodeClient.scan_versions
    real_move_row = RecoveryRunner._move_row

    def recording_scan_versions(self, start_key, end_key, timestamp=None):
        reads.append((start_key, end_key))
        return real_scan_versions(self, start_key, end_key, timestamp)

    def recording_move_row(self, source, target, key, value, version):
        written.append((source._node, target._node, key, version))
        return real_move_row(self, source, target, key, value, version)

    monkeypatch.setattr(LocalNodeClient, "scan_versions", recording_scan_versions)
    monkeypatch.setattr(RecoveryRunner, "_move_row", recording_move_row)
    try:
        source = wait_until(lambda: cluster._shard_leader_node(0),
                            message="shard 0 never elected a leader")
        machine = source._state_machine
        assert source.propose(_set(machine, KEPT_KEY, b"kept", 1)).success
        for index, key in enumerate(SPLIT_KEYS):
            assert source.propose(_set(machine, key, b"v%d" % index, 2 + index)).success

        assert cluster.split_shard(0, SPLIT_KEY), cluster.split_error()

        new_shard = cluster._shard_leader_node(1)
        assert new_shard is not None, "the shard the split made has no leader"
        assert [key for _, _, key, _ in written] == SPLIT_KEYS
        readers = {node for node, _, _, _ in written}
        targets = {node for _, node, _, _ in written}
        assert readers == {source}, "the versions came from another shard"
        assert targets == {new_shard}, "the rows went into another group"
        assert len(reads) == 1, "the source was walked a key at a time"
        start, end = reads[0]
        assert start <= SPLIT_KEYS[0] and SPLIT_KEYS[-1] < end, reads[0]
    finally:
        cluster.shutdown()


def test_the_client_for_a_set_of_nodes_is_the_one_that_leads_them():
    """Asked about a group the table does not name, the handle that comes back leads it.

    The set is the caller's answer to "which group", and the answer has to be about that
    set: node 1 leads a group for shard 0 as well - the one a move copies out of - so a
    lookup that answered about the shard rather than about the set would hand back the
    wrong group's leader, and the copy would go into the shard it was leaving.
    """
    cluster = _started_cluster()
    try:
        _write_rows(cluster)
        cluster._ensure_group_on(TARGET_NODES, 0)
        leader = wait_until(lambda: cluster._leader_on(TARGET_NODES, 0),
                            message="the group a move built elected no leader")

        client = wait_until(lambda: cluster.leader_client_for_nodes(0, TARGET_NODES),
                            message="no client for the group a move built")

        assert client._node is leader, "the leader of the set is the node asked for"
        # And it is a leader to write through: a follower refuses a proposal, so a command
        # that lands is a command that landed on the group's leader.
        assert client.propose(
            serialize_command(CommandType.SET, key=b"k", value=b"v", timestamp=1)).success
    finally:
        cluster.shutdown()


def test_a_set_that_leads_nothing_answers_none():
    """None, because the caller waits; never a handle to a group that will refuse it all.

    Two ways for a set to lead nothing: its members hold no group for the shard at all -
    where a move's target is until it is built - or they hold it and are following.  The
    second is the one worth pinning, because a node that holds the group looks like an
    answer and is not: a write proposed through it is refused, and a copy that took the
    refusal for a missing row would leave the range half moved.
    """
    cluster = _started_cluster()
    try:
        _write_rows(cluster)
        assert cluster.leader_client_for_nodes(0, TARGET_NODES) is None, \
            "nothing holds the shard on those nodes yet"

        cluster._ensure_group_on(TARGET_NODES, 0)
        leader = wait_until(lambda: cluster._leader_on(TARGET_NODES, 0),
                            message="the group a move built elected no leader")
        followers = [node_id for node_id in TARGET_NODES if node_id != leader.node_id]

        assert cluster.leader_client_for_nodes(0, followers) is None, "they are following"
    finally:
        cluster.shutdown()
