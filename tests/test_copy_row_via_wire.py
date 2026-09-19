"""The copy a move makes, written twice: over node objects, and over a client.

A recovery that finishes a move inside a process of its own has no ``MemoryRaftNode`` to
read: it holds ids and addresses, and the leader of a shard may be in another process
altogether.  So the copy has to be expressible through ``NodeClient`` - a range read that
carries versions, a write record, and a proposal - and the way to show that it is, before
anything is switched over to it, is to write it a second time beside the first and compare
the two.

These tests are that comparison.  One cluster's worth of rows goes through both copies, and
what is pinned is what each one did: the keys it copied, the rows the target ends up with
(the version included), and the command bytes it proposed - byte for byte, because a copy
that carried a row across as a *new* write would pass every other check here.  Both branches
of the write-record test are covered: a row a plain ``SET`` wrote has no record, and a row a
transaction committed has one, and only the second travels with a ``start_ts``.

The version read is what makes this possible at all: ``KeyValuePair`` carried no version
until recently, and without one a copy cannot know which moment to stamp a row with - it
would arrive in the new group as the newest thing that has ever happened to that key.

Which group the rows are going *into* is the other half of that.  The group a move builds is
not the group the routing table names yet, so ``leader_client(shard_id)`` cannot answer for
it - that lookup is the table's answer - and the first version of these tests reached the
target's leader with a factory and a node id instead, saying in a comment that the interface
could not name it.  It can name it now: ``leader_client_for_nodes(shard_id, nodes)``, whose
own tests are the two at the end of this file.
"""

from dataclasses import dataclass
from typing import List, Tuple

import msgpack
import pytest

from _wait import wait_until
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
#: The commands, built here rather than taken from either implementation: what the copy
#: has to be shown to do is hand these arguments to ``serialize_command`` and no others.
#: The plain rows carry ``start_ts=None`` explicitly, which is a byte in the command.
EXPECTED_COMMANDS = [
    serialize_command(CommandType.SET, key=PLAIN_KEY, value=PLAIN_VALUE,
                      timestamp=PLAIN_TS, start_ts=None),
    serialize_command(CommandType.SET, key=SECOND_KEY, value=SECOND_VALUE,
                      timestamp=SECOND_TS, start_ts=None),
    serialize_command(CommandType.SET, key=TXN_KEY, value=TXN_VALUE,
                      timestamp=TXN_COMMIT_TS, start_ts=TXN_START_TS),
]

#: The two implementations, as a test parameter.
BOTH = ["objects", "clients"]


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


def _copy_rows(cluster, how):
    """Run one copy over a freshly built target group and record what it did.

    The commands are collected by wrapping the target leader's own ``propose``: both
    implementations reach it - one as the node object, the other through the client that
    wraps the same object - so the bytes compared are the bytes either of them committed.
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

    # The same leader, asked for the way the copy asks for it and reached for the way the
    # recording needs it; the copy is handed whichever one it takes its target through.
    target_client = wait_until(lambda: cluster.leader_client_for_nodes(0, TARGET_NODES),
                               message="the group a move built has no leader to ask")

    target_node.propose = recording_propose
    try:
        if how == "objects":
            assert cluster._copy_rows(source, target_node, state)
        else:
            assert cluster._copy_rows_via_wire(cluster.leader_client(0), target_client,
                                               state)
    finally:
        del target_node.propose

    rows = target_client.scan_versions(*WHOLE_KEYSPACE)
    return _Outcome(list(state.copied_keys), rows, commands)


def _copy_in_its_own_cluster(how):
    """The same rows through one implementation, in a cluster of its own."""
    cluster = _started_cluster()
    try:
        _write_rows(cluster)
        return _copy_rows(cluster, how)
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("how", BOTH)
def test_a_copy_writes_the_rows_the_source_holds_at_the_version_they_are(how):
    """Each copy on its own, against an answer neither of them produced.

    Comparing the two implementations to each other shows they agree; it does not show
    that they are right, and two copies that were wrong in the same way would pass.  So
    this pins both of them to the rows and the commands the data implies.
    """
    cluster = _started_cluster()
    try:
        _write_rows(cluster)
        outcome = _copy_rows(cluster, how)

        assert outcome.copied_keys == EXPECTED_KEYS
        assert outcome.rows == EXPECTED_ROWS
        assert outcome.commands == EXPECTED_COMMANDS
    finally:
        cluster.shutdown()


def test_the_two_implementations_write_the_same_bytes():
    """The assertion the whole exercise is for: same rows, same commands, byte for byte.

    Two clusters rather than one, because a copy into a group that already holds the rows
    copies nothing: running them one after the other would compare the second one's
    silence with the first one's work.  The rows are written the same way both times, so
    the two sources are the same source.
    """
    over_objects = _copy_in_its_own_cluster("objects")
    over_clients = _copy_in_its_own_cluster("clients")

    assert over_clients.copied_keys == over_objects.copied_keys
    assert over_clients.rows == over_objects.rows
    assert over_clients.commands == over_objects.commands


@pytest.mark.parametrize("how", BOTH)
def test_only_the_row_a_transaction_wrote_carries_a_start_ts(how):
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

        outcome = _copy_rows(cluster, how)

        carried = {msgpack.unpackb(command)["key"]: msgpack.unpackb(command)["start_ts"]
                   for command in outcome.commands}
        assert carried == {PLAIN_KEY: None, SECOND_KEY: None, TXN_KEY: TXN_START_TS}
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("how", BOTH)
def test_a_second_copy_of_the_same_rows_writes_nothing(how):
    """What makes a retry of a copy safe: a row already at that version is that row.

    A copy that died half way is retried from the beginning, and the half it had already
    moved has to be recognised rather than proposed again - which for a row the move
    already carried would be the same value at the same timestamp, and for anything that
    had moved on would be a second timestamp on a row that already had one.
    """
    cluster = _started_cluster()
    try:
        _write_rows(cluster)
        first = _copy_rows(cluster, how)
        second = _copy_rows(cluster, how)

        assert first.commands == EXPECTED_COMMANDS
        assert second.copied_keys == []
        assert second.commands == []
        assert second.rows == first.rows
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
