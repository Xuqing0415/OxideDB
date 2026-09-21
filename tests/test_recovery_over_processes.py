"""A process that comes back in the middle of a split, and what it does with the note.

A split writes down what it is doing before the first row moves, and what a node that comes
back does about that note is decided by ``recover_splits`` - the recovery of the side the
node is, which ``ClusterNode.start`` drives the way ``ShardedRaftCluster.start`` drives it:
the notes are read before the publisher starts and finished after it.

The first test asks the half of the recovery that is about the note rather than about the
table - the freeze.  A node that died between its copy and its proposal comes back
with rows in a group the routing table has not been told about, and the one thing that must
not happen is the source taking more rows: a row let into it now is a row nothing will carry
across.  So the note is read, the shard is frozen again, and it stays frozen until somebody
proposes the split again.  The other half is the second test: where the table already names
the range, the restart is what finishes the split, and that is the case a process that ran
once, published a table and was killed comes back as - the one the two phases of
``ClusterNode.start`` are there for, and the one nothing else in this file can reach,
because the note has to outlive the process that reads it.

A move is the same idea one size up: its note names the set the shard is leaving and the one
it is going to, and which of the two the routing table names says how far the move got.  Over
processes there is no move to *make* - the launcher serves every shard on every node, and a
move onto a node that already serves the shard is refused where a move begins - so a note is
the only way one gets into a process, and the half a process can be asked to finish is the one
a landing proposal leaves behind: the note goes, and so does the group the shard left.
"""

import os
import time

import pytest
from _cluster import read_when_ready, start_cluster, write_when_ready
from _notes import MOVE, SPLIT, PendingNote, pending_note, write_pending_note
from _wait import wait_until
from oxidedb.client import NodeUnreachable, RemoteMetadataClient, RemoteNodeClient
from oxidedb.raft.state_machine import CommandType, serialize_command

#: Where the split says it was made, and keys on both sides of it.
SPLIT_KEY = b"m"
KEY = b"n"
BELOW = b"a"
ABOVE = b"z"

#: The value every row here is written with, so that a row that crossed can be told from a
#: row that was written again at the other end.
VALUE = b"a value"

#: The keyspace one shard of a one-shard cluster owns, which is what the table names before
#: any split and what the recovery has to move it out of.
WHOLE_KEYSPACE = (b"", b"\xff")


def table_now(client):
    """The routing table, read from outside over the wire, or None while it answers nothing.

    The group that holds it is electing at the same moment a node that comes back is
    reading, so a read that lands in that window is told there is nothing there: a moment,
    not an answer, and the reason every wait here reads through this one call.
    """
    try:
        return client.table(refresh=True)
    except RuntimeError:
        return None


def routes(client):
    """What the routing table names right now, and nothing while it cannot be read."""
    table = table_now(client)
    return {} if table is None else table.routes()


def published(client):
    """Shard 0, once the table names its range *and* the nodes that serve it, or None.

    Both halves are what a restart reads: the note says which shard was splitting, and the
    side that comes back asks the table who serves it - a range with no replica set is a
    note that cannot be acted on.  The publisher writes them in one pass as two proposals,
    so waiting for the second is waiting for both.
    """
    table = table_now(client)
    placement = None if table is None else table.shard(0)
    if placement is None or (placement.start, placement.end) != WHOLE_KEYSPACE:
        return None
    return placement if placement.nodes else None


def split_landed(client):
    """The routes once the table names more than one shard, or None while it names one."""
    named = routes(client)
    return named if len(named) > 1 else None


def leading_node(client, shard_id):
    """The node the table names as leading ``shard_id``, once the publisher has said so.

    A write goes to the node that leads a shard and to no other: a three-node group has one
    leader and two nodes that refuse and name it, and following that name is the caller's
    step.  So a test that wants a row written reads the leader out of the table, which is
    what a publisher is for, rather than talking to whichever node it happens to know.
    """
    def named():
        table = table_now(client)
        placement = None if table is None else table.shard(shard_id)
        return None if placement is None else placement.leader_id

    return wait_until(named, message=f"the table never named a leader for shard {shard_id}")


def test_a_process_that_comes_back_with_a_split_note_does_not_take_rows(tmp_path):
    """A node with a split written down in its own storage comes back refusing new rows.

    The note is what a node that died between its copy and its proposal leaves behind, and
    the freeze is the part that has to come back with it: until the routing table says where
    the range went, a row let into the source is a row nothing will carry across.  One node
    and one shard, so there is no election and no second replica to confuse the answer with -
    what is being asked is whether the note is read at all.

    This node keeps no table, which is what leaves the question about the note: the range is
    never published, so the split cannot be proposed, and what is left of the recovery is the
    half this test is about.  A node whose table does name the range finishes the split
    instead, and takes rows again once it has.
    """
    base = str(tmp_path / "cluster")
    data_dir = os.path.join(base, "node1")
    note = PendingNote.split(shard_id=0, split_key=SPLIT_KEY, new_shard_id=1)
    write_pending_note(data_dir, 0, note)

    with start_cluster(num_nodes=1, num_shards=1, base_dir=base,
                       bootstrap=False) as running:
        client = RemoteNodeClient(running.shard_address(0))
        result = write_when_ready(
            client, serialize_command(CommandType.SET, key=KEY, value=VALUE))

        assert not result.success, (
            f"node 1 came back with {note.key!r} in its own storage and took a row for "
            f"shard 0 anyway: the note is still "
            f"{pending_note(data_dir, 0, SPLIT) is not None} on disk, and the write "
            f"answered {result.error_code} ({result.error_msg!r})")
        assert pending_note(data_dir, 0, SPLIT) is not None, (
            "shard 0 refused the row and the note is gone: a split that was not "
            "proposed is one somebody still has to finish")


def test_a_process_that_comes_back_with_a_split_note_finishes_the_split(tmp_path):
    """A node killed mid-split finishes it on the way back up: table, rows, note and thaw.

    The first start publishes the table and stops, and what the second start has is that
    table, the rows in the shard's own storage and a note - which is what a process that
    died between its copy and its proposal leaves behind, since the note is written before
    the first row moves.  Nothing outside the recovery can finish a split, so the four
    things asked for here are the whole of it: the table names the range the note was
    about, the rows above the split point answer at the new shard with the value they were
    written with, the note is gone, and the source takes rows again - the thaw being the
    last step of a split rather than a thing of its own.

    A one-node cluster: there is no second copy of anything here to be confused with an
    answer, and the node that comes back is the one that published the table it comes back
    to.
    """
    base = str(tmp_path / "cluster")
    note = PendingNote.split(shard_id=0, split_key=SPLIT_KEY, new_shard_id=1)

    with start_cluster(num_nodes=1, num_shards=1, base_dir=base) as cluster:
        client = RemoteNodeClient(cluster.shard_address(0))
        for key in (BELOW, KEY, ABOVE):
            written = write_when_ready(
                client, serialize_command(CommandType.SET, key=key, value=VALUE))
            assert written.success, written.error_msg

        table = RemoteMetadataClient(cluster.metadata_seeds)
        wait_until(lambda: published(table),
                   message="a one-shard cluster never published its shard")

        data_dir = cluster.data_dirs[1]
        cluster.stop_node(1, crash=True)
        # Written while the node is down, which is the one thing a test can do and a node
        # cannot: the note goes in where the process that was killed would have left it, and
        # the process that comes back is the one that has to act on it.
        write_pending_note(data_dir, 0, note)

        cluster.start_node(1)

        named = wait_until(lambda: split_landed(table),
                           message="a node came back with a split note for shard 0 and the "
                                   "table never named the range it was about")
        assert named == {0: (b"", SPLIT_KEY), 1: (SPLIT_KEY, b"\xff")}, named
        assert pending_note(data_dir, 0, SPLIT) is None, (
            "the table names the split the note was about and the note is still on disk: "
            "a recovery that does not drop the note it finished would do the work again")

        crossed = read_when_ready(RemoteNodeClient(cluster.shard_address(1)), KEY)
        assert crossed.success, crossed.error_msg
        assert crossed.value == VALUE, (
            f"shard 1 answers for {KEY!r} with {crossed.value!r}, and the row was written "
            f"with {VALUE!r}: the range is the table's and the rows never crossed")

        again = write_when_ready(
            RemoteNodeClient(cluster.shard_address(0)),
            serialize_command(CommandType.SET, key=BELOW, value=b"another value"))
        assert again.success, (
            f"the source refused a row below the split point with {again.error_code} "
            f"({again.error_msg!r}): the shard is still frozen under a split that is done")


#: The move the test below writes down: shard 0 leaving three nodes for two of them.  A move
#: onto a node that already serves the shard is one the launcher cannot make - every node
#: serves every shard - so this is a note written by hand, and the second set is here
#: because a note is what names it, not because a process could ever propose it.
MOVE_FROM = (1, 2, 3)
MOVE_TO = (2, 3)


def test_a_process_that_comes_back_after_its_move_landed_lets_the_shard_go(tmp_path):
    """A node killed after the table was told finishes the move on the way back up.

    What a move leaves behind when it stops between its proposal and its cleanup is the
    table the proposal wrote, naming the set the shard went to, and the note in the storage
    of the group it left.  Both are written here while the node is down - the table by a
    client of the table's group, the note by the call the node itself makes - because
    nothing outside ``ShardedRaftCluster`` can begin a move: the launcher serves every shard
    on every node, so there is no set to move a shard to that the table would not refuse,
    and the half of a move a process can be asked to finish is the one after the proposal,
    which asks the table for a read and nothing else.

    That half is ``_commit_move``: the note goes and the group the shard left is closed.  So
    a node that comes back is asked for two things - the note is gone from its own storage,
    and its port for the shard stops answering - and the second is what tells this half of
    the recovery from the half before it: finding the same note and the same table, the
    other half would copy into the set the table names and leave the shard served by both
    groups, which is the one thing a move that has already landed must not come back to.
    """
    base = str(tmp_path / "cluster")
    with start_cluster(num_nodes=3, num_shards=1, base_dir=base) as cluster:
        table = RemoteMetadataClient(cluster.metadata_seeds)
        wait_until(lambda: published(table),
                   message="a one-shard cluster never published its shard")

        client = RemoteNodeClient(cluster.shard_address(0, node_id=leading_node(table, 0)))
        try:
            written = write_when_ready(
                client, serialize_command(CommandType.SET, key=KEY, value=VALUE))
        finally:
            client.close()
        assert written.success, written.error_msg

        data_dir = cluster.data_dirs[1]
        cluster.stop_node(1, crash=True)

        # What the move left: the table its proposal wrote, and the note.  The addresses are
        # the ones the nodes themselves listen on, worked out the way every address here is
        # rather than guessed at - a placement the table cannot name an address for is one a
        # client cannot be sent to.
        def tell_the_table():
            return table.set_shard_nodes(
                0, list(MOVE_TO), {node_id: cluster.shard_address(0, node_id)
                                   for node_id in MOVE_TO})

        started = time.monotonic()
        told = tell_the_table()
        attempts = 1
        while not told.success and time.monotonic() - started < 20.0:
            time.sleep(0.05)
            told = tell_the_table()
            attempts += 1
        print('DIAG set_shard_nodes attempts=%d took=%.3fs ok=%s last=%r'
              % (attempts, time.monotonic() - started, told.success, told.error_msg))
        assert told.success, told.error_msg
        note = PendingNote.move(shard_id=0, source_nodes=MOVE_FROM, target_nodes=MOVE_TO)
        write_pending_note(data_dir, 0, note)

        cluster.start_node(1)

        assert pending_note(data_dir, 0, MOVE) is None, (
            "the node came back with a move note for a shard the table already says it "
            "left, and the note is still on disk: a recovery that read it would have dropped "
            "it, and nothing else drops a move's note")

        left = RemoteNodeClient(cluster.shard_address(0, node_id=1))
        try:
            with pytest.raises(NodeUnreachable):
                left.get(KEY)
        finally:
            left.close()

        placed = table.table(refresh=True).shard(0)
        assert placed.nodes == list(MOVE_TO), (
            f"the table names {placed.nodes} for shard 0, and the move whose note the node "
            f"dropped was going to {list(MOVE_TO)}: a recovery that rewrote the table would "
            f"be undoing the proposal it came back to finish")
