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
"""

import os

from _cluster import read_when_ready, start_cluster, write_when_ready
from _notes import SPLIT, PendingNote, pending_note, write_pending_note
from _wait import wait_until
from oxidedb.client import RemoteMetadataClient, RemoteNodeClient
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
