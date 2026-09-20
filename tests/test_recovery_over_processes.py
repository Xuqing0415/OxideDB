"""A process that comes back in the middle of a split, and what it does with the note.

A split writes down what it is doing before the first row moves, and what a node that comes
back does about that note is decided by ``recover_splits`` - the recovery of the side the
node is, which ``ClusterNode.start`` drives the way ``ShardedRaftCluster.start`` drives it:
the notes are read before the publisher starts and finished after it.

There is one test, and it asks the half of the recovery that is about the note rather than
about the table - the freeze.  A node that died between its copy and its proposal comes back
with rows in a group the routing table has not been told about, and the one thing that must
not happen is the source taking more rows: a row let into it now is a row nothing will carry
across.  So the note is read, the shard is frozen again, and it stays frozen until somebody
proposes the split again.  The other half, where the table already names the range and the
restart is what finishes the split, is the case a node that ran once and came back is.
"""

import os

from _cluster import start_cluster, write_when_ready
from _notes import SPLIT, PendingNote, pending_note, write_pending_note
from oxidedb.client import RemoteNodeClient
from oxidedb.raft.state_machine import CommandType, serialize_command

#: Where the split says it was made, and a key above it.
SPLIT_KEY = b"m"
KEY = b"n"


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
            client, serialize_command(CommandType.SET, key=KEY, value=b"a value"))

        assert not result.success, (
            f"node 1 came back with {note.key!r} in its own storage and took a row for "
            f"shard 0 anyway: the note is still "
            f"{pending_note(data_dir, 0, SPLIT) is not None} on disk, and the write "
            f"answered {result.error_code} ({result.error_msg!r})")
        assert pending_note(data_dir, 0, SPLIT) is not None, (
            "shard 0 refused the row and the note is gone: a split that was not "
            "proposed is one somebody still has to finish")
