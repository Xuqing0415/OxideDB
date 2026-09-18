"""A process that comes back in the middle of a split, and what it does with the note.

A split and a move write down what they are doing before the first row moves, and what a node
that comes back does about that note is decided by ``recover_splits`` and
``recover_migrations`` - two methods of ``ShardedRaftCluster``, called from its ``start`` and
its ``start_network``, and called from nowhere on the path a node takes when it is a process
of its own (``oxidedb/launcher.py``).  ``docs/recovery.md`` is the design for closing that
gap.  What this file holds is the state it starts from: a note written by hand into a node's
own storage, and a node started over it.

There is one test, and it is marked ``xfail``.  That is the point of it rather than an
apology: what it asserts is what should happen, so the marker is what comes off when the
recovery reaches the process path, and a plain run reports the gap as a known failure rather
than leaving the suite red.  For the photograph underneath - what a node does with a note
today - run it with ``--runxfail``: it takes rows for a shard it has written down as mid
split, and the note is still there afterwards.
"""

import os

import pytest

from _cluster import start_cluster, write_when_ready
from _notes import SPLIT, PendingNote, pending_note, write_pending_note
from oxidedb.client import RemoteNodeClient
from oxidedb.raft.state_machine import CommandType, serialize_command

#: Where the split says it was made, and a key above it.
SPLIT_KEY = b"m"
KEY = b"n"


@pytest.mark.xfail(strict=True, reason=(
    "recovery is ShardedRaftCluster's, and a node started as a process does not read its "
    "own notes (docs/recovery.md)"))
def test_a_process_that_comes_back_with_a_split_note_does_not_take_rows(tmp_path):
    """A node with a split written down in its own storage comes back refusing new rows.

    The note is what a node that died between its copy and its proposal leaves behind, and
    the freeze is the part that has to come back with it: until the routing table says where
    the range went, a row let into the source is a row nothing will carry across.  One node
    and one shard, so there is no election and no second replica to confuse the answer with -
    what is being asked is whether the note is read at all.
    """
    base = str(tmp_path / "cluster")
    data_dir = os.path.join(base, "node1")
    note = PendingNote.split(shard_id=0, split_key=SPLIT_KEY, new_shard_id=1)
    write_pending_note(data_dir, 0, note)

    with start_cluster(num_nodes=1, num_shards=1, base_dir=base) as running:
        client = RemoteNodeClient(running.shard_address(0))
        result = write_when_ready(
            client, serialize_command(CommandType.SET, key=KEY, value=b"a value"))

        assert not result.success, (
            f"node 1 came back with {note.key!r} in its own storage and took a row for "
            f"shard 0 anyway: the note is still "
            f"{pending_note(data_dir, 0, SPLIT) is not None} on disk, and the write "
            f"answered {result.error_code} ({result.error_msg!r})")
