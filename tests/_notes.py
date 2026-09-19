"""A note a shard left itself about a job that was in flight, for a test to write by hand.

A split and a move both write down what they are doing, on every replica of the shard they
are about, before the first row moves - and what a node that comes back reads is that note.
``oxidedb/raft/recovery_notes.py`` holds that note: the name it is filed under, the record
it is, and the reading and writing of both.  ``ShardedRaftCluster`` calls it, and so does
this file.

What this file adds is the one thing a test needs and a node does not: a way to write a
note *before* the node that reads it exists.  A test that wants a process to come back with
a job in flight cannot start one, because nothing outside ``ShardedRaftCluster`` can begin
a split or a move; so it opens that shard's storage itself and leaves the note there.

Two things keep that honest rather than a second copy of the format:

* the note goes in through ``recovery_notes.save_note`` - the same call the node makes,
  under the same key, into ``RaftStorage.save_admin``.  A node's own bookkeeping is a note
  by name and not a file, and an encoding of it written somewhere by hand would be a test
  that passes while the node reads a place nothing wrote to;
* it goes into the directory the node builds that shard's storage in - ``<data-dir>/shard-N``
  - which is ``ClusterNode._shard_storage``'s arithmetic.  That one *is* a second copy of a
  name, and it is deliberate: a test that cannot name the directory cannot write a note
  before the node exists.  If the layout moves, the tests that use this stop finding their
  own notes and say so.
"""

import os
from typing import Optional

from oxidedb.raft.recovery_notes import (MIGRATION_RECORD_PREFIX, SPLIT_RECORD_PREFIX,
                                         PendingNote, record_key, save_note)
from oxidedb.raft.storage import create_raft_storage

#: The two kinds of note, under the names the tests read better by.  These are the prefixes
#: the keys are filed under, so the string is the product's rather than a name invented here.
SPLIT = SPLIT_RECORD_PREFIX
MOVE = MIGRATION_RECORD_PREFIX


def shard_storage_dir(data_dir: str, shard_id: int) -> str:
    """Where a node keeps the storage of one of its shard groups."""
    return os.path.join(data_dir, f"shard-{shard_id}")


def write_pending_note(data_dir: str, shard_id: int, note: PendingNote) -> None:
    """Write ``note`` into ``shard_id``'s storage under ``data_dir``, then let it go.

    Meant to be called before the node is started: the storage belongs to that node, it is
    opened and closed here, and the node finds the note where it left it.
    """
    storage = create_raft_storage(shard_storage_dir(data_dir, shard_id))
    try:
        save_note(storage, note)
    finally:
        storage.close()


def pending_note(data_dir: str, shard_id: int, kind: str) -> Optional[bytes]:
    """The note of that kind still on disk, or None - what a test reports having found."""
    storage = create_raft_storage(shard_storage_dir(data_dir, shard_id))
    try:
        return storage.load_admin(record_key(kind, shard_id))
    finally:
        storage.close()
