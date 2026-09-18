"""A note a shard left itself about a job that was in flight, for a test to write by hand.

A split and a move both write down what they are doing, on every replica of the shard they
are about, before the first row moves - and what a node that comes back reads is that note.
The reading half lives in ``oxidedb/raft/shard_server.py`` and is driven from
``ShardedRaftCluster``.  A test that wants a *process* to come back with a job in flight
cannot start one, because nothing outside that class can begin a split or a move; so it
writes the note itself, in the shape ``_remember_split`` and ``_remember_migration`` write
and ``_load_split_record`` and ``_load_migration_record`` read.

Two things keep that honest rather than a second copy of the format:

* the note goes in through ``RaftStorage.save_admin``, the same call the node makes, under
  the same key.  A node's own bookkeeping is a note by name and not a file, and an encoding
  of it written somewhere by hand would be a test that passes while the node reads a place
  nothing wrote to;
* it goes into the directory the node builds that shard's storage in - ``<data-dir>/shard-N``
  - which is ``ClusterNode._shard_storage``'s arithmetic.  That one *is* a second copy of a
  name, and it is deliberate: a test that cannot name the directory cannot write a note
  before the node exists.  If the layout moves, the tests that use this stop finding their
  own notes and say so.
"""

import os
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

import msgpack

from oxidedb.raft.storage import create_raft_storage

#: The two kinds of note.  These are the prefixes the keys are filed under, so the string
#: is the product's rather than a name invented here.
SPLIT = "split"
MOVE = "migrate"


def shard_storage_dir(data_dir: str, shard_id: int) -> str:
    """Where a node keeps the storage of one of its shard groups."""
    return os.path.join(data_dir, f"shard-{shard_id}")


@dataclass(frozen=True)
class PendingNote:
    """A job a shard was in the middle of, in the shape the note on disk has.

    ``kind`` says which of the two it is, and which of the other fields mean anything: a
    split carries the point and the id of the shard it is creating, a move carries the set
    the shard is leaving and the set it is going to.
    """

    kind: str
    shard_id: int
    split_key: bytes = b""
    new_shard_id: int = 0
    source_nodes: Tuple[int, ...] = ()
    target_nodes: Tuple[int, ...] = ()

    @classmethod
    def split(cls, shard_id: int, split_key: bytes, new_shard_id: int) -> "PendingNote":
        """The note a split at ``split_key`` of ``shard_id`` leaves behind."""
        return cls(kind=SPLIT, shard_id=shard_id, split_key=split_key,
                   new_shard_id=new_shard_id)

    @classmethod
    def move(cls, shard_id: int, source_nodes: Iterable[int],
             target_nodes: Iterable[int]) -> "PendingNote":
        """The note a move of ``shard_id`` from one set of nodes to another leaves."""
        return cls(kind=MOVE, shard_id=shard_id,
                   source_nodes=tuple(source_nodes), target_nodes=tuple(target_nodes))

    @property
    def key(self) -> str:
        """The name the note is filed under, which is also what says which kind it is."""
        return f"{self.kind}/{self.shard_id}"

    def record(self) -> bytes:
        """The note's bytes, field for field as the node's own writer packs them."""
        if self.kind == SPLIT:
            return msgpack.packb({"shard_id": self.shard_id,
                                  "split_key": self.split_key,
                                  "new_shard_id": self.new_shard_id},
                                 use_bin_type=True)
        return msgpack.packb({"shard_id": self.shard_id,
                              "source_nodes": list(self.source_nodes),
                              "target_nodes": list(self.target_nodes)},
                             use_bin_type=True)


def write_pending_note(data_dir: str, shard_id: int, note: PendingNote) -> None:
    """Write ``note`` into ``shard_id``'s storage under ``data_dir``, then let it go.

    Meant to be called before the node is started: the storage belongs to that node, it is
    opened and closed here, and the node finds the note where it left it.
    """
    storage = create_raft_storage(shard_storage_dir(data_dir, shard_id))
    try:
        storage.save_admin(note.key, note.record())
    finally:
        storage.close()


def pending_note(data_dir: str, shard_id: int, kind: str) -> Optional[bytes]:
    """The note of that kind still on disk, or None - what a test reports having found."""
    storage = create_raft_storage(shard_storage_dir(data_dir, shard_id))
    try:
        return storage.load_admin(f"{kind}/{shard_id}")
    finally:
        storage.close()
