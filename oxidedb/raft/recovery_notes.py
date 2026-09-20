"""What a shard writes down about a job it was in the middle of, and how it is read back.

A split and a move both write a note before the first row moves, on every replica of the
shard they are about, and a node that comes back reads it out of its own storage: between
the copy and the proposal the work exists nowhere else, so the one place it can be written
down is the thing that survives a restart.

It lives here rather than in ``shard_server.py`` because the note is the part of recovery
that two start-up paths share.  A cluster object and a node that is a process of its own
hold different things - one holds every replica of a group, the other holds itself - but
what they write and what they look for is the same bytes under the same name, and a second
writer would be a second format.

Nothing here reaches into a node.  A note goes to the storages the caller holds, which is
the only thing either side has to hand: for a restart that is the replicas this process
holds, and the invariant that makes it enough is that *every* replica writes a copy (see
``RecoveryRunner.remember_split``).
"""

import msgpack

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple

#: Where a shard's in-progress split is written down, within that shard's storage.
SPLIT_RECORD_PREFIX = "split"

#: The same, for a shard that is being moved to another group.
MIGRATION_RECORD_PREFIX = "migrate"


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
        return cls(kind=SPLIT_RECORD_PREFIX, shard_id=shard_id, split_key=split_key,
                   new_shard_id=new_shard_id)

    @classmethod
    def move(cls, shard_id: int, source_nodes: Iterable[int],
             target_nodes: Iterable[int]) -> "PendingNote":
        """The note a move of ``shard_id`` from one set of nodes to another leaves."""
        return cls(kind=MIGRATION_RECORD_PREFIX, shard_id=shard_id,
                   source_nodes=tuple(source_nodes), target_nodes=tuple(target_nodes))

    @property
    def key(self) -> str:
        """The name the note is filed under, which is also what says which kind it is."""
        return record_key(self.kind, self.shard_id)

    def record(self) -> bytes:
        """The note's bytes, field for field as the node's own writer packs them."""
        if self.kind == SPLIT_RECORD_PREFIX:
            fields = {"shard_id": self.shard_id,
                      "split_key": self.split_key,
                      "new_shard_id": self.new_shard_id}
        else:
            fields = {"shard_id": self.shard_id,
                      "source_nodes": list(self.source_nodes),
                      "target_nodes": list(self.target_nodes)}
        return msgpack.packb(fields, use_bin_type=True)


def record_key(kind: str, shard_id: int) -> str:
    """The name a note of that kind about that shard is filed under."""
    return f"{kind}/{shard_id}"


def note_from_record(kind: str, shard_id: int,
                     record: Dict[str, Any]) -> Optional[PendingNote]:
    """``record`` as the note it was written from, or None if it is about another shard.

    A note about another shard is what a storage reused by an id would look like, and
    there is nothing a reader can do with it - so it is refused rather than acted on,
    which is how a node avoids finishing a job that never belonged to it.
    """
    if int(record["shard_id"]) != shard_id:
        return None
    if kind == SPLIT_RECORD_PREFIX:
        return PendingNote.split(shard_id, bytes(record["split_key"]),
                                 int(record["new_shard_id"]))
    return PendingNote.move(shard_id,
                            [int(node_id) for node_id in record["source_nodes"]],
                            [int(node_id) for node_id in record["target_nodes"]])


def save_note(storage: Any, note: PendingNote) -> None:
    """Write ``note`` into one replica's storage."""
    storage.save_admin(note.key, note.record())


def load_note(storage: Any, shard_id: int, kind: str) -> Optional[PendingNote]:
    """The note of that kind about ``shard_id`` in one replica's storage, or None."""
    raw = storage.load_admin(record_key(kind, shard_id))
    if raw is None:
        return None
    return note_from_record(kind, shard_id, msgpack.unpackb(raw, raw=False))


def write_note(storages: Iterable[Any], note: PendingNote) -> None:
    """Write ``note`` to every storage in ``storages``, which is every replica held."""
    for storage in storages:
        storage.save_admin(note.key, note.record())


def read_note(storages: Iterable[Any], shard_id: int, kind: str) -> Optional[PendingNote]:
    """The note of that kind about ``shard_id``, as any one of ``storages`` has it.

    Any one of them, because every replica writes its own copy: a reader that insisted on
    a particular replica would fail on the one that has not come back yet.
    """
    for storage in storages:
        note = load_note(storage, shard_id, kind)
        if note is not None:
            return note
    return None


def forget_note(storages: Iterable[Any], shard_id: int, kind: str) -> None:
    """Drop that note from every storage in ``storages``.

    A storage that is not there is not in the iterable: the caller can only name the
    replicas it still holds, and a replica whose storage has been closed for good cannot
    be reached at all.  Deleting a note that is already gone is a no-op, which is what the
    second run of a commit finds.
    """
    for storage in storages:
        storage.delete_admin(record_key(kind, shard_id))
