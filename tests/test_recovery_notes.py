"""The note a shard leaves itself, as both start-up paths will read it.

A split and a move both write a note before the first row moves, and what a node that comes
back does about it is the recovery's business.  This file pins the layer underneath: the
name a note is filed under, the record it holds, and the rules a reader follows - a note
about another shard is refused, a replica that never wrote one is not mistaken for one that
did, and forgetting one is safe to do twice.

That layer is ``oxidedb/raft/recovery_notes.py``, and it is the only place the format is
written down.  A cluster object and a node that is a process of its own hold different
numbers of replicas of a shard; what they write and what they look for is the same bytes
under the same name, which is why checking it needs no cluster at all.
"""

import msgpack

from oxidedb.raft.recovery_notes import (MIGRATION_RECORD_PREFIX, SPLIT_RECORD_PREFIX,
                                         PendingNote, forget_note, load_note, read_note,
                                         record_key, save_note, write_note)
from oxidedb.raft.storage import create_raft_storage


def _replicas(count: int):
    """``count`` empty replicas.  A note is written by one into each, and that is all."""
    return [create_raft_storage() for _ in range(count)]


def test_a_split_note_is_filed_under_the_split_prefix():
    note = PendingNote.split(shard_id=0, split_key=b"m", new_shard_id=1)

    assert note.key == "split/0"
    assert note.key == record_key(SPLIT_RECORD_PREFIX, 0)


def test_a_move_note_is_filed_under_the_move_prefix():
    note = PendingNote.move(shard_id=2, source_nodes=[1, 2], target_nodes=[3])

    assert note.key == "migrate/2"
    assert note.kind == MIGRATION_RECORD_PREFIX


def test_a_split_note_comes_back_as_it_went_in():
    storage = create_raft_storage()
    note = PendingNote.split(shard_id=0, split_key=b"\x80m", new_shard_id=7)

    save_note(storage, note)

    assert load_note(storage, 0, SPLIT_RECORD_PREFIX) == note


def test_a_move_note_comes_back_as_it_went_in():
    storage = create_raft_storage()
    note = PendingNote.move(shard_id=2, source_nodes=[1, 2], target_nodes=[3])

    save_note(storage, note)

    assert load_note(storage, 2, MIGRATION_RECORD_PREFIX) == note


def test_the_two_kinds_of_note_do_not_hide_each_other():
    """One shard can have a split written down and a move written down, and be both."""
    storage = create_raft_storage()
    split = PendingNote.split(shard_id=0, split_key=b"m", new_shard_id=1)
    move = PendingNote.move(shard_id=0, source_nodes=[1], target_nodes=[2])

    save_note(storage, split)
    save_note(storage, move)

    assert load_note(storage, 0, SPLIT_RECORD_PREFIX) == split
    assert load_note(storage, 0, MIGRATION_RECORD_PREFIX) == move


def test_a_note_about_another_shard_is_not_read():
    """A storage reused by an id holds a note that is not about the shard asking."""
    storage = create_raft_storage()
    foreign = PendingNote.split(shard_id=1, split_key=b"m", new_shard_id=2)
    storage.save_admin(record_key(SPLIT_RECORD_PREFIX, 0), foreign.record())

    assert load_note(storage, 0, SPLIT_RECORD_PREFIX) is None


def test_a_note_is_written_to_every_replica_the_caller_holds():
    replicas = _replicas(3)
    note = PendingNote.split(shard_id=0, split_key=b"m", new_shard_id=1)

    write_note(replicas, note)

    for storage in replicas:
        assert load_note(storage, 0, SPLIT_RECORD_PREFIX) == note


def test_a_note_is_found_on_whichever_replica_wrote_it():
    """Any replica can be the one that comes back, so any of them can be the one read."""
    replicas = _replicas(3)
    note = PendingNote.move(shard_id=0, source_nodes=[1, 2, 3], target_nodes=[4, 5, 6])
    save_note(replicas[2], note)

    assert read_note(replicas, 0, MIGRATION_RECORD_PREFIX) == note


def test_a_replica_that_never_wrote_a_note_is_not_a_note():
    replicas = _replicas(2)

    assert read_note(replicas, 0, SPLIT_RECORD_PREFIX) is None


def test_forgetting_takes_the_note_off_every_replica():
    replicas = _replicas(3)
    note = PendingNote.split(shard_id=0, split_key=b"m", new_shard_id=1)
    write_note(replicas, note)

    forget_note(replicas, 0, SPLIT_RECORD_PREFIX)

    assert read_note(replicas, 0, SPLIT_RECORD_PREFIX) is None


def test_forgetting_a_note_that_is_already_gone_is_nothing():
    """The second run of a commit finds the note deleted, and that is not an error."""
    replicas = _replicas(2)

    forget_note(replicas, 0, MIGRATION_RECORD_PREFIX)
    forget_note(replicas, 0, MIGRATION_RECORD_PREFIX)

    assert read_note(replicas, 0, MIGRATION_RECORD_PREFIX) is None


def test_the_record_on_disk_is_the_shape_a_running_node_already_wrote():
    """What the fields are named is a contract with the notes already on disk.

    Packed here by hand rather than through ``PendingNote``, because the format is this
    test's subject: a rename is a node that comes up unable to read the note its
    predecessor left, which is the one thing the note exists to prevent.
    """
    storage = create_raft_storage()
    split = msgpack.packb({"shard_id": 0, "split_key": b"m", "new_shard_id": 1},
                          use_bin_type=True)
    move = msgpack.packb({"shard_id": 0, "source_nodes": [1], "target_nodes": [2]},
                         use_bin_type=True)
    storage.save_admin("split/0", split)
    storage.save_admin("migrate/0", move)

    assert load_note(storage, 0, SPLIT_RECORD_PREFIX) == PendingNote.split(0, b"m", 1)
    assert load_note(storage, 0, MIGRATION_RECORD_PREFIX) == PendingNote.move(0, [1], [2])
