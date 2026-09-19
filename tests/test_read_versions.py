"""Which version a value is, said by the read that hands the value over.

A copied row has to keep the timestamp it already had, so whatever copies it has to be able
to ask which version it is, and nothing could answer: a row came back as key and value, and
the timestamp the storage had used to choose it was dropped at the last step of the walk
that found it.  This file pins the two reads that keep it, and the one thing they must not
do while keeping it - a lock is not a version, so a value that is a write intent says 0
rather than claiming a moment it has never been committed at.

``ReadResult.commit_ts`` is the same fact for a single key, and it is a field on the answer
rather than a second call for the reason the wire's field is: a caller that could ask for
the value alone could carry it somewhere else and be wrong with nothing reporting it.
"""

import pytest

from oxidedb.raft.state_machine import (CommandType, MVCCStateMachine, ScanRefused,
                                        serialize_command)
from oxidedb.storage.mvcc import MVCCStorage


def _set(state_machine, key, value, timestamp):
    return state_machine.apply(serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp))


def _prewrite(state_machine, key, value, start_ts):
    return state_machine.apply(serialize_command(
        CommandType.PREWRITE, key=key, value=value, start_ts=start_ts, primary_key=key))


# -- the storage ----------------------------------------------------------------------

def test_a_row_read_at_a_snapshot_says_which_version_it_is():
    storage = MVCCStorage()
    storage.set(b"k", b"v5", 5)
    storage.set(b"k", b"v9", 9)

    assert storage.scan_versions(b"a", b"z", 9) == [(b"k", b"v9", 9)]
    assert storage.scan_versions(b"a", b"z", 5) == [(b"k", b"v5", 5)]


def test_the_plain_scan_is_the_versioned_one_with_the_version_dropped():
    """Not a second walk: a second walk is a second answer to which version wins."""
    storage = MVCCStorage()
    storage.set(b"b", b"v1", 1)
    storage.set(b"a", b"v2", 2)

    versioned = storage.scan_versions(b"a", b"z", 2)

    assert storage.scan(b"a", b"z", 2) == [(key, value) for key, value, _ in versioned]
    assert [key for key, _, _ in versioned] == [b"a", b"b"]


def test_a_deleted_key_is_absent_and_not_a_row_with_no_version():
    """A tombstone hides the key, so a caller never has to read a timestamp to know."""
    storage = MVCCStorage()
    storage.set(b"k", b"v", 1)
    storage.delete(b"k", 2)

    assert storage.scan_versions(b"a", b"z", 2) == []
    assert storage.get_version(b"k", 2) is None


def test_get_version_hands_back_the_value_and_the_moment_together():
    storage = MVCCStorage()
    storage.set(b"k", b"v5", 5)
    storage.set(b"k", b"v9", 9)

    version = storage.get_version(b"k", 9)

    assert (version.value, version.timestamp) == (b"v9", 9)
    assert storage.get(b"k", 9) == version.value


# -- the state machine ----------------------------------------------------------------

def test_a_committed_row_read_through_the_machine_carries_its_version():
    state_machine = MVCCStateMachine()
    _set(state_machine, b"k", b"v", 12)

    result = state_machine.get(b"k")

    assert (result.success, result.value, result.commit_ts) == (True, b"v", 12)


def test_a_key_that_is_not_there_has_no_version():
    state_machine = MVCCStateMachine()

    result = state_machine.get(b"absent")

    assert (result.success, result.value, result.commit_ts) == (True, None, 0)


def test_a_reader_s_own_write_intent_is_a_value_with_no_version():
    """An intent is a lock and not a row: there is no moment it has been committed at."""
    state_machine = MVCCStateMachine()
    _prewrite(state_machine, b"k", b"v2", 10)

    result = state_machine.get(b"k", 10)

    assert (result.success, result.value, result.commit_ts) == (True, b"v2", 0)


def test_a_scan_says_the_same_thing_about_an_intent():
    state_machine = MVCCStateMachine()
    _set(state_machine, b"a", b"committed", 4)
    _prewrite(state_machine, b"b", b"intent", 10)

    rows = state_machine.scan_versions(b"a", b"z", 10)

    assert rows == [(b"a", b"committed", 4), (b"b", b"intent", 0)]


def test_a_lock_the_reader_cannot_judge_still_refuses_the_range():
    """The new walk keeps the rule the old one had: a range read does not guess."""
    state_machine = MVCCStateMachine()
    _set(state_machine, b"k", b"v", 4)
    _prewrite(state_machine, b"k", b"v2", 10)

    with pytest.raises(ScanRefused):
        state_machine.scan_versions(b"k", b"l")
