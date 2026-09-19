"""The version a value is, on the wire, as a shape rather than as an implementation.

A row copied into another group has to keep the timestamp it already had, so the copy has
to be able to ask which version a value is - and nothing among the six primitives answers
that today (`docs/recovery.md`, section 5).  This file holds the part of the answer that is
settled: the field exists, it is where the contract says it is, and a value that carries no
version says so with 0 rather than with a second presence bit.

It is a contract test rather than an implementation test, and deliberately: the servicer
does not fill either field yet, so the shape lands first and the filling second.  Nothing
here touches a shard; what it pins is the generated bindings, which is also the only place
a field number can be got wrong without anything failing.

The rule the two fields come from is worth stating, because one message is left out of it
on purpose.  *A value and the version it is travel together*: `GetResponse` for one key,
`KeyValuePair` for a row of a scan.  There is no third way to get a value out of the client
service, so there is no path that hands one out without its version - which is the property
that makes the copy possible, and the one that would go silently if a later reader had to
remember to ask twice.  `LockRecord.value` is the exception and is not a version at all: it
is a write intent, and an intent is a lock rather than a row.
"""

from oxidedb.proto import client_pb2 as pb


def test_a_row_carries_the_version_it_is_at():
    pair = pb.KeyValuePair(key=b"k", value=b"v", commit_ts=1234)

    back = pb.KeyValuePair.FromString(pair.SerializeToString())

    assert (back.key, back.value, back.commit_ts) == (b"k", b"v", 1234)


def test_one_key_read_carries_it_too():
    """One key is the same fact as a range is a row of them, so the read says it too."""
    response = pb.GetResponse(value=b"v", commit_ts=1234)

    back = pb.GetResponse.FromString(response.SerializeToString())

    assert (back.value, back.commit_ts) == (b"v", 1234)


def test_a_value_with_no_version_says_zero():
    """0 is the contract's "no version", and the field is its own presence bit."""
    assert pb.KeyValuePair(key=b"k", value=b"v").commit_ts == 0
    assert pb.GetResponse(value=b"v").commit_ts == 0


def test_the_two_fields_are_where_the_contract_says_they_are():
    """A renumbering is a silent wire break, which is the one thing a field must not do."""
    assert pb.KeyValuePair.DESCRIPTOR.fields_by_name["commit_ts"].number == 3
    assert pb.GetResponse.DESCRIPTOR.fields_by_name["commit_ts"].number == 5


def test_every_row_a_scan_returns_is_stamped_by_the_same_field():
    """The stamp rides on the row, so a scan cannot be a path that hands out rows bare."""
    entries = pb.ScanResponse.DESCRIPTOR.fields_by_name["entries"]

    assert entries.message_type.full_name == "oxidedb.client.KeyValuePair"


def test_a_lock_record_has_no_version_because_it_has_none_to_carry():
    """A lock's value is a write intent.  An intent is not a version, and is not one yet."""
    assert "commit_ts" not in pb.LockRecord.DESCRIPTOR.fields_by_name
