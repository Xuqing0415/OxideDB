"""The index a read is answered at, as a shape rather than as an implementation.

A leader can answer for a follower: it names the index a quorum confirmed, and the follower
answers the read once its own state machine has caught up to that index.  That is the whole
of a follower read, and the wire half of it is what this file settles - the index a caller
may name, and the index an answer reports it was read at.  Nothing here touches a shard.
What it pins is the generated bindings, which is also the only place a field number can be
got wrong without anything failing: the filling is pinned where it can be seen happening,
beside the node that does it.

The rule the four fields come from is one sentence: *a caller may name the index it needs,
and an answer says which index it was read at*.  The requests carry it as `optional`,
because a caller that names none is asking for the server's own default rather than for
index 0 - 0 is not a position a log has been applied at, and a magic value meaning "not
asked" is a contract the next reader has to be told instead of read, the same choice
`GetRequest.timestamp` makes.  The responses carry it plain, because a server that read
something knows how far its log had got when it looked, and so always has one to give;
`FollowerReadIndexResponse.read_index` is that same field, answering the same question
about the same index, which is where a caller gets the index to name.
"""

from oxidedb.proto import client_pb2 as pb


def test_a_read_may_name_the_index_it_needs():
    """The one key read and the range read ask the same way, so they are set the same way."""
    one = pb.GetRequest(key=b"k", read_index=42)
    many = pb.ScanRequest(start_key=b"a", end_key=b"z", read_index=42)

    back_one = pb.GetRequest.FromString(one.SerializeToString())
    back_many = pb.ScanRequest.FromString(many.SerializeToString())

    assert (back_one.key, back_one.read_index) == (b"k", 42)
    assert (back_many.start_key, back_many.end_key, back_many.read_index) == (b"a", b"z", 42)


def test_an_answer_says_which_index_it_was_read_at():
    """A read happens at an index even when the caller named none, so both carries it."""
    one = pb.GetResponse(value=b"v", read_index=17)
    many = pb.ScanResponse(read_index=17)

    back_one = pb.GetResponse.FromString(one.SerializeToString())
    back_many = pb.ScanResponse.FromString(many.SerializeToString())

    assert back_one.read_index == 17
    assert back_many.read_index == 17


def test_an_index_that_was_not_named_is_unset_rather_than_zero():
    """0 is not a position a log has been applied at, so it cannot be what "not asked" is."""
    assert pb.GetRequest(key=b"k").HasField("read_index") is False
    assert pb.ScanRequest(start_key=b"a").HasField("read_index") is False


def test_the_four_fields_are_where_the_contract_says_they_are():
    """A renumbering is a silent wire break, which is the one thing a field must not do."""
    assert pb.GetRequest.DESCRIPTOR.fields_by_name["read_index"].number == 3
    assert pb.GetResponse.DESCRIPTOR.fields_by_name["read_index"].number == 6
    assert pb.ScanRequest.DESCRIPTOR.fields_by_name["read_index"].number == 4
    assert pb.ScanResponse.DESCRIPTOR.fields_by_name["read_index"].number == 6


def test_a_row_does_not_carry_one_because_a_row_is_not_a_read():
    """The index belongs to the answer, not to each row: it is where the reader looked."""
    entries = pb.ScanResponse.DESCRIPTOR.fields_by_name["entries"]

    assert "read_index" not in entries.message_type.fields_by_name
    assert entries.message_type.full_name == "oxidedb.client.KeyValuePair"
