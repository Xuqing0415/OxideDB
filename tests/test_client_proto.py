"""The wire contract between a client and a node, pinned before anything speaks it.

This file has no business logic in it, on purpose.  The client service used to be a
key/value API - Get, Set, Delete, Scan - and `Set` is the one that cannot be answered by
a server: the timestamp a write carries has to come from the client's own TSO batch, or a
transaction's prewrite and its commit land at timestamps that do not line up, and a
`Prewrite`/`Commit` pair of its own would mean the node deciding which key is primary.
The contract is therefore the node's primitives - Get, Scan, Propose and the three reads
the lock resolver and the range reader need - and transactions stay where they already
are, in the client's coordinator, expressed as Propose calls.

What these tests pin is the part that outlives the implementation: which six methods
exist, that `error_code` has exactly the five cases a caller reacts to differently, that
`leader_address` is the one place a retry target lives, and that every field survives a
round trip - including the ones that are deliberately *unset*, where "no value" and "an
empty value" have to be distinguishable, because a key with an empty value and a key with
no version are different answers.
"""

import pytest

from oxidedb.proto import client_pb2, client_pb2_grpc, groups_pb2, groups_pb2_grpc


def _round_trip(message):
    """Serialise and parse back, the way the wire will."""
    parsed = type(message)()
    parsed.ParseFromString(message.SerializeToString())
    return parsed


def test_the_service_is_the_node_primitives_and_not_a_transaction_api():
    """Six methods, none of them a transaction.

    A Prewrite here would put the primary-key decision on the server, and Set/Delete
    would put the timestamp there.  Both are the client's, so neither is an RPC.
    """
    service = client_pb2.DESCRIPTOR.services_by_name["ClientService"]
    assert [method.name for method in service.methods] == [
        "Get", "Scan", "Propose", "GetLock", "GetWriteRecord", "FollowerReadIndex",
    ]


def test_the_error_code_is_the_five_cases_a_caller_reacts_to_differently():
    """OK, try the leader, resolve the lock, try again, or give up on this call.

    Transport failures stay in the gRPC status, so a client can tell a retry from a dead
    wire without parsing a message.
    """
    code = client_pb2.DESCRIPTOR.enum_types_by_name["ErrorCode"]
    assert [value.name for value in code.values] == [
        "OK", "NOT_LEADER", "LOCKED", "REFUSED", "TIMEOUT",
    ]
    assert client_pb2.OK == 0, "the zero value is the successful one"


def test_a_lock_record_survives_the_wire_field_for_field():
    """The record the lock resolver reads, as MVCCStorage.get_newest_lock returns it."""
    lock = client_pb2.LockRecord(
        key=b"\x80key", start_ts=42, status=b"LOCKED", primary_key=b"\x80key",
        lock_time=1234.5, value=b"committed value")
    message = client_pb2.GetLockResponse(
        error_code=client_pb2.OK, leader_address="127.0.0.1:30000", lock=lock)

    parsed = _round_trip(message)
    assert parsed.error_code == client_pb2.OK
    assert parsed.HasField("lock"), "the lock is there, and saying so is a field"
    assert parsed.lock.key == b"\x80key"
    assert parsed.lock.start_ts == 42
    assert parsed.lock.status == b"LOCKED"
    assert parsed.lock.primary_key == b"\x80key"
    assert parsed.lock.lock_time == 1234.5
    assert parsed.lock.value == b"committed value"

    # A key that is not locked is an answer, not a failure: the field is absent and the
    # error code says the read succeeded.
    unlocked = _round_trip(client_pb2.GetLockResponse(error_code=client_pb2.OK))
    assert not unlocked.HasField("lock")
    assert unlocked.error_code == client_pb2.OK


def test_the_leader_hint_is_the_one_place_a_retry_target_lives():
    """NOT_LEADER carries an address; every other answer leaves it unset."""
    refused = client_pb2.GetResponse(
        error_code=client_pb2.NOT_LEADER, message="not the leader",
        leader_address="127.0.0.1:30001")
    parsed = _round_trip(refused)
    assert parsed.error_code == client_pb2.NOT_LEADER
    assert parsed.HasField("leader_address")
    assert parsed.leader_address == "127.0.0.1:30001"

    ok = _round_trip(client_pb2.GetResponse(error_code=client_pb2.OK, value=b"v"))
    assert not ok.HasField("leader_address"), "nothing to retry, nothing to name"


def test_no_value_and_an_empty_value_are_different_answers():
    """The reason these fields are `optional`: absent is not empty."""
    empty = _round_trip(client_pb2.GetResponse(error_code=client_pb2.OK, value=b""))
    assert empty.HasField("value") and empty.value == b""

    missing = _round_trip(client_pb2.GetResponse(error_code=client_pb2.OK))
    assert not missing.HasField("value"), "the key has no version at this snapshot"


def test_a_propose_response_carries_the_term_and_the_index():
    """A client needs both to tell a deposed leader from a landed write."""
    message = client_pb2.ProposeResponse(
        error_code=client_pb2.OK, term=7, index=91, data=b"\x01\x02")
    parsed = _round_trip(message)
    assert (parsed.term, parsed.index, parsed.data) == (7, 91, b"\x01\x02")
    assert parsed.error_code == client_pb2.OK

    # A proposal that appended nothing has no index: absent, not zero, because index 0
    # is the log's start and a client that read it as "committed at 0" would wait for a
    # read index it already has.
    refused = _round_trip(client_pb2.ProposeResponse(
        error_code=client_pb2.NOT_LEADER, leader_address="127.0.0.1:30002"))
    assert not refused.HasField("index")


def test_a_scan_response_carries_the_rows_and_the_key_that_stopped_it():
    """A refused range names the key, because a dropped key looks like an absent one."""
    rows = [client_pb2.KeyValuePair(key=b"a", value=b"1"),
            client_pb2.KeyValuePair(key=b"b", value=b"")]
    ok = _round_trip(client_pb2.ScanResponse(error_code=client_pb2.OK, entries=rows))
    assert [(entry.key, entry.value) for entry in ok.entries] == [(b"a", b"1"), (b"b", b"")]
    assert not ok.HasField("locked_key")

    refused = _round_trip(client_pb2.ScanResponse(
        error_code=client_pb2.LOCKED, message="locked", locked_key=b"b"))
    assert refused.locked_key == b"b"


def test_the_two_reads_the_lock_resolver_needs_map_onto_what_the_shard_returns():
    """GetWriteRecord is the primary key's fate, and it is optional in both fields.

    A key with no committed version is how a caller tells a lock whose transaction never
    committed from one that did, so the absent case is the interesting one.
    """
    written = _round_trip(client_pb2.GetWriteRecordResponse(
        error_code=client_pb2.OK, start_ts=5, commit_ts=8))
    assert (written.start_ts, written.commit_ts) == (5, 8)

    never = _round_trip(client_pb2.GetWriteRecordResponse(error_code=client_pb2.OK))
    assert not never.HasField("start_ts") and not never.HasField("commit_ts")


def test_the_follower_read_index_is_an_index_and_nothing_else():
    """The follower's whole question: how far has the leader committed."""
    parsed = _round_trip(client_pb2.FollowerReadIndexResponse(
        error_code=client_pb2.OK, read_index=17))
    assert parsed.read_index == 17


def test_the_servicer_the_generated_code_asks_for_has_all_six():
    """The names the node will have to implement, so a missing one is a stale build."""
    servicer = client_pb2_grpc.ClientServiceServicer
    for name in ("Get", "Scan", "Propose", "GetLock", "GetWriteRecord",
                 "FollowerReadIndex"):
        assert hasattr(servicer, name), name

class _RecordingChannel:
    """A channel that writes down what a stub asks it for, and answers nothing.

    Enough to read the wire path of a call: a stub's first act is to hand its path to the
    channel, and the path is the one thing a client in another language is written against.
    """

    def __init__(self):
        self.paths = []

    def unary_unary(self, path, *args, **kwargs):
        self.paths.append(path)
        return lambda *args, **kwargs: None


def test_the_group_services_are_the_table_and_the_clock():
    """One method for the routing table, two for the timestamp group.

    The table is read whole, so there is nothing to ask per key; the counter is asked for
    one timestamp, or for a range of them, which is what makes a batch worth having.
    """
    metadata = groups_pb2.DESCRIPTOR.services_by_name["MetadataService"]
    tso = groups_pb2.DESCRIPTOR.services_by_name["TSOService"]

    assert [method.name for method in metadata.methods] == ["ListShards"]
    assert [method.name for method in tso.methods] == ["GetTimestamp", "GetTimestampBatch"]


def test_a_group_error_code_is_the_same_five_a_shard_uses():
    """So one piece of client code can read either response.

    These two groups never send LOCKED - neither of them has locks - and the number is
    still the same one client.proto gives it, because a shared meaning is what lets a
    caller treat "ask the leader" the same way wherever it is refused.  TIMEOUT is not
    like LOCKED: a proposal to either group can fail to commit, so both files send it.
    """
    shard = {value.name: value.number
             for value in client_pb2.DESCRIPTOR.enum_types_by_name["ErrorCode"].values}
    group = {value.name: value.number
             for value in groups_pb2.DESCRIPTOR.enum_types_by_name["ErrorCode"].values}

    assert group == shard


def test_a_shard_record_survives_the_wire_field_for_field():
    """The record the table keeps, as ShardPlacement describes it."""
    record = groups_pb2.ShardRecord(
        shard_id=1, start_key=b"\x80", end_key=b"\xff",
        nodes=[1, 2, 3], addresses={1: "127.0.0.1:8301", 2: "127.0.0.1:8401"},
        leader_id=2, leader_term=7, leader_address="127.0.0.1:8401")
    message = groups_pb2.ListShardsResponse(
        error_code=groups_pb2.OK, shards=[record], version=4)

    parsed = _round_trip(message)
    assert parsed.error_code == groups_pb2.OK
    assert parsed.version == 4
    assert len(parsed.shards) == 1

    entry = parsed.shards[0]
    assert (entry.shard_id, entry.start_key, entry.end_key) == (1, b"\x80", b"\xff")
    assert list(entry.nodes) == [1, 2, 3]
    assert dict(entry.addresses) == {1: "127.0.0.1:8301", 2: "127.0.0.1:8401"}
    assert (entry.leader_id, entry.leader_term) == (2, 7)
    assert entry.leader_address == "127.0.0.1:8401"


def test_a_shard_whose_leader_has_not_reported_one_says_nothing_rather_than_zero():
    """Unset, not 0: node 0 does not exist, and a client that read it would try to go there.

    The first time this record is built is before any election has happened, so this is the
    ordinary case at startup rather than an edge one.
    """
    entry = _round_trip(
        groups_pb2.ListShardsResponse(
            error_code=groups_pb2.OK,
            shards=[groups_pb2.ShardRecord(shard_id=0, start_key=b"", end_key=b"\x80")],
            version=1)).shards[0]

    assert entry.nodes == []
    assert dict(entry.addresses) == {}
    assert not entry.HasField("leader_id")
    assert not entry.HasField("leader_address")
    assert entry.leader_term == 0, "a term is a number, and no claim is zero"


def test_a_timestamp_batch_is_the_inclusive_range_the_group_allocated():
    """Both ends inclusive, exactly as the allocate command answers it.

    A caller that wants a half-open range adds one to end_ts - which is what TSOClient does
    - and the count is what says how many timestamps the range holds.
    """
    parsed = _round_trip(groups_pb2.GetTimestampBatchResponse(
        error_code=groups_pb2.OK, start_ts=1000, end_ts=1999))
    assert (parsed.start_ts, parsed.end_ts) == (1000, 1999)

    refused = _round_trip(groups_pb2.GetTimestampBatchResponse(
        error_code=groups_pb2.NOT_LEADER, leader_address="127.0.0.1:8401"))
    assert not refused.HasField("start_ts") and not refused.HasField("end_ts")


def test_one_timestamp_is_a_timestamp_and_a_refusal_is_not_a_zero():
    """Zero is a timestamp the group has not handed out yet, so absence has to be a field."""
    parsed = _round_trip(groups_pb2.GetTimestampResponse(
        error_code=groups_pb2.OK, timestamp=41))
    assert parsed.timestamp == 41

    refused = _round_trip(groups_pb2.GetTimestampResponse(
        error_code=groups_pb2.REFUSED, message="no quorum"))
    assert not refused.HasField("timestamp")
    assert refused.error_code == groups_pb2.REFUSED


def test_the_paths_are_the_ones_a_client_in_another_language_would_call():
    """Nothing speaks these two services yet, so the names are all there is to freeze."""
    channel = _RecordingChannel()
    groups_pb2_grpc.MetadataServiceStub(channel)
    groups_pb2_grpc.TSOServiceStub(channel)

    assert channel.paths == [
        "/oxidedb.groups.MetadataService/ListShards",
        "/oxidedb.groups.TSOService/GetTimestamp",
        "/oxidedb.groups.TSOService/GetTimestampBatch",
    ]


def test_the_servicers_the_generated_code_asks_for_exist():
    """The names a node will have to implement, so a missing one is a stale build."""
    for name in ("ListShards",):
        assert hasattr(groups_pb2_grpc.MetadataServiceServicer, name), name
    for name in ("GetTimestamp", "GetTimestampBatch"):
        assert hasattr(groups_pb2_grpc.TSOServiceServicer, name), name
