"""A client in one process, and the node it talks to in another.

``tests/test_remote_node_client.py`` already puts a real gRPC channel between a client and a
node - but the node is an object this interpreter built.  What that cannot cover is
everything on the far side of a process boundary: a node that has to be started and told to
stop, a data directory only it writes, an address instead of a handle, and bytes that are
serialised rather than handed over.

These are the tests for that, and they are deliberately few and shallow.  What they
establish is that the way in is open, so that the tests which need a real cluster - a
transaction across two shards, a leader killed and replaced - have something to stand on.
"""

import os

import pytest

from _cluster import read_when_ready, start_cluster, write_when_ready
from oxidedb.client import RemoteNodeClient
from oxidedb.raft.state_machine import CommandType, serialize_command
from oxidedb.shard.router import default_range_map, locate

#: Keys below 0x80, which is shard 0 on the default two-shard map.
KEYS = [b"alpha", b"beta", b"gamma", b"delta", b"zulu"]


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    """One node with two shards, up for every test in this file.

    Module-wide because a node costs a process and an election to start, and no test here
    changes anything another one reads: each of them writes keys of its own.
    """
    with start_cluster(num_nodes=1, num_shards=2,
                       base_dir=str(tmp_path_factory.mktemp("oxidedb-cluster"))) as running:
        yield running


def client_for(cluster, key):
    """A client to the port serving the shard that owns ``key``.

    A node serves every one of its shards, so the shard id is the whole of what has to be
    worked out: the range map says which shard owns the key, and the node says where that
    shard listens.  Getting this wrong is not an error anywhere - a key sent to the wrong
    port is written in the wrong shard, quietly - which is why it is done with the same two
    functions the server routes by rather than by hand.
    """
    shard_id = locate(default_range_map(cluster.num_shards), key)
    return RemoteNodeClient(cluster.shard_address(shard_id))


def write(client, key, value):
    """Set one key, which is what a caller means by writing."""
    return write_when_ready(client, serialize_command(CommandType.SET, key=key, value=value))


class TestOneNodeOverTheWire:
    """A value out and back, and a range read that stops at its own two ends."""

    def test_the_node_is_a_process_somewhere_else(self, cluster):
        """The premise of every other test here, said out loud.

        A client and a node in one interpreter share the objects between them, so the
        tests that have been written that way cannot fail on a serialisation, an address
        or a hint.  This one can, and only because the node is not in this process.
        """
        node = cluster.node(1)

        assert node.running
        assert node.process.pid != os.getpid()

    def test_a_value_comes_back_as_the_same_bytes(self, cluster):
        client = client_for(cluster, b"alpha")
        value = b"a value with a \x00 zero, a newline\nand a tab\tin it"

        assert write(client, b"alpha", value).success

        result = read_when_ready(client, b"alpha")
        assert result.success
        assert result.value == value

    def test_a_range_read_returns_the_rows_between_its_ends(self, cluster):
        client = client_for(cluster, b"alpha")
        for key in KEYS:
            assert write(client, key, b"value:" + key).success

        # beta in and zulu out, so the range is half-open at both ends: a scan that
        # answered with everything would pass a test that only asked whether the keys it
        # wanted were there.  delta comes back before gamma, which is byte order and not
        # the order they were written in - the keys were written gamma first.
        assert client.scan(b"beta", b"zulu") == [(b"beta", b"value:beta"),
                                                 (b"delta", b"value:delta"),
                                                 (b"gamma", b"value:gamma")]

    def test_a_range_read_with_nothing_in_it_is_empty_not_an_error(self, cluster):
        client = client_for(cluster, b"alpha")

        assert client.scan(b"m", b"n") == []

    def test_a_key_that_was_never_written_reads_as_no_value(self, cluster):
        client = client_for(cluster, b"alpha")

        result = read_when_ready(client, b"absent")

        assert result.success
        assert result.value is None


class TestWhatOnlyASocketBreaks:
    """The bytes a handover between two objects would never have to touch."""

    def test_a_key_that_is_not_text_lands_in_the_shard_its_bytes_name(self, cluster):
        key = b"\x80key"

        shard_one = client_for(cluster, key)
        assert write(shard_one, key, b"written to shard one").success
        assert read_when_ready(shard_one, key).value == b"written to shard one"

        # And the same key asked of shard 0 is absent, which is what says it arrived as
        # 0x80 rather than as text: a key mangled on the way would land in shard 0, and
        # this read would find it there.
        assert read_when_ready(client_for(cluster, b"alpha"), key).value is None

    def test_an_empty_value_is_a_value_and_not_an_absence(self, cluster):
        client = client_for(cluster, b"empty")

        assert write(client, b"empty", b"").success

        result = read_when_ready(client, b"empty")
        assert result.success
        assert result.value == b""

    def test_a_deleted_key_reads_back_as_absent(self, cluster):
        client = client_for(cluster, b"doomed")

        assert write(client, b"doomed", b"here").success
        assert read_when_ready(client, b"doomed").value == b"here"

        deleted = write_when_ready(client,
                                   serialize_command(CommandType.DELETE, key=b"doomed"))
        assert deleted.success
        assert read_when_ready(client, b"doomed").value is None

    def test_a_megabyte_value_survives_the_message_limit(self, cluster):
        key = b"zebra"
        value = b"x" * (1024 * 1024)
        client = client_for(cluster, key)

        assert write(client, key, value).success
        assert read_when_ready(client, key).value == value
