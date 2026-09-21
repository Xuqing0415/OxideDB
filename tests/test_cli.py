"""The CLI end to end: argparse wiring, exit codes, and where it keeps its data.

Run as a subprocess rather than by calling ``main()`` in process, because the
thing being tested is what a user gets from a shell - including the exit code
``get`` uses to report a missing key.

``--server`` is the newest mode and the reason the file grew: it points the same four
commands at a cluster running in other processes.  That mode cannot be tested any
other way - in process there is no wire to be wrong on, no routing table to read and
no second process to be a shard - so those tests start a real node and run the CLI
against it the way a person would.

The last two classes are the level a read is answered at: ``--consistency``, the one
thing about a cluster read a caller chooses besides where it is routed.  They need a
cluster of three nodes, since one node per set makes every level the same read; and
the claim a shell cannot make at all - that a ``cached`` read answers at an index this
client was given earlier - is tested in process, where a store can be held across two
reads.
"""

import os
import subprocess
import sys
import time

import pytest

from _cluster import start_cluster
from oxidedb.cli import ClusterStore
from oxidedb.client import RemoteNodeClient
from oxidedb.transaction.smart_client import Consistency


def _cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "oxidedb.cli", *args],
        capture_output=True,
        text=True,
    )


class TestCli:
    def test_data_dir_makes_writes_persist_between_invocations(self, tmp_path):
        data_dir = str(tmp_path / "demo")

        write = _cli("--data-dir", data_dir, "set", "user:1", "alice")
        assert write.returncode == 0
        assert write.stdout.strip() == "OK"

        read = _cli("--data-dir", data_dir, "get", "user:1")
        assert read.returncode == 0
        assert read.stdout.strip() == "alice"

        scan = _cli("--data-dir", data_dir, "scan", "user:", "user:9")
        assert scan.stdout.strip() == "user:1: alice"

        assert (tmp_path / "demo" / "data.sqlite3").exists()

    def test_without_data_dir_each_invocation_starts_empty(self):
        assert _cli("set", "user:1", "alice").stdout.strip() == "OK"

        missing = _cli("get", "user:1")
        assert missing.returncode == 1
        assert "Key not found" in missing.stdout

    def test_no_command_prints_help_without_creating_a_database(self, tmp_path):
        data_dir = str(tmp_path / "untouched")

        result = _cli("--data-dir", data_dir)

        assert result.returncode == 0
        assert "usage" in result.stdout.lower()
        assert not (tmp_path / "untouched").exists()

    def test_a_directory_and_a_cluster_are_refused_together(self, tmp_path):
        """One key, two places it could be: a command that picked one silently
        would be answering a question nobody asked."""
        result = _cli("--data-dir", str(tmp_path / "demo"),
                      "--server", "127.0.0.1:8001", "set", "user:1", "alice")

        assert result.returncode == 2
        assert "--data-dir" in result.stderr and "--server" in result.stderr
        assert not (tmp_path / "demo").exists()

    def test_server_wants_an_address(self):
        result = _cli("--server", "localhost", "get", "user:1")

        assert result.returncode == 2
        assert "host:port" in result.stderr

    def test_a_write_takes_no_consistency_because_it_has_one_place_to_go(self):
        """A level says which copy of a shard may answer a read, and a write is
        not a read: it has one shard to go to, so the argument is refused rather
        than quietly ignored."""
        result = _cli("set", "user:1", "alice", "--consistency", "cached")

        assert result.returncode == 2
        assert "--consistency" in result.stderr


def _address_that_answers_nothing(cluster) -> str:
    """The same host, at a port inside a node's block that nothing binds.

    A node takes ``port + shard_id`` for each shard it serves and puts the two groups at
    ``port + SHARD_SEGMENT`` and above, so ``+99`` is a hole in its shard segment
    whatever it was started with - up to ninety-nine shards, which no test here is
    near.  A command sent there meets the absence of an answer rather than a refusal,
    which is the state the client reports rather than raises - and the one the wait
    below is given a deadline for.
    """
    port = int(cluster.bootstrap_address.rsplit(":", 1)[1])
    return cluster.bootstrap_address.rsplit(":", 1)[0] + f":{port + 99}"


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    """One node with two shards, in its own process, for every test below.

    Module-wide because a node costs a process and an election to start, and no test
    here reads a key another one wrote.

    Nothing waits here for the cluster to become writable: the fixture returns as soon
    as the node says READY, which is a promise about ports rather than about elections
    or the publisher's first pass.  A command that arrives in that gap is what the CLI's
    own wait is for, so a fixture that waited would be hiding the thing these tests
    should go through - and a command run this soon after READY is exactly the case the
    wait exists for.  The wait itself is asserted below, on a cluster nothing can
    reach.
    """
    with start_cluster(num_nodes=1, num_shards=2,
                       base_dir=str(tmp_path_factory.mktemp("oxidedb-cli-cluster"))) as running:
        yield running


def _cluster_cli(cluster, *args):
    """The CLI against ``cluster``, with its streams pinned to UTF-8.

    Pinned rather than left to the console's code page: the key that puts a row in
    the second shard has to start with a byte at or above 0x80, which makes it a
    character no ASCII code page has, and this test is about shards rather than about
    which code page the machine happens to run.
    """
    return subprocess.run(
        [sys.executable, "-m", "oxidedb.cli",
         "--server", cluster.bootstrap_address, *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )


class TestCliAgainstACluster:
    """The same four commands, over channels, against a node that is a process."""

    def test_a_key_written_by_one_invocation_is_read_by_the_next(self, cluster):
        """The one thing the in-memory mode cannot do.

        Without ``--server`` each invocation starts from an empty keyspace.  Here the
        value is committed to a shard in another process, so the read that finds it
        read it from that process, through the routing table, and not from anything
        this one is holding.
        """
        assert cluster.node(1).process.pid != os.getpid(), "the node is a process"

        assert _cluster_cli(cluster, "set", "user:1", "alice").stdout.strip() == "OK"
        assert _cluster_cli(cluster, "get", "user:1").stdout.strip() == "alice"

    def test_a_range_read_crosses_the_shards_it_covers(self, cluster):
        """Two keys, two shards, one scan.

        ``a:1`` and ``键:1`` differ in their first byte, which is the whole of what the
        default range map splits on, so a range read that asked one shard would come
        back with half of what was written and no sign that anything was missing.
        """
        below = "a:1"     # utf-8 first byte 0x61 -> shard 0
        above = "键:1"    # utf-8 first byte 0xe9 -> shard 1

        assert _cluster_cli(cluster, "set", below, "in shard 0").stdout.strip() == "OK"
        assert _cluster_cli(cluster, "set", above, "in shard 1").stdout.strip() == "OK"

        scan = _cluster_cli(cluster, "scan", below, "\uffff")

        assert scan.returncode == 0
        rows = scan.stdout.strip().splitlines()
        first, second = f"{below}: in shard 0", f"{above}: in shard 1"
        assert first in rows, "the first shard's row is missing from the range read"
        assert second in rows, "the second shard's row is missing from the range read"
        assert rows.index(first) < rows.index(second), (
            "the pieces came back in the order the shards are numbered rather than in "
            "key order"
        )

        # And a range that ends before the second shard begins stops there: the same
        # read, asked for less.
        window = _cluster_cli(cluster, "scan", below, above)
        assert first in window.stdout
        assert second not in window.stdout, (
            "a range that stops at the second shard's own key read past its end"
        )

    def test_a_deleted_key_is_gone_and_its_neighbour_is_not(self, cluster):
        assert _cluster_cli(cluster, "set", "doomed", "here").stdout.strip() == "OK"
        assert _cluster_cli(cluster, "set", "kept", "here").stdout.strip() == "OK"

        assert _cluster_cli(cluster, "delete", "doomed").stdout.strip() == "OK"

        missing = _cluster_cli(cluster, "get", "doomed")
        assert missing.returncode == 1
        assert "Key not found" in missing.stdout
        assert _cluster_cli(cluster, "get", "kept").stdout.strip() == "here"

    def test_a_cluster_that_answers_nothing_says_so_on_one_line(self, cluster):
        """A client that cannot reach the table reports it, rather than raising.

        Nothing is listening at the address, and --wait 0 asks that question once:
        what a shell sees has to be one line and an exit code, the way every other
        command in this file does, without a wait in front of the answer.
        """
        result = subprocess.run(
            [sys.executable, "-m", "oxidedb.cli", "--server",
             _address_that_answers_nothing(cluster), "--wait", "0", "get", "user:1"],
            capture_output=True, text=True,
        )

        assert result.returncode == 1, "a client that reached nothing exited 0"
        assert "Traceback" not in result.stdout + result.stderr
        assert result.stdout.strip().startswith("get:")

    def test_a_command_that_finds_no_cluster_waits_and_then_says_why(self, cluster):
        """A command asks until its deadline, and reports what it kept being told.

        A CLI invocation is one shot.  With no second attempt and no caller holding
        state for it, "not yet" and "not there" arrive as the same failure, so the
        command is the only place that can tell a cluster which is still electing from
        an address with nothing behind it - by waiting, for a bounded time, and then
        reporting the last thing it was told.

        The write rides along with the wait because both claims are about the one
        invocation: a write that did not happen exits 1 and says why, and it never
        says "Key not written".  The clock is asserted too, because a command that
        gave up immediately would satisfy every other line of this test and be exactly
        the bug it is here for.
        """
        start = time.monotonic()
        result = subprocess.run(
            [sys.executable, "-m", "oxidedb.cli", "--server",
             _address_that_answers_nothing(cluster), "--wait", "1",
             "set", "user:1", "alice"],
            capture_output=True, text=True,
        )
        waited = time.monotonic() - start

        assert result.returncode == 1, "a write that could not be made exited 0"
        assert "Traceback" not in result.stdout + result.stderr
        assert result.stdout.strip().startswith("set:")
        assert "was not ready within 1.0s" in result.stdout, result.stdout
        assert "no address answered" in result.stdout, result.stdout
        assert "Key not written" not in result.stdout
        assert waited >= 1.0, f"the wait was over after {waited:.1f}s"


@pytest.fixture(scope="module")
def replicated_cluster(tmp_path_factory):
    """Three nodes on two shards: every shard has three members, in three processes.

    A set of one cannot show what a read level is for.  "Any member of the set" and
    "the leader" are then the same address, and a read that need not lead is served
    by the node the leader would have been, so the levels are tested against a cluster
    whose sets are larger than one.  Everything else is the fixture above: nothing
    waits for an election here either, because a command that arrives in that gap is
    what the CLI's own wait is for.
    """
    with start_cluster(
            num_nodes=3, num_shards=2,
            base_dir=str(tmp_path_factory.mktemp("oxidedb-cli-levels"))) as running:
        yield running


class TestCliReadLevels:
    """``--consistency``: the one thing about a cluster read a caller chooses.

    Three words for three ways of being answered, and the same value from all of them
    - which is what makes the choice a price rather than a promise about the answer.
    """

    def test_every_level_reads_a_key_a_shell_wrote(self, replicated_cluster):
        """The flag has to reach the client, not merely the parser.

        Three reads of one key, each at a different level, and the value comes back
        three times: what differs is the route and what the caller pays for it, not
        the answer.
        """
        cluster = replicated_cluster
        written = _cluster_cli(cluster, "set", "level:key", "answered")
        assert written.stdout.strip() == "OK", written.stdout + written.stderr

        for level in Consistency.ALL:
            read = _cluster_cli(cluster, "get", "level:key", "--consistency", level)

            assert read.returncode == 0, f"{level}: {read.stdout}{read.stderr}"
            assert read.stdout.strip() == "answered", f"{level}: {read.stdout}"

    def test_a_range_read_takes_the_level_too(self, replicated_cluster):
        """A range is a piece per shard, and every piece is read at the level asked
        for."""
        cluster = replicated_cluster
        below = "level:0"     # utf-8 first byte 0x6c -> the shard the range starts in
        above = "\u952e:level"    # utf-8 first byte 0xe9 -> the shard after it

        assert _cluster_cli(cluster, "set", below, "first").stdout.strip() == "OK"
        assert _cluster_cli(cluster, "set", above, "second").stdout.strip() == "OK"

        scan = _cluster_cli(cluster, "scan", below, "\uffff", "--consistency", "cached")

        assert scan.returncode == 0, scan.stdout + scan.stderr
        rows = scan.stdout.strip().splitlines()
        assert f"{below}: first" in rows, rows
        assert f"{above}: second" in rows, rows


def _what_each_read_named(monkeypatch):
    """Every basis a shard read named, and the basis the node answered it at.

    What one read looks like from the caller's side of the wire: the index it names is
    the cache being used, and the index it is answered at is what there is to remember
    for the next read.  Both are on the handle the read goes through, and nowhere else
    a caller without the node's own logs can see them.
    """
    asked = []
    wire_get = RemoteNodeClient.get

    def recording_get(self, key, timestamp=None, read_index=None):
        answer = wire_get(self, key, timestamp, read_index)
        asked.append((read_index, answer.read_index))
        return answer

    monkeypatch.setattr(RemoteNodeClient, "get", recording_get)
    return asked


class TestCliCachedReadsRememberAnIndex:
    """The one claim a shell cannot make: the index is the client's, not the set's.

    ``cached`` is a read answered at an index this client was given earlier, and
    "earlier" is inside one process: two invocations of the CLI are two clients with
    two caches, so the second can prove nothing about the first.  So the level is
    tested where the CLI's own backend can be held across two reads - the
    ``ClusterStore`` the ``--server`` mode is built out of, built the way ``_open``
    builds it - and what is asserted is what that client told the node, which is where
    a cache shows from outside the client.
    """

    def test_the_second_read_names_the_basis_the_first_was_answered_at(
            self, replicated_cluster, monkeypatch):
        store = ClusterStore([replicated_cluster.bootstrap_address], Consistency.CACHED)
        try:
            store.wait_until_routable()
            assert store.set(b"level:cached", b"answered") is True

            asked = _what_each_read_named(monkeypatch)

            assert store.get(b"level:cached") == b"answered"
            assert store.get(b"level:cached") == b"answered"
        finally:
            store.close()

        assert len(asked) == 2, f"two reads asked the nodes {len(asked)} times: {asked}"
        first, second = asked
        assert first[0] is None, "the first cached read named an index it had not been given"
        assert first[1] is not None, "the node answered the first read at no basis at all"
        assert second[0] == first[1], (
            f"the second read named {second[0]} where the first was answered at "
            f"{first[1]}, so it did not read at what this client had been given")

