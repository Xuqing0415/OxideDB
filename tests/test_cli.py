"""The CLI end to end: argparse wiring, exit codes, and where it keeps its data.

Run as a subprocess rather than by calling ``main()`` in process, because the
thing being tested is what a user gets from a shell - including the exit code
``get`` uses to report a missing key.

The last class is the newest mode and the reason the file grew: ``--server``, which
points the same four commands at a cluster running in other processes.  That mode
cannot be tested any other way - in process there is no wire to be wrong on, no
routing table to read and no second process to be a shard - so those tests start a
real node and run the CLI against it the way a person would.
"""

import os
import subprocess
import sys

import pytest

from _cluster import start_cluster


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


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    """One node with two shards, in its own process, for every test below.

    Module-wide because a node costs a process and an election to start, and no test
    here reads a key another one wrote.
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
         "--server", cluster.bootstrap_address,
         "--shards", str(cluster.num_shards), *args],
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

        The address is a node's own block with the group ports moved below the
        shards, where nothing is listening: what a shell sees has to be one line and
        an exit code, the way every other command in this file does.
        """
        port = int(cluster.bootstrap_address.rsplit(":", 1)[1])
        nowhere = cluster.bootstrap_address.rsplit(":", 1)[0] + f":{port + 99}"

        result = subprocess.run(
            [sys.executable, "-m", "oxidedb.cli", "--server", nowhere, "get", "user:1"],
            capture_output=True, text=True,
        )

        assert result.returncode == 1, "a client that reached nothing exited 0"
        assert "Traceback" not in result.stdout + result.stderr
        assert result.stdout.strip().startswith("get:")
