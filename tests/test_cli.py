"""The CLI end to end: argparse wiring, exit codes, and the two storage modes.

Run as a subprocess rather than by calling ``main()`` in process, because the
thing being tested is what a user gets from a shell - including the exit code
``get`` uses to report a missing key.
"""

import subprocess
import sys


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
