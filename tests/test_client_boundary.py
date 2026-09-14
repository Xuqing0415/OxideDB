"""The seam holds: nothing outside a shard reaches into one.

``NodeClient`` is what a caller meets a shard through, and the whole point of it is that a
caller can be moved into another process without being rewritten.  That only holds while
the callers stay on their side of it: a coordinator that reads ``node._state_machine``, a
resolver that reaches into ``_storage`` for a key's newest write record, or a router that
decides where to send a request from a node's own ``state`` is a caller that has to be
rewritten to leave the process.  The first two fail loudly when that happens; the third
fails quietly, because a leader cut off from its peers goes on saying LEADER.

This is a gatekeeper rather than a behaviour test, in the same spirit as
``tests/test_error_codes.py``.  What it prevents is a *new* reach-in, and no behaviour test
notices one while the code being reached into is in the same process: the read answers, the
commit commits, and the test that covers it passes for the wrong reason.  It scans source
rather than imports, because what is being prevented is a line somebody writes in a hurry -
the shortest way to a write record for one caller is ``._storage.get_latest_write``, and
the shortest way to a leadership answer is the node's own ``state``, which is the answer
that is wrong exactly when it matters.

The allowlist is not a list of exemptions.  Every entry is either the shard itself or a
component that runs inside the cluster and is entitled to its own machinery, and each one
is named with what it is doing there.  The second test is what stops the list from rotting
in either direction: an entry for a file that is gone, and an entry for a file that no
longer needs it, are both failures.

The tests themselves are free to reach in - several build a node, a state machine or a
lock by hand - so this scans ``oxidedb/`` and not ``tests/``.  Production code is the
boundary; test code is where a boundary is allowed to be crossed on purpose.
"""

import re
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parent.parent / "oxidedb"

#: What a caller must not do to a shard it does not own: reach past the client protocol
#: into the machinery behind it.
REACHES = ("_state_machine", "_storage")

#: The one that is worse than a reach-in, because nothing fails when it is wrong.  A
#: node's own answer about whether it leads is not evidence about the shard: a leader that
#: has been partitioned away from its peers still says LEADER, and a node that is about to
#: win an election still says FOLLOWER.
STATE_COMPARISON = re.compile(r"\.state\s*[!=]=")

#: The files that may do either, and what each one is doing there.  A key is a file or a
#: directory, relative to the package.
ALLOWED = {
    "raft": "the shard itself: node, state machine, storage, servicer, shard server.  "
            "Everything else on this list is kept out of it",
    "launcher.py": "the process that builds a shard and its groups, the way RaftCluster and "
                   "ShardServer do in process: it hands a gRPC server to a node it has just "
                   "built, and asks the servers it built who leads.  What it asks is the "
                   "cluster's own question, and no client goes through it",
    "client/node_client.py": "the in-process implementation of the seam.  It delegates to "
                             "the node it was handed and hands nothing back, which is why "
                             "it is the one file that may name the machinery",
    "metadata/service.py": "the metadata group's own state machine, which is not a shard",
    "tso/tso.py": "the timestamp group's own state machine, likewise",
    "transaction/lock_cleaner.py": "a sweep that runs inside the shard's process and looks "
                                   "at its own locks the way a collector looks at its own "
                                   "heap: internal housekeeping, not a client call",
    "database.py": "the embedded single-node engine.  The storage it holds is its own - "
                   "there is no shard and no Raft group anywhere in it",
    "transaction/local.py": "the same engine's transaction manager, over the same storage",
}

#: The callers this seam was built for, and the name that says each one went through it.
#: Absence is not evidence by itself: a file that reaches into nothing because it does
#: nothing would pass the scan.
CALLERS = {
    "metadata/cache.py": "NodeClient",
    "sql/executor.py": "ShardLeaders",
    "transaction/coordinator.py": "ask_shard",
    "transaction/lock_resolver.py": "ask_shard",
    "transaction/smart_client.py": "ask_shard",
}


def _package_files():
    for path in sorted(PACKAGE.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        yield path


def _relative(path: Path) -> str:
    return path.relative_to(PACKAGE).as_posix()


def _is_allowed(relative: str) -> bool:
    return any(relative == name or relative.startswith(name + "/") for name in ALLOWED)


def _reach_ins(text: str):
    """Every line of ``text`` that reaches into a shard, as ``(number, line)`` pairs."""
    found = []
    for number, line in enumerate(text.splitlines(), 1):
        if any(reach in line for reach in REACHES) or STATE_COMPARISON.search(line):
            found.append((number, line.strip()))
    return found


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_nothing_outside_a_shard_reaches_into_one():
    """Every production file either is the shard or holds a client for it."""
    offenders = []
    for path in _package_files():
        relative = _relative(path)
        if _is_allowed(relative):
            continue
        offenders.extend(
            f"{relative}:{number}: {line}"
            for number, line in _reach_ins(_source(path)))

    assert not offenders, (
        "these lines reach past the client protocol into a shard:\n"
        + "\n".join(offenders))


def test_the_allowlist_names_files_that_are_there_for_a_reason():
    """An entry for a file that is gone, or for one that no longer needs it, is stale."""
    for name in ALLOWED:
        matching = [path for path in _package_files()
                    if _relative(path) == name or _relative(path).startswith(name + "/")]
        assert matching, f"{name!r} is on the allowlist and is not in the package"
        assert any(_reach_ins(_source(path)) for path in matching), (
            f"{name!r} no longer reaches into a shard, so it should come off the allowlist")


def test_the_callers_this_seam_was_built_for_hold_a_client():
    """Each of them names what it holds, instead of only avoiding what it must not."""
    for name, marker in CALLERS.items():
        text = _source(PACKAGE / name)
        assert marker in text, f"{name} does not name {marker}"


@pytest.mark.parametrize("name, old, new", [
    ("transaction/coordinator.py",
     "client.get_write_record(key)",
     "client._state_machine._storage.get_latest_write(key)"),
    ("transaction/lock_resolver.py",
     "lock = client.get_lock(primary_key)",
     "lock = client._state_machine.get_lock_status(primary_key)"),
    ("metadata/cache.py",
     "        return client",
     "        if client.state != NodeState.LEADER:\n            return None\n        return client"),
])
def test_the_scan_notices_a_line_that_reaches_in(name, old, new):
    """A control, per shape: the reach-in, and the quiet one.

    The scan is only worth having if it fails on the thing it is for, and the way to show
    that without leaving the file broken is to build the offending line in memory.  Each
    of these is a line the code held before the commit that moved the callers onto clients,
    or one character away from a line it holds now.
    """
    source = _source(PACKAGE / name)
    assert source.count(old) == 1, f"{old!r} is no longer the line this control mutates"

    mutated = source.replace(old, new)
    found = _reach_ins(mutated)
    assert found, "the scan let this through"

    line_number = next(number for number, line in enumerate(mutated.splitlines(), 1)
                       if new.splitlines()[0] in line)
    assert line_number in [number for number, _ in found], (
        "the scan found something, but not the line that was changed")
