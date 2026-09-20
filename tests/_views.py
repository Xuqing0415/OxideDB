"""What both sides of a recovery view are held to, for the tests that hold them to it.

``RecoveryView`` is a ``runtime_checkable`` protocol, so ``isinstance`` asks whether the
names are there and nothing else: a class whose methods have the right names and the wrong
arguments passes it, and fails the first time a recovery calls one - in the one code path
that only runs after a crash.  So each side of the wire is asked twice, and the second
question is the same for both: what does each call take?

Annotations are left out of the comparison on purpose.  What the two sides have to agree on
is the calling convention - the names, which may be left out, which are keyword-only - and
the protocol names a few of its types loosely by design: the routing table's client is
deliberately not a type at all, so one side answering with its own kind of client is not a
disagreement about how the call is made.
"""

import inspect
from typing import Any, List, Set, Tuple

from oxidedb.raft.recovery_view import RecoveryView


def calls() -> Set[str]:
    """Every call the protocol names, which is every public name it has."""
    return {name for name in dir(RecoveryView) if not name.startswith("_")}


def convention(callee: Any) -> List[Tuple[str, str, Any]]:
    """What a caller has to agree on: each parameter's name, kind and default."""
    return [(parameter.name, str(parameter.kind), parameter.default)
            for parameter in inspect.signature(callee).parameters.values()]


def mismatches(implementation: Any) -> dict:
    """Every call ``implementation`` takes differently from the protocol, by call name.

    Empty is the answer that matters: the names are there (which ``isinstance`` said) and
    each call is made the way the protocol declares it, which is the half ``isinstance``
    cannot see and a caller passing something by keyword finds out the hard way.
    """
    taken = {}
    for name in sorted(calls()):
        there = convention(getattr(RecoveryView, name))
        here = convention(getattr(implementation, name))
        if here != there:
            taken[name] = (there, here)
    return taken
