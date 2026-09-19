"""The protocol is a gate rather than a document: one call short is not a view.

``RecoveryView`` is what a recovery is handed, and neither side that implements it is
written *as* one: a cluster is a cluster and a node is a node, and their methods are
implementations of it because of the question each one answers.  So the protocol has to be
checkable at run time - a side missing one of the calls has to fail that check rather than
fail half way through a recovery, in the one code path that only runs after a crash.

What is pinned here is the gate itself: that a class answering every call passes it, and
that a class missing any single one does not.  That each of the two sides really answers
all of them is asked of those classes, in the tests that own them.
"""

from typing import Set

from oxidedb.raft.recovery_view import RecoveryView


def _calls() -> Set[str]:
    """Every call the protocol names, which is every public name it has."""
    return {name for name in dir(RecoveryView) if not name.startswith("_")}


def _answering(names: Set[str]):
    """A class that answers exactly ``names``."""
    return type("_Stub", (), {name: (lambda self, *args, **kwargs: None)
                              for name in names})


def test_a_class_that_answers_every_call_is_a_view():
    calls = _calls()
    assert calls, "the protocol names no calls at all"
    assert isinstance(_answering(calls)(), RecoveryView)


def test_a_class_missing_any_one_call_is_not_a_view():
    """One call short is not a view, whichever one it is: that is the whole gate."""
    calls = _calls()
    for missing in sorted(calls):
        short = _answering(calls - {missing})()
        assert not isinstance(short, RecoveryView), \
            f"{missing} is not needed for a class to be a view"
