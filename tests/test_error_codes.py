"""The codes a node refuses with are named, and a gatekeeper keeps them that way.

A refusal carries an `ErrorCode`, and the caller's next step depends on which one: a
node that has stopped leading means "ask the leader instead", a command the state
machine could not read means "this call will never work here".  Two of those used to be
the same number.  A proposal refused because leadership was lost came back as a bare
`2`, which is also `ERR_APPLY_ERROR`, so the code could not tell the two apart and only
the message could - the kind of difference no caller should have to read prose to find.

This scans the source rather than the behaviour on purpose.  What is being prevented is
a literal in a call, and no test that exercises these paths would notice one: the number
is right, the behaviour is right, and only the name is missing - until something
dispatches on it.  `tests/test_client_boundary.py` is the same kind of gatekeeper for
the state machine's internals.

It scans one file, deliberately.  `raft/state_machine.py` has seven refusals that name
no code (their numbers do match the enum, so nothing is wrong there yet),
`metadata/service.py` has twelve that are not in `ErrorCode` at all - the metadata
group's own private space, two of which tests pin - and `tso/tso.py` has one whose
message says "Apply error" under a code that means unknown.  Naming those is a decision
per layer and not a scan, so this holds the line where callers dispatch: the node.
"""

import ast
import pathlib

from oxidedb.raft.state_machine import ErrorCode

NODE_SOURCE = (pathlib.Path(__file__).resolve().parents[1]
               / "oxidedb" / "raft" / "node.py")


def _refusals(source):
    """Every ``....failure(...)`` call in ``source``, as (line, first argument).

    Any object's ``failure`` counts, not only ``ApplyResult``'s:
    ``ReadResult.failure`` takes the same two arguments, and a literal in either is the
    same mistake.
    """
    found = []
    for node in ast.walk(ast.parse(source)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "failure" and node.args):
            found.append((node.lineno, node.args[0]))
    return found


def _bare_codes(source):
    """The lines where a refusal names no code, only a number."""
    return [line for line, first in _refusals(source)
            if isinstance(first, ast.Constant)]


def test_no_bare_error_codes_in_node():
    """Every refusal in the node names its code, so a caller can dispatch on it."""
    refusals = _refusals(NODE_SOURCE.read_text(encoding="utf-8"))
    assert len(refusals) >= 6, (
        "the scan found almost no refusals, so it is not looking at what it thinks")

    offenders = [line for line, first in refusals if isinstance(first, ast.Constant)]
    assert not offenders, (
        "these refusals carry a number instead of an ErrorCode name, so two "
        f"different refusals can share it: lines {offenders}")


def test_the_scan_catches_a_name_turned_back_into_its_number():
    """The control: the real file with one name replaced by its value is caught.

    Without this, the test above would pass just as well if the scan were broken and
    finding nothing at all.
    """
    source = NODE_SOURCE.read_text(encoding="utf-8")
    name = "ErrorCode.ERR_LEADERSHIP_LOST"
    assert source.count(name) == 1

    # The line of the call, not of its argument: the call is what the scan reports,
    # and the two are different lines because the arguments are wrapped.
    name_at = source.index(name)
    mutated = source.replace(name, str(ErrorCode.ERR_LEADERSHIP_LOST))
    call_line = source[:source.rindex("ApplyResult.failure(", 0, name_at)].count("\n") + 1

    assert _bare_codes(mutated) == [call_line]


def test_the_codes_the_proposal_path_refuses_with_are_distinct():
    """A lost leadership and a command that could not be applied are not one code."""
    assert len({ErrorCode.ERR_APPLY_ERROR, ErrorCode.ERR_LEADERSHIP_LOST,
                ErrorCode.ERR_TIMEOUT, ErrorCode.ERR_ENTRY_OVERWRITTEN}) == 4