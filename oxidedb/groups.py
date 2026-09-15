"""What the routing table's service and the clock's service have in common.

Both are the same kind of service: a proxy for a Raft group with one leader, exposed on
every node of the cluster.  So both refuse in the same two shapes - "this node is not the
leader, and here is where the leader is" and "the group answered, and the answer was no" -
and both classify the group's own error codes the same way.

The messages differ (a table and a range of timestamps are not the same message), so what
is shared is the two things that go into every answer the same way: the code a client acts
on, and the leader's address on the one refusal that has somewhere to send it - and, on the
client's side of the wire, the way back from that code to the one a caller branches on.
"""

from typing import Any, Callable, Dict, Optional

from .proto import groups_pb2
from .raft.state_machine import ErrorCode, is_not_leader


def wire_code(error_code: Optional[int]) -> int:
    """Which of the things a client may act on the group's own code is.

    The groups answer with the codes the shard's state machine uses, and a client on the
    wire is told the same four things a shard's client service tells it.  Only three arrive
    here: neither group can be blocked by a lock, so nothing maps to LOCKED.
    """
    if error_code is None or error_code == ErrorCode.SUCCESS:
        return groups_pb2.OK
    if is_not_leader(error_code):
        return groups_pb2.NOT_LEADER
    return groups_pb2.REFUSED


def local_code(error_code: Optional[int]) -> int:
    """The group's own code for a wire classification a caller has just read.

    The other direction of :func:`wire_code`, and the one place a client of a wire
    service reads a refusal: a shard's client service answers with the same four values as
    these two groups - that is why the two enums are written out the same way - so the way
    back is written out once as well.

    Only two of the four have a code of their own, because a caller branches on two of
    them: NOT_LEADER has somewhere to send the caller, and LOCKED is the one a resolver
    has to unpick.  A refusal that answers no is the code for a command the machine would
    not apply, which is what it was, and the message that came with it says why.
    """
    return {groups_pb2.NOT_LEADER: ErrorCode.ERR_NOT_LEADER,
            groups_pb2.LOCKED: ErrorCode.ERR_LOCKED}.get(
                error_code, ErrorCode.ERR_APPLY_ERROR)


def refusal(message: str, error_code: Optional[int],
            leader_address: Optional[Callable[[], Optional[str]]] = None) -> Dict[str, Any]:
    """The arguments for one refusal, in whatever message shape the caller returns.

    ``error_code`` and ``message`` always, and the leader's address on the one refusal that
    has somewhere to send the caller.  The address is a callable because the answer changes
    while a servicer lives: elections happen underneath it.  None from the callable means
    this node has not heard who leads, which leaves the caller to try its other seeds -
    which is the honest answer, and better than naming the node the caller just left.
    """
    code = wire_code(error_code)
    arguments: Dict[str, Any] = {"error_code": code, "message": message}
    if code == groups_pb2.NOT_LEADER and leader_address is not None:
        address = leader_address()
        if address:
            arguments["leader_address"] = address
    return arguments
