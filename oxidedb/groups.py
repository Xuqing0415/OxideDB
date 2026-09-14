"""What the routing table's service and the clock's service have in common.

Both are the same kind of service: a proxy for a Raft group with one leader, exposed on
every node of the cluster.  So both refuse in the same two shapes - "this node is not the
leader, and here is where the leader is" and "the group answered, and the answer was no" -
and both classify the group's own error codes the same way.

The messages differ (a table and a range of timestamps are not the same message), so what
is shared is the two things that go into every answer the same way: the code a client acts
on, and the leader's address on the one refusal that has somewhere to send it.
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
