"""A node, answering the six client primitives over a wire.

``LocalNodeClient`` is what a caller in this process meets a shard through; this is the
same six calls on a port, so that a caller in another process meets the same shard.  The
translation is deliberately thin: the servicer holds a ``LocalNodeClient`` for its node
and does nothing to an answer except put it in a message and classify it.

The classification is the interesting part, and it is lossy on purpose.  A shard refuses
in its own codes - a write conflict, a frozen range, a lock - while a client on the wire
is told one of four things: it worked, ask the leader instead, a lock is in the way, or
the answer is no.  Those four are what a *client* can act on, and the rest of the codes
are the shard's own business: nothing above the seam branches on them, and a caller that
needed to would need the enum to grow.  What a refusal does carry is the message, so an
operator reading a log can still see what the shard said.

Two refusals are folded together on purpose.  ``ERR_LEADERSHIP_LOST`` means the node was
losing its leadership while the proposal was in flight, which from a caller's side is the
same thing as ``ERR_NOT_LEADER``: it should ask whoever leads now.  ``ERR_TIMEOUT`` is not
folded in - it is not an answer about leadership, and the honest classification for "the
shard did not get to a decision" is that the command was refused.

Transport failures do not appear here at all.  Nothing in this file sets a gRPC status:
a request that cannot be answered as a request - a connection that dropped, a message
that arrived malformed - is the wire's problem and reaches the caller as a gRPC error.
What this file answers is the other case: the call arrived, and the node's answer was no.
"""

from oxidedb.proto import client_pb2
from oxidedb.proto.client_pb2_grpc import ClientServiceServicer

from ..client.node_client import LocalNodeClient
from .state_machine import ErrorCode, ScanRefused


class ClientServicer(ClientServiceServicer):
    """One node's six primitives, on the port that node already listens on for Raft.

    ``leader_address`` is a callable and not an address, because the answer changes while
    this servicer lives: elections happen underneath it, and the node it wraps is the one
    that hears about them.
    """

    def __init__(self, node, leader_address=None):
        self._client = LocalNodeClient(node)
        self._node = node
        self._leader_address = leader_address

    def Get(self, request, context):
        timestamp = request.timestamp if request.HasField("timestamp") else None
        result = self._client.get(request.key, timestamp)

        response = client_pb2.GetResponse(
            error_code=self._wire_code(result.error_code),
            message=self._message(result))
        # Unset rather than empty: a key that is not there at this snapshot is a read
        # that answered, and the caller tells it from a failure by the error code.
        if result.success and result.value is not None:
            response.value = result.value
        self._add_leader_hint(response, result.error_code)
        return response

    def Scan(self, request, context):
        timestamp = request.timestamp if request.HasField("timestamp") else None
        try:
            rows = self._client.scan(request.start_key, request.end_key, timestamp)
        except ScanRefused as refusal:
            response = client_pb2.ScanResponse(
                error_code=self._wire_code(refusal.error_code),
                message=refusal.error_msg or "")
            if refusal.key is not None:
                response.locked_key = refusal.key
            self._add_leader_hint(response, refusal.error_code)
            return response

        response = client_pb2.ScanResponse(error_code=client_pb2.OK)
        for key, value in rows:
            entry = response.entries.add()
            entry.key = key
            entry.value = value
        return response

    def Propose(self, request, context):
        result = self._client.propose(request.command)

        response = client_pb2.ProposeResponse(
            error_code=self._wire_code(result.error_code),
            message=self._message(result),
            term=self._node.current_term)
        if result.index is not None:
            response.index = result.index
        if result.data is not None:
            response.data = result.data
        self._add_leader_hint(response, result.error_code)
        return response

    def GetLock(self, request, context):
        record = self._client.get_lock(request.key)

        response = client_pb2.GetLockResponse(error_code=client_pb2.OK)
        if record is not None:
            response.lock.CopyFrom(client_pb2.LockRecord(
                key=record["key"],
                start_ts=record["start_ts"],
                status=record["status"],
                primary_key=record["primary_key"],
                lock_time=record["lock_time"],
                value=record["value"]))
        return response

    def GetWriteRecord(self, request, context):
        record = self._client.get_write_record(request.key)

        response = client_pb2.GetWriteRecordResponse(error_code=client_pb2.OK)
        if record is not None:
            response.start_ts = record["start_ts"]
            response.commit_ts = record["commit_ts"]
        return response

    def FollowerReadIndex(self, request, context):
        read_index, failure = self._client.follower_read_index()

        if read_index is None:
            response = client_pb2.FollowerReadIndexResponse(
                error_code=client_pb2.NOT_LEADER,
                message=failure or "Not leader")
            self._add_leader_hint(response, ErrorCode.ERR_NOT_LEADER)
            return response
        return client_pb2.FollowerReadIndexResponse(
            error_code=client_pb2.OK, read_index=read_index)

    # -- the mapping between the shard's codes and the wire's ---------------

    def _wire_code(self, error_code):
        """Which of the four things a client is allowed to act on this refusal is.

        None is what a successful read carries - the code is only set on a failure - so
        it classifies as OK, the same as the state machine's own ``SUCCESS``.
        """
        if error_code is None or error_code == ErrorCode.SUCCESS:
            return client_pb2.OK
        if error_code in (ErrorCode.ERR_NOT_LEADER, ErrorCode.ERR_LEADERSHIP_LOST):
            return client_pb2.NOT_LEADER
        if error_code == ErrorCode.ERR_LOCKED:
            return client_pb2.LOCKED
        return client_pb2.REFUSED

    @staticmethod
    def _message(result) -> str:
        """The shard's prose for a refusal, and nothing at all for an answer."""
        return "" if result.success else (result.error_msg or "")

    def _add_leader_hint(self, response, error_code):
        """Name the leader, on the one refusal that has somewhere to send the caller."""
        if self._wire_code(error_code) != client_pb2.NOT_LEADER:
            return
        if self._leader_address is None:
            return
        address = self._leader_address()
        if address:
            response.leader_address = address
