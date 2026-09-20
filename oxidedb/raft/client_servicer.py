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

Transport failures do not appear here at all, with one exception that is this file's
own: a node asked for a read index carries the question to the leader it has heard of,
and that leader may be a node which does not answer.  It is still an answer rather than a
gRPC status - the caller asked this node, and this node's answer is that it does not lead
and could not reach whoever does.  Every other transport failure - a connection that
dropped, a message that arrived malformed - is the wire's problem and reaches the caller
as a gRPC error.  What this file answers is the other case: the call arrived, and the
node's answer was no.
"""

from oxidedb.proto import client_pb2
from oxidedb.proto.client_pb2_grpc import ClientServiceServicer

from ..client.node_client import LocalNodeClient, NodeUnreachable
from .state_machine import ErrorCode, ScanRefused, is_not_leader


class ClientServicer(ClientServiceServicer):
    """One node's six primitives, on the port that node already listens on for Raft.

    ``leader_address`` is a callable and not an address, because the answer changes while
    this servicer lives: elections happen underneath it, and the node it wraps is the one
    that hears about them.  ``client_at_address`` turns one of those addresses into a
    client, which is what carrying a question to the leader needs, and both are the
    server's own way of reaching a node it is not.
    """

    def __init__(self, node, leader_address=None, client_at_address=None):
        self._client = LocalNodeClient(node)
        self._node = node
        self._leader_address = leader_address
        #: How to reach a node this one is not, by an address a leader was heard at.
        #: None where there is nothing to build a client from: an in-process server has
        #: its peers as objects rather than as addresses, and ``leader_address`` answers
        #: None there too, so the two absences line up rather than being two cases.
        self._client_at_address = client_at_address

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
            # And which version it is, so that a caller carrying the value somewhere
            # else carries its identity too.  0 is the answer for the one value that
            # is not a version - a reader's own write intent - and for a key that is
            # not there, which has no version for the same reason it has no value.
            response.commit_ts = result.commit_ts
        self._add_leader_hint(response, result.error_code)
        return response

    def Scan(self, request, context):
        timestamp = request.timestamp if request.HasField("timestamp") else None
        try:
            rows = self._client.scan_versions(request.start_key, request.end_key,
                                              timestamp)
        except ScanRefused as refusal:
            response = client_pb2.ScanResponse(
                error_code=self._wire_code(refusal.error_code),
                message=refusal.error_msg or "")
            if refusal.key is not None:
                response.locked_key = refusal.key
            self._add_leader_hint(response, refusal.error_code)
            return response

        response = client_pb2.ScanResponse(error_code=client_pb2.OK)
        for key, value, commit_ts in rows:
            entry = response.entries.add()
            entry.key = key
            entry.value = value
            entry.commit_ts = commit_ts
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
        """The index a read may be served at, from this node or from whoever leads.

        A follower cannot confirm an index, and a caller that asks a follower for one is
        usually talking to a follower on purpose: that is where its read is going.  So
        the question is carried once to the leader this node has heard from - the address
        it would otherwise only hint at - and what comes back is this node's answer.

        Once, and the request that travels on says so.  A node whose leader has moved
        would otherwise pass the question to the leader it used to have, which can be the
        node the question came from, and the two would ask each other for as long as the
        caller was willing to wait.
        """
        read_index, failure = self._client.follower_read_index()
        carried_to = None

        if read_index is None and not request.answer_locally:
            read_index, failure, carried_to = self._ask_the_leader(failure)

        if read_index is None:
            response = client_pb2.FollowerReadIndexResponse(
                error_code=client_pb2.NOT_LEADER, message=failure or "Not leader")
            if carried_to is None:
                # The hint is what makes this refusal worth having, and it is worth
                # having only while nobody has been asked: an address that has just
                # failed is not somewhere to send the caller next.
                self._add_leader_hint(response, ErrorCode.ERR_NOT_LEADER)
            return response
        return client_pb2.FollowerReadIndexResponse(
            error_code=client_pb2.OK, read_index=read_index)

    def _ask_the_leader(self, failure):
        """Ask whoever leads: ``(index, why not, the address it was asked at)``.

        This node does not lead and cannot confirm an index, so the only thing left that
        can answer is the leader it has heard of.  There are two ways for that to come to
        nothing and they are not the same refusal.

        No address was ever heard, or there is no way to reach one: the third value is
        None and the refusal keeps its hint, because naming the leader is still the
        useful part of it.

        An address was named and did not answer, or answered without an index of its own:
        the third value is that address, and there is nothing left to hint at - a node
        that has just denied leading is not somewhere to send the caller, and neither is
        one that has gone quiet.  The answer is ``NOT_LEADER`` rather than a refusal of
        its own, because the caller's move is the one that code asks for, ask somebody
        else; the address goes in the message, so that whoever reads it can see where the
        question went instead of guessing which of its seeds to try next.
        """
        address = self._leader_address() if self._leader_address is not None else None
        if not address or self._client_at_address is None:
            return None, failure, None

        client = self._client_at_address(address)
        if client is None:
            return None, failure, None

        try:
            index, why = client.follower_read_index(answer_locally=True)
        except NodeUnreachable as unreachable:
            why = f"the leader at {address} did not answer: {unreachable}"
            return None, why, address
        if index is None:
            return None, why, address
        return index, None, address

    # -- the mapping between the shard's codes and the wire's ---------------

    def _wire_code(self, error_code):
        """Which of the four things a client is allowed to act on this refusal is.

        None is what a successful read carries - the code is only set on a failure - so
        it classifies as OK, the same as the state machine's own ``SUCCESS``.
        """
        if error_code is None or error_code == ErrorCode.SUCCESS:
            return client_pb2.OK
        if is_not_leader(error_code):
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
