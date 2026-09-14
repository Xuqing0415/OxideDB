"""A node across a wire: the same six primitives, asked over a channel.

The seam ``node_client.py`` draws says a caller should not know whether the node it is
talking to is in this process; this is the side of it that is not.  Every call is one
unary RPC, every answer is rebuilt in the shape ``LocalNodeClient`` returns, and a caller
that holds one of these cannot tell it from the other except by the error codes it loses.

What a refusal loses on the way is the shard's own code.  The wire carries a four-value
classification instead - it worked, ask the leader, a lock is in the way, the answer is no
- and REFUSED comes back here as ``ERR_APPLY_ERROR``, which is the code for a command the
machine would not apply.  Nothing above the seam branches on the finer distinction, and a
caller that needed to would need the enum to grow rather than a string to parse.  What does
survive is ``leader_address``, and it is the reason this file exists: a node that has
stopped leading can name the address of the node that leads now, which turns a retry after
an election from a metadata read into one more RPC.

A node that does not answer at all is ``NodeUnreachable`` and not a refusal.  There is
nothing in the shard's answer to act on because there was no answer, and the retry that fits
- reading the routing table again, in case the leader moved - belongs to ``ask_shard``,
not here.
"""

import threading
from typing import Dict, Optional, Tuple

import grpc

from oxidedb.proto import client_pb2
from oxidedb.proto.client_pb2_grpc import ClientServiceStub

from ..raft.state_machine import ApplyResult, ErrorCode, ReadResult, ScanRefused
from .node_client import NodeClient, NodeClientFactory, NodeUnreachable

#: How long one call waits before the node is written off.  Long enough to cover an
#: election happening underneath a request, which is the slow case a client meets in
#: practice, and short enough that a node which is simply gone does not hold a caller up
#: for long.  It is a per-call deadline, so a commit that needs several calls is bounded by
#: several of these rather than by one.
DEFAULT_TIMEOUT = 4.0


class RemoteNodeClient:
    """One node's six primitives, over a channel to the address that node listens on."""

    def __init__(self, address: str, timeout: float = DEFAULT_TIMEOUT, channel=None):
        self._address = address
        self._timeout = timeout
        #: Kept so that it can be closed: a channel owns a socket and a thread pool of its
        #: own, and a factory that dropped one without closing it would leak both.
        self._channel = grpc.insecure_channel(address) if channel is None else channel
        self._stub = ClientServiceStub(self._channel)

    @property
    def address(self) -> str:
        return self._address

    def get(self, key: bytes, timestamp: Optional[int] = None) -> ReadResult:
        request = client_pb2.GetRequest(key=key)
        if timestamp is not None:
            request.timestamp = timestamp

        response = self._call(self._stub.Get, request)
        if response.error_code == client_pb2.OK:
            return ReadResult.success(
                response.value if response.HasField("value") else None)
        return ReadResult.failure(*self._failure(response))

    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None) -> list:
        request = client_pb2.ScanRequest(start_key=start_key, end_key=end_key)
        if timestamp is not None:
            request.timestamp = timestamp

        response = self._call(self._stub.Scan, request)
        if response.error_code != client_pb2.OK:
            # The refusal is raised rather than returned, exactly as it is in process:
            # rows are the answer's shape, and a refusal returned in their place would be
            # a range read that came back short, which is indistinguishable from a range
            # that is empty.
            code, message, hint = self._failure(response)
            locked_key = response.locked_key if response.HasField("locked_key") else None
            raise ScanRefused(code, message, key=locked_key, leader_address=hint)

        return [(entry.key, entry.value) for entry in response.entries]

    def propose(self, command: bytes) -> ApplyResult:
        response = self._call(self._stub.Propose,
                              client_pb2.ProposeRequest(command=command))
        if response.error_code != client_pb2.OK:
            return ApplyResult.failure(*self._failure(response))

        return ApplyResult.success(
            data=response.data if response.HasField("data") else None,
            index=response.index if response.HasField("index") else None)

    def get_lock(self, key: bytes) -> Optional[Dict]:
        response = self._call(self._stub.GetLock, client_pb2.GetLockRequest(key=key))
        if not response.HasField("lock"):
            return None

        lock = response.lock
        # Field for field what the storage returns for a lock: a caller that resolves
        # locks should not have to know which side of a wire it is on.
        return {"key": lock.key, "start_ts": lock.start_ts, "status": lock.status,
                "primary_key": lock.primary_key, "lock_time": lock.lock_time,
                "value": lock.value}

    def get_write_record(self, key: bytes) -> Optional[Dict]:
        response = self._call(self._stub.GetWriteRecord,
                              client_pb2.GetWriteRecordRequest(key=key))
        if not response.HasField("start_ts") and not response.HasField("commit_ts"):
            return None

        return {"start_ts": response.start_ts, "commit_ts": response.commit_ts}

    def follower_read_index(self) -> Tuple[Optional[int], Optional[str]]:
        response = self._call(self._stub.FollowerReadIndex,
                              client_pb2.FollowerReadIndexRequest())
        if response.error_code != client_pb2.OK:
            return None, self._failure(response)[1]

        return response.read_index, None

    def close(self) -> None:
        """Let the channel go.  A client that has been closed is not usable again."""
        self._channel.close()

    # -- the two things that are not simply a field -------------------------

    def _call(self, method, request):
        """One RPC, with the one failure that is not an answer turned into a named error."""
        try:
            return method(request, timeout=self._timeout)
        except grpc.RpcError as error:
            raise NodeUnreachable(f"{self._address}: {error}") from error

    @staticmethod
    def _failure(response):
        """A refusal's code, message and hint, in the shapes the caller expects.

        The classification is turned back into a code here, which is where the loss
        happens: LOCKED and NOT_LEADER have codes of their own because callers branch on
        them, and everything else is a command the machine would not apply.  See the
        module docstring.
        """
        code = {client_pb2.NOT_LEADER: ErrorCode.ERR_NOT_LEADER,
                client_pb2.LOCKED: ErrorCode.ERR_LOCKED}.get(
                    response.error_code, ErrorCode.ERR_APPLY_ERROR)
        hint = response.leader_address if response.HasField("leader_address") else None
        return code, response.message, hint


class RemoteNodeClientFactory:
    """Handles on nodes across channels, built when first asked for and kept.

    Keyed the way the routing table names a node - a shard and a node id - with the address
    kept alongside, because an address is the only thing a channel can be opened to and the
    only thing a leader hint gives.  The two are remembered together so that a caller that
    later asks for the pair is handed the same channel, and so that ``forget_client`, which
    is given the pair, can find the handle it is meant to drop.

    One channel per address, not per pair: a node serves each shard on a port of its own, so
    an address already identifies a replica, and two callers asking about it are asking about
    the same thing.
    """

    def __init__(self, timeout: float = DEFAULT_TIMEOUT):
        self._timeout = timeout
        self._lock = threading.Lock()
        self._addresses: Dict[Tuple[int, int], str] = {}
        self._clients: Dict[str, RemoteNodeClient] = {}

    def get_client(self, shard_id: int, node_id: int,
                   address: Optional[str] = None) -> Optional[NodeClient]:
        with self._lock:
            if address is None:
                address = self._addresses.get((shard_id, node_id))
            else:
                self._addresses[(shard_id, node_id)] = address

            if address is None:
                # A pair nobody has been given an address for is a node this client has
                # no way to reach, which is an answer and not a failure: the table can
                # name a replica set before anybody has said where those nodes listen.
                return None
            return self._client_for(address)

    def get_client_at(self, shard_id: int, address: str) -> Optional[NodeClient]:
        with self._lock:
            return self._client_for(address)

    def forget_client(self, shard_id: int, node_id: int) -> None:
        """Drop the handle for that node and close its channel.

        A handle is dropped because a caller found it broken, and a broken channel is not
        worth keeping: the next ask opens a new one.  The address stays, because it is not
        the thing that broke - where a node is has not changed, only the way this client was
        getting there - so the next ask for the pair builds a second channel to the same
        place rather than coming back empty-handed.  Forgetting a pair that was never
        registered is not an error: a caller that found one broken and one that never had
        one want the same thing to happen.
        """
        with self._lock:
            address = self._addresses.get((shard_id, node_id))
            client = self._clients.pop(address, None) if address is not None else None

        if client is not None:
            client.close()

    def close(self) -> None:
        """Close every channel this factory opened."""
        with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
            self._addresses.clear()

        for client in clients:
            client.close()

    def _client_for(self, address: str) -> RemoteNodeClient:
        """The handle for ``address``, opened once.  Called with the lock held."""
        client = self._clients.get(address)
        if client is None:
            client = RemoteNodeClient(address, timeout=self._timeout)
            self._clients[address] = client
        return client
