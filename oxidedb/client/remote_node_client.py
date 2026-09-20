"""A node across a wire: the same primitives, asked over a channel.

The seam ``node_client.py`` draws says a caller should not know whether the node it is
talking to is in this process; this is the side of it that is not.  Every call is one
unary RPC, every answer is rebuilt in the shape ``LocalNodeClient`` returns, and a caller
that holds one of these cannot tell it from the other except by the error codes it loses.

What a refusal loses on the way is the shard's own code.  The wire carries a five-value
classification instead - it worked, ask the leader, a lock is in the way, the shard did not
get to a decision, the answer is no - and REFUSED comes back here as ``ERR_APPLY_ERROR``,
which is the code for a command the machine would not apply.  Nothing above the seam
branches on the finer distinction, and a caller that needed to would need the enum to grow
rather than a string to parse.  What does survive is ``leader_address``, and it is the
reason this file exists: a node that has stopped leading can name the address of the node
that leads now, which turns a retry after an election from a metadata read into one more
RPC.

A node that does not answer at all is ``NodeUnreachable`` and not a refusal.  There is
nothing in the shard's answer to act on because there was no answer, and the retry that fits
- reading the routing table again, in case the leader moved - belongs to ``ask_shard``,
not here.
"""

import threading
from typing import Dict, Optional, Sequence, Tuple

import grpc

from oxidedb.proto import client_pb2
from oxidedb.proto.client_pb2_grpc import ClientServiceStub

from ..channels import ChannelPool, DEFAULT_TIMEOUT
from ..groups import local_code
from ..raft.state_machine import ApplyResult, ReadResult, ScanRefused
from .node_client import NodeClient, NodeClientFactory, NodeUnreachable
from .remote_group_client import RemoteMetadataClient, RemoteTSOClient


class RemoteNodeClient:
    """One node's primitives, over a channel to the address that node listens on."""

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

    def get(self, key: bytes, timestamp: Optional[int] = None,
            read_index: Optional[int] = None) -> ReadResult:
        request = client_pb2.GetRequest(key=key)
        if timestamp is not None:
            request.timestamp = timestamp
        if read_index is not None:
            request.read_index = read_index

        response = self._call(self._stub.Get, request)
        if response.error_code == client_pb2.OK:
            # The basis crosses with the answer, as it does in process: it is the index a
            # caller names on the reads that follow, and an answer that arrived without
            # it would leave the caller with nothing to name.
            return ReadResult.success(
                response.value if response.HasField("value") else None,
                commit_ts=response.commit_ts,
                read_index=self._read_index_of(response))
        # A refusal can be an answer as of an index all the same - a key behind a lock was
        # seen at one, and the read that found it is consistent to it - so the basis goes
        # on this result too, and the local carrier leaves it unset for exactly the
        # refusals that never reached a machine.
        refused = ReadResult.failure(*self._failure(response))
        refused.read_index = self._read_index_of(response)
        return refused

    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None, read_index: Optional[int] = None) -> list:
        """The rows of :meth:`scan_versions`, with the version dropped.

        One call to the node and two ways to read what came back, so a caller that
        wants rows does not take a different path from one that wants versions.
        """
        return [(key, value)
                for key, value, _ in self.scan_versions(start_key, end_key, timestamp,
                                                        read_index)]

    def scan_versions(self, start_key: bytes, end_key: bytes,
                      timestamp: Optional[int] = None, read_index: Optional[int] = None
                      ) -> list:
        """The same rows, each with the version it is at - which is what a copy needs.

        The version is the timestamp the value was written at, and it survives the
        wire because the response's rows carry it.  0 means the row has none, which
        is the case for a row that came from this reader's own write intent: an
        intent is a lock rather than a version.
        """
        request = client_pb2.ScanRequest(start_key=start_key, end_key=end_key)
        if timestamp is not None:
            request.timestamp = timestamp
        if read_index is not None:
            request.read_index = read_index

        response = self._call(self._stub.Scan, request)
        if response.error_code != client_pb2.OK:
            # The refusal is raised rather than returned, exactly as it is in process:
            # rows are the answer's shape, and a refusal returned in their place would be
            # a range read that came back short, which is indistinguishable from a range
            # that is empty.
            code, message, hint = self._failure(response)
            locked_key = response.locked_key if response.HasField("locked_key") else None
            raise ScanRefused(code, message, key=locked_key, leader_address=hint)

        return [(entry.key, entry.value, entry.commit_ts) for entry in response.entries]

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

    def follower_read_index(self, answer_locally: bool = False
                            ) -> Tuple[Optional[int], Optional[str]]:
        response = self._call(self._stub.FollowerReadIndex,
                              client_pb2.FollowerReadIndexRequest(
                                  answer_locally=answer_locally))
        if response.error_code != client_pb2.OK:
            return None, self._failure(response)[1]

        return response.read_index, None

    def close(self) -> None:
        """Let the channel go.  A client that has been closed is not usable again.

        A handle this client did not open a channel for - one the factory handed out -
        shares that channel with the factory and with everything else it handed out for the
        same address, so closing one of those closes it for all of them.  The factory's
        ``forget_client`` is what drops a handle that has stopped working; this is for the
        client that opened its own.
        """
        self._channel.close()

    # -- the two things that are not simply a field -------------------------

    def _call(self, method, request):
        """One RPC, with the one failure that is not an answer turned into a named error."""
        try:
            return method(request, timeout=self._timeout)
        except grpc.RpcError as error:
            raise NodeUnreachable(f"{self._address}: {error}") from error

    @staticmethod
    def _read_index_of(response):
        """The index an answer is as of, or None when the wire carried none.

        The response field has no presence, so 0 is how a wire says "none" - no log
        has an index 0 - and a refusal that never reached a state machine is the case
        that uses it.  A refusal the machine did make carries the index the answer was
        seen at, which is a basis like any other: a key behind a lock was observed at
        one.

        A read named at an index that was not applied in time is the exception, and it
        is not a basis: the index comes back so that the same read can be asked again
        at it somewhere else, which is the request rather than an answer.  The local
        carrier leaves the field unset for that read, and reading the field back here
        is what keeps a result from depending on which side of a wire it came from.
        """
        if response.error_code == client_pb2.TIMEOUT:
            return None
        return response.read_index or None

    @staticmethod
    def _failure(response):
        """A refusal's code, message and hint, in the shapes the caller expects.

        The classification is turned back into a code by the one function that does it
        (``groups.local_code``), which is where the loss happens: see the module docstring.
        """
        hint = response.leader_address if response.HasField("leader_address") else None
        return local_code(response.error_code), response.message, hint


class RemoteNodeClientFactory:
    """Handles on nodes across channels, built when first asked for and kept.

    Keyed the way the routing table names a node - a shard and a node id - with the address
    kept alongside, because an address is the only thing a channel can be opened to and the
    only thing a leader hint gives.  The two are remembered together so that a caller that
    later asks for the pair is handed the same channel, and so that ``forget_client``, which
    is given the pair, can find the handle it is meant to drop.

    One channel per address, not per pair: a node serves each shard on a port of its own, so
    an address already identifies a replica, and two callers asking about it are asking about
    the same thing.  The channels themselves are one pool, shared with the two group clients
    below, because a pool per kind of client would be a place per kind of client to leak a
    socket from.

    The two groups that are not a shard are reached through this factory as well, and are
    given seed addresses rather than looked up: a client outside the cluster has nothing to
    look a group up in - the table names shards, and the clock is placed nowhere - so a
    caller that knows where the nodes are hands those addresses in, and the clients walk
    them.  See ``remote_group_client``.
    """

    def __init__(self, timeout: float = DEFAULT_TIMEOUT,
                 metadata_seeds: Sequence[str] = (),
                 tso_seeds: Sequence[str] = ()):
        self._timeout = timeout
        self._lock = threading.Lock()
        self._addresses: Dict[Tuple[int, int], str] = {}
        self._clients: Dict[str, RemoteNodeClient] = {}
        self._channels = ChannelPool()
        #: Where the routing table's group and the clock listen, from this client's side.
        #: They are two lists and not one because they are two groups on two sets of ports.
        self._metadata_seeds = list(metadata_seeds)
        self._tso_seeds = list(tso_seeds)
        self._metadata_client: Optional[RemoteMetadataClient] = None
        self._tso_client: Optional[RemoteTSOClient] = None

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
            if address is not None:
                self._clients.pop(address, None)

        if address is not None:
            # The channel goes as well as the handle: it is the thing that stopped working,
            # and keeping it would hand the same broken channel to the next caller.  The
            # address stays, so the next ask opens a new one to the same place.
            self._channels.forget(address)

    # -- the groups that are not a shard ------------------------------------

    def metadata_client(self) -> RemoteMetadataClient:
        """The routing table's client, built once, over the seeds this factory was given."""
        with self._lock:
            if self._metadata_client is None:
                self._metadata_client = RemoteMetadataClient(
                    self._metadata_seeds, channels=self._channels, timeout=self._timeout)
            return self._metadata_client

    def tso_client(self) -> RemoteTSOClient:
        """The clock's client, built once, over the seeds this factory was given."""
        with self._lock:
            if self._tso_client is None:
                self._tso_client = RemoteTSOClient(
                    self._tso_seeds, channels=self._channels, timeout=self._timeout)
            return self._tso_client

    def close(self) -> None:
        """Close every channel this factory opened.  The handles go with them."""
        with self._lock:
            self._clients.clear()
            self._addresses.clear()
            self._metadata_client = None
            self._tso_client = None

        # One call closes every channel, including the ones the group clients are using:
        # they share this pool, so a client closed here is closed by the channel going, which
        # is the same thing that happens to a caller that has stopped using it.
        self._channels.close()

    def _client_for(self, address: str) -> RemoteNodeClient:
        """The handle for ``address``, opened once.  Called with the lock held."""
        client = self._clients.get(address)
        if client is None:
            client = RemoteNodeClient(address, timeout=self._timeout,
                                      channel=self._channels.channel(address))
            self._clients[address] = client
        return client
