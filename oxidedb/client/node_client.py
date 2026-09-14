"""What a client may ask one node, and the way to ask the node in this process.

Everything that talks to a shard - the transaction coordinator, the lock resolver,
the SQL executor - talks to it through these six calls, because these are the six
things a node can answer without being handed a decision that is not its to make.
``proto/client.proto`` carries the same six over a wire, in the same shapes.

Two things follow from the seam.

The first is that the node in this process and the node across one are the same node
to a caller.  ``LocalNodeClient`` wraps a ``MemoryRaftNode`` it already holds, a
``RemoteNodeClient`` will wrap a channel, and neither the coordinator nor the
resolver changes when the second one arrives.

The second is that nothing here reaches around the client.  A wrapper that handed out
the state machine would be a wrapper that only works in process, which is the one
case that has to keep working while the other one is built - and it would fail
silently, because a local state machine always answers.

The primitives are the node's and not a transaction's.  A client builds the command
bytes itself - ``Propose`` carries them - because the Percolator coordinator is
client-side: it decides which key is primary, and a node that decided that for it
would have to have been told what a transaction is.

The refusals are values, not exceptions.  ``get`` answers with a result whose error
code can say "the key is not there at this snapshot" or "a lock is in the way", and
those are different answers a caller reacts to differently; only ``scan`` raises,
because a range read's shape is rows and a dropped key would look like an absent one.
"""

import threading
from typing import Any, Dict, List, Optional, Protocol, Tuple, runtime_checkable

from ..raft.node import MemoryRaftNode
from ..raft.state_machine import ApplyResult, ReadResult


@runtime_checkable
class NodeClient(Protocol):
    """One node, as a client sees it.

    Each call is a question a node can answer about itself: what a key reads as, what
    a range reads as, whether a command landed, what lock is on a key, what the newest
    committed version of a key was, and how far this node's log has committed.
    """

    def get(self, key: bytes, timestamp: Optional[int] = None) -> ReadResult:
        """Read ``key`` at ``timestamp``, or at the newest version when it is None.

        A result rather than bytes, because a read has three answers and not two: a
        value, no value (a key that is not there at this snapshot), and a key that
        cannot be read *yet* because a lock is in the way.  Telling the last apart
        from the first is the caller's job, so it comes back as an error code.
        """

    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None) -> List[Tuple[bytes, bytes]]:
        """Every key in ``[start_key, end_key)`` at ``timestamp``.

        Rows only, so an empty list means the range is empty and a range read that
        cannot answer raises ``ScanRefused`` instead of coming back short.
        """

    def propose(self, command: bytes) -> ApplyResult:
        """Append ``command``, wait for it to commit and apply, return what it said.

        The command is built on this side of the seam and the node appends the bytes
        as they are.  A refusal - not the leader, a conflict, a frozen shard - is a
        result with an error code, because the caller has to decide whether to retry
        elsewhere, which is a decision the bytes of an exception cannot carry.
        """

    def get_lock(self, key: bytes) -> Optional[Dict[str, Any]]:
        """The newest lock on ``key``, or None when there is none.

        The record itself - status, start_ts, primary_key, lock_time, value - and
        not a judgement about it.  Whether the transaction that left the lock
        committed is a question about the primary key, and it is a separate call.
        """

    def get_write_record(self, key: bytes) -> Optional[Dict[str, Any]]:
        """The newest committed version's write record, or None when there is none.

        Which transaction wrote ``key`` and when it was published.  This is how a
        lock whose transaction never committed is told from one whose transaction
        did, without the row itself being visible to the reader asking.
        """

    def follower_read_index(self) -> Tuple[Optional[int], Optional[str]]:
        """The index a read on this node may be served at, and why not when there is
        none.

        A node that does not lead says so rather than reading locally and hoping;
        carrying the question to the leader is what the wire version of this call
        exists for.
        """


class LocalNodeClient:
    """The node in this process, behind those six calls.

    Not a test double and not a shortcut past the seam: this is what the coordinator
    and the resolver hold, so the day a node is a channel away is a day a second class
    implements the same protocol rather than a day every caller is rewritten.

    The private names it delegates to are the node's own, and each has a public
    counterpart on the far side of the seam - that is what makes this a wrapper rather
    than a bypass.
    """

    def __init__(self, node: MemoryRaftNode):
        self._node = node

    def get(self, key: bytes, timestamp: Optional[int] = None) -> ReadResult:
        return self._node.get(key, timestamp)

    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None) -> List[Tuple[bytes, bytes]]:
        return self._node.scan(start_key, end_key, timestamp)

    def propose(self, command: bytes) -> ApplyResult:
        return self._node.propose(command)

    def get_lock(self, key: bytes) -> Optional[Dict[str, Any]]:
        return self._node._state_machine.get_lock_status(key)

    def get_write_record(self, key: bytes) -> Optional[Dict[str, Any]]:
        return self._node._state_machine.get_write_record(key)

    def follower_read_index(self) -> Tuple[Optional[int], Optional[str]]:
        return self._node._read_index()


@runtime_checkable
class NodeClientFactory(Protocol):
    """Where a caller's handles on nodes come from.

    Keyed the way the routing table names a node, which is a shard *and* a node id and
    not a node id on its own: one server holds a node in every shard's Raft group, so
    node 1 is one group in shard 0 and a different group in shard 1, listening on a
    different address for each.  A factory keyed by node id alone would hand back the
    wrong group - which is why this is a pair even though the id is the part a caller
    thinks it is asking about.

    None means "this caller has no way to reach that node", which is an answer and not
    a failure: the table names nodes a client may never have been given a handle on,
    and every caller already has a path for a shard it cannot reach.
    """

    def get_client(self, shard_id: int, node_id: int) -> Optional[NodeClient]:
        """The client for ``node_id`` in ``shard_id``'s group, or None."""

    def forget_client(self, shard_id: int, node_id: int) -> None:
        """Drop the client for that node, if one is being kept.

        A handle that has stopped working is not worth keeping, and the caller that
        found out is the only one that knows.  This is not a refresh: nothing about
        where the shard is has changed, only the way this client reaches it.
        """


class LocalNodeClientFactory:
    """The nodes in this process, behind the same protocol, built when first asked for.

    The cluster it is built over is the one the routing cache already holds, and the
    lookup is the pair the table publishes - a shard and the id of a server that serves
    it.

    Built lazily and kept, because a handle costs nothing but a caller that asked per
    key would otherwise build one per key.  A client here is a handle and not a
    connection, so what is cached is the wrapper; the node itself is the state.
    """

    def __init__(self, cluster):
        self._cluster = cluster
        self._lock = threading.Lock()
        self._clients: Dict[Tuple[int, int], LocalNodeClient] = {}

    def get_client(self, shard_id: int, node_id: int) -> Optional[NodeClient]:
        with self._lock:
            client = self._clients.get((shard_id, node_id))
        if client is not None:
            return client

        server = self._cluster.get_shard_server(node_id)
        if server is None:
            return None
        node = server.get_shard_node(shard_id)
        if node is None:
            return None

        with self._lock:
            # Two threads can get here at once; the first wrapper wins and the second
            # is dropped.  That costs a wrapper nobody holds, and it keeps a caller
            # from ever being handed two clients for one node.
            return self._clients.setdefault((shard_id, node_id), LocalNodeClient(node))

    def forget_client(self, shard_id: int, node_id: int) -> None:
        """Drop the wrapper for that node, if it was built.

        Nothing is closed - a wrapper around an object in this process holds nothing
        that can be closed - so what is dropped is a handle, and the next ask builds it
        again.  Forgetting a handle nobody is holding is not an error: a caller that
        found one broken and a caller that never had one want the same thing to happen.
        """
        with self._lock:
            self._clients.pop((shard_id, node_id), None)
