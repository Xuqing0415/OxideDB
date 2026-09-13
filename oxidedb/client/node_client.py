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