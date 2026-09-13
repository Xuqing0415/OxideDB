"""Deciding what happened to the transaction that left a lock behind.

Percolator's argument is that a lock is not a decision.  It is a note saying somebody
was mid-transaction; the decision is the primary key's write record, and that is what
makes an abandoned lock recoverable: whoever finds the lock can ask the primary key's
shard what happened, and then either roll the lock forward to the commit that was
already decided, or clear it because nobody decided anything.

Two callers ask that question.  A reader that trips over the lock has to answer it in
order to finish the read, and the background ``LockCleaner`` sweeps for locks whose
coordinator is not coming back.  They must not be able to answer it differently, so
the question is implemented once, here.

The TTL is what separates "still in flight" from "nobody is going to finish this".
Inside it the answer is that the coordinator may yet commit, so the lock has to be
left alone and the reader has to come back.  Past it the transaction is treated as
dead by fiat - the fiat being the trade Percolator makes, since no participant in the
protocol can prove the coordinator is gone.  A transaction that comes back after its
TTL expired can therefore find its lock cleared and its write refused, which is why
the TTL has to be comfortably longer than a commit could take.
"""

import time
from enum import Enum
from typing import Any, Dict, Optional

from ..raft.node import MemoryRaftNode, NodeState
from ..raft.state_machine import CommandType
from ..shard.router import locate

#: How long a lock has to sit untouched before it stops counting as "in flight".
#: This is the trigger for the question, not the answer: the answer comes from the
#: primary key's write record.
DEFAULT_LOCK_TTL = 5.0


class PrimaryStatus(Enum):
    """What the primary key's shard says about a transaction."""

    #: A write record for that start_ts: the transaction committed, and this lock
    #: is a commit that has not been applied on this shard yet.
    COMMITTED = "committed"
    #: The primary's lock is still fresh, so the coordinator may yet commit.
    PENDING = "pending"
    #: No write record and no live lock: the transaction did not commit.
    ABORTED = "aborted"
    #: No leader for the primary key's shard, so there is no answer to be had.
    UNKNOWN = "unknown"


class LockResolver:
    def __init__(self, shard_cluster, lock_ttl: float = DEFAULT_LOCK_TTL):
        self._shard_cluster = shard_cluster
        self._lock_ttl = lock_ttl

    def _get_shard_id(self, key: bytes) -> int:
        return locate(self._shard_cluster._range_map, key)

    def _get_shard_leader(self, shard_id: int) -> Optional[MemoryRaftNode]:
        for server in self._shard_cluster._shard_servers.values():
            node = server.get_shard_node(shard_id)
            if node and node.state == NodeState.LEADER:
                return node
        return None

    def lock_on(self, key: bytes) -> Optional[Dict[str, Any]]:
        """The lock a shard leader currently holds for ``key``, or None."""
        leader = self._get_shard_leader(self._get_shard_id(key))
        if leader is None:
            return None
        return leader._state_machine.get_lock_status(key)

    def primary_status(self, primary_key: bytes, start_ts: int) -> PrimaryStatus:
        """Ask the primary key's shard what happened to the transaction."""
        leader = self._get_shard_leader(self._get_shard_id(primary_key))
        if leader is None:
            return PrimaryStatus.UNKNOWN

        storage = leader._state_machine._storage
        write_record = storage.get_latest_write(primary_key)
        if write_record is not None and write_record["start_ts"] == start_ts:
            return PrimaryStatus.COMMITTED

        lock = storage.get_newest_lock(primary_key)
        if lock is not None and lock["start_ts"] == start_ts:
            if time.time() - lock.get("lock_time", 0) < self._lock_ttl:
                return PrimaryStatus.PENDING

        return PrimaryStatus.ABORTED

    def primary_commit_ts(self, primary_key: bytes, start_ts: int) -> Optional[int]:
        """The timestamp the transaction committed at, if it did."""
        leader = self._get_shard_leader(self._get_shard_id(primary_key))
        if leader is None:
            return None

        write_record = leader._state_machine._storage.get_latest_write(primary_key)
        if write_record is not None and write_record["start_ts"] == start_ts:
            return write_record["commit_ts"]

        return None

    def resolve_lock(self, key: bytes, lock: Optional[Dict[str, Any]] = None) -> bool:
        """Settle the lock on ``key``.  True once it is gone.

        False means the transaction is still in flight, or its shard has no leader -
        either way this caller learned nothing it can act on and should come back
        rather than treat the lock as resolved.
        """
        if lock is None:
            lock = self.lock_on(key)
        if lock is None:
            return True

        start_ts = lock["start_ts"]
        status = self.primary_status(lock["primary_key"], start_ts)

        if status == PrimaryStatus.COMMITTED:
            commit_ts = self.primary_commit_ts(lock["primary_key"], start_ts)
            if commit_ts is None:
                return False
            return self._settle(key, start_ts, CommandType.COMMIT, commit_ts=commit_ts)

        if status == PrimaryStatus.ABORTED:
            return self._settle(key, start_ts, CommandType.ROLLBACK)

        return False

    def _settle(self, key: bytes, start_ts: int, command_type: bytes, **kwargs) -> bool:
        leader = self._get_shard_leader(self._get_shard_id(key))
        if leader is None:
            return False

        command = leader._state_machine.serialize_command(
            command_type, key=key, start_ts=start_ts, **kwargs)
        result = leader.propose(command)
        if result.success:
            return True

        # Somebody else - another reader, or the cleaner - may have settled this lock
        # in between.  What matters is whether the lock this caller asked about is
        # still there, not whether this particular proposal is the one that removed
        # it.
        current = self.lock_on(key)
        return current is None or current["start_ts"] != start_ts
