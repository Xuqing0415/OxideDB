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

from ..client.node_client import NodeClient
from ..client.routing import ShardLeaders, ask_shard
from ..raft.state_machine import CommandType, serialize_command
from ..shard.router import locate

#: How long a lock has to sit untouched before it stops counting as "in flight".
#: This is the trigger for the question, not the answer: the answer comes from the
#: primary key's write record.
DEFAULT_LOCK_TTL = 5.0

#: How long a waiter sleeps between asking the primary key what happened.  The
#: answer changes the moment the coordinator commits, so this is the latency a
#: reader pays for a commit that lands while it is waiting.
LOCK_WAIT_POLL_INTERVAL = 0.05

#: Slack allowed past a lock's TTL before a waiter gives up.  The TTL is the moment
#: the lock stops counting as in flight; settling it takes a Raft round trip.
LOCK_WAIT_SETTLE_MARGIN = 0.1


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
    def __init__(self, shard_cluster, lock_ttl: float = DEFAULT_LOCK_TTL, leaders=None):
        self._shard_cluster = shard_cluster
        self._lock_ttl = lock_ttl
        #: Where the client that leads a shard comes from.  A coordinator hands in its
        #: own, so that the lock a transaction left behind is settled by the same
        #: placement that transaction read and wrote by; on its own - which is how the
        #: lock cleaner uses it - this resolver routes by the cluster it was handed.
        self._leaders = leaders if leaders is not None else ShardLeaders(shard_cluster)

    def _get_shard_id(self, key: bytes) -> int:
        return locate(self._shard_cluster._range_map, key)

    def _get_shard_leader(self, shard_id: int) -> Optional[NodeClient]:
        """The client for whichever node leads ``shard_id``, or None."""
        return self._leaders.leader_for_shard(shard_id)

    def lock_on(self, key: bytes) -> Optional[Dict[str, Any]]:
        """The lock a shard leader currently holds for ``key``, or None."""
        client = self._get_shard_leader(self._get_shard_id(key))
        if client is None:
            return None
        return client.get_lock(key)

    def primary_status(self, primary_key: bytes, start_ts: int) -> PrimaryStatus:
        """Ask the primary key's shard what happened to the transaction."""
        client = self._get_shard_leader(self._get_shard_id(primary_key))
        if client is None:
            return PrimaryStatus.UNKNOWN

        write_record = client.get_write_record(primary_key)
        if write_record is not None and write_record["start_ts"] == start_ts:
            return PrimaryStatus.COMMITTED

        lock = client.get_lock(primary_key)
        if lock is not None and lock["start_ts"] == start_ts:
            if self.remaining_ttl(lock) > 0:
                return PrimaryStatus.PENDING

        return PrimaryStatus.ABORTED

    def primary_commit_ts(self, primary_key: bytes, start_ts: int) -> Optional[int]:
        """The timestamp the transaction committed at, if it did."""
        client = self._get_shard_leader(self._get_shard_id(primary_key))
        if client is None:
            return None

        write_record = client.get_write_record(primary_key)
        if write_record is not None and write_record["start_ts"] == start_ts:
            return write_record["commit_ts"]

        return None

    def remaining_ttl(self, lock: Dict[str, Any]) -> float:
        """How long ``lock`` may still be the lock of a live transaction.

        The arithmetic ``primary_status`` separates PENDING from ABORTED with, in
        the open - a caller that wants to wait for the answer has to know when the
        question can first be answered at all.  Zero once the TTL has passed.
        """
        return max(0.0, self._lock_ttl - (time.time() - lock.get("lock_time", 0.0)))

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

    def await_resolution(self, key: bytes, lock: Optional[Dict[str, Any]] = None) -> bool:
        """Settle the lock on ``key``, waiting out a transaction that is still live.

        ``resolve_lock`` answers at once, and "the coordinator may still commit" is
        a true answer a reader cannot use.  It has a shelf life, though: at the
        lock's TTL the transaction stops counting as in flight and the same
        question answers ABORTED instead.  Waiting until then is what turns a live
        lock into a finished read rather than an error, and the wait is bounded by
        the lock's own remaining lifetime - a reader never waits longer than the
        transaction could have been given.

        True once the lock is gone - rolled forward if the transaction committed
        during the wait, cleared if it did not.  False if it is still there after
        its TTL, which means the shard could not settle it: no leader, not a
        transaction that is still live.
        """
        if lock is None:
            lock = self.lock_on(key)
        if lock is None:
            return True

        deadline = time.monotonic() + self.remaining_ttl(lock) + LOCK_WAIT_SETTLE_MARGIN
        while True:
            if self.resolve_lock(key):
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(LOCK_WAIT_POLL_INTERVAL)

    def _settle(self, key: bytes, start_ts: int, command_type: bytes, **kwargs) -> bool:
        command = serialize_command(
            command_type, key=key, start_ts=start_ts, **kwargs)
        result = ask_shard(self._leaders, self._get_shard_id(key),
                           lambda client: client.propose(command))
        if result is not None and result.success:
            return True

        # Somebody else - another reader, or the cleaner - may have settled this lock
        # in between.  What matters is whether the lock this caller asked about is
        # still there, not whether this particular proposal is the one that removed
        # it, and a shard that refused because it no longer leads is in exactly that
        # position: the question that decides it is the one asked above, of the lock.
        current = self.lock_on(key)
        return current is None or current["start_ts"] != start_ts
