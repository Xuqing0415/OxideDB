import time
import random
from typing import Callable, List, Optional, Tuple, Dict, Any

from ..client.node_client import NodeClientFactory
from ..client.routing import ShardLeaders, ask_shard
from ..raft.state_machine import (CommandType, ErrorCode, ReadResult,
                                  serialize_command)
from ..tso.tso import TSOClient
from .coordinator import SerializationError, TransactionCoordinator


class SmartClient:
    def __init__(self, tso_client: TSOClient, shard_cluster, router=None,
                 factory: Optional[NodeClientFactory] = None):
        self._tso_client = tso_client
        #: Where this client thinks the shards are, and how it reaches them.  Handed
        #: a table it routes by that - the placement the cluster published, which is
        #: the only one a client outside the cluster could have; without one the
        #: cluster's own nodes answer, which is what an in-process test is.  Everything
        #: the client routes, commit included, goes through these leaders: a client
        #: that read by the table and wrote by scanning the cluster would be two
        #: clients wearing one name.
        self._leaders = ShardLeaders(shard_cluster, factory=factory, router=router)
        self._coordinator = TransactionCoordinator(tso_client, shard_cluster, router=router,
                                                   factory=factory)
        self._retry_backoff_base = 0.01
        self._retry_max_backoff = 0.1
        self._retry_max_attempts = 3
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        last_error = None
        for attempt in range(self._retry_max_attempts):
            try:
                result = func(*args, **kwargs)
                return result
            except SerializationError:
                # Running the same transaction again cannot help: the snapshot it
                # decided from is gone.  Only running the *work* again can, and that
                # is run()'s job, not this loop's.
                raise
            except Exception as e:
                last_error = e
                if attempt < self._retry_max_attempts - 1:
                    backoff = min(
                        self._retry_backoff_base * (2 ** attempt),
                        self._retry_max_backoff
                    )
                    time.sleep(backoff + random.uniform(0, 0.005))
        
        raise last_error
    
    def get(self, key: bytes) -> Optional[bytes]:
        """Read the newest committed value of ``key``.

        A lock in the way is a question rather than an error: it goes to the
        coordinator's resolver, which rolls it forward if the transaction that left
        it committed and clears it if it did not, and then the read is retried.
        A transaction that is still live is waited out first, up to that lock's
        remaining TTL; only a lock that outlives its TTL, which means the shard
        could not settle it, is raised - and the backoff in ``_retry_with_backoff``
        is what makes that retry useful instead of a spin.

        A shard that refuses because it no longer leads sends this read back to the
        table once, through ``ask_shard``; a refusal that survives a fresh table is
        not staleness, and the retry loop around this one is what turns it into a
        second attempt with a pause rather than into a busy spin.
        """
        def _do_get():
            shard_id = self._leaders.shard_for_key(key)
            if shard_id is None:
                raise RuntimeError(f"No shard holds {key!r}")

            def _read(client):
                answer = client.get(key)
                if answer.error_code == ErrorCode.ERR_LOCKED:
                    if not self._coordinator.await_lock(key):
                        raise RuntimeError(
                            f"Key {key!r} outlived its lock TTL: "
                            "the shard could not settle it")
                    answer = client.get(key)
                return answer

            result = ask_shard(self._leaders, shard_id, _read)
            if result is None:
                raise RuntimeError("No leader found")
            if result.error_code == ErrorCode.ERR_NOT_LEADER:
                raise RuntimeError("Not leader")

            return result.value
        
        return self._retry_with_backoff(_do_get)
    
    def read(self, txn_id: int, key: bytes) -> Optional[bytes]:
        """Read ``key`` at the transaction's start timestamp, not at the newest one.

        A lock older than that snapshot is resolved by the coordinator rather than
        raised, so a reader is not blocked by a transaction that has already
        decided.
        """
        return self._coordinator.read(txn_id, key)

    def begin(self) -> int:
        txn_id, _ = self._coordinator.begin()
        return txn_id
    
    def add_write(self, txn_id: int, key: bytes, value: bytes):
        self._coordinator.add_write(txn_id, key, value)
    
    def commit(self, txn_id: int) -> Tuple[bool, Optional[int]]:
        """Commit, or report that it could not be committed.

        A transaction aborted by read-set validation raises ``SerializationError``
        instead of returning False: it is retryable, and only by running the work
        again, so it must not look like the other failures.  ``run`` catches it.
        """
        def _do_commit():
            success, commit_ts = self._coordinator.commit(txn_id)

            if not success:
                txn = self._coordinator.get_transaction(txn_id)
                if txn is not None and txn.abort_reason is not None:
                    raise SerializationError(f"Commit aborted: {txn.abort_reason}")
                if txn is not None:
                    raise RuntimeError(f"Commit failed: {txn.status}")
                raise RuntimeError("Commit failed")

            return success, commit_ts

        try:
            return self._retry_with_backoff(_do_commit)
        except SerializationError:
            raise
        except RuntimeError:
            return False, None

    def run(self, work: Callable[[int], None], attempts: int = 3) -> bool:
        """Run ``work`` in a transaction, and run it again if the commit is refused.

        ``work(txn_id)`` reads and writes through this client.  A commit refused by
        read-set validation means the snapshot the work made its decisions from is
        gone, so the decisions themselves have to be made again - which is the only
        thing that can make them right, and why this cannot be a retry of the
        commit.  A fresh transaction gets a fresh start_ts, so the work sees the
        commits that invalidated it.

        Returns whether the work committed.  A commit that failed for any other
        reason is not retried; if every attempt was refused, the last
        ``SerializationError`` is raised.
        """
        last_error = None
        for _ in range(attempts):
            txn_id = self.begin()
            work(txn_id)
            try:
                if self.commit(txn_id)[0]:
                    return True
            except SerializationError as error:
                # Nothing the aborted attempt wrote survives, so running the same
                # work again is safe by construction.
                last_error = error
                continue
            return False

        raise last_error
    
    def rollback(self, txn_id: int) -> bool:
        return self._coordinator.rollback(txn_id)
    
    def put(self, key: bytes, value: bytes) -> bool:
        txn_id = self.begin()
        self.add_write(txn_id, key, value)
        success, _ = self.commit(txn_id)
        return success

    def delete(self, key: bytes) -> bool:
        """Remove ``key``, as a tombstone at a timestamp from the cluster's clock.

        A tombstone is a version like any other and has to be ordered like one: it
        shadows every version at or before its timestamp for every reader after it,
        which is what makes a deleted key read as absent rather than as a value
        nobody replaced.  So the timestamp comes from the same group the transactions
        take theirs from - the only thing that orders this delete against the commits
        around it.

        It goes to the shard's leader as one command rather than through a
        transaction, because what a transaction writes is a value and this writes
        none.  That leaves a blind write: a delete that races a transaction on the
        same key can be overwritten by that transaction's commit, where a delete
        inside the transaction would have been ordered with it.  Writing it as an
        intent would need the lock record to carry "this version is a tombstone" over
        the wire, and until it does, the honest thing is the command the shard already
        has and a caller that knows what it costs.

        False means the shard never took it - no leader this client can reach, or a
        refusal - which is how ``put`` reports the same kind of nothing.
        """
        timestamp = self._tso_client.get_timestamp()
        command = serialize_command(CommandType.DELETE, key=key, timestamp=timestamp)

        def _do_delete():
            shard_id = self._leaders.shard_for_key(key)
            if shard_id is None:
                raise RuntimeError(f"No shard holds {key!r}")

            result = ask_shard(self._leaders, shard_id,
                               lambda client: client.propose(command))
            if result is None or not result.success:
                # False rather than an error, the way ``put`` reports a commit it
                # could not land: the caller's next move is the same either way.
                return False
            return True

        return self._retry_with_backoff(_do_delete)

    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None) -> List[Tuple[bytes, bytes]]:
        """Every key in ``[start_key, end_key)``, from every shard it covers.

        The one question a single shard cannot answer: the ranges a client holds cut
        the range into as many pieces as it crosses, and each piece goes to the leader
        that owns it.  A shard ranges the rows of its own piece, so the pieces are
        concatenated in range order rather than in the order the shards happen to be
        numbered.

        A range no shard of this placement covers is left out rather than refused: the
        placement is the whole of what this client knows, and rows it has no shard for
        are rows it cannot read.

        A piece whose shard has no reachable leader raises rather than coming back
        short - a range read that quietly omitted a shard's rows would look exactly
        like a range with no such rows.
        """
        rows: List[Tuple[bytes, bytes]] = []

        for shard_id, (start, end) in sorted(self._leaders.ranges().items(),
                                             key=lambda item: item[1][0]):
            piece_start = self._piece_start(start_key, start)
            piece_end = min(end_key, end)
            if piece_start >= piece_end:
                continue

            answer = ask_shard(
                self._leaders, shard_id,
                lambda client: client.scan(piece_start, piece_end, timestamp))
            if answer is None:
                raise RuntimeError(f"No leader for shard {shard_id}")
            rows.extend(answer)

        return rows

    @staticmethod
    def _piece_start(caller_start: bytes, shard_start: bytes) -> bytes:
        """Where one shard's piece of a caller's range begins: the larger of the two.

        With one exception, and it is not a preference: the floor of the keyspace is
        b"\x00", which is the separator byte the storage refuses inside a key - so a
        piece that would begin at the floor begins at what the caller asked from
        instead.  That is the same request rather than a wider one, because nothing
        can be stored below the floor.
        """
        piece = max(caller_start, shard_start)
        return caller_start if b"\x00" in piece else piece
