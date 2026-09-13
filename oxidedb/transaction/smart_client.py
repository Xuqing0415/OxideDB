import time
import random
from typing import Callable, Optional, Tuple, Dict, Any

from ..raft.node import MemoryRaftNode, NodeState
from ..raft.state_machine import ReadResult, ErrorCode
from ..tso.tso import TSOClient
from .coordinator import SerializationError, TransactionCoordinator


class SmartClient:
    def __init__(self, tso_client: TSOClient, shard_cluster, router=None):
        self._tso_client = tso_client
        self._shard_cluster = shard_cluster
        #: Where this client thinks the shards are, when it has a table to ask.
        #: "None" is a client of a cluster that publishes nowhere - an in-process
        #: test - and it looks at the cluster's own nodes instead.  Everything the
        #: client routes, commit included, goes through the same cache: a client
        #: that read by the table and wrote by scanning the cluster would be two
        #: clients wearing one name.
        self._router = router
        self._coordinator = TransactionCoordinator(tso_client, shard_cluster, router=router)
        self._retry_backoff_base = 0.01
        self._retry_max_backoff = 0.1
        self._retry_max_attempts = 3
    
    def _get_shard_leader(self, key: bytes) -> Optional[MemoryRaftNode]:
        """The node this client thinks should answer for ``key``.

        With a metadata service it routes by the table - the placement the cluster
        published, and the only one a client that is not inside the cluster could
        have.  Looking at the cluster's own nodes is the fallback for a client of a
        cluster that publishes nowhere, which is what an in-process test is.
        """
        if self._router is not None:
            return self._router.leader_for_key(key)
        result = self._shard_cluster.get_leader_for_key(key)
        if result is not None:
            return result[1]
        return None
    
    def _refresh_leader_cache(self):
        """Read the routing table again, after a shard said this client was wrong.

        A cached table is a decision, so it is not refreshed on a timer: going back
        to the metadata group is the cost the cache exists to avoid, and a shard
        refusing a read is the evidence that paying it is now the cheaper thing.
        """
        if self._router is not None:
            self._router.refresh()
    
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
        """
        def _do_get():
            leader = self._get_shard_leader(key)
            if leader is None:
                raise RuntimeError("No leader found")
            
            result = leader.get(key)
            
            if result.error_code == ErrorCode.ERR_LOCKED:
                if not self._coordinator.await_lock(key):
                    raise RuntimeError(
                        f"Key {key!r} outlived its lock TTL: the shard could not settle it")
                result = leader.get(key)
            
            if result.error_code == ErrorCode.ERR_NOT_LEADER:
                self._refresh_leader_cache()
                raise RuntimeError(f"Not leader, refreshing cache")
            
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