"""The background sweep for locks whose coordinator is not coming back.

A lock is only recoverable because the decision lives in the primary key's write
record, which is what ``lock_resolver`` implements.  This class is the part that
decides *when* to ask, not what the answer is: it walks the leaders it can see, and
for every lock older than the TTL hands it to the resolver - which rolls it forward,
clears it, or leaves it alone because the transaction is still live.
"""

import threading
import time
from typing import Optional

from ..raft.node import NodeState
from ..raft.state_machine import LockStatus
from .lock_resolver import DEFAULT_LOCK_TTL, LockResolver


class LockCleaner:
    def __init__(self, shard_cluster, poll_interval: float = 10,
                 lock_ttl: float = DEFAULT_LOCK_TTL):
        self._shard_cluster = shard_cluster
        self._poll_interval = poll_interval
        self._lock_ttl = lock_ttl
        self._resolver = LockResolver(shard_cluster, lock_ttl=lock_ttl)
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

    def _expired_locks(self):
        """Every lock a leader holds that is older than the TTL.

        Collected before any is resolved, because resolving one writes to the same
        state machine ``iter_locks`` just read from.
        """
        expired = []
        for server in self._shard_cluster._shard_servers.values():
            for shard_id in range(server._num_shards):
                node = server.get_shard_node(shard_id)
                if node is None or node.state != NodeState.LEADER:
                    continue

                for key, lock in node._state_machine._storage.iter_locks():
                    if lock.get("status") != LockStatus.LOCKED:
                        continue
                    if time.time() - lock.get("lock_time", 0) >= self._lock_ttl:
                        expired.append((key, lock))

        return expired

    def _clean_expired_locks(self):
        for key, lock in self._expired_locks():
            self._resolver.resolve_lock(key, lock)

    def start(self):
        with self._lock:
            if self._running:
                return
            self._running = True

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        while self._running:
            try:
                self._clean_expired_locks()
            except Exception:
                pass

            time.sleep(self._poll_interval)

    def stop(self):
        with self._lock:
            self._running = False

        if self._thread is not None:
            self._thread.join(timeout=5)
