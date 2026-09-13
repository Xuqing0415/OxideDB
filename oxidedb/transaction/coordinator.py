import threading
import time
from enum import Enum
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..raft.node import MemoryRaftNode, NodeState
from ..raft.state_machine import CommandType, ApplyResult, ErrorCode
from ..shard.router import locate
from ..tso.tso import TSOClient
from .lock_resolver import DEFAULT_LOCK_TTL, LockResolver

#: How many times a snapshot read resolves a lock and tries again.  One pass is
#: enough in the ordinary case; the spare attempts cover a second lock arriving
#: between the resolution and the retry.
LOCK_RESOLUTION_ATTEMPTS = 3


class TxnStatus(Enum):
    PENDING = "pending"
    PREWRITTEN = "prewritten"
    COMMITTED = "committed"
    ABORTED = "aborted"


class SerializationError(RuntimeError):
    """A commit that read-set validation refused.

    The transaction read a version that somebody else had already superseded by the
    time it tried to commit, so the snapshot it made its decisions from is gone.
    Nothing it wrote survives - committing it would leave effects that no serial
    order can explain - and the only correct response is to run the work again on a
    fresh snapshot.  ``SmartClient.run`` is that retry.
    """


class Transaction:
    def __init__(self, txn_id: int, start_ts: int):
        self.txn_id = txn_id
        self.start_ts = start_ts
        self.commit_ts = None
        self.keys: List[Tuple[bytes, bytes]] = []
        self.primary_key: Optional[bytes] = None
        self.status = TxnStatus.PENDING
        #: Every key this transaction read, for the validation at commit.  The
        #: timestamp it read them at is the transaction's own start_ts.
        self.read_set: set = set()
        #: Why the transaction was aborted, when the reason is worth saying.
        self.abort_reason: Optional[str] = None
    
    def add_key(self, key: bytes, value: bytes):
        self.keys.append((key, value))
        if self.primary_key is None:
            self.primary_key = key


class TransactionCoordinator:
    def __init__(self, tso_client: TSOClient, shard_server,
                 lock_ttl: float = DEFAULT_LOCK_TTL,
                 validate_reads: bool = True):
        self._tso_client = tso_client
        self._shard_server = shard_server
        self._resolver = LockResolver(shard_server, lock_ttl=lock_ttl)
        self._validate_reads = validate_reads
        #: Held across read-set validation and the primary commit.  See commit().
        self._commit_lock = threading.Lock()
        self._transactions: Dict[int, Transaction] = {}
        self._txn_id_counter = 0
        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(max_workers=10)
    
    def _get_shard_id(self, key: bytes) -> int:
        return locate(self._shard_server._range_map, key)
    
    def _get_shard_leader(self, shard_id: int) -> Optional[MemoryRaftNode]:
        for server in self._shard_server._shard_servers.values():
            node = server.get_shard_node(shard_id)
            if node and node.state == NodeState.LEADER:
                return node
        return None
    
    def begin(self) -> Tuple[int, int]:
        with self._lock:
            self._txn_id_counter += 1
            txn_id = self._txn_id_counter
        
        start_ts = self._tso_client.get_timestamp()
        
        with self._lock:
            self._transactions[txn_id] = Transaction(txn_id, start_ts)
        
        return txn_id, start_ts
    
    def add_write(self, txn_id: int, key: bytes, value: bytes):
        with self._lock:
            txn = self._transactions.get(txn_id)
            if txn is None:
                raise RuntimeError(f"Transaction {txn_id} not found")
            if txn.status != TxnStatus.PENDING:
                raise RuntimeError(f"Transaction {txn_id} is {txn.status}")
            txn.add_key(key, value)
    
    def prewrite(self, txn_id: int) -> bool:
        """Lock every key but stop before the commit point.

        ``commit`` is this plus the primary-key commit and then the secondaries.
        It is public because a crash between the two halves is the interesting
        case - the locks are durable and nothing is committed - and a test needs a
        way to put a transaction in that state without also finishing it.
        """
        with self._lock:
            txn = self._transactions.get(txn_id)
            if txn is None:
                return False
            if txn.status != TxnStatus.PENDING:
                return False
            if not txn.keys:
                return False

        shard_groups = self._group_keys_by_shard(txn)

        if not self._prewrite_all_shards(txn, shard_groups):
            with self._lock:
                txn.status = TxnStatus.ABORTED
            return False

        with self._lock:
            txn.status = TxnStatus.PREWRITTEN

        return True

    def read(self, txn_id: int, key: bytes) -> Optional[bytes]:
        """Read ``key`` at this transaction's start timestamp.

        A snapshot read, not a fresh one: whichever version was committed before
        the transaction started, so repeating it later in the same transaction
        returns the same bytes even if someone else commits a newer version in
        between.  A key this transaction has already prewritten reads back the
        value it wrote.

        A key with a lock *older* than this transaction's start_ts may be holding
        the version this snapshot is entitled to, and the answer is not in the
        lock: it is in the primary key's write record.  So the lock goes to the
        resolver - rolled forward if that transaction committed, cleared if it did
        not - and the read is retried.  While the transaction is still live there
        is nothing to be had from it, and the read says so instead of blocking.

        The key is remembered as read.  Whatever the caller decides from it, the
        decision is only valid while this snapshot is: commit() checks every read
        key for a version committed after start_ts and aborts if it finds one.
        """
        with self._lock:
            txn = self._transactions.get(txn_id)
            if txn is None:
                raise RuntimeError(f"Transaction {txn_id} not found")
            start_ts = txn.start_ts

        shard_id = self._get_shard_id(key)
        leader = self._get_shard_leader(shard_id)
        if leader is None:
            raise RuntimeError(f"No leader for shard {shard_id}")

        for _ in range(LOCK_RESOLUTION_ATTEMPTS):
            result = leader.get(key, start_ts)
            if result.error_code != ErrorCode.ERR_LOCKED:
                break
            if not self.resolve_lock(key):
                raise RuntimeError(f"Key {key!r} is locked by a live transaction")
        else:
            raise RuntimeError(
                f"Key {key!r} is still locked after {LOCK_RESOLUTION_ATTEMPTS} attempts")

        if not result.success:
            raise RuntimeError(f"Read failed: {result.error_msg}")

        with self._lock:
            txn.read_set.add(key)

        return result.value

    def resolve_lock(self, key: bytes) -> bool:
        """Settle the lock on ``key`` by asking the primary key what happened.

        True once the lock is gone - rolled forward if the transaction committed,
        cleared if it did not.  False while the transaction is still in flight, or
        while its shard has no leader; either way the caller should come back
        rather than read a lock as if it were a decision.
        """
        return self._resolver.resolve_lock(key)

    def commit(self, txn_id: int) -> Tuple[bool, Optional[int]]:
        with self._lock:
            txn = self._transactions.get(txn_id)
            if txn is None:
                return False, None
            if txn.status != TxnStatus.PENDING:
                return False, None
            if not txn.keys:
                txn.status = TxnStatus.COMMITTED
                txn.commit_ts = txn.start_ts
                return True, txn.start_ts
        
        shard_groups = self._group_keys_by_shard(txn)
        
        prewrite_success = self._prewrite_all_shards(txn, shard_groups)
        if not prewrite_success:
            with self._lock:
                txn.status = TxnStatus.ABORTED
            return False, None
        
        with self._lock:
            txn.status = TxnStatus.PREWRITTEN

        # Validation and the primary commit have to be one step with respect to other
        # commits.  Two transactions that each read what the other is about to
        # overwrite would otherwise be able to validate a moment apart and both
        # commit, which is exactly the write skew the validation exists to stop.  In
        # this design every commit goes through a coordinator, so one lock here
        # orders them; two coordinators committing concurrently would need the
        # conflict graph instead, which is not implemented.
        with self._commit_lock:
            conflict = self._validate_read_set(txn) if self._validate_reads else None
            if conflict is not None:
                self._rollback_all_shards(txn, shard_groups)
                with self._lock:
                    txn.status = TxnStatus.ABORTED
                    txn.abort_reason = f"read set invalidated: {conflict}"
                return False, None

            commit_ts = self._tso_client.get_timestamp()

            with self._lock:
                txn.commit_ts = commit_ts

            primary_shard_id = self._get_shard_id(txn.primary_key)
            primary_leader = self._get_shard_leader(primary_shard_id)

            if primary_leader is None:
                self._rollback_all_shards(txn, shard_groups)
                with self._lock:
                    txn.status = TxnStatus.ABORTED
                return False, None

            primary_commit_result = self._commit_key(primary_leader, txn.primary_key, txn.start_ts, commit_ts)

            if not primary_commit_result.success:
                self._rollback_all_shards(txn, shard_groups)
                with self._lock:
                    txn.status = TxnStatus.ABORTED
                return False, None

            with self._lock:
                txn.status = TxnStatus.COMMITTED

        self._commit_secondary_shards(txn, shard_groups, commit_ts)

        return True, commit_ts

    def _validate_read_set(self, txn: Transaction) -> Optional[str]:
        """Why this transaction's reads are stale, or None if they are not.

        A transaction decides from the versions its snapshot showed it.  If any key
        it read has since had a version committed - which can only have been by
        somebody else, since this transaction has not committed yet - then that
        decision was made from data that has left the serial order, and committing
        would leave an effect no serial order explains.  The classic shape of that is
        two doctors who each check the other is on call, then each go off call.

        A shard with no leader is treated as stale rather than as clean: not being
        able to check is not the same as checking and finding nothing.
        """
        for key in sorted(txn.read_set):
            shard_id = self._get_shard_id(key)
            leader = self._get_shard_leader(shard_id)
            if leader is None:
                return f"no leader for shard {shard_id}, holding {key!r}"

            write_record = leader._state_machine._storage.get_latest_write(key)
            if write_record is not None and write_record["commit_ts"] > txn.start_ts:
                return (f"{key!r} was committed at {write_record['commit_ts']}, "
                        f"after this transaction started at {txn.start_ts}")

        return None
    
    def _group_keys_by_shard(self, txn: Transaction) -> Dict[int, List[Tuple[bytes, bytes]]]:
        shard_groups = {}
        for key, value in txn.keys:
            shard_id = self._get_shard_id(key)
            if shard_id not in shard_groups:
                shard_groups[shard_id] = []
            shard_groups[shard_id].append((key, value))
        return shard_groups
    
    def _prewrite_all_shards(self, txn: Transaction, shard_groups: Dict[int, List[Tuple[bytes, bytes]]]) -> bool:
        # Every leader is resolved before any shard is touched.  Writing to the
        # shards that do have a leader and only then meeting one that does not
        # leaves locks behind for a transaction that is about to be aborted, and a
        # reader that meets one of those locks cannot tell it apart from a live
        # transaction - there is nothing to wait for and nothing to roll forward.
        leaders = {}
        for shard_id in shard_groups:
            leader = self._get_shard_leader(shard_id)
            if leader is None:
                return False
            leaders[shard_id] = leader

        futures = {}
        for shard_id, keys in shard_groups.items():
            future = self._executor.submit(self._prewrite_shard, leaders[shard_id], txn, keys)
            futures[future] = shard_id
        
        results = {}
        for future in as_completed(futures):
            shard_id = futures[future]
            try:
                results[shard_id] = future.result()
            except Exception:
                results[shard_id] = False
        
        if not all(results.values()):
            self._rollback_all_shards(txn, shard_groups)
            return False
        
        return True
    
    def _prewrite_shard(self, leader: MemoryRaftNode, txn: Transaction, keys: List[Tuple[bytes, bytes]]) -> bool:
        for key, value in keys:
            command = leader._state_machine.serialize_command(
                CommandType.PREWRITE,
                key=key,
                value=value,
                start_ts=txn.start_ts,
                primary_key=txn.primary_key,
            )
            
            result = leader.propose(command)
            if not result.success:
                return False
        
        return True
    
    def _rollback_all_shards(self, txn: Transaction, shard_groups: Dict[int, List[Tuple[bytes, bytes]]]):
        for shard_id, keys in shard_groups.items():
            leader = self._get_shard_leader(shard_id)
            if leader is None:
                continue
            
            for key, _ in keys:
                command = leader._state_machine.serialize_command(
                    CommandType.ROLLBACK,
                    key=key,
                    start_ts=txn.start_ts,
                )
                leader.propose(command)
    
    def _commit_key(self, leader: MemoryRaftNode, key: bytes, start_ts: int, commit_ts: int) -> ApplyResult:
        command = leader._state_machine.serialize_command(
            CommandType.COMMIT,
            key=key,
            start_ts=start_ts,
            commit_ts=commit_ts,
        )
        return leader.propose(command)
    
    def _commit_secondary_shards(self, txn: Transaction, shard_groups: Dict[int, List[Tuple[bytes, bytes]]], commit_ts: int):
        for shard_id, keys in shard_groups.items():
            leader = self._get_shard_leader(shard_id)
            if leader is None:
                continue
            
            for key, _ in keys:
                if key == txn.primary_key:
                    continue
                
                self._executor.submit(self._commit_key, leader, key, txn.start_ts, commit_ts)
    
    def rollback(self, txn_id: int) -> bool:
        with self._lock:
            txn = self._transactions.get(txn_id)
            if txn is None:
                return False
            if txn.status == TxnStatus.COMMITTED:
                return False
            if txn.status == TxnStatus.PENDING:
                txn.status = TxnStatus.ABORTED
                return True
        
        shard_groups = self._group_keys_by_shard(txn)
        self._rollback_all_shards(txn, shard_groups)
        
        with self._lock:
            txn.status = TxnStatus.ABORTED
        
        return True
    
    def get_transaction(self, txn_id: int) -> Optional[Transaction]:
        with self._lock:
            return self._transactions.get(txn_id)
    
    def shutdown(self):
        self._executor.shutdown(wait=True)