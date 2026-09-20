import threading
import time
import random
from typing import Callable, List, Optional, Tuple, Dict, Any

from ..client.node_client import NodeClient, NodeClientFactory
from ..client.routing import ShardLeaders, ask_shard
from ..raft.state_machine import (CommandType, ErrorCode, ReadResult,
                                  serialize_command)
from ..tso.tso import TSOClient
from .coordinator import SerializationError, TransactionCoordinator


#: How long an index a client has already been given is worth reading at.  What it buys is
#: the hop that confirms one: a read at an index confirmed a moment ago needs no second
#: confirmation, which is what makes ``Consistency.CACHED`` a read that costs nothing beyond
#: the read itself.  What it costs is age - the basis was confirmed before the read began, so
#: the answer is as of somewhere between now and this much ago - which is why the window is
#: short, and why a caller that wants none of it asks for a level that confirms one.
READ_INDEX_TTL = 0.1


class Consistency:
    """Which copy of the data may answer a read, and who says what its basis is.

    Not a ladder of guarantees but three answers to one question: where does the index the
    answer is promised at come from, and therefore how many nodes a read costs beyond the
    one that answers it.

    ``STRONG`` is a quorum.  The leader confirms an index and answers at it, which is the
    only one of the three that cannot answer out of data that was already stale when the
    read began.  It is the default, and it is what every caller had before there was a
    choice.

    ``FOLLOWER`` is the leader's index, fetched over the wire.  A replica answers, and the
    index it answers at is one the leader confirmed for it, so the hop happens on the node
    the caller chose rather than on the caller.

    ``CACHED`` is the client's own memory.  A replica answers at an index this client was
    given earlier, and what the caller pays for skipping the confirmation is the age of
    that index (see :data:`READ_INDEX_TTL`).
    """

    STRONG = "strong"
    FOLLOWER = "follower"
    CACHED = "cached"
    ALL = (STRONG, FOLLOWER, CACHED)


class ReadIndexCache:
    """The index each shard was last read at, for as long as that is worth using.

    One entry per shard, expiring by time rather than by being told it is wrong: an index is
    a fact about a log, and a fact about a log does not become false, it becomes old - which
    is the same thing to a read that has to be consistent as of now.  Nothing else is kept,
    because nothing else is needed to read at it: the shard, the key and the timestamp all
    come from the caller.
    """

    def __init__(self, ttl: float = READ_INDEX_TTL):
        self._ttl = ttl
        self._lock = threading.Lock()
        #: shard id -> (index, when it was kept).  Monotonic, because what a reader needs to
        #: know is how long ago the index was confirmed and not what day it was.
        self._indices: Dict[int, Tuple[int, float]] = {}

    def cached_index(self, shard_id: int) -> Optional[int]:
        """The index this client may read ``shard_id`` at, or None when it has none fresh.

        A miss and not a refusal: a caller that gets None asks the node the way it would
        have anyway.
        """
        with self._lock:
            entry = self._indices.get(shard_id)
        if entry is None:
            return None
        read_index, kept_at = entry
        if time.monotonic() - kept_at >= self._ttl:
            return None
        return read_index

    def remember(self, shard_id: int, read_index: int) -> None:
        """Keep ``read_index`` as the basis for the reads of ``shard_id`` that come next.

        Whatever produced it: an index a node answered at is the same promise whether the
        node confirmed it itself or was handed it, and a caller that has just been told one
        is the caller that can pass it on.
        """
        with self._lock:
            self._indices[shard_id] = (read_index, time.monotonic())


def _check_consistency(consistency: str) -> None:
    """Refuse a read this client cannot make as asked, rather than answering it another way.

    Two refusals, and they are different mistakes: a word that is not a consistency at all,
    and the one level this client does not implement yet.  Neither is answered strongly,
    because a caller that asked for a replica read and was given a linearizable one would
    have no way to tell - which is the one thing a caller chooses this parameter to be able
    to say.
    """
    if consistency not in Consistency.ALL:
        raise ValueError(f"{consistency!r} is not a consistency: "
                         f"{' or '.join(Consistency.ALL)}")
    if consistency == Consistency.CACHED:
        raise NotImplementedError(
            f"a {consistency} read is not implemented yet: this client asks the node it "
            f"reaches for the index, and cannot yet read at one of its own remembering")


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
        #: The index each shard was last read at, kept for the reads that do not confirm
        #: one of their own (``Consistency.CACHED``).
        self._read_index_cache = ReadIndexCache()
    
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
    
    def get(self, key: bytes, consistency: str = Consistency.STRONG) -> Optional[bytes]:
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

        ``consistency`` is which copy of the data may answer, and it is the one thing
        about a read that a caller chooses: ``strong`` - the default, and what every
        caller got before there was a choice - is the shard's leader confirming an
        index, and :class:`Consistency` describes the others.  A level this client
        cannot make the read at raises rather than being answered as one it can.
        """
        _check_consistency(consistency)

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

            result = ask_shard(self._leaders, shard_id, _read,
                               first=self._member_to_ask(shard_id, consistency))
            if result is None:
                raise RuntimeError("No leader found")
            if result.error_code == ErrorCode.ERR_NOT_LEADER:
                raise RuntimeError("Not leader")

            # The basis this answer was given at, kept for the reads that will not ask for
            # one (see ``Consistency.CACHED``): a reader that has just been told an index is
            # the reader that can pass it on.  Only a read that reached the state machine has
            # one - a refusal is not an answer as of anything - so that is the whole test.
            if result.read_index is not None:
                self._read_index_cache.remember(shard_id, result.read_index)

            return result.value
        
        return self._retry_with_backoff(_do_get)

    def _member_to_ask(self, shard_id: int, consistency: str) -> Optional[NodeClient]:
        """Where a read at ``consistency`` starts: a member of the set, or the leader.

        ``STRONG`` is the shard's leader confirming an index, so the walk starts where it
        always has.  ``FOLLOWER`` asks a member that need not lead, because the index such
        a read needs is obtained by the node that answers it - the leader's, fetched over
        the wire - and the point of the level is that the hop happens on a node the caller
        picked rather than on the caller.

        None when no member can be named, which the walk reads as the leader.  It is the
        same answer the strong path would have given, and no weaker: a member of the set
        the caller can reach is where a follower read is served, and the leader is one of
        them - so a client with no table, or one whose table names a set nothing can reach,
        loses the spread and not the read.
        """
        if consistency == Consistency.STRONG:
            return None
        return self._leaders.replica_for_shard(shard_id)

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
        """Commit, or say why it could not be committed.

        A transaction aborted by read-set validation raises ``SerializationError``
        instead of returning False: it is retryable, and only by running the work
        again, so it must not look like the other failures.  ``run`` catches it.

        A commit that could not be *made* raises as well, with the reason it got: no
        shard this client can find for a key, a group with no leader, a shard that
        refused the prewrite.  It used to answer ``False`` and keep the reason, and
        that is the one answer a caller cannot do anything with: "not written" reads
        the same whether the table was read before the cluster published a range, a
        group is mid-election, or the key is not this client's to write - and each of
        those is a different next move.  A read has raised for these failures since it
        was written; a write does now, which is also what the CLI prints.
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

        # No ``except`` for the two failures: ``_retry_with_backoff`` already re-raises a
        # refusal without retrying it, and lets a failure it could not retry away out.
        return self._retry_with_backoff(_do_commit)

    def run(self, work: Callable[[int], None], attempts: int = 3) -> bool:
        """Run ``work`` in a transaction, and run it again if the commit is refused.

        ``work(txn_id)`` reads and writes through this client.  A commit refused by
        read-set validation means the snapshot the work made its decisions from is
        gone, so the decisions themselves have to be made again - which is the only
        thing that can make them right, and why this cannot be a retry of the
        commit.  A fresh transaction gets a fresh start_ts, so the work sees the
        commits that invalidated it.

        Returns whether the work committed.  A commit that could not be made is not
        this loop's to absorb - it raises out of ``commit`` with its reason, because
        running the same work again would not tell the caller anything the first
        attempt did not.  If every attempt was refused, the last
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

        raise last_error
    
    def rollback(self, txn_id: int) -> bool:
        return self._coordinator.rollback(txn_id)
    
    def put(self, key: bytes, value: bytes) -> bool:
        """Write ``key``, as a transaction of one key.

        True means it committed - and it is the only answer this method gives: a commit
        that could not be made raises with its reason (see :meth:`commit`), which is the
        difference a shell sees, ``set: No shard holds b'user:1'`` rather than an exit
        code with nothing behind it.
        """
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

        False means the shard never took it: no leader of it this client could reach, or
        a shard that refused the command.  That is not the answer ``put`` gives any more
        - a commit that could not be made raises, with its reason - and the difference is
        deliberate: there is no transaction here to abort and no reason gathered on the
        way to a decision, so a refusal is an answer rather than a failure, and the
        caller's next move is the same either way.
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
                # False rather than an error: the caller's next move - read the table
                # again, ask again in a moment - is the same either way, and there is no
                # transaction here whose reason would have to come with it.
                return False
            return True

        return self._retry_with_backoff(_do_delete)

    def scan(self, start_key: bytes, end_key: bytes,
             timestamp: Optional[int] = None,
             consistency: str = Consistency.STRONG) -> List[Tuple[bytes, bytes]]:
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

        ``consistency`` is :meth:`get`'s, and every piece of the range is read the way
        the caller asked for: a range is a piece per shard it crosses, and each piece is
        one read of that shard rather than a second kind of read.  A piece read at a level
        that need not lead is served by a member of that shard's set, which is what the
        level means one shard at a time.
        """
        _check_consistency(consistency)

        rows: List[Tuple[bytes, bytes]] = []

        for shard_id, (start, end) in sorted(self._leaders.ranges().items(),
                                             key=lambda item: item[1][0]):
            piece_start = self._piece_start(start_key, start)
            piece_end = min(end_key, end)
            if piece_start >= piece_end:
                continue

            answer = ask_shard(
                self._leaders, shard_id,
                lambda client: client.scan(piece_start, piece_end, timestamp),
                first=self._member_to_ask(shard_id, consistency))
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
