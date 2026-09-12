# Design notes

The README describes *what* is implemented.  This file records *why* it is shaped
the way it is: the decisions behind the keyspace encoding, the snapshot, the
two-phase commit protocol and the read path, and what each one buys.  File
references point at the code that implements the decision.

## 1. The keyspace: a namespace byte, a separator, a timestamp

Four logical keyspaces share one byte-ordered engine.  Each one starts with a
single namespace byte:

```
MVCC versions    \x01 || key || \x00 || ts(8B BE)          -> flag(1B) || value
write record     \x03 || key || \x00 || commit_ts(8B BE)   -> start_ts(8B BE)
lock record      \x04 || key || \x00 || start_ts(8B BE)    -> msgpack{status,
                                                              primary_key,
                                                              lock_time, value}
```

The engine contract is deliberately primitive: keys are ordered by plain byte
comparison (`memcmp`), and that is the whole index.  The encoding is what turns
that one property into everything the storage layer needs.

**One byte of namespace.**  Because ordering is byte ordering, every namespace is
a contiguous range: the entire MVCC keyspace is `[\x01, \x05)`.  `dump()` and
`clear()` are that one range and cannot touch anything else living in the same
engine.  Within a namespace, "everything belonging to exactly key `k`" is
`[ns || k || \x00, ns || k || \x01)` - `ns || k || \x01` is the tightest key that
sorts after all of `k`'s entries and before any longer key that merely shares a
prefix.

**The timestamp is a fixed-width big-endian suffix.**  Two properties come out of
that.  Numeric order equals byte order, so a range scan needs no decoding and no
comparison function.  Fixed width means the timestamp is always the trailing
eight bytes, at a computable offset (`len(body) - 9` in `_split_version_key`); a
variable-width encoding would force a reverse search for the separator, and that
search would find a `0x00` byte *inside* a small timestamp instead of the
separator.

**`\x00` is the field separator, and user keys may not contain it.**  The byte is
the smallest one, and `_check_user_key` rejects it on the way in rather than
letting it corrupt the index.  With the separator doing that job, appending
`b"\x00"` to a key - the usual idiom for an exclusive upper bound - would be
exactly wrong, which is why `oxidedb.storage.next_key` exists instead.

### What MVCC gets out of it

* "All versions of `k`, oldest first" is a forward prefix scan.  No sorting, no
  secondary index, no in-memory version chain.
* "The value of `k` as of timestamp `t`" is the same scan *truncated* at `t`,
  taking the last row: `_get_version_at` builds the upper bound as
  `prefix + ts(t + 1)`, so the engine's key range does the truncation and the
  storage layer just reads `rows[-1]`.
* A delete is a tombstone with its own timestamp, not an erasure.  A reader older
  than the delete still sees the previous version, and a newer reader sees the
  marker and reports the key as absent.  Erasing would break both.

### What 2PC gets out of it

* The lock record is the **durable write intent**.  It carries the value a commit
  will publish, the primary key that decides the transaction's fate, and the
  moment the lock was taken.  Keeping it in the engine rather than in a dict on
  the state machine is what lets a restarted replica still commit or clean up a
  transaction it had already prewritten.
* The write record is the **commit marker**: `commit_ts -> start_ts`.  One range
  scan answers both "did the transaction that wrote this key commit?" and "which
  transaction was it?" - the question `LockCleaner` asks of a primary key before
  it rolls anything forward or back.
* Both are ordered by `(key, timestamp)`, so "newest lock for `k`" and "latest
  write for `k`" are `rows[-1]` of their prefixes rather than a full scan.

The `\x02` namespace used to hold a bare write intent.  The lock record subsumes
it - same value, plus primary key, status and TTL - so the intent namespace was
retired and `set_write_intent` is a thin wrapper over `put_lock`.

## 2. Snapshots are the whole keyspace in one blob, locks included

`MVCCStateMachine.snapshot()` is `msgpack{ts, storage}` where `storage` is
`MVCCStorage.dump()`, i.e. every row in `[\x01, \x05)`.  Locks are in there on
purpose, and so are write records, because the snapshot has to be a *complete
restart* rather than a copy of the data:

* **Versions** are the data itself.
* **Write records** are how a replica answers "did transaction `T` commit key
  `k`?".  That is not a theoretical need: `LockCleaner` asks exactly that of the
  primary key (`_get_primary_status`) before resolving someone else's lock.  A
  replica restored without write records would make a different decision from the
  leader - it would call committed transactions aborted and delete their locks.
* **Locks** are the part that is easiest to get wrong in both directions, and one
  blob is what makes both work:
  * A lock still in the `LOCKED` state belongs to a transaction that may yet
    commit.  Drop it and three things break: readers stop seeing the intent and
    read a value the transaction is about to overwrite; a later `COMMIT` for that
    key fails with `ERR_NO_LOCK` on the restored replica while succeeding on the
    leader, which is permanent divergence; and the cleaner has nothing left to
    roll forward or back.
  * A lock the snapshot already resolved must not reappear either, which would
    resurrect an intent that was already resolved.  `dump` takes the whole
    namespace under one lock, so there is no ordering in which the locks and the
    versions disagree with each other: the snapshot is a consistent cut of all
    three namespaces, not three pieces stitched together.
* **`_last_applied_timestamp` travels with it.**  Reads are served at that
  timestamp (`get` -> `storage.get(key, self._last_applied_timestamp)`), so a
  machine restored without it would read as of timestamp 0 and see nothing.

Ordering on the node matters as much as the contents: `_maybe_snapshot` writes
the snapshot *before* compacting the log.  A crash in between leaves entries that
the next replay skips because `_last_applied` already covers them; the other
order would throw away the only copy of that state.

Two costs are accepted deliberately (see Known gaps in the README): the blob is
built and restored while holding the node lock, so the node serves no RPCs for
the duration, and over gRPC it must fit in one message.

## 3. Why Percolator, and not another 2PC variant

The rule that makes the protocol work is: **a transaction's fate is decided by
exactly one row**, the primary key's write record.  The coordinator's in-memory
`TxnStatus` is a cache of that decision, never the decision itself.

The sequence in `TransactionCoordinator.commit`:

1. `start_ts` from the TSO; collect the transaction's keys.
2. **Prewrite** every key, in parallel across shards.  In the state machine this
   reads the existing lock and the latest version/write record and rejects the
   prewrite on conflict (`ERR_LOCKED`, `ERR_WRITE_CONFLICT`).  Success leaves a
   `LOCKED` row per key holding the primary key and the value.
3. `commit_ts` from the TSO - after every prewrite, so all of the transaction's
   keys publish at the same timestamp.  A reader never sees half of it: a key
   whose commit has not landed yet still holds its lock, and the read path
   reports that as locked rather than as a value.
4. **Commit the primary key.**  This single Raft entry is the moment the
   transaction becomes committed: it writes the version, writes the write record
   and removes the lock.
5. Only then is the client told "committed", and secondaries are committed
   asynchronously.

If the primary commit fails, everything is rolled back.  If a secondary commit
fails or never happens, nothing is lost: the primary's write record already says
the transaction committed, and anyone who trips over the leftover lock rolls it
forward.

**Why this beats "the coordinator decides and then writes".**  The coordinator is
a Python object with a thread pool and a dict; it is not durable, and it can die
between any two steps.  With a decision stored only in the coordinator, recovery
needs a durable intent log *and* a recovery procedure.  With the decision in the
primary key's write record, recovery needs one read - and it can be performed by
anyone, not just by the coordinator that made the decision.  That is the whole
point: the decision is *derived from the keyspace* instead of remembered.

**Why locks carry a TTL.**  A coordinator that dies *before* the primary commit
leaves locks that nobody is going to resolve.  `lock_time` is what stops those
keys from being blocked forever: after 5 seconds a lock no longer blocks readers
(`MVCCStateMachine.get`), and `LockCleaner` re-derives the outcome from the
primary key - committed if the primary has a write record for that `start_ts`,
locked if it is still fresh, aborted otherwise.  The TTL is a trigger for asking
the question, not the answer itself.

**Why secondary commits are asynchronous.**  Commit latency does not grow with the
number of shards: the client waits for the primary only.  The price is that a
reader can be told to retry on a secondary key whose transaction is in fact
already committed, until that commit lands.  Correctness does not depend on the
secondary write happening at all - the primary's write record is the decision,
and a leftover lock is rolled forward from it.

What this design does *not* have: every key pays a prewrite even when the
transaction lives in a single shard (the whole multi-key transaction could have
been one Raft entry there), `lock_time` comes from each replica's local clock
rather than from the entry, and there is no lease or session that would let the
coordinator clean up after itself.

### The embedded path is not this

`oxidedb/transaction/local.py`, which `Database` and the CLI use, shares the MVCC
storage but not the protocol.  It buffers writes in memory, takes one `commit_ts`
at commit time and writes each version with it.  Reads use the transaction's
`start_timestamp`, so they are snapshot reads, but there are no lock records and
no primary key, so nothing marks the transaction as in-flight: a process that
dies between two keys, or a reader whose timestamp lands above `commit_ts`
mid-commit, can observe part of a multi-key transaction.  The Percolator path
above is what makes a multi-key commit atomic; the embedded path trades that for
not needing a coordinator or a TSO.

## 4. ReadIndex: proving a read is fresh

A leader's `commit_index` is not proof that it is still the leader, and its state
machine is not proof that it has applied everything committed.  ReadIndex is the
Raft answer: get a majority to confirm the term, then serve the read at the
commit point that majority implies.

### The bug that was there

`MemoryRaftNode.get` used to send AppendEntries to every peer, collect the
responses - and then never look at them.  It looked like ReadIndex without
containing a quorum proof, so a leader that had been deposed or partitioned away
kept answering reads from whatever its own state machine happened to hold.  Those
reads could be stale (missing entries committed by the real leader) or even
uncommitted.  Linearity was assumed, not established.

### What the fix does, in order

1. Under the lock: snapshot `read_index = commit_index`, the current term, and
   the log view the RPCs will carry.
2. Send AppendEntries to every peer - the ordinary replication path, where a
   heartbeat with no entries is the common case - and count the responses that
   are `success` *and* carry the current term.
3. Our own acknowledgement counts as one; a majority is `(N + 1) // 2 + 1` for
   `N = len(peers)`.
4. Fewer than that: return `ERR_NOT_LEADER`.  The read is refused rather than
   answered from a state that cannot be justified.
5. Under the lock again: confirm we are still leader at the same term, because
   leadership can move while the RPCs are in flight.
6. `_update_commit_index()`.  Those acknowledgements may have advanced
   `match_index`, and therefore the commit point, so the second read of
   `commit_index` is the one the answer uses.
7. `_wait_for_apply(read_index)`, then read the local state machine.

### Why the quorum proves what it needs to

An acknowledgement from a majority at term `T` means no other node can have won an
election in a higher term: a candidate needs a majority too, and each voter votes
once per term, so the two majorities must overlap.  And because a majority
overlaps every previously committed entry, at least one node that acknowledged
holds every committed entry - so the leader's `commit_index`, re-read after the
acks, is at least as large as any entry committed before the read began.  Serving
the state machine at that index is then a linearizable read.

The no-op entry per election is the other half of the argument.  `_update_commit_index`
may only commit by counting replicas when the entry at that index is from the
*current* term (Raft 5.4.2), so a leader that has not yet committed anything in
its own term cannot advance its commit point at all - it may be missing entries
committed in an earlier term and cannot detect that.  Appending and committing
the no-op closes that window, which is what makes a read immediately after an
election both safe and possible.

The remaining cost is latency: every read pays a round of AppendEntries.  Batching
many reads behind one confirmation, or leader leases, are the standard
optimizations; neither is implemented.
