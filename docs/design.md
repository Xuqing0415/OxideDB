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
* **`_last_applied_timestamp` travels with it.**  A read that is not given a
  timestamp is served at that one (`get` -> `storage.get(key,
  self._last_applied_timestamp)`), so a machine restored without it would read as
  of timestamp 0 and see nothing.

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
leaves locks that nobody is going to resolve.  `lock_time` is what stops those keys
from being blocked forever: past `DEFAULT_LOCK_TTL` - 5 seconds, an argument to the
resolver - the transaction counts as dead, committed if the primary has a write
record for that `start_ts` and aborted otherwise.  The TTL is a trigger for asking
the question, not the answer itself.

The TTL lives only there, which took a correction.  `MVCCStateMachine.get` used to
stop reporting a lock as an obstacle once it was five seconds old and answer with the
newest applied version.  That is fine when the transaction died, and wrong when it
committed with only the secondary commit missing: the newest applied version is then
older than one that is already committed, so the read would go backwards for a
transaction it should have seen.  Expiry is a policy, and the state machine is not
where policies live - it now reports the lock however old it is, and the caller asks.

That derivation is `LockResolver`, and it has two callers.  A reader that trips
over a lock uses it to finish its read (section 5).  The `LockCleaner` uses it to
sweep for locks whose coordinator is not coming back, and the cluster starts that
cleaner itself (`ShardedRaftCluster.start` and `start_network`) rather than leaving
it to whoever calls them: nothing guarantees that somebody will read the key a dead
coordinator locked, and a lock nothing reads is a lock nothing resolves.  The scan
interval (30 s) and the TTL are both arguments, and `lock_cleaner_interval=None`
leaves it off for a test that wants to watch a lock stay unresolved.  Two callers,
one question, one answer - which is the point: the answer is in the primary key's
write record, so it does not matter that the coordinator that asked is gone.

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

Steps 1-6 are one method, `_read_index`, because `scan` needs exactly the same
handshake: a range read at a timestamp is only as safe as the replica serving it, so
the quorum is a requirement of reading at all rather than a cost of reading one key.
Only step 7 differs - the range instead of the key.

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

## 5. Which timestamp a read uses

A read has two possible answers and only one of them can be "the value of `k`".
Reads with no timestamp - `node.get(key)`, the CLI, the SQL layer - answer with the
newest version the replica has applied, and section 4 is what makes that answer
fresh.  A transaction answering that way would be wrong: the newest version is not
the one it started with, and two reads of the same key inside one transaction could
straddle somebody else's commit.  So the read path takes a timestamp, and a
transaction passes its own `start_ts`.

The ReadIndex handshake stays, and it is what makes the older timestamp safe.  A
snapshot read at `start_ts` needs every commit up to `start_ts` to be applied
locally; the quorum puts this replica at or past everything committed before the
read began, and the TSO issued `start_ts` before the read began, so everything at
or below that timestamp is included.  Freshness is a requirement for the newest
read, not a cost that a snapshot read could skip.

Which locks a read has to respect follows from the same idea - a lock only hides
the key from a reader that could have seen the version the lock holds:

* `lock.start_ts == read_ts`: the lock is this transaction's own write intent, so
  the value it carries is the answer.  This is what makes a read-modify-write
  inside a transaction behave.
* `lock.start_ts > read_ts`: the writer started after this snapshot, so it cannot
  have committed into it.  The lock is ignored and the older version is returned.
  Blocking here would let any concurrent writer stall a reader that is not
  looking at its key.
* `lock.start_ts < read_ts`: the writer may have committed at or below this
  snapshot, and then the version to return is the one the lock holds.  The lock
  does not say which way it went, so the read asks the primary key's write record
  (`LockResolver`): committed means the lock is rolled forward into the version
  this snapshot was owed, nothing there means the lock is cleared, and either way
  the read is retried.  While the transaction is still inside its TTL nothing has
  been decided and there is no answer to be had *yet* - which is not the same as no
  answer at all, so the read waits that out instead of reporting it.  The wait is the
  lock's own remaining lifetime, because that is exactly the moment `primary_status`
  stops calling the transaction live; a reader gives up only on a lock that outlives
  its TTL, which means the shard could not settle it.  A reader is never stopped by a
  transaction that has already decided, and never waits longer than the transaction
  could have been given.

`LockResolver` is the same code the cleaner sweeps with (section 3), deliberately:
a reader tripping over a lock and a cleaner hunting for abandoned ones are asking
the same question, and a second implementation of it would be a second answer.

Waiting is the same arithmetic as the decision, so it lives in the same place too.
`remaining_ttl` is the comparison `primary_status` uses to tell "in flight" from
"nobody is coming", in the open, and `await_resolution` is the loop over it: ask,
sleep a poll interval, ask again.  The poll interval is the latency a reader pays for
a commit that lands while it waits, and the deadline is the lock's remaining TTL plus
one round trip - so a lock that the shard cannot settle (no leader to ask) ends in an
error after a bounded wait rather than in a hang.

A range read is the same read over a range and gets the same rules per key: `scan`
takes the timestamp, does the same handshake, and applies all three lock rules to every
key it covers.  What it does not do is invent an answer.  A key whose lock it cannot
decide about, and a replica that is not the leader, both raise `ScanRefused` instead of
coming back with rows, because a key left out and a key that was never written are the
same thing to a caller - and an empty list is a legitimate answer, so a refusal
delivered as one would be indistinguishable from it.

What is still missing is a range read anyone can reach.  No client or transaction path
passes a timestamp to `scan`, and the read set is a set of keys, so a range read is not
part of a transaction and a phantom is not detected.  What makes the rest of it
serializable is section 6.

## 6. Isolation: what the read set buys

Snapshot isolation gives a transaction a fixed snapshot and stops it from writing over a
version it did not see.  It does not stop two transactions from deciding *opposite*
things from that same snapshot and then writing keys that do not overlap.  The example
this section is named for: two doctors are on call, each checks that the other is there,
each concludes the shift is covered, and each writes its own key to say it is going
home.  Percolator's prewrite check looks at the key being written and nothing else, so it
has nothing to say about either write, and both commit.  The rule that somebody stays on
call is broken by two commits that each look fine on their own, so no amount of care in
the write path finds it.

**What the read set is.**  A transaction records every key `coordinator.read` served it.
At commit, after its locks are taken and before the primary key is committed, each of
those keys is checked for a version committed after the transaction's `start_ts`.  A
commit after the snapshot means the decision the transaction made from that read was made
from data the serial order no longer contains, so the transaction is rolled back - locks
released - and reported as a `SerializationError`: a refusal, not a failure.

Why that catches the two doctors: each of them read what the other was about to write, so
whichever commits second finds its own read invalidated.  One of them has to lose, which
is the whole point - it is the rule, not either doctor, that the database is being asked
to keep.

**Why validation and the commit have to be one step.**  Validation alone is not enough.
If two transactions validate and only then commit, they can validate a moment apart and
both pass: at the instant each looks, the other has not committed yet, so both conclude
their reads are still good.  That is the same anomaly one layer up.  `commit` therefore
holds one lock across the validation and the primary commit.  It is the coordinator's
lock, which is also the honest limit of the guarantee: two coordinators, or two
processes, committing at once would need the conflict graph, and there is one of those.

**Why not a conflict graph.**  PostgreSQL's SSI keeps rw-antidependencies in both
directions and refuses only transactions that close a cycle.  That is less pessimistic
than what is here - this refuses any transaction whose snapshot was superseded, even when
no cycle exists - but it needs a registry of live transactions, edges between them, and a
rule for which one to sacrifice.  The simpler rule is sound, needs no shared state beyond
the keyspace, and costs one lookup per read key.  What it is not is SSI, and the README
says so.

**Why not read locks.**  The other way to catch it is to mark keys as read while a
transaction is open and have a writer refuse to prewrite a key with a live reader.  That
is the read half of two-phase locking, and here it would mean a second kind of lock
record going through Raft with its own TTL and its own resolver - a second lifecycle to
get right, for a guarantee that validation already gives.

**What is not covered.**  `scan` is not part of the transaction path, so a range read
records nothing and a phantom is not detected; the read set is a set of keys.  The
embedded `Database` path (`local.py`) has no read set at all, for the reasons in section
3.  And a read set of n keys costs n lookups at commit, with no batching and no Bloom
filter, which is the price of the simple version.
