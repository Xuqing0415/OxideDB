# Design notes

The README describes *what* is implemented.  This file records *why* it is shaped
the way it is: the decisions behind the keyspace encoding, the snapshot, the
two-phase commit protocol and the read path, and what each one buys.  File
references point at the code that implements the decision.

## How this repository is worked on

Nine habits, each of them learned by getting something wrong first, and between them the
reason the rest of this file can be believed rather than interesting on their own.

**Measure before building, and fix the estimate rather than the plan.**  More than one
correction below is of this kind: regenerating the protobuf bindings was written down as
costing "an unreadable diff" when it costs nothing, and the first plan for cutting suite
time was built on the guess that cluster startup dominated - which one timing run showed
to be the tests themselves.  An estimate that is wrong by an order of magnitude is worse
than none: it makes a cheap operation look expensive, and the plan then avoids the change
it should make.

**A workaround in a test names itself as a gap.**  When a test has to go around an
interface - it needs something the interface cannot hand it and reaches for it another
way - the comment says what the interface cannot say, not "for now".  The first version
of the wire-copy tests reached the group a move copies into with a factory and a node id,
and the comment said in as many words that `leader_client(shard_id)` answers about the
table's group and this one is not in the table yet; the next change added
`leader_client_for_nodes` and deleted the workaround.  A gap written down as a gap is a
gap the next change can be asked to close, and the note is what makes it findable.

**A control experiment needs a control of its own.**  A test is shown to be load-bearing
by removing the thing it checks and watching it fail.  If the removal was never reached -
a probe inside a docstring, a patch against a path the test does not take - the test stays
green and nothing was proved, and green is exactly what a proof looks like.  So the
removal has to be verified to have taken effect, by the run rather than by the source: the
wall time, the failure count, or a probe that raises where the injection was meant to land.

**A second implementation is switched away from as soon as it has proved its point.**  A
shadow body exists to answer one question - can this seam carry the work at all - and it
answers it by producing what the first one produced.  That proof expires at the switch:
kept past it, the two bodies are every later change made twice, and the test that compares
them stops meaning anything, because the thing it compares against has no reason to stay
still.  The copy a move makes was written twice for exactly this, the two were shown to
propose the same bytes for the same rows, and the body that reached through node objects
is gone - the one that remains is the second, and `tests/test_copy_row.py` pins it against
commands written out in the test rather than against its own answer.

**A signature change moves a spy; an assertion change is the regression.**  A test that
watches a piece of work from inside it - a wrapper around one call, counting what went
through - is written against the shape of that call, and when the shape changes the
wrapper has to change with it or it raises instead of watching: the run turns red with
errors, which looks like a red test and is not evidence about anything.  What may not
change is what the test asserts.  The switch of the copy onto the client seam took an
argument on `_move_row` and moved three spies onto `source._node` to read a freeze that
lives on the node a client wraps; every assertion in those files stood as it was, and that
is what makes them evidence that the behaviour did not move with the code.  Read the other
way, it is the answer to "did this change the tests?": adding an argument to a spy is not
changing a test, and relaxing what it claims is.

**Sweep what a change touches with the syntax tree, not by eye.**  "Which places reach
into this state" is the question that decides how far a refactor has to go, and reading
answers it for the paths the reader already had in mind and misses the rest: a field read
inside a loop over a dict, or on a branch that had been filed as uninteresting, is exactly
the access a count by eye drops, and the one that decides whether the extraction is
mechanical.  The sweep this repository's recovery interface was drawn from came out of
`ast` rather than out of reading - parse the module, walk the class for `self.<name>` - and
it found more of the cluster's own state inside the recovery's bodies than reading them
had.  The walk is a script and a minute, and it cannot be too tired to check a branch.

**The code is the truth; a document that disagrees with it is what gets fixed.**  A
design file is a reading of the code, taken at one moment and checked by nothing
afterwards, so where the two part it is the prose that is stale and the source that is
right - and the danger is not the stale sentence, it is the next change made to match
it.  Two statements in the recovery's own notes were settled against the files while its
seam was drawn, rather than by reading the notes again: a rule filed under one section
that belonged in another, and a migration's error string described there as the
finishing side's when a refusal before the note is where it is most often set.

**A branch's own tests passing says nothing about the code it shares.**  Moving one
branch can reach past it without meaning to - an import, a helper, a field - and the
tests that cover the branch will not notice, because what breaks is not under them.
Moving the split's recovery out of the cluster took a name out of that module's imports
that the move's half still used, and the nine files covering the split's paths went
green while the full run came back red in eight tests that never mention a split.  An
import is shared by every definition in the file, and a field by every branch that
touches it: after a move like that the suite is the test, or at least the tests of the
branch on the other side.

**A control experiment injects one failure per duty, not one per method.**  A test that
runs a call and then asserts several things about it stops at the first assertion that
fails, so breaking the whole call shows that one duty is covered and hides the rest behind
it - and what that looks like is "the test is shallow", which is the wrong reading: the
test covers all of them and the experiment never reached the others.  `_abort_move` has
four duties and one test, and breaking the body wholesale and then breaking only the note
it writes both landed on the same assertion; it took one injection per duty - drop the
in-memory state, drop the note, close no group, delete the copy - to see that each duty has
an assertion of its own.  The unit of injection is the thing that can fail on its own, and
the first red assertion is what tells you how far the experiment got, not how far the test
goes.

One thing is decided and deliberately not done yet, written down so that it is not
decided twice.  The lists of what is owed are going to be two lists with one rule between
them: an entry that has a finished state is a *gap*, and an entry only a change to the
shape of the system could cross is a *boundary*.  The re-sort waits for the recovery's
interface to exist, because that interface brings boundaries of its own - so until then an
entry may be in the list it does not belong to, and this paragraph is the promise that it
was thought about rather than missed.

## Pitfalls we have hit

Five things that cost time here and are not design decisions: tools that do one thing and
look like they did another, a rule that reads as stricter than the code, and a correctness
that comes from two places happening to agree.  Each one is reproducible in a minute and
invisible while it is happening, so they are written down here rather than left to be
recognised.  The habits above say how to work; these say what to check before believing a
result.

**A rule written stricter than the code makes the code look wrong.**  Section 6 of the
recovery's notes said an in-process test "must pass unchanged", which reads as a promise
that no test file was ever allowed to change - and the move of the copy onto the client
seam had changed the shape of three spies while leaving every assertion alone.  A rule
stricter than what was done fails nothing; it leaves the next reader thinking someone
before them broke it.  The sentence now says what was true: the assertions may not change,
a spy's shape may.

**Two files naming one thing differently look right in both.**  `metadata()` in the notes
and `metadata_client()` in the protocol were one call under two names, and neither file
looks wrong on its own, so grepping the old name in the file that defines the new one finds
nothing to fix.  Nothing catches this one: the prose reads as a name, the code reads as a
name, and only the interface says which name the recovery can call.  A rename is swept
from the definition outwards, and a name that appears in one file and not the other is the
shape of it.

**A right answer two places only agree on is not a right answer the call gives.**  `freeze`
used to work out for itself which of a moved shard's two groups to stop, and it worked it
out from `_serving_nodes` - which answers with the source set, but only while a move is in
flight, and only because the note has been loaded.  So the group it froze was the right
one for two reasons the call did not state, and one statement in the other order - the
freeze before the note - freezes every node in the cluster instead.  The set is an
argument now.  The question to ask of a piece of correctness is not whether it holds but
what it holds *by*: two places that happen to agree are two places the next edit pulls
apart, and a signature that names what it needs is the version of it that survives an edit.

**`Path.read_text` normalises line endings, and `newline=""` writes them back that way.**
Reading a CRLF file with it and writing the result with `newline=""` - the argument that
means "do not translate" - turns every `\r\n` into `\n`, so an edit to two lines shows
up as a whole-file change that `git diff` will not report, because this repository has
`core.autocrlf=true` and git compares the normalised forms.  Read and write such a file as
bytes, or pass `newline=""` on the way in as well, and then count: a file that kept its
endings has as many `\r\n` as `\n`.

**An experiment is not undone until the file matches HEAD byte for byte.**  With
`autocrlf` on, `git diff` and `git status` normalise both sides, so an injection that also
rewrote the endings leaves an empty diff over a file that is not the one you started from -
and the next commit then carries whatever the experiment did to it.  Comparing bytes
against `git show HEAD:<path>` is what says "restored", and it is the habit above applied
to a file rather than to a claim: check the state, not the report of the state.

## Invariants

Five things that are true of this system's shape rather than of any one change, each
found while trying to change something around it and none of them written down until
now.  They are here to be checked against rather than to be admired: nearly every bug
this repository has had is one of them being violated somewhere, which is why they sit
in front of the decisions they constrain.

**The version space holds no write intent.**  An intent is a lock, in a key space of its
own (`MVCCStorage.put_lock`, under `_LOCK`); a version is a row a commit published
(under `_VERSION`).  Two consequences, and the second is the one that gets forgotten: a
read at a timestamp cannot see a write that has not committed, and *a scan of the
version space cannot see one either*.  That is what makes it safe to read a shard's
rows at one moment and copy them somewhere else - Raft is what makes them applied, and
this is what makes them committed.  A change that folded intents into the version space
- a read-your-writes optimisation, say - would break the copy silently, with nothing to
report until the rows were already somewhere else and wrong.

**A value travels with the version it is.**  Everywhere the client service hands out a
value - `GetResponse` for one key, `KeyValuePair` for a row of a scan - it carries the
timestamp that value was written at (`commit_ts`), and 0 says there is none.  There is
deliberately no way to ask for the value alone: a caller holding a value without its
version could put it somewhere else and be wrong in a way nothing reports.  A lock's
value is the exception, and is not a version at all - an intent has no `commit_ts` to
carry, which is the same fact as the first invariant seen from the other side.

**`has_committed_in_its_own_term` is `follower_read_index`.**  Two names for one
question: has this leader confirmed an entry of its own term with a quorum, so that a
read it serves is linearizable.  `_read_index` answers it by collecting those
acknowledgements and the node's own flag reports the same thing, so a caller about to
add a call for one of the two should check whether the other already is it.

**A `RecoveryView` method's arguments and its answer cross a process boundary.**  No
method that names `MemoryRaftNode` can be in that interface, because a process holds
its own shard server and the leader of a shard may be inside another one: there is no
object to return.  That is a type-level impossibility rather than a preference, and it
is what turned `_shard_leader_node` into `leader_client(shard_id)`.  The check comes
before any question about how wide the interface is.

**One call re-maps one range.**  `locate` answers a key that falls outside every range
by handing it to shard 0, silently - that is its fallback, not an error.  So a split
that moved its ranges in two steps would have a window in which the keys of a live
range were routed back to the shard that had just given them away, with nothing failing
and the wrong shard answering.  `apply_split_locally` is one call for precisely this
reason: the side that holds the table does the whole re-mapping at once, so no window
exists for a key to be misrouted in.

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
7. `_wait_for_apply(read_index)`, then read the local state machine.  That wait
   is bounded (`APPLY_TIMEOUT`): a replica whose apply loop has stopped would
   otherwise hold the reader for as long as the node lives, and a read that
   hangs cannot be told from a read that is slow.  Past the bound the read is
   refused with `ERR_TIMEOUT` - which the wire carries as `TIMEOUT`, an answer of
   its own rather than a refusal or a word about leadership, because the reader's
   next move is to ask again and not to ask elsewhere or to give up.

Steps 1-6 are one method, `_read_index`, because `scan` needs exactly the same
handshake: a range read at a timestamp is only as safe as the replica serving it, so
the quorum is a requirement of reading at all rather than a cost of reading one key.
Only step 7 differs - the range instead of the key.

### When the caller already has an index

A read does not have to come this way.  `get` and `scan` also take an index from the
caller, and a caller that has one replaces steps 1-6 and the leadership they establish
with the wait alone: the replica waits for its own apply to reach the index and reads
its state machine, whether or not it leads, and whether or not it still leads by the
time the wait is over.  `FollowerReadIndex` on a leader is where such an index comes
from, which is the point of the shape - the proof of freshness is done once, by the
node that can do it, and the index it produces is what a replica that cannot do it
answers at.

Nothing is being assumed by that.  The index was set by a node a majority had confirmed,
and the entries a replica has applied are committed entries, so a state machine at or
past that index holds everything that was committed when it was named - which is what
makes a follower read a read rather than a guess.  The wait is the whole of what is
left, and it is bounded the same way it is above: a replica that cannot reach the index
is refused with `ERR_TIMEOUT` rather than held.  The wire has the field for it
(`GetRequest.read_index`), and a read that arrives without one is given it: the client
service asks `FollowerReadIndex` on the caller's behalf - carrying the question to the
leader, over the wire, when the node it is on does not lead - and passes the answer down.
So a caller that names no index is still answered at one, which is what makes a read
sent to a follower a read rather than a refusal.

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

## 7. Where the shards are: one table, read like any other

Sharding was frozen because nothing owned the routing table.  Every process built one at
startup and kept its own copy, so "which shard holds this key" was a local belief: no
split could change it and no client had anyone to ask.  The table now has an owner - its
own Raft group, three nodes like the timestamp group, holding one document.

**Why a group of its own.**  A table that lives inside one of the shards it describes
cannot be read while that shard is electing, and the client that needs the table most is
the one whose shard has just moved.  The metadata group is the one group whose placement
never changes, which makes it the one thing a client can find without first being told
where to look.

**Why one document, not a keyspace.**  Reading is a point-in-time act.  A client that
read the ranges and then, separately, the leaders of the shards those ranges point at
has routed on a table that never existed - the torn read of section 5, one layer up.  So
the whole table is one key, read in one call, and the version travels with it.  That read
is `MemoryRaftNode.get`, so the ReadIndex handshake of section 4 applies to it unchanged:
a metadata leader that cannot reach a quorum refuses to serve the table rather than hand
out one it cannot justify.

**Why terms.**  A shard's leader is discovered by the shard, not decided by the table:
the node that wins an election reports it, with the term it won at, and the table keeps
the newest claim.  Without the term, two reports crossing on the wire would let a deposed
leader write itself back in, and the table would point clients at a node that no longer
leads - the ReadIndex bug one layer up, and just as quiet.  A report from a node outside
the shard's replica set is refused outright, and replacing a replica set clears the
leader if the node that claimed it is not in the new set.

**Why the client caches it.**  Routing is on the hot path; the table changes when a
shard splits or a leader moves, which is to say rarely.  So a client reads the table once
and hands out `shard_for` and `leader_for_shard` from that read, refreshing when a shard
tells it the answer was stale.  The cost is one linearizable read per refresh instead of
one per key, and the thing that makes it safe is that the cached table was a decision
when it was read - not that it is still current.  A stale cached table routes a key to a
shard that will refuse it, which is a retry; a guessed table routes it somewhere nothing
checks.

**How the cache is kept honest.**  There are two ways back to the table and no third.
A shard that refuses a request because this client reached a node that is no longer its
leader sends the client back for the new answer: one read of the table and one second
question, because a refusal that survives a fresh table is the shard's current answer and
asking a third time would only make a wrong one slower.  Asking the node itself whether it
still leads is deliberately *not* one of the ways.  The node this client reached is the one
whose belief is in question - a leader cut off from its peers goes on answering reads as if
nothing had happened, and only a refusal from the shard can say otherwise - which is also
why a lookup here hands out a client for the placement the table names rather than the node
behind it.  And a table that names nobody is read again,
because "nobody leads this shard" and "this table was read before the leader was published"
are the same answer from the client's side, and only one of them is worth waiting out.
Placement arrives a command at a time - the ranges, then a shard's replica set, then its
leader - so a client whose first read lands in the gap holds a range nobody leads, and a
client that believed that for ever would never route again.  It is the one read that
repeats, and it is rate-limited to the publisher's own poll interval per shard
(`MISSING_LEADER_REFRESH_INTERVAL`): re-reading faster than the publisher can write cannot
learn anything, and a shard that really has no leader must not turn every read into a
metadata round trip.  Nothing else is refreshed on a timer, because the table is on the hot
path and a fetch per key is the cost the cache exists to avoid.

**Why a split is a command, and checked by the group.**  A split is proposed after
the rows have moved - the table may only say a range belongs to a new shard once the
data is there - so by the time the group hears about it the caller has already done
the dangerous part.  That is exactly why the group does not take the caller's word for
the shape of it.  The split point has to be strictly inside the shard's range (a half
of zero width answers for nothing), the new id has to be free, neither half may
overlap a range another shard owns, and all of it is checked before anything is
replaced: a half-applied split is a range nobody owns.  A retry is a success, because
a caller cannot tell a lost response from a lost proposal and the group has to answer
both the same way - but only for the split it actually applied, so the same ids with a
different replica set is a different proposal and is refused.  The left half keeps the
old shard's id: it is the same group with less to answer for, and giving it a new one
would mean standing up a second group to hold a copy of a shard that is already there.

**The half that tells the table.**  The publishing half of the join is in place: the
cluster starts a `MetadataPublisher`, which proposes the ranges once, each shard's replica
set and addresses once, and a leader report only when the leader or its term moves.  That
restraint is the design, not an optimisation - every write to the table is a reason for
every client's cache to refresh, so a pass that found nothing has to say nothing, and the
publisher compares before it proposes rather than rewriting the table on a timer.  A
refused report is normal, because two reports racing is what terms are for; a table holding
ranges the cluster cannot account for is not, and the publisher stops with the disagreement
attached rather than overwrite a keyspace belonging to another cluster.  The maps it can
account for are the one it routes by and the one its own split in flight will produce,
which it asks the cluster for rather than guessing: a split reaches the group from the
shard's own thread, so the publisher's first pass can find the table a step ahead of the
cluster, and a map of one's own is not a reason to stop.

**The half that moves the rows.**  A split and a move are one protocol with different
subjects - a point inside a range, and the whole range - and the order is the whole of it:
freeze the range, read its rows at one moment, copy them into the group that will own them,
and propose to the table last, because the table is what clients route by and a range it
hands out has to be a range whose data is already there.  The rows are copied *as the
versions they already were*: a copy stamped with the moment of the move is newer than any
timestamp the TSO will hand out, so the row would be present and unreadable at once, and
every prewrite against it refused as a write conflict.  `split_shard` freezes the range,
waits out the writes admitted before the freeze, refuses rather than copy out from under a
lock that may be an unapplied commit, moves the rows above the split point, and only then
proposes the split.  It re-ranges its own servers before thawing the source, because until
the thaw the shard still answers for a range it is no longer allowed to add to, and a thaw
without the table would send clients to a shard whose right half has been copied away.  A
refused proposal leaves the shard frozen and the split remembered, and asking again
finishes that same split instead of starting a second one.  The intent is written into the
source shard's own storage before the first row moves, so a cluster that dies between the
copy and the proposal comes back, freezes the shard again, copies only what the new shard
is missing - read back out of the source's state, not out of the note, because a note
carrying rows is a second copy of the shard taken at some earlier moment - and makes the
proposal, which the group recognises as the split it already applied.  The new shard's
leader is asked for those rows only once it has committed an entry of its own term: a node
that has just been elected cannot yet tell which of the entries in its log ever committed,
and taking "not there" for an answer at that moment would copy a row that had already
arrived.

**Why a move replaces a whole group.**  The Raft here does not change a group's membership,
so there is no way to put one replica of a shard somewhere else: a move builds the group
the shard is going to on nodes that do not already serve it, copies the range into it, and
lets the old group go.  The set being replaced is read out of the table on every attempt
and never taken from this process, because the metadata group refuses a move whose
expectation of the current set is wrong (code 14) and this process's own view is exactly
the stale thing that check exists to catch.  The set being moved to is the caller's to
name: which nodes a shard should end up on is a policy, and a mover that derived one would
also be choosing a replica count.  A target set that overlaps the one serving the shard is
refused before anything is frozen - a node cannot hold two groups for one shard - and
asking again with the same target set is a retry that finishes the move in flight, while a
different target set is refused, because there is no answer to that which would not lose
one of the two.

**The window: what the group the shard left is still for.**  Once the table names the new
group, the group the shard left stays up for a fixed window (`MIGRATION_DRAIN_SECONDS`)
before it is closed and its storage put aside.  It is there for one caller: a client routes
by a table it cached, so the node it was sent to a moment ago is a node it may still ask.
What it meets in that window is one of three things.  A read is answered, and the answer is
the shard's - the group has taken no new rows since before the copy, so what it holds is
what the copy carried away.  A command that would bring it new rows - a set, a delete, a
prewrite - is refused (`ERR_MIGRATING`, "the shard is moving to another group"), because
that has been true since before its rows were read; a commit or a rollback is not, since
neither adds anything to the range.  And nothing in the refusal says where the shard went,
because this group does not know: it was told to stop taking rows, not where they were
going.  Over the wire that refusal is the one code the client service has for "the shard
said no", with the shard's words left in the message and no address to follow - a refusal
whose answer is one read away, and whose shape on the wire is what the open decision at
the end of this section is about.

**After the window: the walk, and what it costs.**  When the window is over, the group and
its port are gone, so a client still holding the old table asks an address that nothing
accepts a call on.  That is the same evidence a refusal is - not the node to ask - so the
walk is the one this section already describes for a leader change: the address the table
named, then the shard's other replicas one at a time, and the table only after they run
out.  The table is where the group the shard moved to comes from, so the reread a refusal
buys is what finds it, and nothing had to tell the client anything.  The cost is one call
timeout per address of the set the client cached - the set is finite and the table is read
once, so it does not grow with how long the shard is gone.  What is not settled is that
bound: it grows with the size of a replica set, so a placement with more replicas makes
recovering by walk more expensive, and a client that could tell a shard that moved from a
node that is down would spend no timeouts at all.  Whether it should be able to is the same
question as the wire code below.

**Coming back to a move that was in flight.**  A move writes down what it is doing before
it does it, in the source shard's own storage: the shard, the set it is leaving, the set it
is going to.  The note goes only once the table names the new group and the one it left has
gone, so a cluster that comes back and finds one was moving a shard when the process
stopped, and the table is what says how far it got:

* the table names the set the move was going to, so the proposal landed and what is left
  is the half after it: the group the shard is moving to is built if this cluster does not
  already serve it, the note goes, and the group it left is let go;
* the table still names the set the move was leaving, so nothing was proposed, and the
  move is finished the way a retry of it is - copy what the new group is missing, and
  propose;
* the table names neither, or cannot be read: the shard stays frozen, the note stays, and
  the caller is told what the table said.  An operator who moved the shard by hand and a
  second move computed from the same table look the same from here, and guessing between
  two live groups is how a range ends up served by one of them while its rows are in the
  other.

The source goes back to being frozen before any of that, because a freeze is a local fact
and it died with the process that set it: a row let into the old group while the move is
half done is a row nothing will carry across.

And it does not wait the window out.  A window is what lets a client that cached the old
table finish the read it arrived with, and a process that has just started has no such
client: the move is committed with no drain, and the group it left is gone by the end of
the call.  A split that died is finished the same way, out of its own note.

**What the client service changed here.**  Two things this section used to say are no
longer true, and both of them were about the distance between a client and a shard.  A
client outside the cluster exists now: `ClientService` serves six node-level primitives -
get, scan, propose, the lock record, the write record and the read index a follower would
ask for - on the port a node already listens on for Raft, with no transaction among them,
because the primary key of a transaction and its timestamp are the client's to choose;
`RemoteNodeClient` dials an address the table published, and the client that routes by the
table is the same one either way, since what a lookup hands out is a client and not a node.
And the lock resolver is not a second placement any more: it asks `ShardLeaders`, like the
coordinator and the SQL executor, so the transaction path has one lookup and one placement.
What the classification does to a refusal is what the open decision below is about.

### Open decision: the wire shape of the refusal a moving shard gives

A shard frozen because its range is moving refuses a write, and what it refuses with does
not survive the client service: the shard's own code is flattened into the one code for
"the shard said no", with the shard's words in the message and nothing to follow.  This one
matters more than the other flattened codes, because it is evidence about *placement*
rather than about the command: the table the caller holds is old - the range is not this
shard's any more - and the answer is knowable, one read away.  Today `ask_shard` walks on a
refusal to lead and on nothing else, so a write that arrives in the window is handed back
to the caller, whose only usable next move is to read the table itself.

**What the caller should be told to do is settled: read the table again.**  The group the
shard moved to already owns the range, so a write routed by a fresh table lands, and the
refusal is evidence about placement rather than a reason to wait.  It is also what the
metadata machine already says one layer up about a placement that has gone stale: a move
whose expectation of the current set is wrong is answered with "read the table again
before believing yourself" (code 14), and this refusal is the same news one layer down.
Waiting is the candidate this section has refused from the start, for a reason that does
not stop being true here: a caller told to wait cannot tell "wait" from "give up" without
knowing how long the window is, which would make the window a promise clients depend on.
The command line draws the same line for the same reason, and had to in order to exist: a
`--server` command waits for a cluster to exist and not for a command to succeed.

**What is still owed is the shape that carries it.**  Two are possible, and the choice
between them is not decidable on its own.  A code of its own beside `LOCKED` and
`NOT_LEADER` would widen the client service contract, and it would be the next of the
shard's own codes to be *named* rather than flattened - `TIMEOUT` is the one before it -
so taking it is less a decision about this refusal than about how that whole family is
treated, which is a question the next widening of `ClientService` is the natural place
to settle.  `REFUSED` carrying something to act on is the other shape: a smaller change
to the enum and a larger change to what a refusal means, because the field that carries
"somewhere else to ask" is an address, and a group that is moving has none to give.

**Milestone.**  Nothing writes to the wrong place today - the refusal is honest and the
caller can read the table - so this is not a correctness debt, and what a client should do
about it is no longer in question: only the carrier is.  That is due with the next
widening of `ClientService`, and before follower reads: follower reads put a second
decision about placement in the client, and a caller that has to read prose to act on a
refusal is a caller that cannot be anything but this repository's own code.  Until then
it is written down in the README as well as here.

That widening is no longer only about this refusal.  Recovery needs a read that says
which version a row is at, and no combination of the six calls the client service had
could produce it (`docs/recovery.md`, section 5): a write record is written by a
transaction's commit and not by a plain `SET`, and the scans answered key and value
with no version.  So a recovery could not copy a row from another process until the
service carried a version-stamped read, which put the next widening - and this decision
with it - in front of the recovery instead of after it.  The stamped read is done and
the carrier is not: `GetResponse` and `KeyValuePair` have a `commit_ts`, the servicer
fills it, and a client asks for `scan_versions` where `scan` drops the version for the
callers that want rows.  Filling that field moved nothing about the refusal family,
which is the sense in which the two are decided together: one contract, two changes,
and only the carrier is still open.

### What is not covered.

The split can end up behind a write that resolved to the old shard just before the range
moved: it refuses while a lock is held in the range, because that lock may be a commit that
has not been applied, but it cannot see one that committed somewhere else first.  The
copies it leaves in the old shard are stale from then on and nothing reclaims them, and
neither does anything reclaim the directory a move puts aside (`orphan-shard-<id>-<when>`):
both are kept deliberately, and a leaked directory costs disk where a lost range costs the
data.

Nothing chooses a split point or a move.  There is no rebalancer and no node reports its
load, a move's target set is the caller's, and two splits of different shards are not
serialised against each other.  A move in flight is not coordinated with a write that
resolved to the old shard just before it started, either, which is the split's hole under
another name.

There is no membership change, so a move is a whole group replaced rather than a replica
swapped: a shard cannot be given a replica on a node that already serves it, and the group
it leaves is gone rather than shrunk.

The write that arrives in the window is still the caller's to act on, because the refusal
does not cross the wire: what a client should do about it is settled above, and the shape
that would carry it is what is still owed.

Nothing routes a read to a follower.  A read that arrives at one is answered there now -
the client service fills in the index, as above - but nothing chooses a follower to send
a read to, so every client that has a table still asks the leader, including the CLI's
`--server`; picking the replica to read from is a client-side choice that does not exist
yet.
