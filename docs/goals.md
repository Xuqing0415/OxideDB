# The four goals

> Moved out of the README, which links here instead.  The text is the README's, with the
> references between these notes updated to name the file each one now lives in, and each
> heading moved up a level.

What this project set out to build, and where each goal stands: what works, how to reach
it from outside, and what it does not do.  The detail behind each of these is in
`docs/design.md` and in the notes each section points at.

## Strongly consistent reads

**Attained.**  A read asks the shard's leader for an index, the leader confirms one with a
quorum, and the answer is the state machine at that index - so a read cannot return a
value that was already stale when it began, however far behind the node that answers has
fallen (a replica too far behind for the log is caught up with a snapshot).

**How to use it.**  It is the default, and it is what every read in this repository did
before there was a choice: `oxidedb --server 127.0.0.1:8001 get user:1`, or any of the
node primitives with no index named.

**Limits.**  The confirmation is a round of RPCs per read, and no leader can skip it:
nothing here assumes two nodes' clocks agree, so there is no lease to read under.
Availability is a quorum's, so a leader cut off from its peers refuses reads rather than
answering them out of its own state.

## Cross-process ACID transactions

**Attained.**  A Percolator-style two-phase commit: writes are staged as intents at the
transaction's start timestamp, the decision is one record on a primary key, and a lock
left by somebody else is resolved rather than waited on - rolled forward if that
transaction committed, cleared if it did not.  It holds across a cluster of real
processes, and a transaction whose client died is finished or cleared by the lock cleaner
rather than left holding its keys.

**How to use it.**  `SmartClient.begin` / `add_write` / `commit`, or `run(work)` which
runs the work again when the commit is refused; `examples/basic_usage.py` shows both.  A
`set` from the shell is one of these with a single key in it.

**Limits.**  An `oxidedb delete` is a blind write rather than an intent, so a delete that
races a transaction on the same key can be overwritten by that transaction's commit.  A
lock's TTL is written from each replica's own wall clock rather than derived from the log
entry, so replicas disagree about it by milliseconds and it is not covered by Raft.

## Serializable snapshot isolation

**Attained.**  Serializable for transactions whose reads are point reads.  A transaction
reads at one timestamp - a snapshot, so it never sees half of another transaction - and a
commit is refused when any key it read has been committed over since that snapshot.  That
validation is what makes the committed history serializable: write skew is refused rather
than allowed, and the refusal is ordered with the primary commit, so the check and the
commit cannot come apart.

**How to use it.**  `SmartClient.run(work, attempts)`, which runs the work again after a
refusal: a refusal is about the snapshot the work decided from, so it is the decisions
that have to be made again, and running the commit again cannot help.

**Limits.**  The read set is keys rather than ranges, and that is where the boundary in
the sentence above comes from: `scan` is not part of the transaction path and records
nothing, so a phantom is not detected - a transaction that ranges over keys it never
point-read is outside the guarantee rather than wrong.  The check is optimistic
validation rather than SSI: it refuses every read-write overlap, including the ones a
conflict graph would allow, so it aborts more than a serializable algorithm has to.  And
the ordering that makes the check atomic with the commit is one lock on one coordinator,
so the guarantee is for the transactions that commit through one of those.

## Horizontal scaling

**Attained.**  The keyspace is a set of ranges, each owned by a shard with a Raft group of
its own.  A shard can be split in two and moved to other nodes while the cluster runs; a
client routes by a routing table published to the metadata group rather than asking
around; and a read that need not lead is served by any member of a shard's replica set,
so reads spread over the set instead of landing on the leader.

**How to use it.**  `--server` routes by that table, and
`oxidedb --server 127.0.0.1:8001 get user:1 --consistency follower` (or `cached`) reads
from a member rather than from the leader.  `split_shard` and `move_shard` split and move
a shard on a cluster that is running.

**Limits.**  Nothing rebalances and nothing chooses a split point: both are a caller's
decision.  A process serves the shard ids its own start-up names and nothing on that path
reads the table, so every node is a member of every shard - scaling out means every node
holding everything, and a subset placement needs a node that is already running to be
told the table names it, which is a message this repository does not have.  A move leaves
the rows it copied behind in the source shard, and nothing reclaims them.
