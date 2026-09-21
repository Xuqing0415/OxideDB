# Architecture

> Moved out of the README, which links here instead.  The text is the README's, with the
> references between these notes updated to name the file each one now lives in, and each
> heading moved up a level.

A write travels through five layers:

```
client / SQL          encode a command
      |
      v
Raft (per shard)      replicate + commit the command   oxidedb/raft/node.py
      |
      v
State machine         apply the committed command      oxidedb/raft/state_machine.py
      |
      v
MVCC storage          version the write                oxidedb/storage/mvcc.py
      |
      v
Engine                durable ordered key/value store  oxidedb/storage/engine.py
```

* **Raft** — leader election, log replication, commit index, durable term/vote,
  a no-op entry per election (so a new leader can commit what it inherited),
  log compaction behind a state-machine snapshot, `InstallSnapshot` for a
  replica that fell behind that snapshot, a ReadIndex read path, and a bounded
  window of apply results for a waiting `propose` to read back.  Runs either
  fully in-process (embedded, used by most tests) or over gRPC
  (`RaftCluster.start` vs `RaftCluster.start_network`).
* **MVCC** — every write becomes a version keyed by timestamp, so a read at
  timestamp `t` sees a consistent snapshot and an old transaction keeps seeing
  the data it started with.  The read path takes that timestamp (`node.get(key,
  ts)`, `node.scan(start, end, ts)`, `coordinator.read(txn_id, key)`); a read with no
  timestamp still means the
  newest version, which is what a linearizable read wants.  A lock in the way is a
  question rather than an error: the reader resolves it - see Transactions.  Deletes
  are tombstones, not erasures.
* **Transactions** — Percolator-style 2PC: `prewrite` locks each key, the
  primary key's commit decides the transaction, then the secondary keys are
  committed.  Locks live in the engine rather than in memory, so a restarted
  replica still holds the intents it wrote before the crash; they carry a TTL
  and `LockCleaner` resolves abandoned ones by consulting the primary key's
  state.  The cluster starts that cleaner itself - 30 s scan interval, 5 s TTL,
  both arguments to `start`/`start_network` - because a cleaner only tests ever
  started is a cleaner nobody runs.  A reader that trips over such a lock resolves
  it on the spot with the same code the cleaner uses, and retries; a transaction
  that is still live is waited out up to the lock's remaining TTL first, so only a
  lock that outlives its TTL - one the shard could not settle - stops a read.
* **Isolation** — snapshot reads plus a read set validated at commit: a transaction
  remembers every key it read and is refused if any of them was committed over
  since its snapshot.  That is what stops write skew - two doctors who each check
  the other is on call, each decide they can go home, and each write their own key,
  with no write conflict between them.  It is optimistic validation rather than the
  conflict-graph SSI, so it refuses more than SSI would, and `SmartClient.run`
  reruns the work on a fresh snapshot when a commit is refused.
* **Timestamps** — a `TSO` Raft group hands out monotonic timestamps in batches;
  clients cache a batch to avoid a round trip per transaction.
* **Sharding** — experimental and frozen; see `known-gaps.md`.  The keyspace is split
  into ranges, each range served by its own Raft group, and the table that says which
  range is where has an owner of its own (`metadata/service.py`, another Raft group).
  The placement is published by the cluster's own nodes rather than written by hand, and a
  report reaches whichever member leads that group - the same walk a client makes, following
  the name a refusal gives - because the node that leads a shard is usually not the node that
  leads the group.
  The cluster publishes it through `metadata/publisher.py`: the ranges once, each
  shard's replica set and addresses once, and a leader report whenever a shard's leader
  or its term moves, so a client that reads the table can route without being told.
  Clients route by it too (`metadata/cache.py`): one read of the table instead of a
  lookup per key; a shard that refuses a request names where the leader is when it knows,
  so following an election costs one hop, and a refusal that names nowhere is answered by
  the shard's other replicas - one call each, in the order the table lists them - before the
  table is read again.
  `split_shard` is wired end to end: it freezes the range,
  copies the rows into the new shard's group as the versions they already were, proposes
  the split to the table, and re-ranges the servers locally before thawing the source.
  A move of a shard to another group is wired end to end too: `move_shard` freezes the
  source, copies its rows into a group on the nodes the caller named - at the timestamps
  they already had, so a snapshot read still finds them - tells the routing table, and then
  lets the group the shard left go, keeping its storage under an orphan name rather than
  deleting it.  A split or a move that died part way through is picked up on the next start
  out of the note the source shard left and the answer the routing table gives - by
  `ShardedRaftCluster` and by a node started through `launcher.py`, which drives the same
  recovery in the same order.  See `known-gaps.md` for what is still missing around
  all of this: something that decides to move a shard in the first place, and something that
  chooses where a shard should live.

```
oxidedb/
  raft/          consensus: node, state machine, log/meta storage, the Raft and client
                 gRPC servicers, shard server
  storage/       engine abstraction, MVCC, timestamp allocator
  transaction/   2PC coordinator, local transactions, lock cleaner, retrying client
  tso/           timestamp oracle on its own Raft group
  metadata/      shard map - ranges, placement, leaders - on its own Raft group,
                 its publisher, and the client-side cache of what it says
  shard/         the one routing rule for keys to shards
  sql/           SQL parser and executor
  client/        the six primitives a client may ask one node, the in-process and the
                 wire implementation of them, the factories a caller gets one from, and
                 the one place a placement becomes a handle (`proto/client.proto` is the
                 wire form)
  database.py    embedded single-process database (MVCC + local transactions)
  cli.py         command line front end: the embedded database, or a cluster
                 through --server
docs/            the notes the README points at, and the posts
  goals.md       the four goals: how far each got, how to use it, and its limits
  consistency.md the three read levels, and what `cached` costs
  quickstart.md  the three CLI modes, running a node, ports and `READY`
  architecture.md the five-layer write path, and this layout
  storage.md     the engine interface, the keyspace layout, durable mode
  testing.md     what the test suite covers, file by file
  known-gaps.md  what is not done, and what is out of scope by design
  design.md      why the keyspace, snapshot, 2PC and read path are shaped this way
  recovery.md    how a split or a move that died is finished after a restart
  blog/          the ReadIndex story: a read path that passed every test while wrong
proto/           gRPC service definitions: raft.proto, client.proto's six node-level
                 primitives (served by raft/client_servicer.py), and groups.proto's
                 routing-table and timestamp services (served by metadata/service.py and
                 tso/tso.py, asked by client/remote_group_client.py)
tests/           pytest suite
