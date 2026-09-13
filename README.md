# OxideDB

A distributed transactional key/value store written in Python: Raft for
replication, a Percolator-style two-phase commit for distributed transactions,
MVCC for snapshot reads, and a range-sharded keyspace.

It is a working prototype rather than a production database.  `docs/design.md`
records why the keyspace encoding, the snapshot, the 2PC protocol and the read
path are shaped the way they are.  The sections below describe what is actually
implemented, how the storage layers fit together, and which gaps are known and
deliberate.

## Quickstart

Developed and tested on Python 3.14.  From a fresh clone:

```
pip install -e ".[test]"     # runtime dependencies, plus pytest
pytest tests -q             # 139 tests, roughly three minutes
```

`pip install -e .` on its own installs what the library needs; the `[test]` extra
adds `pytest` and `pytest-asyncio`, and `pip install -r requirements.txt`
installs the same set.  The tests start dozens of local gRPC servers and need a
writable temp directory.

Next, the example, which runs set/get, a range scan, a transaction and a rollback
against the embedded database:

```
python examples/basic_usage.py
```

There is also a small CLI, installed as `oxidedb` (equivalently
`python -m oxidedb.cli`):

```
oxidedb set user:1 alice
oxidedb get user:1
oxidedb scan user: user:9
oxidedb delete user:1
```

Without `--data-dir` the CLI drives the embedded database in memory, so **every
invocation starts from an empty keyspace**: a `set` in one process is not visible
to the next, and `get` exits 1 with `Key not found`.  That mode is a way to poke
at one command at a time.  `--data-dir` (before the subcommand) keeps the data in
`<dir>/data.sqlite3` behind the SQLite engine instead, and the commands persist:

```
oxidedb --data-dir ./demo set user:1 alice
oxidedb --data-dir ./demo get user:1      # alice
```

## Architecture

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
* **Sharding** — experimental and frozen; see Known gaps.  The keyspace is split
  into ranges, each range served by its own Raft group, and the table that says which
  range is where has an owner of its own (`metadata/service.py`, another Raft group).
  The cluster publishes it through `metadata/publisher.py`: the ranges once, each
  shard's replica set and addresses once, and a leader report whenever a shard's leader
  or its term moves, so a client that reads the table can route without being told.
  Clients route by it too (`metadata/cache.py`): one read of the table instead of a
  lookup per key, read again when a shard refuses a request or when the node the table
  names has stopped leading.  What is still missing is a split: `split_shard` moves the
  rows into the new shard's group and re-ranges the servers locally, but nothing tells
  the table, so a client that routes by it keeps reading the shard they came from; see
  Known gaps.

## Storage engines (plan E)

Persistence is delegated to a pluggable engine rather than hand-rolled.  All the
database keeps is the *encoding* of its logical keyspaces; the engine guarantees
durability and byte-ordered iteration.  The contract is deliberately small:
`put`, `get`, `delete`, `put_batch`, `delete_range`, `scan([start, end))`,
`flush`, `close`, with keys ordered by plain `memcmp`.

| Engine | Durability | Use |
| --- | --- | --- |
| `MemoryEngine` | none | embedded clusters, tests |
| `SQLiteEngine` | WAL, crash-safe | durable single-node and `data_dir`-backed clusters |

`SQLiteEngine` uses only the standard library and stores keys as `BLOB`s in a
`WITHOUT ROWID` table, so the primary key *is* the clustered index and a range
scan is a B-tree walk in exactly the order the callers expect.  Swapping in
RocksDB or LMDB later means implementing one class; no caller changes.

### Keyspace layout

Because timestamps are appended as big-endian suffixes, "all versions of a key,
oldest first" is a contiguous range scan, and a read at `t` is that range
truncated at `t`:

```
MVCC versions    \x01 || key || \x00 || ts(8B BE)          -> flag(1B) || value
write record     \x03 || key || \x00 || commit_ts(8B BE)   -> start_ts(8B BE)
lock record      \x04 || key || \x00 || start_ts(8B BE)    -> msgpack{status,
                                                              primary_key,
                                                              lock_time, value}

Raft log entry   \x01 || index(8B BE)                      -> msgpack{term, command}
Raft meta        \x02                                      -> msgpack{current_term, voted_for}
Raft commit      \x03                                      -> commit_index(8B BE)
Raft snapshot    \x04                                      -> msgpack{index, term, data}
```

The snapshot record carries its own `index`/`term`, so it is written before the
log prefix it covers is dropped: a crash in between leaves entries that replay
skips (they are at or below the snapshot index) rather than state with no entry
to rebuild it from.

`\x02` in the MVCC keyspace used to hold a bare write intent.  A lock record
carries the same value plus the primary key, the status and the TTL, so the
intent namespace was retired and `set_write_intent` is now a thin wrapper over
`put_lock`.

The MVCC namespaces and the Raft namespaces never meet: they live in separate
engine instances (`<data_dir>/data.sqlite3`, `<data_dir>/raft.sqlite3`).

`\x00` is the field separator inside the MVCC keyspace, so **user keys must not
contain `0x00`** (the same constraint TiKV-style encoders impose).  It is
validated on the way in rather than silently corrupting the index; use
`oxidedb.storage.next_key` to build exclusive range bounds instead of appending
`b"\x00"`.

## Using durable mode

```python
from oxidedb.raft.node import RaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine
from oxidedb.raft.storage import EngineRaftStorage

def storage_factory(node_id):
    return EngineRaftStorage(data_dir=f"/var/lib/oxidedb/node{node_id}")

cluster = RaftCluster(num_nodes=3)
cluster.start(lambda: MVCCStateMachine(), storage_factory)
```

For a single process holding the whole keyspace on disk:

```python
from oxidedb.storage.engine import SQLiteEngine
from oxidedb.storage.mvcc import MVCCStorage

store = MVCCStorage(SQLiteEngine("/var/lib/oxidedb/data.sqlite3"))
store.set(b"user:1", b"alice", timestamp=1)
assert store.get(b"user:1", timestamp=2) == b"alice"
store.close()
```

Omitting the engine keeps the original in-memory behaviour, which is what the
embedded tests rely on.

## Tests

```
pip install -e ".[test]"
python -m pytest tests -q
```

139 tests.  `tests/test_durability.py` covers the correctness properties that
used to be missing: committed-only replay after restart, durable log truncation,
SQLite-backed MVCC and lock round trips, durable locks across a node restart,
committing entries inherited from a previous term, single-node commit, and
ReadIndex quorum.  `tests/test_routing_consistency.py` pins the one routing rule:
the shard server, the transaction coordinator and the client router are asserted
to put the same key in the same shard, on the default range map and on a
post-split one.  `tests/test_shard_split.py` drives a split of its own: a row written
through a transaction, a split above it, and then that row read from the shard which
now owns it - at the timestamp it was committed at, at the newest one, and written
again - because a move has to preserve the version the row already was (a copy
stamped with the moment of the move is newer than any timestamp the TSO will ever
hand out, so the row is there and unreadable at once) and carry its write record with
it.  A second test holds a lock in the range and asserts that the split refuses rather
than move a row under a commit that has not been applied.
`tests/test_cross_shard_transaction.py` commits two keys that land in different shards,
which it did not used to do, and covers the two ways a
cross-shard prewrite fails: one shard refusing the lock, and one shard with no
leader at all.  `tests/test_snapshot_read.py` reads a key at a timestamp older
than its newest version and gets the older one, and through the coordinator checks
that a transaction sees its own prewrite while a snapshot taken before it still
cannot.  A third one reads two keys that live in different shards, changes one of
them in between, and asserts both reads come out of the one snapshot - a torn read
across two Raft groups is the failure it is there to catch.  Three more do it over a
range: a range read at a timestamp older than the newest version answers as of that
timestamp, a key behind a lock the snapshot may be owed refuses the whole range
instead of being left out, and a leader that cannot reach a quorum refuses the range
the way a single-key read is refused - without that handshake it would answer with
rows.
`tests/test_lock_cleaner_wiring.py` starts a cluster and lets the cluster's own
cleaner resolve a lock whose coordinator never came back; the same lock is left
alone when the cleaner is switched off, which is what makes the first test evidence
rather than coincidence.
`tests/test_lock_resolution.py` reads a key whose lock was left behind by a
coordinator that died between its primary commit and its secondary commits: the read
asks the primary key what happened, rolls the lock forward, and returns the committed
value that used to be an error.  A second test covers the other answer - a lock from a
transaction that never committed is cleared, and the read gets the version from before
it, after waiting the lock's TTL out instead of reporting it.  `SmartClient` gets the
same treatment twice: a stranded lock is resolved, and a read that meets a lock whose
transaction is still live blocks until the writer commits, then returns what it
committed - a value no TTL expiry could have produced.  The give-up path is pinned as
well, with the resolver stubbed to answer the way it does when no leader can.  A last
test is the layer below: a lock past any TTL is still reported as an obstacle, because
expiry is the resolver's policy and the state machine deciding it would answer with a
version older than one that is already committed.
`tests/test_metadata_service.py` starts the metadata group and checks what a routing
table has to get right: the client routes every key exactly where
`shard.router.locate` puts it, the ranges and the leaders arrive in one read at one
version, every replica applies the same table, a leader report from an older term
loses and one from outside the replica set is refused, a second bootstrap does not undo
a table that has since changed, a table restores from a snapshot with its version, and
a group that cannot reach a quorum refuses to serve the table instead of answering from
a local copy.
`tests/test_metadata_wiring.py` closes the join: it starts the metadata group beside a
real sharded cluster and asserts that the table a client reads names each shard's
actual leader, at its actual term, at an address that is really listening - the test
opens the socket rather than comparing strings.  A second test stops a leader's node
and watches the table follow the election to the node that took over, at a higher term.
The publisher's own rules are pinned without a cluster: it writes when something moved
and not on a timer, so the table's version stands still across polls; a restart that
re-proposes the same ranges is not a disagreement; and a table holding different
ranges is refused rather than overwritten.
`tests/test_client_routing.py` is the other end of that join.  A client with a table
routes by it and never looks at the cluster's own nodes - the test turns that into a
failure rather than a convention - and a transaction reads and writes by the same
placement, because a client that read by the table and wrote by scanning the cluster
would be two clients wearing one name.  The two ways the cache is kept honest are
pinned with fakes: a lookup that finds the node the table names has stopped leading
reads the table again, while a table that names nobody is left alone - the shard having
no leader is a fact about the shard, not staleness in the table - and a shard that
refuses a read sends the client back for the new answer.  The last test is all real:
a three-node cluster, a live metadata group, and a leader whose node is shut down
while the client holds a table naming it.  The read still returns what was committed.
`tests/test_serializable.py` is the write-skew story: two transactions read the same
snapshot and write disjoint keys, which snapshot isolation alone lets through.  With
the read set validated the second commit is refused and one doctor stays on call;
with `validate_reads=False` - the control - both commits land and nobody is on call,
which is the anomaly the validation exists for.  A third test pins the other
direction, that a read nobody superseded does not abort, and a fourth drives the
retry: `SmartClient.run` reruns the work, which this time sees the commit that
invalidated it and decides not to go off call.
`tests/test_snapshot.py` covers snapshots and log compaction in process: the
storage round trip behind them, what a snapshot has to contain
(MVCC history and unresolved locks included), a restart that rebuilds from the
snapshot because the entries are gone, and a replica catching up through
`InstallSnapshot` after the leader compacted past what it was missing.
`tests/test_network_snapshot.py` drives the same catch-up over a real gRPC
channel: a replica whose disk was wiped comes back with an empty log and is
rebuilt from the leader's snapshot, and the protobuf request/response mapping is
checked byte for byte.  `tests/test_apply_results.py` covers the bounded window
of apply results: it evicts oldest-first, and the result of the command a
proposal waited for is still there afterwards, a rejected one included.
`tests/test_cli.py` runs the command line front end as a subprocess: the default
in-memory mode starts empty every time, `--data-dir` persists, and `get` reports a
missing key with exit code 1.

Three environment notes:

* the durability tests need a writable system temp directory, because pytest's
  `tmp_path` lives there.  Inside a sandbox that blocks it they fail at fixture
  setup, not in the code under test;
* the network tests allocate non-overlapping port blocks through
  `tests/_ports.py`.  `ShardServer` derives shard `s` of a node at
  `base + 100 * s`, so node bases must be spaced `100 * num_shards` apart or two
  nodes claim the same port;
* the network tests wait for the condition they care about (`tests/_wait.py`)
  instead of sleeping a fixed number of seconds.  A fixed sleep is a race: the
  suite starts dozens of local gRPC servers, and a loaded machine can spend
  longer than the sleep just electing a leader.

## Known gaps

Honest list of what is *not* done, roughly in priority order.

* **`lock_time` comes from the local wall clock.**  Each replica writes
  `time.time()` into the lock record while applying the same log entry, so
  replicas hold TTLs that differ by a few milliseconds and the value is not
  covered by Raft.  Deriving it from the entry itself would make the state
  machine deterministic.
* **Snapshot reads are reachable from the state machine and the node, not from a
  client.**  `node.get(key, ts)`, `node.scan(start, end, ts)` and
  `coordinator.read(txn_id, key)` all take a timestamp, but the gRPC client path is
  scaffolding (below) and nothing outside the tests passes one, so a user still gets
  the newest version.  A read whose snapshot is hidden by a lock is resolved by asking
  the primary key's write record and then rolled forward or cleared, and a lock whose
  transaction is still live is waited out up to the lock's remaining TTL - a reader is
  stopped only by a lock that outlives its TTL, which means the shard could not settle
  it.  A range read refuses rather than guesses (`ScanRefused`): a key it cannot decide
  about, or a replica that is not the leader, is an error, because an empty range and a
  range nobody could answer must not look the same.
* **Serializable isolation is validated, not SSI.**  A transaction is refused when any
  key it read was committed over after its snapshot.  That prevents write skew, but it
  also refuses read-write overlaps that a conflict graph would allow, so it aborts more
  than SSI would.  The validation and the primary commit are ordered by one lock on the
  coordinator, which is what makes the check atomic with the commit - and bounds the
  guarantee to the transactions that commit through one coordinator; two committing at
  the same time would need the graph.  The read set covers keys, not ranges: `scan` is
  not part of the transaction path - it can be read at a timestamp, but it records
  nothing - so a phantom is not detected, and a transaction with a large read set pays
  one lookup per key with no batching or Bloom filter.
* **A snapshot is the whole keyspace in one blob.**  `MVCCStorage.dump` returns
  every row in a single msgpack payload, so the cost of a snapshot grows with the
  data set, and it is taken and restored while holding the node lock - the node
  serves no RPCs for the duration.  Chunked, incremental snapshots are the next
  step; the interface (`snapshot()`/`restore(data)`) does not have to change.
  Over gRPC the payload also has to fit in one message: the 4 MiB default limit
  means a snapshot past that size is dropped by the peer until the channel is
  configured for more.
* **No membership change.**  Cluster size is fixed at construction; there is no
  joint-consensus configuration change.
* **Timing is a thread per node, not an event loop.**  Elections and heartbeats
  are driven by one long-lived ticker thread per node (`MemoryRaftNode._tick`)
  and RPCs run on a small bounded pool.  That replaced a `threading.Timer`
  schedule that created 50-80 threads per second on an idle three-node cluster.
  An `asyncio` event loop per node is the intended end state.
* **`synchronous=NORMAL` trades power-loss durability for throughput.**  A
  process crash loses nothing, because every write is its own committed SQLite
  transaction, but a machine crash can drop the tail of the WAL because commits
  are not fsynced.  Use `SQLiteEngine(path, synchronous="FULL")` when that
  matters.
* **`JSONFileStorage` is legacy.**  Kept because existing tests construct it; it
  rewrites the whole log per append.  Prefer `EngineRaftStorage`.
* **The gRPC client path is scaffolding.**  `proto/client.proto` defines
  `ClientService` but nothing implements it server-side, so `OxideDBClient`
  cannot be used yet.  The CLI drives a local `Database`, not a cluster.
* **Sharding is experimental and frozen - do not use it.**  Every component routes
  through one range lookup (`shard/router.py`), and the table that lookup needs has an
  owner: `metadata/service.py` is a Raft group holding each shard's range, its replica
  set and the leaders that reported themselves, and `MetadataClient` reads it with the
  same ReadIndex path as any other read (design.md, section 7).  The cluster writes it
  too: `ShardedRaftCluster` starts a `MetadataPublisher` that publishes the ranges once,
  each shard's replica set and addresses once, and a leader report whenever a shard's
  leader or its term moves, so the table names the node that actually won the election -
  and stops rather than overwrite a table holding a different range map.  A client reads
  that table and routes by it (`metadata/cache.py`), reading it again when a shard
  refuses a request or when the node it names has stopped leading.  What that client
  cannot do is reach a shard it does not already hold a handle on: it resolves the node
  the table names to an object in its own process, which makes it a client *inside* the
  cluster, and the hop across a process boundary is what the unimplemented gRPC client
  service would be.  The lock resolver - the other thing in the transaction path that
  looks for a leader - still scans the cluster's own nodes.  Beyond that, two things are
  missing from a split.  It does not tell the table: `split_shard` copies the newest
  committed version of each row into the new shard's group and updates every server's
  range map locally, but nothing publishes the new ranges, so a client routing by the
  table keeps reading the shard the rows came from.  And it does not coordinate with a
  write: it refuses while a transaction holds a lock in the range, because that lock
  may be a commit that has not been applied, but one that resolved to the old shard
  just before the range moved still lands there and the copy ends up behind by it.  The
  old copies are left in the old shard, and there is no migration and no follower
  reads.  The cross-shard test also drives the coordinator directly rather than through
  a client, so no hop of it crosses a process boundary.
* **The SQL layer is minimal.**  `SELECT` and `INSERT` only; no schema, types,
  multi-row insert, `AND`/`OR`, `UPDATE`, `DELETE`, joins, or secondary indexes.
* **No multi-version garbage collection.**  Old versions are never reclaimed.
* **A multi-key commit is atomic only on the Percolator path.**
  `oxidedb/transaction/local.py` - the embedded `Database` and the CLI - has no
  locks and no primary key: it writes every key of a transaction with one shared
  `commit_ts`, so a crash between two of those writes, or a reader arriving
  mid-commit, can observe part of a transaction.  See `docs/design.md`.

## Layout

```
oxidedb/
  raft/          consensus: node, state machine, log/meta storage, gRPC servicer, shard server
  storage/       engine abstraction, MVCC, timestamp allocator
  transaction/   2PC coordinator, local transactions, lock cleaner, retrying client
  tso/           timestamp oracle on its own Raft group
  metadata/      shard map - ranges, placement, leaders - on its own Raft group,
                 its publisher, and the client-side cache of what it says
  shard/         the one routing rule for keys to shards
  sql/           SQL parser and executor
  client/        gRPC client SDK (server side not implemented)
  database.py    embedded single-process database (MVCC + local transactions)
  cli.py         command line front end for the embedded database
docs/            design notes and posts
  design.md      why the keyspace, snapshot, 2PC and read path are shaped this way
  blog/          the ReadIndex story: a read path that passed every test while wrong
proto/           gRPC service definitions
tests/           pytest suite
