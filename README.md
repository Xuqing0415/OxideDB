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
pytest tests -q             # 297 tests, roughly ten minutes
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

`--server` is the third mode, and the only one that talks to a cluster: it names a
node of one - the address that node's shard 0 listens at - and the same four commands
then travel over gRPC, through the routing table, to whichever shard owns each key:

```
oxidedb --server 127.0.0.1:8001 --shards 2 set user:1 alice
oxidedb --server 127.0.0.1:8001 --shards 2 get user:1            # alice, out of whatever shard owns it
oxidedb --server 127.0.0.1:8001 --shards 2 scan a: z            # every shard it covers
```

`--shards` is how many shards that node was started with, because a node's group
ports sit above its shard ports - the arithmetic is `ports_for` in
`oxidedb/launcher.py`, imported by the CLI rather than worked out a second time - and
it defaults to the launcher's own default of two.  Several nodes may be given,
comma-separated or repeated, and all of them are used as seeds: no client knows which
member of the routing table's group leads, so one address it cannot use would
otherwise end the walk.  `--wait SECONDS` bounds how long a `--server` command asks a
cluster that has not finished starting before it reports, as one line, the reason it
could not be routed; it defaults to five, which is ten of the publisher's own poll
intervals, and `0` asks once.  `--data-dir` and `--server` together are refused, since
the same key cannot be in two places.

## Running a node

A node of a cluster can be a process of its own:

```
python -m oxidedb.launcher --node-id 1 --port 8001 --data-dir ./node1
```

That is a cluster of one: every group the node holds elects itself, and a client can
write to its shards over the wire.  Three nodes are the same command three times, each
naming the others by the address their shard 0 listens at:

```
python -m oxidedb.launcher --node-id 2 --port 8001 --data-dir ./node2 \
    --peers "1@10.0.0.1:8001,3@10.0.0.3:8001"
```

A node takes a block of ports from the one it is given: shard `s` at `port + 100 * s`,
the routing table's group above the last shard, and the timestamp group above that.
That arithmetic is one exported function, `ports_for` in `oxidedb/launcher.py`, and a
program that starts these nodes - a test, a script - works their ports out by importing it
rather than by doing the sum again, so the address it waits on is the address that was
bound.
It prints `READY <host> <port>` once every port is bound, which is *before* the groups
have elected - a client's first call may be refused for a moment, and is retried - and
`STOPPED` once it has stopped, which happens on a `SIGTERM`, a `Ctrl-C`, or the line
`stop` on stdin.  The last of those is how a program that started a node stops it on
Windows, where one process cannot send another a signal.  With `--data-dir` each group
keeps its log under that directory, so a node restarts as the same node; without one, a
restart is a new node.

Each of a node's group ports answers two kinds of caller on the one server: the Raft
traffic its own members send it, and - for the two groups that are not a shard - the one
question a client outside the cluster has.  `proto/groups.proto` is that contract, pinned
before anything spoke it by `tests/test_client_proto.py`: the routing table read whole, and
the clock asked for one timestamp or a run of them.  `oxidedb/client/remote_group_client.py`
is the client side of it and `MetadataServicer` in `metadata/service.py` and `TSOServicer` in
`tso/tso.py` serve it, so a program outside the cluster can use the six shard primitives,
read the table and take a timestamp, and route by the table it read: a client holding one
needs no cluster object at all, which is what `oxidedb/cli.py --server` is - that same
client with a shell in front of it.  What no client can do yet is read from a follower;
see Known gaps.

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
  the split to the table, and re-ranges the servers locally before thawing the source -
  and a split that dies before its proposal is finished on the next start.  A move of a
  shard to another group is wired end to end too: `move_shard` freezes the source, copies
  its rows into a group on the nodes the caller named - at the timestamps they already had,
  so a snapshot read still finds them - tells the routing table, and then lets the group the
  shard left go, keeping its storage under an orphan name rather than deleting it.  A move
  that dies part way through is finished on the next start, out of the note it left in the
  source shard and the answer the routing table gives.  See
  Known gaps for what is still missing: something that decides to move a shard in the first
  place, and follower reads.

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

297 tests.  `tests/test_durability.py` covers the correctness properties that
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
`tests/test_split_proposal.py` covers the one command that changes which range a
shard answers for.  A split is proposed after the rows have moved, so the group
checks the proposal against the table instead of believing the caller: the split
point has to be strictly inside the shard's range, the new id has to be free, and
neither half may overlap a range another shard owns - and anything refused leaves the
table at the same version, because a half-applied split is a range nobody owns.  A
retry of the split that applied is a success, since a caller cannot tell a lost
response from a lost proposal, while a retry carrying a different replica set is
refused; and every replica is asserted to apply the same two ranges.
`tests/test_split_freeze.py` and `tests/test_split_group.py` are the two things a split
needs before it can be proposed at all.  The first pins the freeze: the source shard
refuses the commands that would add rows to it while a split is copying, still accepts a
commit for a transaction that prewrote before the freeze, waits the proposals that were
admitted before the freeze out rather than reading past them, and is thawed again when the
proposal is refused - because a split whose rows are in a group the table has not been told
about must not take rows for the range it is giving away, and a split that moved nothing
has nothing that could be written outside of.  The second pins the shard it creates: built
through the same `add_shard` as the shards the node started with, in network mode bound to
and listening on the address the table will publish, electing a leader and replicating into
it.  In-process clusters get peer links for it too, which they did not have before - which
is why an in-process cluster had never elected anything.
`tests/test_split_publish.py` is the order that matters: the rows are in the new shard while
the table and the cluster's own range map still say what they said before, and the test
stands inside the proposal to assert exactly that.  A refused proposal leaves the source
frozen and the split remembered, and asking again finishes that same split rather than
starting a second one; a proposal whose response is lost is applied once and copies once.
`tests/test_split_recovery.py` kills the process between the copy and the proposal, which is
the one state nobody else can see: rows in a group the table has not been told about, and a
freeze that died with the process that held it.  The split is written down in the source
shard's own storage before the first row moves, so a cluster that comes back freezes the
shard again, copies only what the new shard is missing - the rows read back out of the
source's state, not out of the note, because a note carrying rows would be a second copy of
the shard taken at some earlier moment - and makes the proposal, which the group recognises
as the split it already applied.  A third test is the control for the note itself: a split
that finished leaves none behind, so the next start has nothing to pick up.
`tests/test_metadata_wiring.py` closes the join: it starts the metadata group beside a
real sharded cluster and asserts that the table a client reads names each shard's
actual leader, at its actual term, at an address that is really listening - the test
opens the socket rather than comparing strings.  A second test stops a leader's node
and watches the table follow the election to the node that took over, at a higher term.
The publisher's own rules are pinned without a cluster: it writes when something moved
and not on a timer, so the table's version stands still across polls; a restart that
re-proposes the same ranges is not a disagreement; a table holding different
ranges is refused rather than overwritten; and a shard a move is in flight for is left to
the move, which is the one writer of its replica set - the cluster's own answer for a moving
shard is the group it is leaving, so a publisher that wrote it would undo a proposal that had
landed.
`tests/test_client_routing.py` is the other end of that join.  A client with a table
routes by it and never looks at the cluster's own nodes - the test turns that into a
failure rather than a convention - and a transaction reads and writes by the same
placement, because a client that read by the table and wrote by scanning the cluster
would be two clients wearing one name.  The ways a placement is kept honest are pinned with
fakes: a lookup hands out the client for the node the table names without inspecting it,
because that node's own belief about leading is the thing in question; a table that names
nobody is read again, but no faster than the publisher could have written an answer - a
range published before its leader is, and a shard that really has no leader, look identical
from the client's side and only one of them is worth waiting out; a refusal is answered by
the address the shard names, and by the other replicas of its set when it names none - one
call each, and no read of the table until they run out; a table read after that is the last
answer rather than the first thing to try, and a second refusal is taken as the answer
rather than retried; and a handle that has stopped working is dropped and
rebuilt while the placement is left alone.  The last
tests are all real:
a three-node cluster, a live metadata group, and a leader whose node is shut down
while the client holds a table naming it.  The read still returns what was committed, and a
write reaches the leader that replaced it without the table being read at all.
`tests/test_client_across_a_move.py` puts a client on the far side of a move's window.  A
read that arrived before the move is answered by the group the shard left, because that group
is still up and its rows are the ones the copy carried away; a write to it is refused with the
shard's words and no leader to follow, since the shard's own code for a frozen range does not
cross the wire; and once the window closes the client asks the addresses it holds - one
timeout each, which is what the walk costs - and reads the table again, which is what finds
the group the shard moved to.
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
in-memory mode starts empty every time, `--data-dir` persists, `--data-dir` and
`--server` together are refused, and `get` reports a missing key with exit code 1.
Its last class starts a real node - `tests/_cluster.py`, so a process - and runs the
CLI against it: a key set by one invocation read by the next, a range read that
crosses both shards, a delete whose neighbour survives, and a `--server` that answers
nothing reported on one line rather than as a traceback.
`tests/test_client_proto.py` pins the wire contract before anything speaks it, and it
is the one file here with no business logic in it: six node-level methods and no
transaction among them, because a `Prewrite` RPC would put the primary-key choice on
the server and a `Set` would put the timestamp there; an `error_code` of exactly the
four cases a caller reacts to differently, so transport failures stay in the gRPC
status; a leader hint that is present for `NOT_LEADER` and absent otherwise; and the
difference between a field that is unset and one that is empty, which is the reason
those fields are `optional` - a key with an empty value and a key with no version are
different answers.
`tests/test_local_node_client.py` is the evidence that the seam does not change an
answer.  `LocalNodeClient` wraps a node the caller already holds, and the test asks the
node and the client the same questions on a real state machine - a committed key, a
locked key, an empty snapshot, and a node that never led - and requires the answers to
match field for field, refusals and "no value" included.  Then it drives a cross-shard
transaction with one client per shard, having first asserted that the two keys really
are in different shards *and* in different Raft groups, so that a routing change cannot
quietly turn it back into a single-shard test that still passes.
`LocalNodeClientFactory` is the same claim about where clients come from.  It is keyed by
a shard *and* a node id, because one server holds a node in every shard's Raft group and an
id on its own would name the wrong one - the test asks for the same id in two shards and
requires two clients - and it answers `None` rather than raising for a node it has no way
to reach, which is a placement the table may well name and a client may never have been
given a handle on.
`tests/test_client_boundary.py` is what keeps all of that true.  It scans the package's
source for the three ways a caller could skip the seam - a state machine, a storage, or a
node's own `state` - and fails if one appears in a file that is not the shard itself or a
component running inside the cluster, with the allowlist written out and each entry saying
what it is doing there.  It is a gatekeeper and not a behaviour test, in the same spirit as
`tests/test_error_codes.py`: a new reach-in does not fail any test while the code it
reaches into is in the same process, which is exactly why one file here is about the shape
of the code rather than what it answers.  Three of its tests are controls - a source line
that reaches in is built in memory and the scan has to point at it - because a scan that
never fails is not evidence of anything.
`tests/test_client_servicer.py` and `tests/test_remote_node_client.py` are the wire
itself.  The first drives the servicer through a bare stub - the six primitives, a
lock and a write record field for field, a range read stopped by a lock, a node that
has stopped leading refusing in the response rather than in the gRPC status, and a
hint present when the node has one and absent when it does not - and the second asks
a client across a wire and a client in this process the same questions, requiring the
answers to match, refusal codes and leader hints included.  What the hint buys is the
point of it: a retry follows a refusal straight to the address it names, and the test
asserts the routing cache was never asked for a new table, because a retry that reads
the table is a retry that waited for the publisher.
`tests/test_group_clients.py` is those same two groups over a real socket, against launcher
processes rather than objects.  The table arrives whole, with every address in it opened
rather than compared as a string; a client whose only seed is a member that does not lead
still reads it, because the refusal it gets names the leader - that refusal is read with a
bare stub first, so the hint is not taken on faith - and a seed that answers nothing is
walked past, until the only seed that answers nothing at all is `NodeUnreachable` rather
than a refusal.  The clock is asked the same way: timestamps that only go up, a run that is
used up replaced by one above it (the boundary an off-by-one would hand out twice), the
single-timestamp call allocating exactly one, and a batch of none refused instead of
rounded up to one.  The leader column is where the two cluster sizes part company: with
three nodes and four shards every shard has to be named, and the node that names a shard is
the one leading that shard rather than the one leading the table's group - so a publisher
that could not reach a group it does not lead would leave the column empty - while against
one node the answer is decided rather than raced, because there the only node it could name
is the one that answered.  The table's own writes are read over the same socket: the
placement the group already holds, written back through a member that does not lead it; a
command the table refuses; and a report it takes, which is what the leader column is made of.

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
* **Snapshot reads are reachable from the state machine, the node and a client, but no
  user-facing entry point passes a timestamp.**  `node.get(key, ts)`,
  `node.scan(start, end, ts)`, `coordinator.read(txn_id, key)` and the six primitives over
  a wire all take one, but nothing outside the tests passes one - the CLI and the examples
  drive a local `Database` - so a user still gets the newest version.  A read whose
  snapshot is hidden by a lock is resolved by asking
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
* **A node can be a process, but a client outside one cannot do everything yet.**
  The contract is six node-level primitives in
  `proto/client.proto`, with a four-value `error_code` and a leader hint.
  `oxidedb/client/node_client.py` is the protocol a caller meets a shard through,
  `LocalNodeClient` implements it over a node in this process and `RemoteNodeClient` over
  a channel, `raft/client_servicer.py` serves it from the same port a shard's Raft traffic
  arrives on, and `LocalNodeClientFactory` and `RemoteNodeClientFactory` are where a caller
  gets one.  Every caller that is not the shard itself now holds one of those and nothing
  else: the coordinator, the lock resolver, the SQL executor and the routing cache reach a
  shard through a `NodeClient`, `client/routing.py` is the one place that turns a placement
  into a handle - from the routing table when the client has a table, taking the address
  from the same placement as the node id, and from the cluster's own leader lookup when it
  does not, which is the in-process case - and `tests/test_client_boundary.py` is what
  keeps it that way: no caller outside `raft/` reads a node's `state`, its state machine or
  its storage.
  A refusal is what a client acts on.  The node answering it names where the leader is when
  it has heard from one - `MemoryRaftNode.leader_id` is set from an AppendEntries or an
  InstallSnapshot for the current term and dropped wherever the node steps out of that term
  - so following an election costs one hop to a new address instead of a read of the table,
  and `ShardLeaders` consumes that hint once, for the retry that follows it.  A refusal that
  names nowhere is answered by the shard's other replicas, which the table publishes along
  with the leader: the client walks them one at a time, remembering every address it has
  been to, and reads the table only when the set is spent - which is what a leader that has
  been killed costs a client over a wire, because the node that would have named its
  successor is exactly the node that is gone.  A node that does not answer at all is
  `NodeUnreachable`, which is not a refusal and is walked the same way, because a table that
  still names a node which is gone is exactly the case it is for.  A walk ends when a shard
  answers it (`ShardLeaders.answered`), so the next doubt is a new walk rather than a queue
  one session has already drained.
  `oxidedb/launcher.py` is the other end: it runs one node - its shards, the routing
  table's group and the timestamp group, each on ports of its own - and publishes its
  placement, so a client in another process can reach a shard and use all six
  primitives, read the routing table and take a timestamp: both groups serve their one
  client question on their own port - `MetadataServicer` and `TSOServicer`, registered
  beside the Raft service they already answered - and `client/remote_group_client.py` is
  the client side of it, walking the seed addresses it was given, following a refusal that
  names the leader and stepping over an address that answers nothing.  A client outside the
  cluster routes by the table it read - a lookup that resolves a shard's leader there needs no
  cluster object to fall back on, and a client in another process has none - and
  `oxidedb/cli.py --server` is that client with a shell in front of it: the same four
  commands, over channels, through the same routing table.
  `tests/_cluster.py` starts real `python -m oxidedb.launcher` processes, waits for their
  `READY` line rather than for a number of seconds, and stops each one by writing `stop` to
  its stdin - failing if a node goes without saying `STOPPED` - and
  `tests/test_client_over_processes.py` writes, reads and range-reads through that socket,
  including the bytes two objects in one interpreter never have to serialise: a key whose
  first byte is 0x80, an empty value, a tombstone, a megabyte value.  `tests/test_cli.py` is
  the same commands against the same kind of node, with a person's shell in the middle.
  What is still a test with the cluster in the test process is everything above one shard:
  the cross-shard transaction, whose coordinator is built over the cluster's own nodes
  because the test is the cluster's own process.
  The old key/value `ClientService` and the `OxideDBClient` written against it are gone:
  `Set` cannot be answered correctly by a server, because the timestamp a write carries
  has to come from the client's own TSO batch for a transaction's prewrite and commit to
  line up.
* **The publisher reaches the table's group from any node that serves it, and not from a
  node that does not.**  A proposal is not forwarded by the service: a member that does not
  lead answers `ERR_NOT_LEADER` and names the leader, and the caller is the one that moves.
  The launcher hands the publisher that walk - a `RemoteMetadataClient` over the group's own
  port - so a node that leads a shard gets its report in whether or not it leads the table's
  group.  Before that it held the in-process `MetadataClient`, which could only reach a leader
  that was this node, and the leader column of a cluster of processes was written by the one
  node that happened to lead the group while the shards were led by the others.  What is
  still open is a node that serves no member of the group at all: the publisher starts on the
  nodes that hold one (`ClusterNode._start_background`), so in a cluster wider than
  `metadata_group_size` a shard led by one of the others is published with its range, its
  replica set and its addresses and with no leader.  Closing that is starting a publisher on
  every node, for which no test here would be evidence - the clusters in `tests/` are exactly
  as wide as the metadata group.  What `tests/test_group_clients.py` pins is the rest: every
  shard of a four-shard, three-node cluster is named, each name is a node of that shard's
  replica set at an address something answers at, a one-node cluster names itself for every
  shard, and a client outside the cluster can write the table through a member that does not
  lead it.
* **Sharding is experimental, and nothing moves a shard by itself: no rebalancing, no
  follower read.**  Every component routes
  through one range lookup (`shard/router.py`), and the table that lookup needs has an
  owner: `metadata/service.py` is a Raft group holding each shard's range, its replica
  set and the leaders that reported themselves, and `MetadataClient` reads it with the
  same ReadIndex path as any other read (design.md, section 7).  The cluster writes it
  too: `ShardedRaftCluster` starts a `MetadataPublisher` that publishes the ranges once,
  each shard's replica set and addresses once, and a leader report whenever a shard's
  leader or its term moves, so the table names the node that actually won the election -
  and stops rather than overwrite a map it cannot account for.  The maps it can account
  for are the one it routes by and the one its own split in flight will produce, which it
  is asked for rather than left to guess: a split reaches the group from the shard's own
  thread, so the publisher's first pass can find the table a step ahead of the cluster.
  A client reads that table and routes by it (`metadata/cache.py`), and what a lookup
  hands out is a client for the node it names rather than the node: it does not ask that
  node whether it still leads, because the node it reached is the one whose belief is in
  question.  A refusal is answered out of the same table before the table is read again:
  `replica_addresses` hands out the set of nodes that serve the shard, leader first, and
  `ask_shard` walks the ones it has not been to, which is what makes an election visible to
  a client whose publisher has not written it down yet.  What sends a client back to the
  table is that walk running out (`refresh_for_shard`, called by the caller that met the
  refusal) or a table that names nobody while the read being held is older than the
  publisher's own poll interval - a range whose leader report has not been published yet and
  a shard that has no leader are the same thing to a client, and only one of them is worth
  waiting out.
  A split is wired end to end.  `split_shard` freezes the range, waits out the writes that
  were admitted before the freeze, refuses rather than copy under a lock that may be an
  unapplied commit, moves the rows above the split point into the new shard's group as the
  versions they already were, proposes `SPLIT` to the metadata group, and applies the new
  range map locally before thawing the source.  The group checks the proposal against its
  own table rather than believing the caller - the split point strictly inside the range,
  the new id free, neither half overlapping another shard - and answers a retry of a split
  it already applied with success, because a caller cannot tell a lost response from a lost
  proposal.  The intent is written into the source shard's own storage before the first row
  moves, so a cluster that dies between the copy and the proposal comes back, freezes the
  shard again, copies only what the new shard is missing, and proposes the split.
  What is still missing around it: it does not coordinate with a write that resolved to the
  old shard just before the range moved, so the copy can end up behind such a write; the
  copies left in the old shard are never reclaimed; nothing chooses split points, and nothing
  chooses where a shard should live; and there are no follower reads - every read goes to the
  leader.
  A move is wired end to end.  `move_shard(shard_id, target_nodes)` freezes the source -
  refusing its writes with `ERR_MIGRATING`, which is deliberately not the split's answer,
  because a caller told a shard is moving has to look the range up again - reads its rows at
  that one moment, builds a group on the nodes the caller named, copies the rows into it as
  the versions they already were, and tells the table.  Nothing is copied out of a shard
  that can still take a row: the freeze comes first, and the move is written into the source
  shard's own storage before the first row moves, so a process that dies part way through
  the copy comes back knowing which shard was moving and where it was going.  The target set is the caller's to name
  and has to be disjoint from the one serving the shard: a node cannot hold two groups for
  one shard, and a set that overlapped would need a member changed in place, which is not
  something this project's Raft does.  Until the proposal lands the table keeps naming the
  group the shard is leaving, and every question the cluster answers about *the* shard -
  `shard_replica_ids`, `shard_addresses`, `shard_leader`, `get_leader_for_key` - is answered
  with that group (`_serving_nodes`), so a group nothing routes to yet is not published -
  which is also why the publisher leaves a shard a move is in flight for alone: the table's
  entry for it is the move's to write, and a first pass that wrote the cluster's own answer
  would undo a proposal that had landed.  A cluster that comes back finds the note, freezes
  the shard again - the freeze is a local fact, and it died with the process that set it - and
  finishes the move itself: `recover_migrations`, which the cluster calls as it starts and
  which a caller may call again, because every step of it is a no-op the second time.  The
  routing table is what says how much of the move is left, and its three answers are the three
  below read backwards: the set the move was going to means the proposal landed and only the
  half after it is left, the set it was leaving means the copy is where the move stopped and
  the proposal is still owed, and anything else means somebody else has moved the shard -
  which is not a question this cluster can answer for itself, so it stays frozen and says so.
  What the cluster can answer it with is the whole of what happens to the freeze next.  It
  landed - the table names the new group, this cluster's own answers follow it, and the group
  the shard left is closed and put aside.  It
  was refused, which is final, because the machine would answer the same command the same
  way: the shard goes back to serving, because it is still the group the table names and a
  refusal is not a reason to leave a range unserved, and the group built to receive the rows
  is closed and put aside without being deleted.  Or nothing answered, which is not final,
  because the proposal may have landed: the shard stays frozen with the move written down,
  and asking again with the same target set is how a caller finds out which of the two it
  was.  `_propose_move` tells the table the shard's new replica set, reading the set being
  replaced out of the table on every attempt rather than proposing this process's own view
  of it: the machine refuses a move whose expectation of the current set is wrong (code 14),
  so a guessed one is refused every time two moves were computed from one table.  `_commit_move` then makes the
  cluster's own answers follow the table, keeps the group the shard left answering for a
  fixed window - `MIGRATION_DRAIN_SECONDS`, because a client routes by a table it cached and
  the node it was sent to a moment ago is one it may still ask - closes that group on every
  node that is not serving the shard now, drops the move's note while the storage holding it
  is still open, and renames the storage it leaves behind to `orphan-shard-<id>-<when>`
  rather than deleting it: a table that has to be put back - after a bug in the machine that
  holds it, or an operator - finds the rows still on disk, and a leaked directory costs disk
  where a lost range costs the data.  What a move still does not have: nothing decides that
  a shard should move - there is no rebalancer here and no node reports its load - so the
  target set is a caller's to choose.  What the window is for has a client on both sides of
  it now: a read that arrived before the move is answered by the group the shard left, a
  write to it is refused, and a client whose table is old is walked to the group the shard
  moved to once the window is over.  See
  `tests/test_move_proposal.py` for the window's two ends and
  `tests/test_client_across_a_move.py` for a client between them, and
  `tests/test_migration_recovery.py` for a move killed in each of the three states a restart
  can find it in, and the half of the move each one leaves.
  A client can reach a shard it was never handed a handle on, by the address the table names
  for it (`client/remote_node_client.py`), and a
  cluster for that client to connect to is something the repository starts on its own
  (`launcher.py`, and `tests/_cluster.py` over it), which is how one shard's client
  service is tested across processes, and the CLI reaches a cluster the same way
  (`--server`).  What is left of this is narrower than it was: a client with a table routes
  by it - the coordinator and the lock resolver ask `ShardLeaders` which shard a key belongs
  to, the same lookup that finds that shard's leader, so nothing above the client needs a
  cluster object at all - but a client *without* one still routes by the cluster's own
  nodes, and the cross-shard transaction test is still driven in the cluster's own process.
  What the CLI cannot do is read from a follower; every read goes to a leader.  What it does
  that no other client here does is wait for a cluster that has only just started: a
  `--server` command asks first - for a timestamp, and for a table naming every shard it was
  started with and a leader for each - and sends the command only once both answer, so a run
  in the first moments of a cluster's life is slow rather than failed (`READY` is a promise
  about ports, not about elections or the publisher's first pass).  `--wait SECONDS` is the
  deadline: five by default, which is ten of the publisher's poll intervals, and `0` asks
  once and reports the reason it could not.  The wait is the CLI's rather than the client's
  because it is a property of a process with one shot at its command: a client answers or
  raises with its reason and a caller holding state decides what to do next - and a command
  that retried itself would be overruling, on every caller's behalf, the answer the design
  notes give for a write refused while its range is moving - read the table again, since the
  range is not that shard's any more.  Every test that routes
  now goes through this wait rather than a fixture's own loop (`tests/test_cli.py`).
* **A refusal from a state machine loses its own code on the way to a client.**  The
  machine answers a refused command with a code of its own - a split point outside the
  range is 8, a move whose expectation of the replica set does not hold is 14 - and the
  wire has one code for all of them (`ERR_APPLY_ERROR`), with the detail left in the
  message.  A caller that wants to decide whether to retry from the code rather than from
  prose cannot, and closing that means widening the `ClientService` contract rather than
  adding a line, which is why it is written down rather than done: the callers that exist
  today read the message, and `tests/test_group_clients.py` pins what the wire answers.
  A move's proposal is the first caller with a reason to mind.  It reads nothing but the
  code - a refusal is final, whatever it said, and only silence is retried - so it is not
  the message that decides anything today.  A caller that wanted to re-read the table after
  a 14, and to walk away from a 15, would be telling them apart by prose.
* **The remote metadata client's two placement-changing writes are smoke-tested, not
  driven.**  `RemoteMetadataClient.split_shard` was missing outright until a move needed
  its twin, and the failure that would have caused is an `AttributeError` inside
  `ShardedRaftCluster._publish_split` - unseen because every cluster that splits a shard in
  `tests/` holds the in-process client.  Both writes are now tested as far as the wire:
  that the command arrives as itself and is checked by the machine
  (`tests/test_group_clients.py`).  What is still owed is a cluster driven end to end
  through the remote client.
* **A write from a client that is still routing to the old group is refused in words, and
  the words are all the client gets.**  A move's window has a client on the other side of it
  now (`tests/test_client_across_a_move.py`): a read that arrived before the move is answered
  by the group the shard left, a write to it is refused, and once the window closes the client
  walks the addresses it holds - one timeout each - and reads the table again, which is what
  finds the group the shard moved to.  What the client is to do with the refused write is
  settled: read the table again, because the group the shard moved to already owns the
  range.  What is not there is the carrier - the shard's own code for it (`ERR_MIGRATING`)
  does not cross the wire, so a caller is told `REFUSED`, with the shard's prose and no
  leader to follow, and `ask_shard` does not walk on it.  Closing that wants the same
  widening the gap above about a refusal's own code is waiting for: a code on the wire for a
  range that is frozen, beside the one for a lock and the one for a lost leadership.  The two
  shapes it could take, and why the choice waits for that widening rather than being made
  here, are written out as an open decision in `docs/design.md`, section 7.
* **A shard's state machine is built without being told which shard it is.**  The factory
  `ShardServer` calls takes no argument (`launcher.py`'s `_state_machine` is handed nothing),
  so a state machine that opened storage of its own - one file per shard - cannot be written
  against that contract, and what `--data-dir` keeps is each group's log, its metadata and
  its snapshots rather than a state machine's own memory.  A restarted shard comes back by
  replaying (or restoring) what its log holds, which is why this is a constraint on the
  factory's signature and not a hole in durability; widening it is a change to `ShardServer`
  and to every caller that builds one.
* **The SQL layer is minimal.**  `SELECT` and `INSERT` only; no schema, types,
  multi-row insert, `AND`/`OR`, `UPDATE`, `DELETE`, joins, or secondary indexes.
* **No multi-version garbage collection.**  Old versions are never reclaimed.
* **The generated protobuf bindings are checked in and pinned by nothing.**  The
  `*_pb2.py` files carry the toolchain that produced them in their header - protobuf
  7.35.0 and grpcio 1.82.1 - and the `*_pb2_grpc.py` files need one hand edit the
  generator does not make: `from . import x_pb2`, so that the module is importable as
  part of the package rather than as a top-level module.  Nothing fails if someone
  regenerates them with another toolchain until the version stamp is *newer* than the
  installed runtime, and a diff of a regenerated file is unreadable.  A test that pins the
  four files' sha256 is owed.
* **A delete is a tombstone written outside the transaction path.**  What the CLI's
  `delete` sends is the shard's own `DELETE` command - one command to the leader, stamped
  from the same clock the transactions take their timestamps from, which is what puts the
  tombstone in the same order as the commits around it, so a reader at or after it sees the
  key as absent.  What it does not do is take a lock, because a transaction's write is a
  value and a tombstone is not one: a delete that races a transaction on the same key can be
  overwritten by that transaction's commit, where a delete written as an intent would have
  been ordered with it.  Writing it as one means carrying "this version is a tombstone" in
  the lock record and over the wire, which is a change to `proto/client.proto` and to the
  state machine rather than to the command.
* **A multi-key commit is atomic only on the Percolator path.**
  `oxidedb/transaction/local.py` - the embedded `Database` and the CLI - has no
  locks and no primary key: it writes every key of a transaction with one shared
  `commit_ts`, so a crash between two of those writes, or a reader arriving
  mid-commit, can observe part of a transaction.  See `docs/design.md`.

## Layout

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
docs/            design notes and posts
  design.md      why the keyspace, snapshot, 2PC and read path are shaped this way
  blog/          the ReadIndex story: a read path that passed every test while wrong
proto/           gRPC service definitions: raft.proto, client.proto's six node-level
                 primitives (served by raft/client_servicer.py), and groups.proto's
                 routing-table and timestamp services (served by metadata/service.py and
                 tso/tso.py, asked by client/remote_group_client.py)
tests/           pytest suite
