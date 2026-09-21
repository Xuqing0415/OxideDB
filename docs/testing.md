# Tests

> Moved out of the README, which links here instead.  The text is the README's, with the
> references between these notes updated to name the file each one now lives in, and each
> heading moved up a level.

```
pip install -e ".[test]"
python -m pytest tests -q
```

The suite takes 133.70s measured, down from 557.79s, and none of that is a test that got
weaker: fixed sleeps became waits on an observable condition (`tests/_wait.py`), and the
`LockCleaner` now wakes on an event instead of a shutdown being joined out of a `sleep` of
its poll interval.  That second one was a product bug, and by then it was the largest single
thing in the suite - five seconds for every stop of a sharded cluster, almost half of the
whole run - which is the reason to measure a slow suite rather than to assume it is the
tests: the slowest thing in it can be the product.

415 tests.  `tests/test_durability.py` covers the correctness properties that
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
five cases a caller reacts to differently, so transport failures stay in the gRPC
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
  `tests/_ports.py`.  A node's ports are one block of `block_width()` - its shard
  segment and the two group ports above it - so node bases must be a whole block
  apart or two nodes claim the same port;
* the network tests wait for the condition they care about (`tests/_wait.py`)
  instead of sleeping a fixed number of seconds.  A fixed sleep is a race: the
  suite starts dozens of local gRPC servers, and a loaded machine can spend
  longer than the sleep just electing a leader.

## Running part of it

The suite is one pytest run, so selecting part of it is pytest's own selection.  The
two forms below that need a name - the keyword and the node id - were run against
this suite to check that they select what they say:

```
python -m pytest tests -q                       # the whole suite, quiet
python -m pytest tests/test_mvcc.py -q          # one file
python -m pytest tests -k mvcc                  # a keyword, across every file
python -m pytest tests/test_mvcc.py::TestMVCCStorage::test_set_and_get
python -m pytest tests -x                       # stop at the first failure
python -m pytest tests --lf                     # only what failed last time
python -m pytest tests --collect-only -q        # count them, and run none
```

The node id in the fourth line is `file.py::Class::test`, and `--collect-only -q`
is how to get one: it prints exactly that, one per test, without running any.

## Reading a leader

`ShardedRaftCluster.get_leader_for_key` answers with the node whose state is `LEADER`
for the shard that owns the key, so it answers `None` whenever no node is leading: before
the first election, and again for a moment while a leadership change is in flight
(measured here, a ~0.2s window after the leader's server stopped, with a leader reported
again afterwards).  A test that reads it is reading a snapshot, and a snapshot taken after
an earlier wait is not the same thing as waiting for what it reads - the run that put this
section here waited for the shards' leaders, then waited for the TSO, then read a leader
and got `None`.  `tests/_wait.py` has `wait_for_keys_leader` for "every one of these keys
has a leader" and `wait_for_leader_of_key` for the leader itself; use those rather than
subscripting the call, which raises `TypeError` instead of saying what happened.

## A client pinned to a leader

The TSO group is the same shape with a different refusal.  A `TSOClient` is pinned to
the node that led when it was asked for, and once that node stops leading the client
*raises* - `RuntimeError: TSO node is not leader` - rather than answering with an
address the way a shard's client does.  Measured by taking a client and stopping the
node it names: the group had a new leader 0.20s later and the held client still raised.
`_timestamp_from_whichever_leads` in `tests/test_local_node_client.py` is that group's
version of the same move - take a client again, follow that one failure, and let every
other one out - and it is what the timestamps in that file are read through.
