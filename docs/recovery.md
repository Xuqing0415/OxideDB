# Recovery for a shard that was moving when the process stopped

One shard leaves a note on every replica that says what it was in the middle of, and
one of the two ways this repository starts a cluster reads it.  This is the design for
the other one: what the recovery needs from the thing it runs inside, so that a node in
a process of its own can pick up a split or a move the way the in-process cluster does.

It is written before the code because the shape of that interface decides whether the
second implementation can exist at all.  The first attempt at it - "give the process
path a cluster object" - cannot be built: the first thing recovery does is freeze the
shard, and a process cannot freeze a group it does not hold.

## 1. The problem

`ShardedRaftCluster` recovers and a node started as a process does not.  `recover_splits`
and `recover_migrations` (`oxidedb/raft/shard_server.py`) are called from its `start` and
its `start_network`, they read the notes through `_load_pending_splits` and
`_load_pending_migrations`, and they are written against that cluster's own `_migrations`,
`_pending_splits`, `_placed_shards`, `_orphan_dirs` and `_shard_servers`.  `ClusterNode`
(`oxidedb/launcher.py`), which is what runs when a node is a process, holds one node of
each group and none of those five.

What the gap costs, in the order it bites:

* A process killed between a move's copy and its proposal comes back with the note
  unread and the source shard *unfrozen*.  A freeze is `_writes_frozen`, a flag in
  memory (`oxidedb/raft/node.py`), and nothing puts it back on the way up.  The group
  the routing table has already given away takes new rows again: rows nothing will
  carry across, on a range served by one group while its rows are in another.
* A split killed before its proposal has the same hole without the double write.  A
  split copies rather than moves, so the source still holds its rows; what is lost is
  the split itself - the new group is on disk, its rows are there, and the routing
  table never hears about it.
* The tests that cover this - `tests/test_migration_recovery.py` and
  `tests/test_split_recovery.py` - both drive `ShardedRaftCluster`, so nothing in the
  suite covers the path a deployed node takes.  "Killed at every step, restarted,
  converges" has only ever been met by the cluster object.

Two qualifications, because they decide how urgent this is rather than whether it is
real:

* **Nothing in a process can start a move or a split yet.**  Both are methods on
  `ShardedRaftCluster` (`move_shard`, `split_shard`), and the only production callers of
  the routing table's own `move_shard` and `split_shard` are the two proposals inside
  that same class.  A process-mode cluster cannot reach the state this document is
  about, so the gap is latent rather than live.
* That is the argument for doing it now rather than when it starts to hurt: the feature
  that lets a move be started from outside a test is the feature that turns this gap
  into data that has gone somewhere nobody can find it.  The README now says which of
  the two start-up paths recovers, so a reader is not told a guarantee the product does
  not keep.

**A split's range has to move in one call, and that is the router's doing rather than a
preference about names.**  `locate` (`oxidedb/shard/router.py`) routes a key that falls
outside every range to shard 0 rather than refusing it, so a split applied in two steps -
the new group built, then the range handed over, or the same two the other way round - has
a window between the calls where a key in the range being handed over is outside every
range and is routed back to the shard that just gave it away.  That is a silent wrong
answer rather than a failure, and it is why the interface in section 2 has one
`apply_split_locally` and why it is asked of whichever side is holding the table.

## 2. The interface

**The interface is per process, and that is not a preference.**  `_remember_split`
writes a note on *every replica* of the shard it is about, and says why: "the note says
which shard is being split, and any replica holding that shard can be the one that finds
it".  The design already assumes that recovery is done by the replica that comes back,
not by a coordinator that looks at the group.  A `freeze_the_group(shard_id)` call -
the shape a cluster-object interface wants - cannot be expressed from one process at all.

So the unit of recovery is *the replicas this process holds*, and the invariant is the
union: every process freezes what it has, reads the notes on what it has, and the set
that ends up frozen is the whole group.  A node that never restarted never lost its
freeze, and a node that did puts it back on the way up, which is what makes "freeze on
load" enough rather than a race.

```python
@dataclass
class Note:
    """What a shard left behind, whichever of the two it is."""
    shard_id: int
    kind: str                                  # "split" or "migrate": the two prefixes
    split_key: Optional[bytes] = None          # split only
    new_shard_id: Optional[int] = None         # split only
    source_nodes: List[int] = field(default_factory=list)   # move only
    target_nodes: List[int] = field(default_factory=list)   # move only


class RecoveryView(Protocol):
    """What a recovery needs from wherever it is running."""

    def shard_ids(self) -> List[int]: ...
    def ranges(self) -> RangeMap: ...
    def note(self, shard_id: int) -> Optional[Note]: ...
    def forget_note(self, shard_id: int) -> None: ...
    def freeze(self, shard_id: int, reason: str) -> None: ...
    def unfreeze(self, shard_id: int) -> None: ...
    def leader_client(self, shard_id: int) -> Optional[NodeClient]: ...
    def serving_nodes(self, shard_id: int) -> List[int]: ...
    def ensure_serving(self, shard_id: int, nodes: List[int]) -> List[int]: ...
    def apply_split_locally(self, shard_id: int, split_key: bytes,
                            new_shard_id: int) -> None: ...
    def metadata(self): ...
```

* **`shard_ids()`** - the shards this process looks after: the keys of its range map,
  which is the set the notes are looked for in, and never the table - a process does not
  learn its shards from the table (section 5).  Deliberately not "the shards it holds a
  group for": that is a different question, an implementation answers it from its groups
  rather than from its range map, and it is the one `ensure_serving` is there to change.
  A read.
* **`ranges()`** - the ranges this process believes in, the source shard's included.  A
  split needs the range it is splitting, a move needs the range it is copying.  A read.
* **`note(shard_id)`** - the split or the move a replica this process holds wrote down, as
  it is on disk, or None.  One call for both kinds: which one it is is the note's business
  and not the caller's.  A read.
* **`forget_note(shard_id)`** - drop that note on every replica this process holds.  A
  replica whose storage has already been closed cannot be reached, which is why that is a
  no-op here rather than an error.  Idempotent.
* **`freeze(shard_id, reason)`** - refuse commands that add rows on the replicas this
  process holds, with the reason the refusal quotes.  Commits and rollbacks still go
  through: they are the end of a transaction that prewrote before this.  Idempotent.
* **`unfreeze(shard_id)`** - the reverse, for the source of a split that is done and for a
  move the table refused.  Idempotent.
* **`leader_client(shard_id)`** - a `NodeClient` for whatever leads the shard, wherever
  that is, or None while nobody does.  This is the only way the recovery moves rows: it
  reads the source and proposes into the target through it, and in a process the client it
  gets is a socket.  A read.
* **`serving_nodes(shard_id)`** - the replica set the routing table names, or - with no
  table, as in an in-process test - this cluster's own answer.  It is how a cluster that
  comes back learns how far a move got, so it is a read of the table and of nothing local.
* **`ensure_serving(shard_id, nodes)`** - make what this process holds match that set:
  build its member of the group if it is in the set and holds nothing, close its member if
  it holds one and is not.  It also becomes this cluster's own answer for who serves the
  shard, which is what the publisher follows: the replica set and the addresses it
  publishes are read from the group this process holds, so closing one stops both from
  naming this process - see the last constraint in section 5.  What it returns is the nodes
  whose group it closed, which is how a caller that ran twice tells a call that did
  something from one that found the work already done.  What a close leaves on disk - the
  group's storage, put aside under an orphan name rather than deleted - is the cluster
  side's half of this and is not written on the process side yet.  Idempotent.
* **`apply_split_locally(shard_id, split_key, new_shard_id)`** - the range map this process
  routes by becomes the new one, so that the publisher sees a cluster that agrees with the
  table rather than one mid-split.  Idempotent.
* **`metadata()`** - the routing table's group as a client, for reading placements and for
  the two writes that finish a split or a move.  Deliberately not named as a type, exactly
  as in `MetadataPublisher`: the in-process `MetadataClient` and the `RemoteMetadataClient`
  both answer `table`, `split_shard`, `move_shard` and `list_shards`, and which one a node
  holds is the difference between a recovery that only works where it leads the group and
  one that works anywhere.

Everything else the recovery does - reading the source's rows, deciding which row the
target is missing, building the command, retrying a proposal whose answer never came -
is written once, in the recovery, against `NodeClient` and `metadata()`.  That is why
the sketch this document grew out of had two calls that are not here: `copy_rows(...)`
and `propose(...)` are not things the two sides do differently, they are the shared
implementation reaching through `leader_client`.

## 3. Where the thirteen primitives went

**The rule that decides what can be in the interface at all: a `RecoveryView` method's
arguments and its answer have to be values that cross a process boundary.**  A method
that returns a node object - `_shard_leader_node`, `_wait_for_shard_leader`, `_leader_on`
- cannot be implemented on the process side, and not because it would be awkward there: a
process holds its own `ShardServer` and the shard's leader may be inside another one, so
there is no object to return.  That is a type-level impossibility rather than a
preference, and it is what turned `_shard_leader_node` into `leader_client(shard_id)` - a
handle on whatever leads the shard, wherever it is.  A method that names
`MemoryRaftNode` is not an interface method, and that check comes before any question
about how wide the interface is.

The differential was thirteen primitives only `ShardedRaftCluster` has, nine both sides
already have, and three neither has.  The nine go straight in (`shard_ids`, `range_map`,
`metadata_client`, `get_shard_server`, `shard_replica_ids`, `shard_addresses`,
`shard_leader`, and, under both, `ShardServer.add_shard` / `get_shard_node` /
`shutdown_shard` and `RaftStorage.save_admin` / `load_admin` / `delete_admin`).  The
thirteen and the three land like this:

* `_shard_nodes`, `_nodes_on` - internal: "the nodes this process holds for the shard" is
  what a view answers, and it is a helper here rather than a call.
* `_freeze_shard`, `_unfreeze_shard` - `freeze` and `unfreeze`.
* `_drain_shard` - internal: waiting out the writes admitted before the freeze belongs to
  the recovery's own protocol, not to the view.
* `_serving_nodes` - `serving_nodes`, under the interface's name.  `_placed_shards` is what
  it reads, and it is written where the routing table is: by `_commit_move`, when the table
  has agreed, and never by `ensure_serving`.
* `_ensure_shard`, `_retire_source` - `ensure_serving`, which is one call for both
  directions.  `_close_group_on` is what it closes through.  `_ensure_group_on` is *not*
  part of it, and the reason is the one asymmetry between the two implementations: that
  call builds a group a move is still copying into, so it closes nothing and takes the
  target nodes as its members while the source is still the group the table names.  A
  process is never in that state, because it holds one of the two groups rather than both.
* `_rename_storage`, `orphan_dirs` - internal to `ensure_serving`: putting storage aside is
  what closing a group means, and remembering what was set aside is the view's business.
* `_leader_on`, `_wait_for_leader_on`, `_wait_for_shard_leader`, `_shard_leader_node` -
  `leader_client`, with the waiting inside it and a deadline on that wait.  The test
  that wait makes is one the client service already has a call for:
  `has_committed_in_its_own_term` and `NodeClient.follower_read_index` are the same
  question - may this leader answer a linearizable read yet - because `_read_index`
  answers it by collecting a quorum of acknowledgements of the current term, which is
  what the node's own flag reports.  So the wait is a retry of a call that exists rather
  than a new one.
* `_rows_above`, `_committed_rows`, `_locks_in_range` - internal, over
  `leader_client(shard_id).scan(...)`.
* `_move_row`, `_copy_rows` - internal, over `leader_client(...).propose(...)` and
  `leader_client(...).get_write_record(...)`.
* `_addresses_on` - not needed by the recovery: the addresses a move proposes are the ones
  the nodes it is proposing to serve at, and whoever holds those nodes works them out.
* `_finish_split`, `_publish_split`, `_copy_what_is_missing` - internal to the recovery.
  `_apply_split_locally` is the exception: it is the view's, under the name
  `apply_split_locally`.
* `_finish_move`, `_propose_move`, `_commit_move`, `_abort_move` - internal to the
  recovery.
* `split_error`, `migration_error`, `pending_splits`, `migration_state`, `migrations` - the
  recovery's own accessors, for tests and for an operator.

The three that exist nowhere yet are the process-side answers, and they are section 5.

## 4. The cluster side

`ShardedRaftCluster` keeps its public surface and gains the interface underneath it.
The public methods stay public and keep their meaning; what changes is who they delegate
to.  Nothing here is an implementation plan - the work is moving bodies, not rewriting
them.

Becomes the view (public, one implementation of `RecoveryView`):

* `shard_ids`, `range_map`, `metadata_client` and `get_shard_server` already exist and
  already mean what the interface means.
* `serving_nodes(shard_id)` is `_serving_nodes` under the interface's name.
* `ensure_serving(shard_id, nodes)` is `_retire_source` + `_close_group_on` + a build
  for every node that holds nothing, in the order `_commit_move` does them today.  The
  write to `_placed_shards` stays where it is, at the front of `_commit_move`: that is the
  routing table's placement and it moves when the table does, so it is the one thing this
  call must not touch.
* `freeze` / `unfreeze` are `_freeze_shard` / `_unfreeze_shard` for the whole group.
* `leader_client(shard_id)` is a `ShardLeaders` over the cluster's own nodes with a
  `LocalNodeClientFactory` - which is the same object the coordinator and the resolver
  already ask, so a recovery in process reads its shards the way every other caller
  does.
* `apply_split_locally` is `_apply_split_locally`.
* `metadata()` is `self._metadata_client`.

Stays internal (helpers the recovery calls through the view, or uses itself):

* `_shard_nodes`, `_nodes_on`, `_addresses_on`, `_drain_shard`, `_rename_storage`,
  `_close_group_on`, `_ensure_group_on`, `_shard_leader_node`, `_wait_for_leader_on`,
  `_wait_for_shard_leader`, `_shard_leader_on`, `_load_split_record`,
  `_load_migration_record`, `_remember_*`, `_forget_*`.
* `_finish_split`, `_publish_split`, `_copy_what_is_missing`, `_finish_move`,
  `_propose_move`, `_commit_move`, `_abort_move`: these are the *recovery's* body, not
  the view's.  They move into it, so that the two start-up paths cannot drift into two
  slightly different protocols.
* `_migrations`, `_pending_splits`, `_orphan_dirs`, `_placed_shards` stay where they
  are.  The view is what hides them; nothing outside the cluster's own implementation
  reads them, which is the property the interface is for.

What the public entry points become:

```python
def recover_splits(self) -> List[int]:        # kept: the tests and the operator's hand
    return self._recovery.recover(kind="split")

def recover_migrations(self) -> List[int]:
    return self._recovery.recover(kind="migrate")
```

`start` and `start_network` keep the order they have - start the shards, load the notes,
start the publisher, finish - because that order is load-bearing (see section 6).

## 5. The process side

The three things that exist nowhere yet, and what each one actually costs.

**1. Reading the routing table from a process that does not lead it.**  Already built,
and already in the launcher: `_metadata_writer` is a `RemoteMetadataClient` over the
metadata group's seeds, constructed for the publisher precisely because the in-process
`MetadataClient` only answers where this node leads the group.  The recovery's
`metadata()` is that same client, handed in rather than constructed a second time.
`serving_nodes` is `list_shards()`, and a placement it does not name means the shard is
not in the table at all - which is the third answer `recover_migrations` already knows
how to refuse.

**2. Moving rows between groups whose leaders are in other processes.**  This is the
part that decides whether the design works, and the pieces are all in
`oxidedb/client/routing.py` and `oxidedb/client/remote_node_client.py`; a launcher-side
`leader_client(shard_id)` is the same three objects the CLI's `--server` mode builds
(`oxidedb/cli.py`, `ClusterStore.__init__`):

```python
factory = RemoteNodeClientFactory(metadata_seeds=..., tso_seeds=...)
table = RoutingCache(None, factory.metadata_client(), factory=factory)
leaders = ShardLeaders(None, factory=factory, router=table)   # leader_for_shard(shard_id)
```

Two things the copy needs that the six primitives were not obviously enough for, and
both turn out to be expressible with them:

* *The rows, in one read.*  `_committed_rows` scans the source's storage at
  `_last_applied_timestamp`; over the wire that is `NodeClient.scan(start, end)` with no
  timestamp, which is a leader read at the newest versions.  The same rows.
* *The version a row already is.*  `_copy_rows` skips a row whose target version is
  already at the source's timestamp, which is what makes the copy idempotent, and
  `_move_row` stamps the `SET` with that same timestamp, because a row re-stamped with
  the moment of the move is the newest thing that has ever happened to the key.  **The
  wire cannot answer it, and the route this section first claimed is not a route.**
  `scan` answers key and value (`KeyValuePair`) and nothing else; a write record is
  written by a transaction's commit and by a moved row, and by nothing else - a plain
  `SET` writes a version and no record at all - so a row the SQL path wrote has a version
  and nothing to read it from.  `_move_row` already assumes the two can differ: it takes
  `timestamp` from the version and its `start_ts` from the record only when the record's
  `commit_ts` matches the version.  This was the one thing the six calls could not
  express, so the copy could not move onto `leader_client` until the client service
  carried the version - and the open decision in `docs/design.md`, section 7, is that
  read, which is now in place: `KeyValuePair.commit_ts` and `GetResponse.commit_ts` are
  filled by the servicer, and a client reads them as `scan_versions` and as
  `ReadResult.commit_ts`.  0 says a row has no version, which is the case for a row that
  came from the reader's own write intent: an intent is a lock, and a lock is not a row.
  The copy can move onto `leader_client` as soon as it is written.

* *What a lock in the range means.*  `_committed_rows` reads the storage's version space,
  and a write intent is not in it: an intent is a lock, and a lock is not a version, so
  the read answers straight through one.  The state machine's own `scan` - the one the
  wire reaches - refuses with `ScanRefused` when a lock it cannot judge is in the range.
  The two are not two answers to one question, because the read is never the first thing
  asked: a lock in the range is a transaction in the middle of changing a row in it, and
  the version space holds the *old* version of that row - which nothing downstream could
  tell from a row that is simply the row.  So all four calls that read a range in order to
  move it check `_locks_in_range` first and refuse over a lock: `split_shard` and
  `move_shard`, and now `recover_splits` and `recover_migrations` at the point where each
  of them reads the rows - the same read, in the same place, for the same reason.  What a
  refusing recovery leaves is the state section 6 describes: the note on disk, the shard
  frozen, and the lock clearing on its own, since it belongs to a transaction.
* The command is a `serialize_command(CommandType.SET, ...)` call with the value's
  timestamp and the write record's `start_ts`, and it is a module-level function in
  `oxidedb/raft/state_machine.py` for exactly this reason: "those bytes have to be the same
  on both sides of the seam between a client and a node".  A process builds the same bytes
  that a node in the same process would.

**3. The local answer for who serves a shard.**  This is the one that changed the
interface, and the change is in the *verb*: the process side cannot be told to "set the
placement", because it has nowhere to put it.  `NodeClusterView.shard_replica_ids` reads
what its own `ShardServer` holds and was told the members are; so the honest operation is
`ensure_serving(shard_id, nodes)` - build my member with those members, or close it -
and the placement the node publishes follows from the group it holds.  That is also why
the method is idempotent and why it is one call rather than two: in a process, "the set
changed" and "my holding changed" are the same event.

Five constraints found while writing this section, all of which the implementation has
to respect:

* `ShardServer.add_shard` must not be called for a shard the server already holds: it
  builds a fresh node and a fresh gRPC server on the same address.  `_ensure_group_on`
  guards on `get_shard_node(shard_id) is None` and `ensure_serving` has to keep that
  guard; the `add_shard` that already carries `members=` is otherwise exactly right.
* A process does not learn its shards from the table.  It serves the `--num-shards` shards
  it was started with, every node serving all of them, which is also why a split's new
  shard has to be built by a step of its own: the shard the recovery is finishing may not
  be one this
  node was started with.  The process side needs the same step, and `shard_ids()` cannot be
  the table's list.  A shard the node was not started with is a group it has to build, so
  this is `ensure_serving`'s other half - and the port for it is the segment's, up to the
  bound below.
* A group a process was started with holds every node of the cluster.  `start_shards` is
  called without `shard_nodes`, so `_get_peer_nodes` answers "every other node" and
  `shard_replica_ids` is the whole cluster - which is what the publisher has been sending,
  and what `tests/test_group_clients.py` asserts.  So `ensure_serving` on the process side
  cannot be the build-or-close the cluster side gets away with: a shard the node already
  holds, whose members are not the set the table now names, has to be torn down and built
  again with those members, because `MemoryRaftNode` takes its peers once and keeps them.
* A node's ports were, until this was written, sized for exactly the shards it was
  started with: shard `s` listened at `base + 100 * s`, the metadata group at
  `base + 100 * num_shards`, so the shard a first split creates wanted the port the
  metadata group was already holding.  `grpc` raises out of `add_insecure_port` for a port
  that is already bound rather than reporting a failure, so a recovery wired in as it
  stood would not have come up - and `_resume_split` builds the new shard's group *after*
  freezing the source, so that would have been a start that died with the shard frozen.
  The layout is now three fixed segments - shard `s` at `base + s`, the two groups at
  `base + SHARD_SEGMENT` and above - so a split's new shard has a port of its own, and the
  bound is `SHARD_SEGMENT`, which `ClusterConfig.validate` refuses a configuration above.
  What is still missing is the same check on the split itself: it is the split that would
  want shard `SHARD_SEGMENT` and find the routing table's group there.
* **A group `ensure_serving` closes stops being a shard the view names.**  The members
  half of the call follows for free: `add_shard(members=...)` writes them down and
  `shard_replica_ids` reads them back.  The holding half has to be asked of the group
  rather than of the range map, and that is the half this constraint is about.  The range
  map stays as it is, and should: `shard_ids` still names the shard, because the range map
  is what a node routes by and what it reads its pending-split notes by, so a shard's own
  note has to stay reachable through it - and a note is written on every replica, so a
  node that stopped looking after the shard would stop finding the split.  What changes is
  the two answers the table is written from: `shard_replica_ids` and `shard_addresses`
  read the group this process holds, and answer nothing for a shard whose group was
  closed.  Asked the other way, a close left the view still naming this node -
  `shard_replica_ids(0) == [3]`, a closed group having no peers to list while the shard
  server's own answer always names itself, and `shard_addresses(0)` naming the port it had
  just stopped listening on - and a publisher on that node would have written it into the
  table as a replica of a shard it does not serve, which is the one thing section 1 says a
  routing table must not do.  A *fresh* publisher is what finds this, which makes it worse
  rather than better: a node that has just come back has published nothing yet, so the
  shard it just closed is one it has every licence to write.  The two answers that make a
  close visible are held by `tests/test_ensure_serving.py`, and the table that stays right
  because of them by `tests/test_metadata_wiring.py`.

**Open decision: how the version reaches a caller.**  The recovery needs, per row, the
timestamp of the source's newest committed version and the version the target already
holds, and the client service answers neither today.  Two shapes:

* *A stamped read*, with the version travelling in the answer: a `version_ts` on
  `GetResponse` and on `KeyValuePair`, so a `scan` returns rows as key, value and
  version.  No new call, no new refusal, additive on the wire, and `_move_row` keeps
  taking its `start_ts` from `get_write_record` exactly as it does now.
* *A call of its own*, `version_of(key)`, beside `get_write_record`: the same
  information, one more method on `NodeClient` and one more RPC, for a reader that wants
  the version and not the value.

The first is smaller, and it is the one this document assumes.  What it may not be is a
change slipped in with the copy.  It is the first widening of the client service, which
is the place `docs/design.md` already says its open decision about the refusal family is
due, and the two should be taken together even though this one adds no code - a recovery
that cannot read a version cannot run at all, so this widening is not after the
recovery, it is in front of it.

## 6. Order, failure, tests

**Order.**  The launcher's `ClusterNode.start` becomes the cluster's `start`, in the
cluster's order:

1. the groups are built (`_start_metadata_group`, `_start_tso_group`, `_start_shards`);
2. the notes are *loaded* - every shard with one is frozen again, before anything could
   take a row for it;
3. the publisher starts, because it is what tells the table's group that this node's
   shards exist;
4. the notes are *finished*, which is the part that reads the table, copies rows and
   proposes (and by then there is a publisher to see the result).

The one thing that must not move earlier is the publisher: a publisher that ran before
the notes were loaded could see a table a step ahead of this node and take the shard's
own move for somebody else's.

**Failure.**  The four outcomes the move protocol already has are what recovery reports,
and the recovery adds nothing to them: a proposal is OK, refused, or unanswered; a
`leader_client` is there or it is not.  The mapping to what happens to the freeze is
already settled and does not change with the caller:

* refused by the table - the move is over, the source goes back to serving, the group
  built to receive the rows is closed and set aside;
* no answer - the source stays frozen with the note on disk, and the next start (or the
  next call) asks again: this is the outcome a restarted process is most likely to meet,
  because the election or the table it needs may not be there yet in the first seconds;
* no leader yet - the same: frozen, remembered, retried;
* a lock in the range - frozen and remembered too, because a copy taken over one would be
  of a row a transaction is in the middle of changing.  This is the one outcome whose exit
  is not built: the lock goes by itself, but the note is only read again by the next start
  or the next call, and nothing calls on its own (README, "Known gaps");
* done - the note goes, the source of a split is thawed, and the group a move left is
  closed and set aside.

Nothing here is a new final state, and the recovery is safe to call as often as it is
asked.  A recovery that cannot finish leaves the shard frozen, which is the same
intermediate state the mover left it in - the one state that cannot lose a row.

**Tests.**  Three layers, in the order they are worth writing:

1. The in-process tests that exist (`tests/test_migration_recovery.py`,
   `tests/test_split_recovery.py`) are the behavior contract for the refactor: they
   drive `ShardedRaftCluster`, and they must pass unchanged.  If they need editing, the
   move of the bodies went wrong.
2. A hand-made note in a real process - `tests/_notes.py` writes one, and
   `tests/test_recovery_over_processes.py` is the test that starts a node over it, marked
   `xfail` until this work lands.  The note goes into the shard's own storage
   (`<data-dir>/shard-N`, through `RaftStorage.save_admin`: a node's bookkeeping is a note
   by name and not a file), under `MIGRATION_RECORD_PREFIX/{shard_id}` or
   `SPLIT_RECORD_PREFIX/{shard_id}`, holding the msgpack record `_remember_migration` or
   `_remember_split` writes.  Start the node and assert the job is finished - the table
   names the target set, the source is closed and set aside.  This is deterministic in a
   way a kill is not, and it drives the same path: a node that comes up with a note is a
   node whose predecessor died.
3. A kill, for the part the hand-made note cannot show: start a real cluster through
   `tests/_cluster.py`, begin a move, `kill()` one node between its copy and its
   proposal, start it again, and assert that the move ends and a client can write the
   range.  This needs a way to start a move in process mode, which does not exist yet
   (section 1) - so it is the test that arrives with whatever adds one, and until then
   layer 2 is what covers the path.

**One thing deliberately not built.**  The copy costs one read per row on the side it is
copying into, which is a round trip a batched primitive would remove - but the batched
primitive is not this change's subject.  The version-stamped read it would be built on has
landed (section 5), so what is deliberately not built here is only the batching, for the
reason it always was.
