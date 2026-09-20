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
its `start_network`, they read the notes through `RecoveryRunner.load_pending_splits`
(`oxidedb/raft/recovery_runner.py`) and `_load_pending_migrations`, and the move's half of
what follows is written against that cluster's own `_migrations`, `_placed_shards`,
`_orphan_dirs` and `_shard_servers`.  The split's half has moved out to the runner, which
holds `_pending_splits` itself and asks the side it runs on for everything else.
`ClusterNode` (`oxidedb/launcher.py`), which is what runs when a node is a process, holds
one node of each group and none of those four - and no runner, which is this document's
subject.

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

**The interface is per process, and that is not a preference.**  The split's
`RecoveryRunner.remember_split` writes a note on *every replica* of the shard it is about,
and says why: "the note says which shard is being split, and any replica holding that
shard can be the one that finds it".  The design already assumes that recovery is done
by the replica that comes back, not by a coordinator that looks at the group.  A
`freeze_the_group(shard_id)` call - the shape a cluster-object interface wants - cannot
be expressed from one process at all.

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
    def node_ids(self) -> List[int]: ...
    def range_map(self) -> RangeMap: ...
    def pending_notes(self, shard_id: int) -> List[PendingNote]: ...
    def remember_note(self, note: PendingNote) -> None: ...
    def forget_note(self, shard_id: int) -> None: ...
    def serving_nodes(self, shard_id: int) -> Optional[List[int]]: ...
    def shard_replica_ids(self, shard_id: int) -> List[int]: ...
    def addresses_on(self, shard_id: int, nodes: List[int]) -> Dict[int, str]: ...
    def leader_client(self, shard_id: int) -> Optional[NodeClient]: ...
    def leader_client_for_nodes(self, shard_id: int,
                                nodes: List[int]) -> Optional[NodeClient]: ...
    def freeze(self, shard_id: int, reason: str) -> None: ...
    def unfreeze(self, shard_id: int) -> None: ...
    def ensure_serving(self, shard_id: int, nodes: List[int]) -> List[int]: ...
    def ensure_group_on(self, shard_id: int, nodes: List[int]) -> None: ...
    def close_group_on(self, shard_id: int, nodes: List[int]) -> List[int]: ...
    def apply_split_locally(self, shard_id: int, split_key: bytes,
                            new_shard_id: int) -> None: ...
    def apply_move_locally(self, shard_id: int, nodes: List[int]) -> None: ...
    def metadata_client(self): ...
```

* **`shard_ids()`** - the shards this process looks after: the keys of its range map,
  which is the set the notes are looked for in, and never the table - a process does not
  learn its shards from the table (section 5).  Deliberately not "the shards it holds a
  group for": that is a different question, an implementation answers it from its groups
  rather than from its range map, and it is the one `ensure_serving` is there to change.
  A read.
* **`node_ids()`** - the nodes this side holds at all, which is the set a group built
  beside the shard is made of: a split's new shard is served by every node, and the group
  a move builds has the target set as its members and not the source's.  A read.
* **`range_map()`** - the ranges this process believes in, the source shard's included.  A
  split needs the range it is splitting, a move needs the range it is copying.  A read.
* **`pending_notes(shard_id)`** - every note a replica this process holds wrote down for
  the shard, as it is on disk, and an empty list when there are none.  One call for both
  kinds: which kind a note is is the note's own business, and a recovery that picks between
  notes is reading the one field that says.  A read.
* **`remember_note(note)`** - write one down, on every replica this side holds for the
  shard it is about.  Written before the first row moves, because the window between the
  copy and the proposal is the one place the work exists nowhere else: rows in a group the
  table has not been told about, and a freeze nothing has recorded.  Idempotent.
* **`forget_note(shard_id)`** - drop that note on every replica this process holds.  A
  replica whose storage has already been closed cannot be reached, which is why that is a
  no-op here rather than an error.  Idempotent.
* **`serving_nodes(shard_id)`** - the replica set the routing table names, or - with no
  table, as in an in-process test - this cluster's own answer.  It is how a cluster that
  comes back learns how far a move got, so it is a read of the table and of nothing local.
  None is the process side's third answer, a table that names the shard and a placement of
  its own it does not have.
* **`shard_replica_ids(shard_id)`** - the members of the group for the shard, as this side
  has them.  What a proposal publishes as the new set, and not the same question as
  `serving_nodes`: a shard being moved has two groups for a moment and this one names the
  group the side is building rather than the one the table names.  A read.
* **`addresses_on(shard_id, nodes)`** - where each of `nodes` serves the shard, as those
  nodes bound it.  Asked of a set, because the nodes a move is going to are not the nodes
  the shard is served by yet, and a proposal that took its addresses from the shard's own
  answer would hand the table the addresses of the group it is leaving.  A read.
* **`leader_client(shard_id)`** - a `NodeClient` for whatever leads the shard, wherever
  that is, or None while nobody does.  This is the only way the recovery moves rows: it
  reads the source and proposes into the target through it, and in a process the client it
  gets is a socket.  A read.
* **`leader_client_for_nodes(shard_id, nodes)`** - the same question asked about a set of
  nodes rather than about the shard.  `leader_client` answers with the group the routing
  table names, and the group a move copies *into* is exactly the one the table does not
  name yet, so the set has to come from the caller that built it.  A call of its own
  rather than a default argument on the other one: a caller that forgot the set would be
  handed the wrong group's leader, and the wrong group's leader here is the shard the
  rows are being copied out of.  A read.
* **`freeze(shard_id, reason)`** - refuse commands that add rows on the replicas this
  process holds, with the reason the refusal quotes.  Commits and rollbacks still go
  through: they are the end of a transaction that prewrote before this.  Idempotent.
* **`unfreeze(shard_id)`** - the reverse, for the source of a split that is done and for a
  move the table refused.  Idempotent.
* **`ensure_serving(shard_id, nodes)`** - make what this process holds match that set:
  build its member of the group if it is in the set and holds nothing, close its member if
  it holds one and is not.  What the publisher follows is the group this side holds: the
  replica set and the addresses it publishes are read from that group, so closing one stops
  both from naming this side - see the last constraint in section 5.  A side with nowhere
  to keep a placement, which a process is, reads its answer to "who serves this shard" off
  the group it holds too, so there the two are one event; a cluster's answer is
  `_placed_shards`, written one step earlier by `apply_move_locally`, and section 4 has
  that order.  What it returns is the nodes whose group it closed, which is how a caller
  that ran twice tells a call that did something from one that found the work already
  done.  What a close leaves on disk - the group's storage, put aside under an orphan name
  rather than deleted - is the cluster side's half of this and is not written on the
  process side yet.  Idempotent.
* **`ensure_group_on(shard_id, nodes)`** - build this side's member of the group and close
  nothing, which is the one asymmetry between the two sides.  A move builds the group its
  rows are going into while the source is still the group the table names, so the nodes
  holding that shard are not any one replica set: a cluster holds both groups, and this is
  how it builds the second; a process holds one, and this is the whole of what it does
  about the group it is being asked to join.  Idempotent.
* **`close_group_on(shard_id, nodes)`** - close this side's member of the group on
  exactly `nodes`, and build nothing.  The other half of `ensure_group_on` and the
  closing half of `ensure_serving`, but not that call: the set is the caller's,
  where `ensure_serving` answers to the set the routing table names - and builds
  on its nodes as well.  The caller is a move the table refused, and a refusal
  does not say what the table names instead, so the only group to take down is
  the one built to receive the rows; mending to any other set would close the
  groups of the set the table does name.  Returns the nodes whose group it
  closed.  Idempotent.
* **`apply_split_locally(shard_id, split_key, new_shard_id)`** - the range map this process
  routes by becomes the new one, so that the publisher sees a cluster that agrees with the
  table rather than one mid-split.  A move's second write has the call beside this one,
  `apply_move_locally`.  Idempotent.
* **`apply_move_locally(shard_id, nodes)`** - where the table now says the shard is served:
  the placement, written down before the group that left is let go, because between the two
  the table names the new set while the old group is still up and still answering - which
  is the window a client that cached the old table finishes its read in.  A side that keeps
  a placement writes it here; a process keeps none, because who serves a shard there is
  read off the group it holds, so it has nothing to write down and must not go on
  answering with the group it is leaving, which is `ensure_serving`, the step after.
  Idempotent.
* **`metadata_client()`** - the routing table's group as a client, for reading placements
  and for the two writes that finish a split or a move.  Deliberately not named as a type,
  exactly as in `MetadataPublisher`: the in-process `MetadataClient` and the
  `RemoteMetadataClient` both answer `table`, `split_shard`, `move_shard` and
  `list_shards`, and which one a node holds is the difference between a recovery that only
  works where it leads the group and one that works anywhere.

One boundary worth writing down beside these calls, because it costs a walk through the
code to find again: `NodeClient.follower_read_index` answers with an index and a reason,
and not with the address a `NOT_LEADER` refusal carries on the wire.  So walking a set
of nodes and asking each whether it leads is what a caller holding clients can do, which
is what `leader_client_for_nodes` does - and following a hint to a node outside that set
is what it cannot: the address never reaches the caller.  Nothing here needs it to.  It
is the same kind of fact as a state machine built without being told which shard it is,
or a refusal whose code is flattened on the way to a client: not a gap this work opens,
but a limit on what the seam can say, written down so the next caller reads it instead
of finding it.

Everything else the recovery does - reading the source's rows, deciding which row the
target is missing, building the command, retrying a proposal whose answer never came -
is written once, in the recovery, against `NodeClient` and `metadata_client()`.  That is
why the sketch this document grew out of had two calls that are not here: `copy_rows(...)`
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

**The seam is at the note, and everything after one is written down is the recovery's
side of it.**  `_num_shards` hands a split its new shard's id, `_drain_shard` waits out the
writes the freeze admitted, and the rows a move copies are read - all three *before*
`RecoveryRunner.remember_split` or `_remember_migration` writes anything.  That part
begins the work: `split_shard` and `move_shard` today, and a process that can begin one
whenever it grows that.  The recovery neither needs it nor could get it: the note carries
what the beginning decided, the new shard's id among it.  Everything after the note -
reading it back, freezing, building, copying, proposing, forgetting, re-ranging - is the
recovery, one body, whichever side runs it.

Three things straddle the line and are worth naming, because the extraction has to carry
them: `_pending_splits` and `_migrations` are written by the beginning - the entry it puts
there is the in-memory copy of the note - and finished by the recovery, which is why
`possible_ranges` reads the first of them; and `_last_migration_error` is written by both,
by `move_shard`'s precondition refusals before the note and by the finish path after it.
The split's two are on the runner now, and the beginning reaches them through it:
`remember_split` writes the note and the entry in one call, and `pending_split` is the
question `split_shard` asks before beginning a second split of the same shard.

The differential was thirteen primitives only `ShardedRaftCluster` has, nine both sides
already have, and three neither has.  The nine go straight in (`shard_ids`, `range_map`,
`metadata_client`, `get_shard_server`, `shard_replica_ids`, `shard_addresses`,
`shard_leader`, and, under both, `ShardServer.add_shard` / `get_shard_node` /
`shutdown_shard` and `RaftStorage.save_admin` / `load_admin` / `delete_admin`).  The
thirteen and the three land like this:

* `_shard_nodes`, `_nodes_on` - internal: "the nodes this process holds for the shard" is
  what a view answers, and it is a helper here rather than a call.
* `_freeze_shard`, `_unfreeze_shard` - `freeze` and `unfreeze`.
* `_drain_shard` - the beginning's, and internal: it waits out the writes the freeze
  admitted, and it runs before anything is written down, so it sits on the far side of the
  seam drawn above rather than in the recovery.
* `_serving_nodes` - `serving_nodes`, under the interface's name.  `_placed_shards` is what
  it reads, and it is written where the routing table is: by `apply_move_locally`, which
  `_commit_move` calls once the table has agreed, and never by `ensure_serving`.
* `_ensure_shard`, `_retire_source` - `ensure_serving`, which is one call for both
  directions.  `_close_group_on` is what it closes through, and it is `close_group_on`
  as well: a refused move takes down the group it built and mends to nothing, which is
  not what answering to the table's set would do.  `_ensure_group_on` is *not*
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
  than a new one.  `_leader_on` is `leader_client_for_nodes`, which is the same walk
  over a set of nodes with the client handed back instead of the object.
* `_rows_above`, `_committed_rows`, `_locks_in_range` - internal, over
  `leader_client(shard_id).scan(...)`.  The split's copies of the first and the third are
  in the runner, and the `_locks_in_range` there asks the question by reading the range
  and taking a refusal over a lock for the answer; the cluster keeps the move's.
* `_move_row`, `_copy_rows`, `_copy_what_is_missing` - internal, over
  `leader_client(...).propose(...)`, `leader_client(...).get_write_record(...)` and
  `leader_client(...).scan_versions(...)`.  Each was written a second time beside
  the body that reached through node objects, to show the seam could carry a copy
  at all and that the two proposed the same bytes for the same rows
  (`tests/test_copy_row.py`).  The proof is spent and the node-object body is gone:
  what is left is the one the callers reach, which is the one a process can run.  The
  split's two went with the rest of its branch, into `RecoveryRunner._copy_what_is_missing`
  and `RecoveryRunner._move_row`; `_copy_rows` is the move's, and reaches the same
  `_move_row` through the cluster, which delegates to the runner's.
* `_client_for_node` - the cluster side's way of handing the copy a client, and
  deliberately not an interface method.  Its argument is a node object, which is the
  one thing a process does not have, so the rule at the top of this section excludes
  it; a process asks a factory for a pair of ids instead.  Both hand back the same
  wrapper around the same node, so nothing downstream can tell which produced it.
* `_addresses_on` - not needed by the recovery: the addresses a move proposes are the ones
  the nodes it is proposing to serve at, and whoever holds those nodes works them out.
* `_finish_split`, `_publish_split` - internal to the recovery, and since the split's
  branch moved, the runner's: `finish_split` and `_publish_split`.  `_apply_split_locally`
  is the exception: it is the view's, under the name `apply_split_locally`.
* `_finish_move`, `_propose_move`, `_commit_move`, `_abort_move` - internal to the
  recovery.
* `split_error`, `migration_error`, `pending_splits`, `migration_state`, `migrations` - the
  recovery's own accessors, for tests and for an operator.

One reader crosses that line and has to be said out loud: `possible_ranges` asks the
publisher's question - both maps a cluster mid-split could be in - and the second of them
comes out of `_pending_splits`.  That table has moved into the recovery, and
`possible_ranges` now asks the runner for it rather than reading it here: a caller of the
boundary, not a second owner of the state.

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
  for every node that holds nothing.  It is the second of the two steps `_commit_move`
  takes, and the order between them is what made section 2 and this list read as if they
  disagreed.  The first step writes the routing table's answer down, and it is
  `apply_move_locally`: `_placed_shards` becomes the set the proposal landed, the
  `_migrations` entry that was answering with the old set goes, and from there
  `serving_nodes` names the new one.  The second step is this call, and it changes the
  groups the cluster actually holds.  What lies between them is the drain window: the table
  already names the new set while the old group is still up and still answering, which is
  what lets a client that routed by the table it cached finish the read it arrived with.
  So `ensure_serving` must not write `_placed_shards` - that is the table's answer, and it
  moves when the table does, one step earlier.

  A process has no such order and loses nothing by that: it has nowhere to keep a
  placement, so who serves a shard there is read off the group it holds, and the two steps
  are one event.  Both sides answer one question - who serves this shard - and the
  difference is only which of the two carriers is written down and which is derived, which
  is the difference the interface is there to hide.
* `freeze` / `unfreeze` are `_freeze_shard` / `_unfreeze_shard` for the whole group.
* `leader_client(shard_id)` is a `ShardLeaders` over the cluster's own nodes with a
  `LocalNodeClientFactory` - which is the same object the coordinator and the resolver
  already ask, so a recovery in process reads its shards the way every other caller
  does.  `leader_client_for_nodes(shard_id, nodes)` asks the same factory for each node
  of a set and keeps the first that answers a follower read index, which is the client
  service's own way of saying "I lead and I have confirmed it with a quorum".
* `apply_split_locally` is `_apply_split_locally`.
* `metadata_client()` is `self._metadata_client`.

Stays internal (helpers the recovery calls through the view, or uses itself):

* `_shard_nodes`, `_nodes_on`, `_addresses_on`, `_drain_shard`, `_rename_storage`,
  `_close_group_on`, `_ensure_group_on`, `_shard_leader_node`, `_wait_for_leader_on`,
  `_wait_for_shard_leader`, `_shard_leader_on`, `_load_migration_record`,
  `_remember_migration`, `_forget_migration`.
* `_finish_split`, `_publish_split`, `_copy_what_is_missing`, `_load_split_record` - the
  split's half of the *recovery's* body, not the view's, and they have moved: they are
  `RecoveryRunner`'s, held by the cluster as `self._recovery_runner`.  `_finish_move`,
  `_propose_move`, `_commit_move` and `_abort_move` are the other half, and have not.
* `_orphan_dirs` and `_placed_shards` stay here: what has been set aside and what the
  routing table names are this cluster's own bookkeeping.  `serving_nodes` reads the
  second of them and `apply_move_locally` writes it, which is the pair the interface has
  for it; `ensure_serving` deliberately does neither, because the placement moves when the
  table moves and not when the groups do - the drain window is exactly the stretch in which
  the two disagree, and it is what lets a client that cached the table finish its read.
* `_migrations`, `_pending_splits` and the two `_last_*_error` strings go with the
  recovery body, because they are a running recovery's working state - the boundary
  section 3 draws.  The split's two have gone; `_migrations` and `_last_migration_error`
  go when the move's body does, and until then the cluster keeps those and the runner's
  own copies of them are fields nothing reads.  Three of them are written from the
  beginning too: `split_shard` and `move_shard` put the entry in the table the note is the
  disk copy of, and `move_shard`'s precondition refusals are most of where
  `_last_migration_error` is written (`_last_split_error` is the finish path's alone).  So
  the beginning reaches them through `self._recovery_runner`, the way the public accessors
  and `possible_ranges` do.

What the public entry points become:

```python
def recover_splits(self) -> List[int]:      # kept: the tests and the operator's hand
    return self._recovery_runner.recover_splits()

def recover_migrations(self) -> List[int]:
    self._load_pending_migrations()
    return self._finish_pending_migrations()
```

`start` and `start_network` keep the order they have - start the shards, load the notes,
start the publisher, finish - because that order is load-bearing (see section 6).  It is
why the split's half is two calls on the runner, `load_pending_splits` and
`finish_pending_splits`, with `recover_splits` being those two in a row.

## 5. The process side

The three things that exist nowhere yet, and what each one actually costs.

**1. Reading the routing table from a process that does not lead it.**  Already built,
and already in the launcher: `_metadata_writer` is a `RemoteMetadataClient` over the
metadata group's seeds, constructed for the publisher precisely because the in-process
`MetadataClient` only answers where this node leads the group.  The recovery's
`metadata_client()` is that same client, handed in rather than constructed a second time.
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
  `move_shard`, and now `recover_splits` and `recover_migrations`, at the point where each
  of them reads the rows - the same read, in the same place, for the same reason.  One of
  the four asks it differently: the check that moved with the split's recovery is
  `RecoveryRunner._locks_in_range`, which asks by reading the range and takes a refusal
  over a lock for the answer, where the cluster's three ask the shard's own storage.  What
  a refusing recovery leaves is the state section 6 describes: the note on disk, the shard
  frozen, and the lock clearing on its own, since it belongs to a transaction.
* The command is a `serialize_command(CommandType.SET, ...)` call with the value's
  timestamp and the write record's `start_ts`, and it is a module-level function in
  `oxidedb/raft/state_machine.py` for exactly this reason: "those bytes have to be the same
  on both sides of the seam between a client and a node".  A process builds the same bytes
  that a node in the same process would.

Writing that copy a second time turned up one thing the interface as sketched could not
say: which group the rows are going *into*.  `leader_client(shard_id)` answers about
the group the routing table names, and the group a move copies into is exactly the one
the table does not name yet.  It is a call of its own, `leader_client_for_nodes(shard_id,
nodes)`, and not a set of nodes defaulted on the other one: a signature that let a caller
forget the set would hand it the wrong group's leader in precisely the case this call
exists for, and the wrong group's leader here is the group the rows are coming out of.

Neither side needed a new RPC for it.  `ClientService.FollowerReadIndex` already answers
a node that is not the leader with `NOT_LEADER` and, when that node knows who leads, with
`leader_address` beside it - and a leader answers with its commit index, which it has
confirmed with a quorum.  So the walk is one call per node in the set, and it is asked that
way rather than trusted to `NodeState`: a node that merely believes it leads is
exactly the node whose belief is in question, and here it is the one that would be handed
a copy.  The hint is not followed, because every member of the group is in the set - the
set is what the caller answered "which group" with - so walking it is the whole answer.
One thing the hint does not do on the other path either: `NodeClient.follower_read_index`
returns an index and a reason, and drops the address, so a caller holding only clients
cannot follow a hint through that call.  Nothing here needs it to; the boundary is
written down with the rest of the interface, in section 2.

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
  stood would not have come up - and `RecoveryRunner._resume_split` builds the new
  shard's group *after* freezing the source, so that would have been a start that died
  with the shard frozen.
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
   drive `ShardedRaftCluster`, and their assertions must pass unchanged.  What may be
   edited is a spy's own shape, because a spy is written against the signature of the
   call it wraps - the split's move did edit three of them, and `docs/design.md` says why
   that is not a test changing.  If an assertion needs editing, the move went wrong.
2. A hand-made note in a real process - `tests/_notes.py` writes one, and
   `tests/test_recovery_over_processes.py` is the test that starts a node over it, marked
   `xfail` until this work lands.  The note goes into the shard's own storage
   (`<data-dir>/shard-N`, through `RaftStorage.save_admin`: a node's bookkeeping is a note
   by name and not a file), under `MIGRATION_RECORD_PREFIX/{shard_id}` or
   `SPLIT_RECORD_PREFIX/{shard_id}`, holding the msgpack record `_remember_migration`
   or `RecoveryRunner.remember_split` writes.  Start the node and assert the job is
   finished - the table names the target set, the source is closed and set aside.  This
   is deterministic in a way a kill is not, and it drives the same path: a node that
   comes up with a note is a node whose predecessor died.
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
