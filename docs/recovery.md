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
    def ensure_serving(self, shard_id: int, nodes: List[int]) -> None: ...
    def apply_split_locally(self, shard_id: int, split_key: bytes,
                            new_shard_id: int) -> None: ...
    def metadata(self): ...
```

* **`shard_ids()`** - the shards this process holds a replica of.  The set the notes are
  looked for in, and never the table: a process does not learn its shards from the table
  (section 5).  A read, not a step.
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
  build its member of the group if it is in the set and holds nothing, close and set aside
  its member if it holds one and is not.  It also becomes this cluster's own answer for who
  serves the shard, which is what the publisher follows.  Idempotent.
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
* `_serving_nodes`, `_placed_shards` - `serving_nodes` to read, and this cluster's own half
  of `ensure_serving` to write.
* `_ensure_shard`, `_ensure_group_on`, `_retire_source`, `_close_group_on` - all four are
  `ensure_serving`, which is one call for both directions.
* `_rename_storage`, `orphan_dirs` - internal to `ensure_serving`: putting storage aside is
  what closing a group means, and remembering what was set aside is the view's business.
* `_leader_on`, `_wait_for_leader_on`, `_wait_for_shard_leader`, `_shard_leader_node` -
  `leader_client`, with the waiting inside it and a deadline on that wait.
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
* `ensure_serving(shard_id, nodes)` is `_ensure_group_on` + `_retire_source` +
  `_close_group_on` + the write to `_placed_shards`, in the order `_commit_move` does
  them today.
* `freeze` / `unfreeze` are `_freeze_shard` / `_unfreeze_shard` for the whole group.
* `leader_client(shard_id)` is a `ShardLeaders` over the cluster's own nodes with a
  `LocalNodeClientFactory` - which is the same object the coordinator and the resolver
  already ask, so a recovery in process reads its shards the way every other caller
  does.
* `apply_split_locally` is `_apply_split_locally`.
* `metadata()` is `self._metadata_client`.

Stays internal (helpers the recovery calls through the view, or uses itself):

* `_shard_nodes`, `_nodes_on`, `_addresses_on`, `_drain_shard`, `_rename_storage`,
  `_close_group_on`, `_retire_source`, `_shard_leader_node`, `_wait_for_leader_on`,
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
  already at the source's timestamp, which is what makes the copy idempotent, and the
  wire's `scan` carries key and value only.  The version is recoverable:
  `get_write_record(key)` on each side, compared by `commit_ts`, which is the timestamp
  the row's version has.  One extra call per row on the side being copied into, and the
  one place a batched primitive would pay - noted in section 6 rather than built.
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
  it was started with, every node serving all of them, which is also why `_ensure_shard`
  exists for a split's new shard: the shard the recovery is finishing may not be one this
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
* **A group `ensure_serving` closes does not change what the view says it holds.**  The
  members half of the call follows: `add_shard(members=...)` writes them down and
  `shard_replica_ids` reads them back.  The holding half does not.  With shard 0 closed on
  node 3 and the call told `[1, 2]`, `NodeClusterView` still answers `shard_ids() == [0]`,
  `shard_replica_ids(0) == [3]` (a closed group leaves no peers to list, and the shard
  server's own answer always names itself) and `shard_addresses(0)` still naming this node
  at the port it just stopped listening on.  A publisher running on that node would put it
  in the table as a replica of a shard it does not serve, which is the one thing section 1
  says a routing table must not do.  Which of the two ways out it takes - the view answers
  from the groups it holds, or whoever closes a group also stops routing to its range - is
  a decision for the recovery rather than a detail of `ensure_serving`, and one of the two
  has to be in place before a recovery can close anything.

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

**One thing deliberately not built.**  The copy costs one `get_write_record` per row on
the side it is copying into, which is a round trip a batched primitive would remove.  A
batch belongs in the client service's next widening, with the wire's refusal carrier
that is already owed there - not in this change, which is about which code runs the
recovery rather than about what it costs.
