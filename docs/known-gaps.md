# Known gaps

> Moved out of the README, which links here instead.  The text is the README's, with the
> references between these notes updated to name the file each one now lives in, and each
> heading moved up a level.

Honest list of what is *not* done, roughly in priority order.  Every entry here has a
completion state - a shape the finished thing would have, written into the entry itself.
What could not be written that way, because only a different design reaches it, is in
Design boundaries.

* **A split cannot be asked for the shard beyond a node's port segment.**  A node's ports
  are its shard segment with the two groups above it, so a cluster can serve
  `SHARD_SEGMENT` shards and a split asking for the shard after that has to be refused -
  a check the split path does not make yet (`docs/recovery.md`, section 5).
* **A recovery that refuses has nothing that asks it again.**  `recover_splits` and
  `recover_migrations` are called from each of the three start-up paths and by a caller
  that wants them, and by nothing else: a note left over because its range held a lock, or
  because the table could not be read, waits for the next restart.  The lock clears on its
  own - a TTL, or the lock cleaner - but the note does not, and the shard stays frozen while
  it waits.  This includes the case where the table never names the range the note refers to
  - a start killed before its first publisher pass leaves exactly that - where the split
  proposal is rejected forever and the shard stays frozen.  The natural hook is the lock
  cleaner's pass, which already knows that a range has unlocked; what is missing is a
  decision about which object owns the retry, because the cleaner reaches a cluster through
  `_shard_servers` and the recovery it would call is the one both start-up paths already
  call.
* **A process serves the shards its own start-up names, not the shards the table names it
  in.**  A node builds a group for the shard ids its configuration carries and for the ones
  its own recovery builds; nothing on that path reads the routing table, so a node that has
  just come up is not woken by a table that names it in a set it does not hold.  Both halves
  of that are measured on a two-node cluster: a split's new shard is one voter short and
  never lands - the node that built no group answers nothing on the shard's port, so the
  group cannot elect a leader - and a move can build the target's group only on the node
  that runs the recovery, which holds the note and so is a node the shard is leaving, never
  a node it is going to.  The half of a move that runs *after* the proposal asks nothing of
  the target, and that is the half a process can be shown to finish
  (`tests/test_recovery_over_processes.py`); the half that copies into the new group has
  nowhere to copy to, and a move that died inside its copy is the same shape.  What a
  process does serve, it serves whole: every node is a member of every shard its start-up
  names, which is why a cross-process transaction works over a three-node cluster while a
  subset placement does not.  The completion state is a node that reads the table as it
  starts and serves what it says; a node already running when the table changes needs to be
  told, which is a message this repository does not have - `proto/groups.proto` is where it
  would go.
* **`lock_time` comes from the local wall clock.**  Each replica writes
  `time.time()` into the lock record while applying the same log entry, so
  replicas hold TTLs that differ by a few milliseconds and the value is not
  covered by Raft.  Deriving it from the entry itself would make the state
  machine deterministic.
* **Snapshot reads are reachable from the state machine, the node and a client, but no
  user-facing entry point passes a timestamp.**  `node.get(key, ts)`,
  `node.scan(start, end, ts)`, `coordinator.read(txn_id, key)` and the wire's two reads,
  `Get` and `Scan` - the only calls that carry one - all accept it, but nothing outside
  the tests passes one: the CLI and the examples drive a local `Database`, so a user still
  gets the newest version.  A read whose
  snapshot is hidden by a lock is resolved by asking
  the primary key's write record and then rolled forward or cleared, and a lock whose
  transaction is still live is waited out up to the lock's remaining TTL - a reader is
  stopped only by a lock that outlives its TTL, which means the shard could not settle
  it.  A range read refuses rather than guesses (`ScanRefused`): a key it cannot decide
  about, or a replica that is not the leader, is an error, because an empty range and a
  range nobody could answer must not look the same.  The completion state is a read a person
  can ask for at a timestamp of their own: a flag on the CLI's `get` and `scan`, carried the
  way `--consistency` is.
* **A snapshot is the whole keyspace in one blob.**  `MVCCStorage.dump` returns
  every row in a single msgpack payload, so the cost of a snapshot grows with the
  data set, and it is taken and restored while holding the node lock - the node
  serves no RPCs for the duration.  Chunked, incremental snapshots are the next
  step; the interface (`snapshot()`/`restore(data)`) does not have to change.
  Over gRPC the payload also has to fit in one message: the 4 MiB default limit
  means a snapshot past that size is dropped by the peer until the channel is
  configured for more.
* **A node can be a process, but a client outside one cannot do everything yet.**
  The contract is six node-level primitives in
  `proto/client.proto`, with a five-value `error_code` and a leader hint.  Two of its
  fields are where a value says which version it is, `KeyValuePair.commit_ts` and
  `GetResponse.commit_ts`, and the servicer fills both - on a key, and on every row of a
  scan - which is what lets a recovery copy a row out of another process
  (`docs/recovery.md`, section 5).  `NodeClient.scan_versions` and
  `ReadResult.commit_ts` are how a caller reads it, and
  `tests/test_client_wire_versions.py` is what holds the shape.
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
* **Sharding is experimental, and nothing moves a shard by itself: no rebalancing, and
  nothing that chooses a split point.**  Every component routes
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
  copies left in the old shard are never reclaimed; and nothing chooses split points or
  where a shard should live, which is a decision the caller makes.  The completion state
  for the first two is a copy that cannot end up behind a write the source had admitted,
  and a source shard whose copied rows are released once the move has landed; the third
  is not owed work, because choosing a split point or a placement is a different piece of
  software rather than the next step of this one.  A read that need not lead is a level a
  client asks for rather than something the cluster decides: the flag that names it is in
  `consistency.md`.
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
  finishes the move itself: `recover_migrations`, which both start-up paths call as they
  start, and which a caller may call again, because every step of it is a no-op the second
  time.  The routing
  table is what says how much of the move is left, and its three answers are the three
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
  A read from the CLI need not go to a leader: `get` and `scan` take a `--consistency`
  (`consistency.md`).  What it does that no other client here does is wait for a cluster
  that has only just started: a `--server` command asks first - for a timestamp, and for a
  table naming every shard it was
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
  `RecoveryRunner._publish_split` - unseen because every cluster that splits a shard in
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
  The completion state is a layer that can refuse as well as answer: `SQLExecutor.execute`
  answers a statement it cannot parse with `[]` (`oxidedb/sql/executor.py`), and a `SELECT` whose
  shard has no leader with `[]` as well, so what the layer cannot answer and what it has
  nothing to say about look the same - the distinction `ScanRefused` keeps everywhere else.
* **No multi-version garbage collection.**  Old versions are never reclaimed.
  The completion state is a watermark below which a version may be dropped: the oldest
  timestamp a reader may still ask for, published to the store rather than guessed, because
  nothing in `MVCCStorage` is told today what a reader is still holding.
* **The generated protobuf bindings are checked in and pinned by nothing.**  The
  `*_pb2.py` files carry the toolchain that produced them in their header - protobuf
  7.35.0 and grpcio 1.82.1 - and the `*_pb2_grpc.py` files need one hand edit the
  generator does not make: `from . import x_pb2`, so that the module is importable as
  part of the package rather than as a top-level module.  Regenerating them is one
  command, and then that hand edit:

  `python -m grpc_tools.protoc -I proto --python_out=oxidedb/proto --grpc_python_out=oxidedb/proto proto/raft.proto proto/client.proto proto/groups.proto`

  Nothing fails if someone regenerates them with another toolchain until the version
  stamp is *newer* than the installed runtime, and a diff of a regenerated file is
  unreadable.  Which version that is comes from the toolchain the command is run with
  rather than from the checkout, so it belongs in the sentence: with grpcio-tools
  1.82.1, which is what produced the checked-in files, regenerating all three protos
  into a scratch directory gives back the code and both stamps unchanged, up to that
  hand edit.  With 1.83.1 two lines per file move instead - `*_pb2.py` says 7.35.1
  where the checked-in file says 7.35.0, and `GRPC_GENERATED_VERSION` says 1.83.1
  where the checked-in file says 1.82.1.  Both are installed on the machine this was
  measured on, in different interpreters, which is why "the toolchain this checkout
  has" was the wrong way to say it.  A test that pins the six files' sha256 is owed.
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
  mid-commit, can observe part of a transaction.  See `docs/design.md`.  The completion state
  is a local transaction ordered the way the distributed one is - through the coordinator, or
  with a primary key and locks of its own - so that a reader never sees half of a commit.
* **A range read has no index to read at again, so a cached read cannot cover one.**
  `ScanResponse.read_index` is filled by the servicer and read by nobody: `scan` and
  `scan_versions` answer with rows rather than with a result, on both carriers, so a client
  that read a range has nothing to keep and name on the range that follows.  Single-key
  reads are not affected - `ReadResult.read_index` crosses the wire and `GetResponse` is
  read back - which is why this is the range half of the feature and not the whole of it.
  The completion state is a range read that answers with the index it was made at, which is
  a return type on `NodeClient` and on both implementations of it.
* **The two `_read_shape` helpers disagree about what a read answers with.**  The one in
  `tests/test_remote_node_client.py` compares `read_index`; the one in
  `tests/test_local_node_client.py` does not, and could not be given it for free, because
  two of its assertions name a result field by field - adding the index there would pin
  this log's absolute positions in a test that is about two carriers agreeing.  The
  completion state is one way of writing a result's shape that both files use, so that a
  field added to a read cannot be compared on one side of a wire and not the other.
* **`consistency.md`'s copy of the read-index window is a copy.**  That note writes the
  window out as `0.1s`, which is what `READ_INDEX_TTL` holds today, and prose cannot
  import a constant: a line here that quotes a number from the code goes stale the moment
  the number changes, and nothing fails.  The completion state is a test that reads the
  window out of `docs/consistency.md` and compares it with the constant.
* **`JSONFileStorage` is legacy.**  Kept because existing tests construct it - three files
  under `tests/` - and because `raft/__init__.py` exports it; it rewrites the whole log per
  append, and `EngineRaftStorage` is the one for new code, which `raft/storage.py` says in
  its own docstring.  The completion state is those callers moved onto the engine storage or
  onto the factory and the class deleted, which is cleanup waiting to happen rather than a
  design this project chose: `raft/shard_server.py` already imports it without using it, so
  the callers are fewer than the imports suggest.

Where the design stops.  Nothing here is waiting to be done: each is a choice this
project makes, and what would move it is a different design rather than the next piece
of work.

* **No membership change.**  Cluster size is fixed at construction; there is no
  joint-consensus configuration change.
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
