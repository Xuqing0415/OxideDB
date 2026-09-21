# OxideDB, in one page

OxideDB is a distributed transactional key/value store that runs as a cluster of processes
and is reached over the wire.  The keyspace is a set of key ranges, each range owned by a
shard with a Raft group of its own, and every row is a version carrying the timestamp it
was written at, so a read happens at a timestamp and a transaction reads one snapshot for
its whole life.  A write inside one shard is a command replicated by Raft; a write across
shards is a Percolator-style two-phase commit, staged as intents at the transaction's start
timestamp and decided by one record on a primary key, so a lock left behind by a client
that died is resolved - by asking that key what happened - rather than waited on.  A
running cluster splits a shard in two and moves a shard to other nodes without stopping,
publishes the placement into a routing group of its own so that a client can route without
being told, and answers a read from any member of a shard's replica set when the caller
does not need the leader.  It is one Python package: a library, a client CLI, and a
launcher that runs one node of a cluster.

## A session

Three nodes, in three terminals.  A node takes a block of ports from the one it is given,
and each of them names the other two by the address their first shard listens at.

```
$ python -m oxidedb.launcher --node-id 1 --host 127.0.0.1 --port 8001 \
      --data-dir ./node1 --peers "2@127.0.0.1:10001,3@127.0.0.1:12001"
metadata: 127.0.0.1:9001 (peers [2, 3])
tso: 127.0.0.1:9002 (peers [2, 3])
ShardServer 1 started with 2 of 2 shards (network mode)
node 1: shards at 127.0.0.1:8001, 127.0.0.1:8002, metadata at 127.0.0.1:9001, tso at 127.0.0.1:9002
READY 127.0.0.1 8001
```

`READY` is a promise about ports rather than about elections, so a command sent in the
first moment of a cluster's life is slow to answer rather than refused: the CLI asks for
a timestamp and for a table naming every shard and its leader before it sends anything.

The other two are the same command with the ports moved up a block - node 2 takes `10001`,
node 3 takes `12001`, and each names the other two in `--peers`.

```
$ python -m oxidedb.launcher --node-id 2 --host 127.0.0.1 --port 10001 \
      --data-dir ./node2 --peers "1@127.0.0.1:8001,3@127.0.0.1:12001"
$ python -m oxidedb.launcher --node-id 3 --host 127.0.0.1 --port 12001 \
      --data-dir ./node3 --peers "1@127.0.0.1:8001,2@127.0.0.1:10001"
```

A fourth terminal is a client of that cluster, holding no cluster object at all.

```
$ oxidedb --server 127.0.0.1:8001 set user:1 alice
OK
$ oxidedb --server 127.0.0.1:8001 get user:1
alice
$ oxidedb --server 127.0.0.1:8001 get user:1 --consistency follower
alice
```

The write is a transaction with one key in it.  The first read is answered at an index the
shard's leader confirmed with a quorum; the second asks for a level that does not need the
leader, and `--consistency` is something only the reads take.

Now one node goes away - node 3's terminal, stopped the way `Ctrl-C` stops it (`stop` on
its stdin).

```
SHUTDOWN
STOPPED
```

Two nodes of three are left - still a quorum for the table's group and for every shard - so
both reads are answered and a new key still commits.

```
$ oxidedb --server 127.0.0.1:8001 get user:1
alice
$ oxidedb --server 127.0.0.1:8001 get user:1 --consistency follower
alice
$ oxidedb --server 127.0.0.1:8001 set user:2 bob
OK
$ oxidedb --server 127.0.0.1:8001 get user:2
bob
```

## What it does, and what it does not

**Strongly consistent reads.**  A read names no index, so the shard's leader confirms one
with a quorum and the answer is the state machine at that index - a replica too far behind
for the log is caught up with a snapshot rather than answered from.  That confirmation is a
round of RPCs on every read and cannot be skipped, because nothing here assumes two nodes'
clocks agree; and a leader cut off from its peers refuses the read rather than answering it
out of its own state.

**Cross-process ACID transactions.**  Writes commit through a Percolator-style two-phase
commit that holds across processes, and a lock left by a client that died is resolved by
asking the primary key what happened - rolled forward if that transaction committed,
cleared if it did not - including by the lock cleaner the cluster starts on its own.  A
`delete` is a blind write rather than an intent, so a delete racing a transaction on the
same key can be overwritten by that transaction's commit; and a lock's time to live is
written from each replica's own wall clock, so replicas disagree about it by milliseconds.

**Serializable snapshot isolation.**  A transaction reads at one timestamp and its read set
is validated at commit, so a write skew is refused rather than allowed, and the refusal is
ordered with the primary commit so that the check and the commit cannot come apart.  The
read set is keys and not ranges, so `scan` records nothing and a phantom is not detected;
and the validation is optimistic, so it refuses read-write overlaps that a conflict graph
would allow.

**Horizontal scaling.**  The keyspace is ranges, each owned by a Raft group of its own; a
shard splits and moves while the cluster is running; a client routes by a table the cluster
publishes; and a read need not go to the leader.  Nothing rebalances and nothing
chooses a split point, so both are the caller's decision; every node serves every shard its
own start-up names, so a placement narrower than the cluster is not there yet; and a move
leaves the rows it copied behind in the source shard.

Smaller things are missing around those four: old versions are never reclaimed, the SQL
layer parses `SELECT` and `INSERT` and little else, and the port block a node takes bounds
how many shards it can serve.  `README.md` lists the rest, and says which of them are
waiting for work and which would take a different design.

## Trying it, and reading further

```
pip install oxidedb                                  # library, client CLI and launcher
python -m oxidedb.launcher --node-id 1 --port 8001 --data-dir ./node1
oxidedb --server 127.0.0.1:8001 set user:1 alice
```

That is a cluster of one: every group in it elects itself.  The session above is the same
commands three times with `--peers` filled in, and `pip install -e ".[test]"` followed by
`python -m pytest tests -q` runs the suite - 400-odd tests over real processes, in about
two minutes.

* `README.md` - the reference: every layer, the four goals and how far each got, the known
  gaps and the boundaries of the design.
* `docs/design.md` - why the keyspace encoding, the snapshot, the two-phase commit and the
  read path are shaped the way they are.
* `docs/recovery.md` - a split or a move that died part way through, and how a cluster that
  comes back finishes it.
