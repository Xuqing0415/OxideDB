# OxideDB

[![version 0.1.0](https://img.shields.io/badge/version-0.1.0-blue)][rel]
[![tests at v0.1.0](https://img.shields.io/badge/tests%40v0.1.0-414_passed-brightgreen)][rel]
[![python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)][pypi]
[![license MIT](https://img.shields.io/badge/license-MIT-green)][license]

A distributed transactional key/value store written in Python: Raft for replication, a
Percolator-style two-phase commit for transactions that span shards, MVCC for snapshot
reads, and a range-sharded keyspace.

It is a working prototype rather than a production database.  What is implemented is tested;
what is not is written down with its reason in [the known gaps][gaps] - including which of
them are waiting for work and which would take a different design.

## What it does

* **Strongly consistent reads** - a read asks the shard's leader for an index, the leader
  confirms it with a quorum, and the answer is the state machine at that index, so a read
  cannot return a value that was already stale when it began.
* **Cross-process ACID transactions** - Percolator-style two-phase commit that holds across
  a cluster of real processes; a lock left behind by a client that died is resolved rather
  than waited on, by the reader that meets it and by the cluster's own lock cleaner.
* **Serializable snapshot isolation** - a transaction reads at one timestamp and its read
  set is validated at commit, so write skew is refused rather than allowed.
* **Horizontal scaling** - ranges each owned by a shard with a Raft group of its own, split
  and moved while the cluster runs; the cluster publishes the placement, and a read that
  need not lead is answered by any member of a shard's replica set.
* **MVCC over a pluggable engine** - an in-memory engine, and a SQLite engine in WAL mode
  for durable mode.
* **A suite for the hard parts** - durability, routing consistency, split and move recovery,
  lock resolution, and clients over real processes.  It runs in about two minutes; the
  number on the badge above is the count at the release, and the suite has grown since.

## Quickstart

```
pip install oxidedb      # the library, the client CLI and the cluster launcher
```

`oxidedb` reads and writes a database inside its own process, or a cluster over the wire;
[Quickstart and operations][quickstart] has all three modes, how to start a node, and a
session against a cluster end to end.

```
# Durable: the data goes to ./demo/data.sqlite3, so the next command finds it
oxidedb --data-dir ./demo set user:1 alice
oxidedb --data-dir ./demo get user:1              # alice

# With no --data-dir the store is in memory and is thrown away when the command exits,
# so a bare `set` and then a bare `get` finds nothing.

# Against a running cluster, with a read that need not go to the leader
oxidedb --server 127.0.0.1:8001 get user:1 --consistency follower
```

From a fresh clone:

```
pip install -e ".[test]"
python -m pytest tests -q          # the suite, roughly two minutes
python examples/basic_usage.py     # set/get, a scan, a transaction and a rollback
```

## Documentation

| Note | What it covers |
|---|---|
| [Quickstart and operations][quickstart] | the three modes, running a node, ports and `READY` |
| [The four goals][goals] | what each goal set out to be, how to reach it, and its limits |
| [Consistency levels][levels] | `strong`, `follower` and `cached`, and what `cached` costs |
| [Architecture][architecture] | the five-layer write path, and what is in each directory |
| [Storage engines][storage] | the engine interface, the keyspace layout, durable mode |
| [Tests][tests] | what the suite covers, file by file |
| [Known gaps and boundaries][gaps] | what is not done, and what is out of scope by design |
| [Design notes][design] | why the keyspace, snapshot, 2PC and read path are shaped this way |
| [Recovery][recovery] | a split or a move that died part way through, and how it is finished |

## License

MIT.  See [LICENSE][license].

[rel]: https://github.com/Xuqing0415/OxideDB/releases/tag/v0.1.0
[pypi]: https://pypi.org/project/oxidedb/
[license]: https://github.com/Xuqing0415/OxideDB/blob/main/LICENSE
[goals]: https://github.com/Xuqing0415/OxideDB/blob/main/docs/goals.md
[levels]: https://github.com/Xuqing0415/OxideDB/blob/main/docs/consistency.md
[quickstart]: https://github.com/Xuqing0415/OxideDB/blob/main/docs/quickstart.md
[architecture]: https://github.com/Xuqing0415/OxideDB/blob/main/docs/architecture.md
[storage]: https://github.com/Xuqing0415/OxideDB/blob/main/docs/storage.md
[tests]: https://github.com/Xuqing0415/OxideDB/blob/main/docs/testing.md
[gaps]: https://github.com/Xuqing0415/OxideDB/blob/main/docs/known-gaps.md
[design]: https://github.com/Xuqing0415/OxideDB/blob/main/docs/design.md
[recovery]: https://github.com/Xuqing0415/OxideDB/blob/main/docs/recovery.md
