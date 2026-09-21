# Quickstart

> Moved out of the README, which links here instead.  The text is the README's, with the
> references between these notes updated to name the file each one now lives in, and each
> heading moved up a level.

From the index:

```
pip install oxidedb     # the library, the client CLI and the cluster launcher
oxidedb --help          # get, set, delete and scan against a running cluster
oxidedb-launcher --help # one node of a cluster: its shards, the routing table, the clock
```

Developed and tested on Python 3.14.  From a fresh clone:

```
pip install -e ".[test]"     # runtime dependencies, plus pytest
pytest tests -q             # 415 tests, roughly two minutes
```

`pip install -e .` on its own installs what the library needs; the `[test]` extra
adds `pytest`, and `pip install -r requirements.txt` is the same set plus
`grpcio-tools`, which regenerates the checked-in `*_pb2.py` modules rather than
being needed to run them.  The tests start dozens of local gRPC servers and need
a writable temp directory.

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
oxidedb --server 127.0.0.1:8001 set user:1 alice
oxidedb --server 127.0.0.1:8001 get user:1            # alice, out of whatever shard owns it
oxidedb --server 127.0.0.1:8001 scan a: z            # every shard it covers
```

Nothing else has to be said about the cluster: a node's ports are three fixed
segments above the address it was given - its shards, then the routing table's
group, then the timestamp group - so the CLI works the group ports out with
`ports_for` in `oxidedb/launcher.py` instead of being told how the cluster was
built.  Several nodes may be given, comma-separated or repeated, and all of them
are used as seeds: no client knows which member of the routing table's group
leads, so one address it cannot use would otherwise end the walk.  `--wait SECONDS`
bounds how long a `--server` command asks a cluster that has not finished starting
before it reports, as one line, the reason it could not be routed; it defaults to
five, which is ten of the publisher's own poll intervals, and `0` asks once.
`--data-dir` and `--server` together are refused, since the same key cannot be in
two places.

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

A node takes a block of ports from the one it is given: shard `s` at `port + s`, the
routing table's group at `port + SHARD_SEGMENT`, and the timestamp group above that.
Three fixed segments, so nothing about the block depends on how many shards the node was
started with - and the segment is what bounds that number: a `--num-shards` above it is
refused when the node is configured rather than discovered when a port is bound.
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
client with a shell in front of it.  A read need not go to the leader either:
`--consistency` on `get` and `scan` names one of three levels, and `consistency.md`
says which copy of a shard may answer each one and what it costs.
