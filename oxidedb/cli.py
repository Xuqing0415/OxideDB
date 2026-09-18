"""The command line: the embedded database, or a cluster across a wire.

Four commands - get a key, set one, delete one, scan a range - and one shape for
them, with the mode deciding what is underneath.  Without arguments the embedded
database answers, in memory, which is a way to poke at one command at a time;
``--data-dir`` keeps that database's keyspace in a directory; and ``--server`` names a
node of a cluster that is already running, which is what makes this front end the
first caller in the repository that is not one of the cluster's own tests.

The two backends are not the same object and are not meant to be.  A local ``Database``
holds MVCC storage in this process; a ``SmartClient`` holds a routing table read from
the metadata group, a clock read from the TSO group, and channels to the nodes that
serve each shard.  What they share is the four commands, and this file is where those
are said once rather than once per backend.

``--server`` wants the address a node's shard 0 listens at, because a node's other
ports are derived from that one - by ``ports_for`` in ``oxidedb/launcher.py``, which is
imported here rather than worked out a second time.  Nothing has to be said about the
cluster for that: a node's ports are three fixed segments above its base port, so the
two group ports follow from the address alone.  Several nodes may be named,
comma-separated or repeated, and all of them are used as seeds: no client knows which
member of either group leads, so one address it cannot use would otherwise be the end
of the walk.

A node is READY before it can be written to, so a ``--server`` command asks before it
sends: a timestamp from the clock's group, and a table naming every shard and its
leader.  Nothing else here waits, and that wait is this file's rather than the
client's for a reason ``wait_until_routable`` gives.
"""

import argparse
import sys
import time

from oxidedb.client import RemoteNodeClientFactory
from oxidedb.database import Database
from oxidedb.launcher import ports_for
from oxidedb.metadata.cache import RoutingCache
from oxidedb.transaction.smart_client import SmartClient


#: How long a ``--server`` command waits for a cluster that is still starting, and how
#: often it asks while it waits.  A node says READY once its ports are bound, and what a
#: first command needs after that is a clock group that has elected and a publisher that
#: has written every shard's placement - placement arrives a command at a time and the
#: publisher writes on its own poll interval
#: (``metadata.publisher.DEFAULT_PUBLISH_INTERVAL``, half a second), so the lag is a few
#: of those intervals rather than a number about the machine.  Five seconds is ten of
#: them: long enough that a cluster which is merely starting is never reported as a
#: failure, short enough that a command against an address with nothing behind it fails
#: rather than hangs.  ``--wait 0`` asks once and reports.
CLUSTER_WAIT_SECONDS = 5.0
CLUSTER_WAIT_INTERVAL = 0.1


class ClusterStore:
    """The four commands against a cluster, over the transports that reach it.

    The transports are why this is an object at all: a factory owns a channel per
    address, and something has to close them when the command is over.  Everything
    else is delegation - the client underneath is the same ``SmartClient`` a test
    drives, so a key set from a shell travels the path a key set from a test does,
    through the same routing table and the same transaction coordinator.
    """

    def __init__(self, servers):
        metadata_seeds, tso_seeds = _group_seeds(servers)
        self._factory = RemoteNodeClientFactory(metadata_seeds=metadata_seeds,
                                                tso_seeds=tso_seeds)
        #: The group's client, held here rather than only inside the cache: the wait
        #: asks the same one, and asking the factory twice would be asking for the same
        #: object anyway - so this is where it is kept rather than a second channel.
        self._metadata = self._factory.metadata_client()
        #: The placement, read from the group the first time it is needed and kept
        #: for the life of the command.  ``None`` where a cluster would go because
        #: there is no cluster object here to ask: what this client knows about
        #: placement is the table, which is the point of the table.
        self._table = RoutingCache(None, self._metadata, factory=self._factory)
        self._client = SmartClient(self._factory.tso_client(), None,
                                   router=self._table, factory=self._factory)

    def wait_until_routable(self, timeout=CLUSTER_WAIT_SECONDS):
        """Wait for a cluster that has only just started, and say why if it never is.

        A CLI invocation is one shot.  With no second attempt and no caller holding
        state for it, "not yet" and "not there" arrive as the same failure, and the
        difference is the whole of what the user needs to know: a cluster that is still
        electing is a reason to wait, an address with nothing behind it is a reason to
        look at the command.  So the command asks the two questions a command needs
        answered - see ``_why_not_routable`` - and reports the last answer it got.

        The wait is here rather than in the client because of what it would mean there:
        a client answers or raises with its reason, and a caller with state to keep
        decides what to do next.  A command that retried itself would also be deciding,
        on every caller's behalf, that a refusal is a reason to wait - and the design
        notes settle the opposite for the one refusal that is about placement: a range
        that is moving is a reason to read the table again.  What this does is
        narrower: it waits for the cluster to exist, and then sends the command once.
        """
        deadline = time.monotonic() + max(0.0, timeout)
        reason = None
        while True:
            reason = self._why_not_routable()
            if reason is None:
                return
            if time.monotonic() >= deadline:
                break
            time.sleep(CLUSTER_WAIT_INTERVAL)

        raise RuntimeError(
            f"the cluster behind --server was not ready within {timeout:.1f}s: "
            f"{reason}.  If it is still starting, give it longer with --wait; if "
            f"nothing is listening at that address, that is the reason.")

    def _why_not_routable(self):
        """What a command still lacks, or ``None`` once a command could be routed.

        The two things READY does not promise, asked the way any client has to ask for
        them: a timestamp from the clock's group, and a table - read fresh, since the
        cache can only hold what an earlier read found - that names every shard and a
        leader for each.  A sentence comes back rather than a flag because giving up
        reports the last one it met, and the sentence is what a person can act on.
        """
        try:
            self._factory.tso_client().get_timestamp()
        except RuntimeError as failure:
            return f"the clock group has no leader yet ({failure})"

        try:
            table = self._metadata.table(refresh=True)
        except RuntimeError as failure:
            return f"the routing table cannot be read yet ({failure})"

        if not table.shards:
            return "the table names no shard yet"

        # Every key has a shard, or this table is one the publisher has not finished
        # writing: the ranges are contiguous, so what is missing is a placement nobody
        # has published yet.  Counted rather than compared against a number the caller
        # had to know - how many shards a cluster has is the table's own business, and a
        # caller told to expect the wrong number would wait for a table that is as
        # complete as it is ever going to be.  Where the table starts depends on how the
        # ranges were split, so the check is that it starts at the bottom of the
        # keyspace rather than partway up it.
        placements = sorted(table.shards.values(), key=lambda placed: placed.start)
        covered = placements[0].start
        if covered not in (b"", b"\x00"):
            return (f"the table starts at {covered!r} rather than at the bottom of the "
                    f"keyspace, so the shards below it have not been published")
        for placement in placements:
            if placement.start != covered:
                return (f"the table names no shard for the range from {covered!r} to "
                        f"{placement.start!r}, so it is not finished being written")
            covered = placement.end
        if covered != b"\xff":
            return (f"the table covers the keyspace up to {covered!r}, and the shards "
                    f"above it have not been published")

        nameless = [shard_id for shard_id, placement in table.shards.items()
                    if placement.leader_id is None]
        if nameless:
            return f"the table names no leader for shard {nameless[0]}"
        return None

    def get(self, key):
        return self._client.get(key)

    def set(self, key, value):
        return self._client.put(key, value)

    def delete(self, key):
        return self._client.delete(key)

    def scan(self, start, end):
        return self._client.scan(start, end)

    def close(self):
        self._factory.close()


def _group_seeds(servers):
    """The table's addresses and the clock's, derived from the nodes' own blocks.

    A node takes its shards from its base port upwards, the routing table's group above
    the shard segment, and the timestamp group above that.  That is the arithmetic of
    ``oxidedb/launcher.py``, imported rather than repeated, so an address worked out
    here is the address those nodes bound - which is the whole reason it lives in one
    function there, and the reason this needs nothing but the addresses.
    """
    metadata, tso = [], []
    for server in servers:
        host, port = server.rsplit(":", 1)
        ports = ports_for(int(port))
        metadata.append(f"{host}:{ports.metadata}")
        tso.append(f"{host}:{ports.tso}")
    return metadata, tso


def _servers(text):
    """The addresses named by ``--server``: one, or several comma-separated."""
    return [address.strip() for address in text.split(",") if address.strip()]


def _parser():
    parser = argparse.ArgumentParser(description="OxideDB CLI")
    parser.add_argument(
        "--data-dir",
        default=None,
        metavar="DIR",
        help="keep the data in DIR/data.sqlite3 (SQLite engine) instead of an "
             "in-memory store that is discarded when the command exits",
    )
    parser.add_argument(
        "--server",
        default=None,
        metavar="HOST:PORT",
        help="a node of a running cluster, at the address its shard 0 listens at, "
             "comma-separated for several nodes; the commands then go to that "
             "cluster instead of to a local database",
    )
    parser.add_argument(
        "--wait",
        type=float,
        default=CLUSTER_WAIT_SECONDS,
        metavar="SECONDS",
        help="how long a --server command waits for a cluster that is still starting "
             "before it reports the reason it could not be routed (default: "
             "%(default)s, which is a few of the publisher's poll intervals; "
             "0 asks once and reports)",
    )
    subparsers = parser.add_subparsers(dest="command")

    get_parser = subparsers.add_parser("get", help="Get a value by key")
    get_parser.add_argument("key", help="The key to retrieve")

    set_parser = subparsers.add_parser("set", help="Set a key-value pair")
    set_parser.add_argument("key", help="The key")
    set_parser.add_argument("value", help="The value")

    delete_parser = subparsers.add_parser("delete", help="Delete a key")
    delete_parser.add_argument("key", help="The key to delete")

    scan_parser = subparsers.add_parser("scan", help="Scan keys in range")
    scan_parser.add_argument("start_key", help="Start key (inclusive)")
    scan_parser.add_argument("end_key", help="End key (exclusive)")

    return parser


def _open(parser, args):
    """The backend: the embedded database, or a cluster.

    One of the two, never both - a directory and a cluster are two different places
    for the same key to be, and a command that quietly wrote to one of them would be
    answering the wrong question.
    """
    servers = _servers(args.server) if args.server else []

    if servers and args.data_dir is not None:
        parser.error("--data-dir and --server are two different places to keep data")

    if not servers:
        return Database(data_dir=args.data_dir)

    for address in servers:
        host, _, port = address.rpartition(":")
        if not host or not port.isdigit():
            parser.error(f"--server wants host:port, and {address!r} is not that")

    store = ClusterStore(servers)
    try:
        store.wait_until_routable(args.wait)
    except RuntimeError:
        # The wait failed, so no command will be sent: the channels this store opened
        # are closed here rather than left to the caller's ``finally``, which has no
        # store to close.
        store.close()
        raise
    return store


def _run(args, store):
    """The four commands, written once for both backends."""
    if args.command == "get":
        value = store.get(args.key.encode())
        if value is None:
            print("Key not found")
            return 1
        print(value.decode())
    elif args.command == "set":
        # No check: a write that could not be committed raises with its reason, so
        # the only failure this command has is one main() already prints.  A delete
        # is not in that position - it writes no value, and a refusal is its answer.
        store.set(args.key.encode(), args.value.encode())
        print("OK")
    elif args.command == "delete":
        if store.delete(args.key.encode()) is False:
            print("Key not deleted")
            return 1
        print("OK")
    elif args.command == "scan":
        for key, value in store.scan(args.start_key.encode(), args.end_key.encode()):
            print(f"{key.decode()}: {value.decode()}")
    else:
        raise AssertionError(f"a subcommand with no branch here: {args.command!r}")
    return 0


def main():
    parser = _parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 0

    store = None
    try:
        store = _open(parser, args)
        return _run(args, store)
    except RuntimeError as failure:
        # What a cluster says no with arrives as the client's own errors - no leader
        # this client can reach, a placement that covers no such key, a cluster that
        # never became ready - and a shell wants that on one line rather than as a
        # traceback.
        print(f"{args.command}: {failure}")
        return 1
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    sys.exit(main())