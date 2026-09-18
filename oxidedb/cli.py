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
imported here rather than worked out a second time.  ``--shards`` is how many shards
that node was started with, since the two group ports sit above the shards; it
defaults to the launcher's own default.  Several nodes may be named, comma-separated
or repeated, and all of them are used as seeds: no client knows which member of either
group leads, so one address it cannot use would otherwise be the end of the walk.
"""

import argparse
import sys

from oxidedb.client import RemoteNodeClientFactory
from oxidedb.database import Database
from oxidedb.launcher import DEFAULT_NUM_SHARDS, ports_for
from oxidedb.metadata.cache import RoutingCache
from oxidedb.transaction.smart_client import SmartClient


class ClusterStore:
    """The four commands against a cluster, over the transports that reach it.

    The transports are why this is an object at all: a factory owns a channel per
    address, and something has to close them when the command is over.  Everything
    else is delegation - the client underneath is the same ``SmartClient`` a test
    drives, so a key set from a shell travels the path a key set from a test does,
    through the same routing table and the same transaction coordinator.
    """

    def __init__(self, servers, num_shards):
        metadata_seeds, tso_seeds = _group_seeds(servers, num_shards)
        self._factory = RemoteNodeClientFactory(metadata_seeds=metadata_seeds,
                                                tso_seeds=tso_seeds)
        #: The placement, read from the group the first time it is needed and kept
        #: for the life of the command.  ``None`` where a cluster would go because
        #: there is no cluster object here to ask: what this client knows about
        #: placement is the table, which is the point of the table.
        self._table = RoutingCache(None, self._factory.metadata_client(),
                                   factory=self._factory)
        self._client = SmartClient(self._factory.tso_client(), None,
                                   router=self._table, factory=self._factory)

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


def _group_seeds(servers, num_shards):
    """The table's addresses and the clock's, derived from the nodes' own blocks.

    A node takes shard ``s`` at ``port + 100 * s``, the routing table's group above
    the last shard, and the timestamp group above that.  That is the arithmetic of
    ``oxidedb/launcher.py``, imported rather than repeated, so an address worked out
    here is the address those nodes bound - which is the whole reason it lives in one
    function there.
    """
    metadata, tso = [], []
    for server in servers:
        host, port = server.rsplit(":", 1)
        ports = ports_for(int(port), num_shards)
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
        "--shards",
        type=int,
        default=DEFAULT_NUM_SHARDS,
        metavar="N",
        help="how many shards the cluster behind --server was started with, which "
             "is what locates its group ports (default: %(default)s, the launcher's "
             "own default)",
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

    return ClusterStore(servers, args.shards)


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

    store = _open(parser, args)
    try:
        return _run(args, store)
    except RuntimeError as failure:
        # What a cluster says no with arrives as the client's own errors - no leader
        # this client can reach, a placement that covers no such key - and a shell
        # wants that on one line rather than as a traceback.
        print(f"{args.command}: {failure}")
        return 1
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())