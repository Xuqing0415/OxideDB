"""The routing rule must be the same in every component that routes.

There used to be two: ``shard/router.py`` hashed the key with md5 while
``shard_server.py`` looked it up in a range map, so the client-side router and
the server disagreed about which shard owned a key.  Every path now goes through
``oxidedb.shard.router.locate``, and this test keeps it that way: the moment a
second rule appears, one of the assertions below disagrees.
"""

import random

from oxidedb.raft.shard_server import ShardedRaftCluster, ShardServer
from oxidedb.shard.router import ShardRouter, default_range_map, locate
from oxidedb.transaction.coordinator import TransactionCoordinator


def _keys():
    """Edge cases plus 100 keys with random first bytes."""
    keys = [
        b"",
        b"\x00",
        b"\x00\x00",
        b"a",
        b"key_shard_a",
        b"key_shard_b",
        b"\x7f",
        b"\x80",
        b"\x80key",
        b"\xfe",
        b"\xff",
    ]
    rng = random.Random(20260913)
    for _ in range(100):
        keys.append(bytes(rng.randrange(256) for _ in range(rng.randrange(1, 9))))
    return keys


def _routers(range_map, num_shards):
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=num_shards)
    cluster.update_range_map(range_map)
    server = ShardServer(node_id=1, num_shards=num_shards, total_nodes=3)
    server.set_range_map(range_map)
    router = ShardRouter(num_shards=num_shards, range_map=range_map)
    coordinator = TransactionCoordinator(tso_client=None, shard_server=cluster)
    return router, server, coordinator


def test_every_router_agrees_on_the_default_map():
    range_map = default_range_map(2)
    router, server, coordinator = _routers(range_map, num_shards=2)

    for key in _keys():
        expected = locate(range_map, key)
        assert router.get_shard_id(key) == expected, key
        assert server._get_shard_id(key) == expected, key
        assert coordinator._get_shard_id(key) == expected, key


def test_every_router_agrees_after_a_split():
    # What split_shard produces: one wide shard 0 and a new shard 1.
    range_map = {0: (b"", b"n"), 1: (b"n", b"\xff")}
    router, server, coordinator = _routers(range_map, num_shards=2)

    for key in _keys():
        expected = locate(range_map, key)
        assert router.get_shard_id(key) == expected, key
        assert server._get_shard_id(key) == expected, key
        assert coordinator._get_shard_id(key) == expected, key


def test_router_follows_a_new_range_map():
    router = ShardRouter(num_shards=2)
    assert router.get_shard_id(b"n") == 0

    router.set_range_map({0: (b"", b"n"), 1: (b"n", b"\xff")})
    assert router.get_shard_id(b"n") == 1


def test_the_boundary_is_written_down():
    """Pin the map the cross-shard tests depend on.

    ``key_shard_a`` and ``key_shard_b`` share a first byte (0x6b), so on a
    2-shard default map both belong to shard 0 - which is why
    test_cross_shard_transaction.py could not have been exercising two shards.
    """
    range_map = default_range_map(2)
    assert range_map == {0: (b"\x00", b"\x80"), 1: (b"\x80", b"\xff")}

    assert locate(range_map, b"key_shard_a") == 0
    assert locate(range_map, b"key_shard_b") == 0
    assert locate(range_map, b"\x80key") == 1
    assert locate(range_map, b"\xffkey") == 0  # past the last range, falls back