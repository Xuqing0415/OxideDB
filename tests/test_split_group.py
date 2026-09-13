"""A shard a split creates is a Raft group, and it is served like the others.

The new shard used to be a group of one kind in one mode and another kind in the other:
in-process nodes wired to each other by a lambda that reached into the cluster's own
objects, and, in network mode, three nodes with no listening port at all.  Either way
the range the routing table was about to publish belonged to a group no client could
reach - the table would name an address nothing answered on, which is the one thing a
routing table must not do.

What these tests pin: after a split, every node of the cluster holds a group for the new
shard; in network mode each of them is listening on the address the table will publish,
and the group elects a leader and replicates to its followers over the wire; in process
there is no address to publish, and the group still elects a leader and replicates.
"""

import socket

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_until
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import CommandType, MVCCStateMachine
from oxidedb.shard.router import locate

MOVED_KEY = b"z_key"   # at or above the split point, so the new shard owns it
NEW_SHARD = 1


def _set(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp)


def _replicas(cluster, shard_id):
    return {node_id: cluster.get_shard_server(node_id).get_shard_node(shard_id)
            for node_id in (1, 2, 3)}


def _serves(cluster, shard_id, key, value):
    """Every replica of ``shard_id`` has applied ``key`` with ``value``."""
    def ready():
        for replica in _replicas(cluster, shard_id).values():
            version = replica._state_machine._storage.get_latest_version(key)
            if version is None or version.value != value:
                return None
        return True

    return wait_until(ready, message=f"shard {shard_id} never replicated {key!r}")


def _split_with_a_row():
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        # Two shards' worth of ports: the split binds one for the shard it creates.
        peer_addresses=free_addresses(num_shards=2),
        lock_cleaner_interval=None,
    )
    wait_for_keys_leader(cluster, [MOVED_KEY])
    source = cluster.get_leader_for_key(MOVED_KEY)[1]
    assert source.propose(_set(source._state_machine, MOVED_KEY, b"moved", 1)).success
    return cluster


def test_the_new_shard_is_a_group_on_every_node_with_a_port_of_its_own():
    cluster = _split_with_a_row()
    try:
        assert cluster.split_shard(0, b"n"), "the split has to happen"
        assert locate(cluster.range_map(), MOVED_KEY) == NEW_SHARD

        for node_id, replica in _replicas(cluster, NEW_SHARD).items():
            assert replica is not None, f"node {node_id} does not serve the new shard"

        addresses = cluster.shard_addresses(NEW_SHARD)
        assert sorted(addresses) == [1, 2, 3], "every node publishes where it serves it"
        for address in addresses.values():
            host, port = address.split(":")
            with socket.create_connection((host, int(port)), timeout=2):
                pass  # the address the table will hand out is really answering

        leader_id, _ = wait_until(lambda: cluster.shard_leader(NEW_SHARD),
                                  message="the new shard elected no leader")
        leader = cluster.get_shard_server(leader_id).get_shard_node(NEW_SHARD)
        assert leader is not None

        # The row the split copied is readable from the new shard's own leader, and
        # the group takes a new write and replicates it to its followers.
        assert leader.get(MOVED_KEY).value == b"moved"
        assert leader.propose(_set(leader._state_machine, MOVED_KEY, b"again", 2)).success
        assert _serves(cluster, NEW_SHARD, MOVED_KEY, b"again")
    finally:
        cluster.shutdown()


def test_an_in_process_cluster_serves_the_shard_a_split_created():
    """No sockets, but the same group: a leader, followers, and replicated writes."""
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start(lambda: MVCCStateMachine(), lock_cleaner_interval=None)
    try:
        wait_for_keys_leader(cluster, [MOVED_KEY])
        source = cluster.get_leader_for_key(MOVED_KEY)[1]
        assert source.propose(_set(source._state_machine, MOVED_KEY, b"moved", 1)).success

        assert cluster.split_shard(0, b"n"), "the split has to happen"
        assert locate(cluster.range_map(), MOVED_KEY) == NEW_SHARD
        assert cluster.shard_addresses(NEW_SHARD) == {}, "nothing listens in process"

        leader_id, _ = wait_until(lambda: cluster.shard_leader(NEW_SHARD),
                                  message="the new shard elected no leader")
        leader = cluster.get_shard_server(leader_id).get_shard_node(NEW_SHARD)
        assert leader.get(MOVED_KEY).value == b"moved"
        assert leader.propose(_set(leader._state_machine, MOVED_KEY, b"again", 2)).success
        assert _serves(cluster, NEW_SHARD, MOVED_KEY, b"again")
    finally:
        cluster.shutdown()
