import time
import threading
import random

import pytest

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_leader, wait_for_tso_client
from oxidedb.client import LocalNodeClientFactory
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import RoutingTable
from oxidedb.raft.node import RaftCluster
from oxidedb.raft.shard_server import ShardedRaftCluster as ShardedCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType
from oxidedb.tso.tso import TSOCluster
from oxidedb.transaction.coordinator import TransactionCoordinator
from oxidedb.transaction.smart_client import SmartClient


class _ClockWithoutAGroup:
    """A clock that hands out timestamps with no group behind it.

    What a write needs from a cluster to be attempted at all, and nothing more: the
    subject of the test below is what the client says when there is nowhere to send the
    write, so the clock is the one part that does not have to be real.
    """

    def __init__(self, first: int = 100):
        self._next = first

    def get_timestamp(self) -> int:
        self._next += 1
        return self._next


class _TableThatCoversNothing:
    """A table read before the cluster had published a range: no shard holds any key.

    What a client holds in the moment between its first read of the routing table and
    the publisher's first write to it, which is what the CLI was hitting.
    """

    def table(self, refresh=False):
        return RoutingTable(version=1, shards={})


def test_readonly_transaction():
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())
    
    shard_cluster = ShardedCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(num_shards=2),
    )
    wait_for_keys_leader(shard_cluster, [b"test_key"])
    
    tso_client = wait_for_tso_client(tso_cluster)
    coordinator = TransactionCoordinator(tso_client, shard_cluster)
    
    txn_id1, start_ts1 = coordinator.begin()
    success, commit_ts1 = coordinator.commit(txn_id1)
    
    assert success, "Readonly commit should succeed"
    assert commit_ts1 == start_ts1, "Readonly transaction should use start_ts as commit_ts"
    
    txn_id2, start_ts2 = coordinator.begin()
    coordinator.add_write(txn_id2, b"test_key", b"test_value")
    success2, commit_ts2 = coordinator.commit(txn_id2)
    
    assert success2, "Write transaction commit should succeed"
    assert commit_ts2 > start_ts2, "Write transaction should get new commit_ts"
    
    coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("Readonly transaction test passed!")


def test_smart_client_retry():
    shard_cluster = ShardedCluster(num_nodes=3, num_shards=1)
    shard_cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(),
    )
    
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(free_addresses())
    
    wait_for_keys_leader(shard_cluster, [b"retry_key"])
    tso_client = wait_for_tso_client(tso_cluster)
    smart_client = SmartClient(tso_client, shard_cluster)
    
    smart_client.put(b"retry_key", b"retry_value")
    
    result = None
    for _ in range(10):
        try:
            result = smart_client.get(b"retry_key")
            break
        except RuntimeError:
            time.sleep(0.1)
    
    assert result == b"retry_value", f"SmartClient should read value after retry, got {result}"
    
    smart_client._coordinator.shutdown()
    shard_cluster.shutdown()
    tso_cluster.shutdown()
    print("SmartClient retry test passed!")


def test_a_write_with_nowhere_to_send_it_says_why():
    """``put`` may not answer False and keep the reason to itself.

    A client whose table covers no such key is where the CLI was: it was told "Key not
    written", and nothing said that the table it held had been read before the cluster
    published a range.  What the caller gets now is the reason, because that is the only
    thing it can act on - read the table again, or ask again in a moment - and "not
    written" reads the same for a table read too early, a group mid-election and a key
    that is not this client's to write.
    """
    factory = LocalNodeClientFactory(None)
    cache = RoutingCache(None, _TableThatCoversNothing(), factory=factory)
    client = SmartClient(_ClockWithoutAGroup(), None, router=cache, factory=factory)

    with pytest.raises(RuntimeError, match="No shard holds"):
        client.put(b"user:1", b"alice")


def test_read_index_consistency():
    cluster = RaftCluster(num_nodes=3)
    cluster.start(lambda: MVCCStateMachine())
    
    leader = cluster.get_node(wait_for_leader(cluster))
    
    command = leader._state_machine.serialize_command(
        CommandType.SET,
        key=b"consistency_key",
        value=b"consistency_value",
        timestamp=100,
    )
    leader.propose(command)
    time.sleep(0.5)
    
    result = leader.get(b"consistency_key")
    assert result.success, f"Read should succeed: {result.error_msg}"
    assert result.value == b"consistency_value", f"Value should be consistency_value, got {result.value}"
    
    cluster.shutdown()
    print("ReadIndex consistency test passed!")


if __name__ == "__main__":
    test_readonly_transaction()
    print("\n" + "="*60 + "\n")
    test_smart_client_retry()
    print("\n" + "="*60 + "\n")
    test_a_write_with_nowhere_to_send_it_says_why()
    print("\n" + "="*60 + "\n")
    test_read_index_consistency()
