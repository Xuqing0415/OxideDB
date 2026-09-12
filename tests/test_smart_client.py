import time
import threading
import random

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_leader, wait_for_tso_client
from oxidedb.raft.node import RaftCluster
from oxidedb.raft.shard_server import ShardedRaftCluster as ShardedCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType
from oxidedb.tso.tso import TSOCluster
from oxidedb.transaction.coordinator import TransactionCoordinator
from oxidedb.transaction.smart_client import SmartClient


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
    test_read_index_consistency()
