import time
from _ports import allocate_port
import hashlib
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType, LockStatus
from oxidedb.tso.tso import TSOCluster, TSOClient
from oxidedb.transaction.coordinator import TransactionCoordinator
from oxidedb.transaction.lock_cleaner import LockCleaner


def get_free_port():
    return allocate_port()


def test_read_with_lock():
    shard_peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    shard_cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=shard_peer_addresses)
    
    time.sleep(5)
    
    leader_info = shard_cluster.get_leader_for_key(b"test_lock_key")
    assert leader_info is not None
    _, leader = leader_info
    
    set_cmd = leader._state_machine.serialize_command(CommandType.SET, key=b"test_lock_key", value=b"original", timestamp=50)
    leader.propose(set_cmd)
    time.sleep(0.5)
    
    prewrite_cmd = leader._state_machine.serialize_command(
        CommandType.PREWRITE,
        key=b"test_lock_key",
        value=b"locked_value",
        start_ts=100,
        primary_key=b"test_lock_key",
    )
    leader.propose(prewrite_cmd)
    time.sleep(0.5)
    
    lock_status = leader._state_machine.get_lock_status(b"test_lock_key")
    assert lock_status is not None
    
    result = leader.get(b"test_lock_key")
    assert not result.success, "Read should fail when key is locked"
    assert result.error_code == 101, f"Expected ERR_LOCKED (101), got {result.error_code}"
    
    commit_cmd = leader._state_machine.serialize_command(
        CommandType.COMMIT,
        key=b"test_lock_key",
        start_ts=100,
        commit_ts=200,
    )
    leader.propose(commit_cmd)
    time.sleep(0.5)
    
    result = leader.get(b"test_lock_key")
    assert result.success, f"Read should succeed after commit, got error: {result.error_msg}"
    assert result.value == b"locked_value", f"Read should return committed value, got {result.value}"
    
    shard_cluster.shutdown()
    print("Read with lock test passed!")


def test_lock_cleaner_abort():
    shard_peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    shard_cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    shard_cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=shard_peer_addresses)
    
    time.sleep(5)
    
    leader_info = shard_cluster.get_leader_for_key(b"test_key")
    assert leader_info is not None
    server_id, leader = leader_info
    
    set_cmd = leader._state_machine.serialize_command(CommandType.SET, key=b"test_key", value=b"original", timestamp=50)
    leader.propose(set_cmd)
    time.sleep(0.5)
    
    prewrite_cmd = leader._state_machine.serialize_command(
        CommandType.PREWRITE,
        key=b"test_key",
        value=b"locked_value",
        start_ts=100,
        primary_key=b"test_key",
    )
    leader.propose(prewrite_cmd)
    time.sleep(0.5)
    
    lock_status = leader._state_machine.get_lock_status(b"test_key")
    assert lock_status is not None
    
    lock_cleaner = LockCleaner(shard_cluster, poll_interval=1)
    lock_cleaner.start()
    
    time.sleep(7)
    
    leader_info = shard_cluster.get_leader_for_key(b"test_key")
    assert leader_info is not None
    _, current_leader = leader_info
    
    lock_status = current_leader._state_machine.get_lock_status(b"test_key")
    assert lock_status is None, "Lock should be cleaned"
    
    result = current_leader.get(b"test_key")
    assert result.success, f"Read should succeed, got error: {result.error_msg}"
    assert result.value == b"original", f"Value should be original, got {result.value}"
    
    lock_cleaner.stop()
    shard_cluster.shutdown()
    print("Lock cleaner abort test passed!")


if __name__ == "__main__":
    test_read_with_lock()
    print("\n" + "="*60 + "\n")
    test_lock_cleaner_abort()
