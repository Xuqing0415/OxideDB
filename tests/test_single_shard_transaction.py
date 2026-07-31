import time
import socket
from oxidedb.raft.node import RaftCluster, NodeState
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType, ApplyResult


def get_free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def test_single_shard_prewrite_commit():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    cluster = RaftCluster(num_nodes=3)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None
    leader = cluster.get_node(leader_id)
    
    key = b"txn_key"
    value = b"txn_value"
    start_ts = 100
    commit_ts = 200
    primary_key = key
    
    prewrite_cmd = leader._state_machine.serialize_command(
        CommandType.PREWRITE,
        key=key,
        value=value,
        start_ts=start_ts,
        primary_key=primary_key,
    )
    
    prewrite_result = leader.propose(prewrite_cmd)
    assert prewrite_result.success, f"Prewrite should succeed: {prewrite_result.error_msg}"
    
    time.sleep(0.5)
    
    lock_status = leader._state_machine.get_lock_status(key)
    assert lock_status is not None, "Lock should exist after prewrite"
    assert lock_status["start_ts"] == start_ts
    assert lock_status["value"] == value
    
    intent = leader._state_machine._storage.get_write_intent(key, start_ts)
    assert intent == value, "Write intent should store value"
    
    commit_cmd = leader._state_machine.serialize_command(
        CommandType.COMMIT,
        key=key,
        start_ts=start_ts,
        commit_ts=commit_ts,
    )
    
    commit_result = leader.propose(commit_cmd)
    assert commit_result.success, f"Commit should succeed: {commit_result.error_msg}"
    
    time.sleep(0.5)
    
    lock_status = leader._state_machine.get_lock_status(key)
    assert lock_status is None, "Lock should be released after commit"
    
    read_result = leader._state_machine.get(key)
    assert read_result.success, f"Read should succeed: {read_result.error_msg}"
    stored_value = read_result.value
    assert stored_value == value, f"Value should be {value}, got {stored_value}"
    
    write_record = leader._state_machine._storage.get_latest_write(key)
    assert write_record is not None, "Write record should exist"
    assert write_record["commit_ts"] == commit_ts
    assert write_record["start_ts"] == start_ts
    
    print(f"Prewrite/Commit test: key={key.decode()}, value={value.decode()}")
    print(f"  start_ts={start_ts}, commit_ts={commit_ts}")
    print(f"  Lock released: {lock_status is None}")
    print(f"  Data readable: {stored_value == value}")
    
    cluster.shutdown()
    print("Single shard Prewrite/Commit test passed!")


def test_single_shard_prewrite_rollback():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    cluster = RaftCluster(num_nodes=3)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None
    leader = cluster.get_node(leader_id)
    
    key = b"rollback_key"
    original_value = b"original"
    new_value = b"new_value"
    
    set_cmd = leader._state_machine.serialize_command(
        CommandType.SET,
        key=key,
        value=original_value,
        timestamp=50,
    )
    leader.propose(set_cmd)
    time.sleep(0.5)
    
    start_ts = 100
    primary_key = key
    
    prewrite_cmd = leader._state_machine.serialize_command(
        CommandType.PREWRITE,
        key=key,
        value=new_value,
        start_ts=start_ts,
        primary_key=primary_key,
    )
    
    prewrite_result = leader.propose(prewrite_cmd)
    assert prewrite_result.success, f"Prewrite should succeed: {prewrite_result.error_msg}"
    
    time.sleep(0.5)
    
    lock_status = leader._state_machine.get_lock_status(key)
    assert lock_status is not None, "Lock should exist after prewrite"
    
    rollback_cmd = leader._state_machine.serialize_command(
        CommandType.ROLLBACK,
        key=key,
        start_ts=start_ts,
    )
    
    rollback_result = leader.propose(rollback_cmd)
    assert rollback_result.success, f"Rollback should succeed: {rollback_result.error_msg}"
    
    time.sleep(0.5)
    
    lock_status = leader._state_machine.get_lock_status(key)
    assert lock_status is None, "Lock should be released after rollback"
    
    read_result = leader._state_machine.get(key)
    assert read_result.success, f"Read should succeed: {read_result.error_msg}"
    stored_value = read_result.value
    assert stored_value == original_value, f"Value should rollback to {original_value}, got {stored_value}"
    
    latest_version = leader._state_machine._storage.get_latest_version(key)
    assert latest_version is not None
    assert latest_version.timestamp == 50
    
    print(f"Rollback test: key={key.decode()}")
    print(f"  Original value restored: {stored_value == original_value}")
    
    cluster.shutdown()
    print("Single shard Rollback test passed!")


def test_single_shard_lock_conflict():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    cluster = RaftCluster(num_nodes=3)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None
    leader = cluster.get_node(leader_id)
    
    key = b"conflict_key"
    start_ts_1 = 100
    start_ts_2 = 150
    
    prewrite_cmd_1 = leader._state_machine.serialize_command(
        CommandType.PREWRITE,
        key=key,
        value=b"value1",
        start_ts=start_ts_1,
        primary_key=key,
    )
    
    prewrite_result_1 = leader.propose(prewrite_cmd_1)
    assert prewrite_result_1.success, f"First prewrite should succeed: {prewrite_result_1.error_msg}"
    
    time.sleep(0.5)
    
    prewrite_cmd_2 = leader._state_machine.serialize_command(
        CommandType.PREWRITE,
        key=key,
        value=b"value2",
        start_ts=start_ts_2,
        primary_key=key,
    )
    
    prewrite_result_2 = leader.propose(prewrite_cmd_2)
    assert not prewrite_result_2.success, "Second prewrite should fail due to lock conflict"
    assert prewrite_result_2.error_code == 101, f"Expected error code 101, got {prewrite_result_2.error_code}"
    
    print(f"Lock conflict test: Second prewrite correctly rejected with error {prewrite_result_2.error_code}")
    
    rollback_cmd = leader._state_machine.serialize_command(
        CommandType.ROLLBACK,
        key=key,
        start_ts=start_ts_1,
    )
    leader.propose(rollback_cmd)
    
    cluster.shutdown()
    print("Single shard lock conflict test passed!")


def test_single_shard_write_conflict():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    cluster = RaftCluster(num_nodes=3)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None
    leader = cluster.get_node(leader_id)
    
    key = b"write_conflict_key"
    
    set_cmd = leader._state_machine.serialize_command(
        CommandType.SET,
        key=key,
        value=b"committed_value",
        timestamp=200,
    )
    leader.propose(set_cmd)
    time.sleep(0.5)
    
    start_ts = 100
    
    prewrite_cmd = leader._state_machine.serialize_command(
        CommandType.PREWRITE,
        key=key,
        value=b"new_value",
        start_ts=start_ts,
        primary_key=key,
    )
    
    prewrite_result = leader.propose(prewrite_cmd)
    assert not prewrite_result.success, "Prewrite should fail due to newer write"
    assert prewrite_result.error_code == 102, f"Expected error code 102, got {prewrite_result.error_code}"
    
    print(f"Write conflict test: Prewrite correctly rejected with error {prewrite_result.error_code}")
    
    cluster.shutdown()
    print("Single shard write conflict test passed!")


if __name__ == "__main__":
    test_single_shard_prewrite_commit()
    print("\n" + "="*60 + "\n")
    test_single_shard_prewrite_rollback()
    print("\n" + "="*60 + "\n")
    test_single_shard_lock_conflict()
    print("\n" + "="*60 + "\n")
    test_single_shard_write_conflict()