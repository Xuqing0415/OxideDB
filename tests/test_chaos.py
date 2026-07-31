import time
import threading
import random
import multiprocessing
from typing import Dict, List

from oxidedb.raft.node import RaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType


class ChaosInjector:
    def __init__(self, cluster: RaftCluster):
        self._cluster = cluster
        self._running = False
        self._thread: threading.Thread = None
    
    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._inject_faults, daemon=True)
        self._thread.start()
    
    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join()
    
    def _inject_faults(self):
        while self._running:
            time.sleep(random.uniform(2, 5))
            
            if random.random() < 0.3:
                self._kill_random_node()
            
            time.sleep(random.uniform(1, 3))
    
    def _kill_random_node(self):
        node_ids = list(self._cluster._nodes.keys())
        if not node_ids:
            return
        
        node_id = random.choice(node_ids)
        node = self._cluster.get_node(node_id)
        if node:
            print(f"[Chaos] Killing node {node_id}")
            node.shutdown()


class TransferClient:
    def __init__(self, cluster: RaftCluster):
        self._cluster = cluster
        self._results: List[bool] = []
        self._lock = threading.Lock()
        self._txn_id = 0
    
    def transfer(self, from_key: bytes, to_key: bytes, amount: int):
        leader_id = self._cluster.get_leader()
        if leader_id is None:
            self._results.append(False)
            return False
        
        leader = self._cluster.get_node(leader_id)
        if leader is None:
            self._results.append(False)
            return False
        
        try:
            from_result = leader.get(from_key)
            if not from_result.success:
                self._results.append(False)
                return False
            
            from_value = from_result.value
            from_balance = int(from_value.decode()) if from_value else 0
            
            to_result = leader.get(to_key)
            if not to_result.success:
                self._results.append(False)
                return False
            
            to_value = to_result.value
            to_balance = int(to_value.decode()) if to_value else 0
            
            if from_balance < amount:
                self._results.append(False)
                return False
            
            with self._lock:
                self._txn_id += 1
                start_ts = self._txn_id * 10000
                commit_ts = start_ts + 1
            
            prewrite_cmd1 = leader._state_machine.serialize_command(
                CommandType.PREWRITE,
                key=from_key,
                value=str(from_balance - amount).encode(),
                start_ts=start_ts,
                primary_key=from_key,
            )
            prewrite_result1 = leader.propose(prewrite_cmd1)
            if not prewrite_result1.success:
                self._results.append(False)
                return False
            
            prewrite_cmd2 = leader._state_machine.serialize_command(
                CommandType.PREWRITE,
                key=to_key,
                value=str(to_balance + amount).encode(),
                start_ts=start_ts,
                primary_key=from_key,
            )
            prewrite_result2 = leader.propose(prewrite_cmd2)
            if not prewrite_result2.success:
                rollback_cmd1 = leader._state_machine.serialize_command(
                    CommandType.ROLLBACK,
                    key=from_key,
                    start_ts=start_ts,
                )
                leader.propose(rollback_cmd1)
                self._results.append(False)
                return False
            
            commit_cmd1 = leader._state_machine.serialize_command(
                CommandType.COMMIT,
                key=from_key,
                start_ts=start_ts,
                commit_ts=commit_ts,
            )
            commit_result1 = leader.propose(commit_cmd1)
            if not commit_result1.success:
                rollback_cmd1 = leader._state_machine.serialize_command(
                    CommandType.ROLLBACK,
                    key=from_key,
                    start_ts=start_ts,
                )
                rollback_cmd2 = leader._state_machine.serialize_command(
                    CommandType.ROLLBACK,
                    key=to_key,
                    start_ts=start_ts,
                )
                leader.propose(rollback_cmd1)
                leader.propose(rollback_cmd2)
                self._results.append(False)
                return False
            
            commit_cmd2 = leader._state_machine.serialize_command(
                CommandType.COMMIT,
                key=to_key,
                start_ts=start_ts,
                commit_ts=commit_ts,
            )
            commit_result2 = leader.propose(commit_cmd2)
            
            success = commit_result2.success
            self._results.append(success)
            return success
        except Exception:
            self._results.append(False)
            return False


def test_transfer_no_chaos():
    cluster = RaftCluster(num_nodes=3)
    cluster.start(lambda: MVCCStateMachine())
    time.sleep(3)
    
    leader = cluster.get_node(cluster.get_leader())
    
    initial_balances = {
        b"account_a": b"1000",
        b"account_b": b"1000",
        b"account_c": b"1000",
        b"account_d": b"1000",
        b"account_e": b"1000",
    }
    
    for key, value in initial_balances.items():
        command = leader._state_machine.serialize_command(
            CommandType.SET,
            key=key,
            value=value,
            timestamp=100,
        )
        leader.propose(command)
    
    time.sleep(1)
    
    total_initial = sum(int(v.decode()) for v in initial_balances.values())
    print(f"Initial total: {total_initial}")
    
    clients = []
    for _ in range(5):
        clients.append(TransferClient(cluster))
    
    threads = []
    for client in clients:
        def run_transfers(c):
            for _ in range(20):
                accounts = list(initial_balances.keys())
                from_acc = random.choice(accounts)
                to_acc = random.choice([a for a in accounts if a != from_acc])
                amount = random.randint(1, 100)
                c.transfer(from_acc, to_acc, amount)
                time.sleep(random.uniform(0.01, 0.05))
        
        t = threading.Thread(target=run_transfers, args=(client,), daemon=True)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    time.sleep(1)
    
    leader = cluster.get_node(cluster.get_leader())
    total_final = 0
    final_balances = {}
    for key in initial_balances.keys():
        result = leader.get(key)
        if result.success and result.value:
            balance = int(result.value.decode())
            final_balances[key] = balance
            total_final += balance
    
    print(f"Final balances: {final_balances}")
    print(f"Final total: {total_final}")
    
    success_count = sum(len(c._results) - c._results.count(False) for c in clients)
    total_count = sum(len(c._results) for c in clients)
    print(f"Transfer success rate: {success_count}/{total_count} = {success_count/total_count*100:.1f}%")
    
    cluster.shutdown()
    
    assert total_final == total_initial, f"Total should be {total_initial}, got {total_final}"
    print("Transfer without chaos test passed!")


def test_leader_failure_recovery():
    cluster = RaftCluster(num_nodes=3)
    cluster.start(lambda: MVCCStateMachine())
    time.sleep(3)
    
    leader = cluster.get_node(cluster.get_leader())
    original_leader_id = cluster.get_leader()
    
    command = leader._state_machine.serialize_command(
        CommandType.SET,
        key=b"test_key",
        value=b"test_value",
        timestamp=100,
    )
    leader.propose(command)
    time.sleep(0.5)
    
    print(f"Original leader: {original_leader_id}")
    
    leader.shutdown()
    print(f"Leader {original_leader_id} shut down")
    
    time.sleep(3)
    
    new_leader_id = cluster.get_leader()
    print(f"New leader: {new_leader_id}")
    
    new_leader = cluster.get_node(new_leader_id)
    result = new_leader.get(b"test_key")
    
    cluster.shutdown()
    
    assert new_leader_id is not None, "Should have new leader"
    assert new_leader_id != original_leader_id, "Should have different leader"
    assert result.success, f"Read should succeed: {result.error_msg}"
    assert result.value == b"test_value", f"Value should be test_value, got {result.value}"
    print("Leader failure recovery test passed!")


if __name__ == "__main__":
    test_transfer_no_chaos()
    print("\n" + "="*60 + "\n")
    test_leader_failure_recovery()