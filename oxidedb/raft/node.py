import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Callable
from enum import Enum
from .state_machine import StateMachine, ApplyResult, ReadResult, ErrorCode


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class LogEntry:
    def __init__(self, term: int, index: int, command: bytes):
        self.term = term
        self.index = index
        self.command = command


class RequestVoteRequest:
    def __init__(self, term: int, candidate_id: int, last_log_index: int, last_log_term: int):
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term


class RequestVoteResponse:
    def __init__(self, term: int, vote_granted: bool):
        self.term = term
        self.vote_granted = vote_granted


class AppendEntriesRequest:
    def __init__(self, term: int, leader_id: int, prev_log_index: int, prev_log_term: int, entries: List[LogEntry], leader_commit: int):
        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries
        self.leader_commit = leader_commit


class AppendEntriesResponse:
    def __init__(self, term: int, success: bool, match_index: int):
        self.term = term
        self.success = success
        self.match_index = match_index


class MemoryRaftNode:
    def __init__(
        self,
        node_id: int,
        peers: List[int],
        state_machine: StateMachine,
        get_peer_node: Optional[Callable[[int], 'MemoryRaftNode']] = None,
        election_timeout_min: int = 150,
        election_timeout_max: int = 300,
        heartbeat_interval: int = 50,
        storage: Optional['RaftStorage'] = None,
        network_client: Optional['RaftNetworkClient'] = None,
        grpc_server: Optional = None,
    ):
        self._node_id = node_id
        self._peers = peers
        self._state_machine = state_machine
        self._get_peer_node = get_peer_node
        self._storage = storage
        self._network_client = network_client
        self._grpc_server = grpc_server
        
        self._state = NodeState.FOLLOWER
        self._current_term = 0
        self._voted_for: Optional[int] = None
        self._log: List[LogEntry] = []
        
        self._commit_index = 0
        self._last_applied = 0
        
        self._next_index: Dict[int, int] = {}
        self._match_index: Dict[int, int] = {}
        
        self._election_timeout_min = election_timeout_min
        self._election_timeout_max = election_timeout_max
        self._heartbeat_interval = heartbeat_interval
        
        self._election_timer: Optional[threading.Timer] = None
        self._heartbeat_timer: Optional[threading.Timer] = None
        
        self._lock = threading.RLock()
        
        self._votes_received: Dict[int, bool] = {}
        
        self._apply_cond = threading.Condition(self._lock)
        
        self._shutdown_flag = False
        
        self._apply_results: Dict[int, ApplyResult] = {}
        
        self._load_from_storage()
        
        self._start_election_timer()
    
    def _load_from_storage(self) -> None:
        if self._storage is None:
            return
        
        current_term, voted_for = self._storage.load_meta()
        self._current_term = current_term
        self._voted_for = voted_for
        
        self._log = self._storage.load_log()
        
        self._last_applied = len(self._log)
        
        for entry in self._log:
            self._state_machine.apply(entry.command)
    
    def _save_meta(self) -> None:
        if self._storage is not None:
            self._storage.save_meta(self._current_term, self._voted_for)
    
    def _save_log_entry(self, entry: LogEntry) -> None:
        if self._storage is not None:
            self._storage.append_log_entry(entry)

    @property
    def node_id(self) -> int:
        return self._node_id

    @property
    def state(self) -> NodeState:
        with self._lock:
            return self._state

    @property
    def current_term(self) -> int:
        with self._lock:
            return self._current_term

    @property
    def commit_index(self) -> int:
        with self._lock:
            return self._commit_index

    @property
    def last_applied(self) -> int:
        with self._lock:
            return self._last_applied

    @property
    def log_length(self) -> int:
        with self._lock:
            return len(self._log)

    def _reset_election_timer(self) -> None:
        with self._lock:
            if self._election_timer:
                self._election_timer.cancel()
            timeout = random.randint(self._election_timeout_min, self._election_timeout_max) / 1000.0
            self._election_timer = threading.Timer(timeout, self._start_election)
            self._election_timer.daemon = True
            self._election_timer.start()

    def _start_election_timer(self) -> None:
        with self._lock:
            if self._election_timer:
                self._election_timer.cancel()
            timeout = random.randint(self._election_timeout_min, self._election_timeout_max) / 1000.0
            self._election_timer = threading.Timer(timeout, self._start_election)
            self._election_timer.daemon = True
            self._election_timer.start()

    def _cancel_election_timer(self) -> None:
        with self._lock:
            if self._election_timer:
                self._election_timer.cancel()
                self._election_timer = None

    def _start_heartbeat(self) -> None:
        with self._lock:
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()
            interval = self._heartbeat_interval / 1000.0
            self._heartbeat_timer = threading.Timer(interval, self._send_heartbeats)
            self._heartbeat_timer.daemon = True
            self._heartbeat_timer.start()

    def _cancel_heartbeat(self) -> None:
        with self._lock:
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()
                self._heartbeat_timer = None

    def _start_election(self) -> None:
        with self._lock:
            if self._state == NodeState.LEADER:
                return
            
            self._state = NodeState.CANDIDATE
            self._current_term += 1
            self._voted_for = self._node_id
            self._votes_received = {self._node_id: True}
            
            self._save_meta()
            
            self._reset_election_timer()
        
        for peer_id in self._peers:
            threading.Thread(target=self._request_vote, args=(peer_id,), daemon=True).start()

    def _request_vote(self, peer_id: int) -> None:
        try:
            with self._lock:
                if self._shutdown_flag:
                    return
                last_log_index = len(self._log)
                last_log_term = self._log[-1].term if self._log else 0
                term = self._current_term
                current_state = self._state
            
            if current_state != NodeState.CANDIDATE:
                return
            
            request = RequestVoteRequest(
                term=term,
                candidate_id=self._node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            )
            
            if self._network_client is not None:
                response = self._network_client.request_vote(peer_id, request)
            else:
                peer_node = self._get_peer_node(peer_id)
                if peer_node is None:
                    return
                response = peer_node.request_vote(request)
            
            if response is None:
                return
            
            with self._lock:
                if response.term > self._current_term:
                    self._current_term = response.term
                    self._state = NodeState.FOLLOWER
                    self._voted_for = None
                    self._votes_received.clear()
                    self._reset_election_timer()
                    return
                
                if self._state != NodeState.CANDIDATE:
                    return
                
                if response.vote_granted:
                    self._votes_received[peer_id] = True
                    self._check_vote_count()
        except Exception:
            pass

    def _check_vote_count(self) -> None:
        with self._lock:
            if self._state != NodeState.CANDIDATE:
                return
            
            total_nodes = len(self._peers) + 1
            majority = (total_nodes // 2) + 1
            
            if len(self._votes_received) >= majority:
                self._state = NodeState.LEADER
                self._votes_received.clear()
                self._cancel_election_timer()
                
                for peer_id in self._peers:
                    self._next_index[peer_id] = len(self._log) + 1
                    self._match_index[peer_id] = 0
                
                self._send_heartbeats()
                self._start_heartbeat()

    def _send_heartbeats(self) -> None:
        with self._lock:
            if self._state != NodeState.LEADER:
                return
            
            peers = list(self._peers)
            term = self._current_term
            next_index = dict(self._next_index)
            match_index = dict(self._match_index)
            log = list(self._log)
            leader_commit = self._commit_index
        
        for peer_id in peers:
            threading.Thread(target=self._append_entries, args=(peer_id, term, next_index.get(peer_id), log, leader_commit), daemon=True).start()
        
        self._start_heartbeat()

    def _append_entries(self, peer_id: int, term: int, next_index: Optional[int], log: List[LogEntry], leader_commit: int) -> None:
        try:
            if next_index is None:
                next_index = len(log) + 1
            
            prev_log_index = next_index - 1
            prev_log_term = log[prev_log_index - 1].term if prev_log_index > 0 else 0
            
            entries = []
            if prev_log_index < len(log):
                entries = log[prev_log_index:]
            
            request = AppendEntriesRequest(
                term=term,
                leader_id=self._node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=leader_commit,
            )
            
            if self._network_client is not None:
                response = self._network_client.append_entries(peer_id, request)
            else:
                peer_node = self._get_peer_node(peer_id)
                if peer_node is None:
                    return
                response = peer_node.append_entries(request)
            
            if response is None:
                return
            
            with self._lock:
                if response.term > self._current_term:
                    self._current_term = response.term
                    self._state = NodeState.FOLLOWER
                    self._voted_for = None
                    self._cancel_heartbeat()
                    self._reset_election_timer()
                    return
                
                if self._state != NodeState.LEADER:
                    return
                
                if response.success:
                    self._next_index[peer_id] = response.match_index + 1
                    self._match_index[peer_id] = response.match_index
                    self._update_commit_index()
                else:
                    self._next_index[peer_id] = max(1, self._next_index.get(peer_id, 1) - 1)
        except Exception:
            pass

    def _update_commit_index(self) -> None:
        with self._lock:
            match_indices = sorted(self._match_index.values())
            match_indices.append(len(self._log))
            
            n = len(self._peers) + 1
            majority = (n // 2) + 1
            
            if len(match_indices) >= majority:
                new_commit_index = match_indices[-majority]
                if new_commit_index > self._commit_index:
                    if new_commit_index > 0 and new_commit_index <= len(self._log):
                        if self._log[new_commit_index - 1].term == self._current_term:
                            self._commit_index = new_commit_index
                            self._apply_committed_entries()

    def _apply_committed_entries(self) -> None:
        with self._lock:
            while self._last_applied < self._commit_index:
                self._last_applied += 1
                if self._last_applied - 1 < len(self._log):
                    entry = self._log[self._last_applied - 1]
                    result = self._state_machine.apply(entry.command)
                    self._apply_results[self._last_applied] = result
            
            with self._apply_cond:
                self._apply_cond.notify_all()

    def _wait_for_apply(self, index: int) -> None:
        with self._lock:
            while self._last_applied < index:
                self._apply_cond.wait()

    def request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        with self._lock:
            last_log_index = len(self._log)
            last_log_term = self._log[-1].term if self._log else 0
            
            if request.term > self._current_term:
                self._current_term = request.term
                self._state = NodeState.FOLLOWER
                self._voted_for = None
                self._votes_received.clear()
                self._save_meta()
                self._reset_election_timer()
            
            if request.term < self._current_term:
                return RequestVoteResponse(term=self._current_term, vote_granted=False)
            
            can_vote = self._voted_for is None or self._voted_for == request.candidate_id
            log_ok = (request.last_log_term > last_log_term or
                      (request.last_log_term == last_log_term and 
                       request.last_log_index >= last_log_index))
            
            if can_vote and log_ok:
                self._voted_for = request.candidate_id
                self._save_meta()
                self._reset_election_timer()
                return RequestVoteResponse(term=self._current_term, vote_granted=True)
            
            return RequestVoteResponse(term=self._current_term, vote_granted=False)

    def append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        with self._lock:
            term_changed = False
            if request.term > self._current_term:
                self._current_term = request.term
                self._state = NodeState.FOLLOWER
                self._voted_for = None
                self._votes_received.clear()
                term_changed = True
            
            if request.term < self._current_term:
                if term_changed:
                    self._save_meta()
                return AppendEntriesResponse(term=self._current_term, success=False, match_index=0)
            
            self._reset_election_timer()
            
            if request.prev_log_index > 0:
                if request.prev_log_index > len(self._log):
                    if term_changed:
                        self._save_meta()
                    return AppendEntriesResponse(term=self._current_term, success=False, match_index=0)
                
                if self._log[request.prev_log_index - 1].term != request.prev_log_term:
                    if term_changed:
                        self._save_meta()
                    return AppendEntriesResponse(term=self._current_term, success=False, match_index=0)
            
            for entry in request.entries:
                if entry.index <= len(self._log):
                    if self._log[entry.index - 1].term != entry.term:
                        del self._log[entry.index - 1:]
                        self._log.append(entry)
                        self._save_log_entry(entry)
                else:
                    self._log.append(entry)
                    self._save_log_entry(entry)
            
            if term_changed:
                self._save_meta()
            
            match_index = len(self._log)
            
            if request.leader_commit > self._commit_index:
                self._commit_index = min(request.leader_commit, len(self._log))
                self._apply_committed_entries()
            
            return AppendEntriesResponse(term=self._current_term, success=True, match_index=match_index)

    def propose(self, command: bytes, timeout: float = 5.0) -> ApplyResult:
        with self._lock:
            if self._state != NodeState.LEADER:
                return ApplyResult.failure(1, "Not leader")
            
            entry = LogEntry(term=self._current_term, index=len(self._log) + 1, command=command)
            self._log.append(entry)
            self._save_log_entry(entry)
            entry_term = self._current_term
            entry_index = entry.index
            
            peers = list(self._peers)
            term = self._current_term
            next_index = dict(self._next_index)
            log = list(self._log)
            leader_commit = self._commit_index
        
        for peer_id in peers:
            threading.Thread(target=self._append_entries, args=(peer_id, term, next_index.get(peer_id), log, leader_commit), daemon=True).start()
        
        with self._lock:
            deadline = time.time() + timeout
            while self._commit_index < entry_index:
                if self._state != NodeState.LEADER or self._current_term != entry_term:
                    return ApplyResult.failure(2, "Lost leadership")
                remaining = deadline - time.time()
                if remaining <= 0:
                    return ApplyResult.failure(3, "Proposal timeout")
                self._apply_cond.wait(timeout=remaining)
            
            if entry_index <= len(self._log) and self._log[entry_index - 1].term != entry_term:
                return ApplyResult.failure(4, "Entry overwritten")
        
        with self._lock:
            result = self._apply_results.get(entry_index)
            if result is not None:
                return result
        
        return ApplyResult.success()

    def get(self, key: bytes) -> ReadResult:
        with self._lock:
            if self._state != NodeState.LEADER:
                return ReadResult.failure(ErrorCode.ERR_NOT_LEADER, "Not leader")
            
            read_index = self._commit_index
            current_term = self._current_term
            
            peers = list(self._peers)
            next_index = dict(self._next_index)
            log = list(self._log)
            leader_commit = self._commit_index
        
        if peers:
            responses = []
            for peer_id in peers:
                try:
                    response = self._append_entries(peer_id, current_term, next_index.get(peer_id, 1), log, leader_commit)
                    responses.append(response)
                except Exception:
                    responses.append(None)
            
            with self._lock:
                if self._state != NodeState.LEADER or self._current_term != current_term:
                    return ReadResult.failure(ErrorCode.ERR_NOT_LEADER, "Lost leadership during read")
        
        with self._lock:
            self._wait_for_apply(read_index)
            
            if self._state != NodeState.LEADER:
                return ReadResult.failure(ErrorCode.ERR_NOT_LEADER, "Not leader")
            
            return self._state_machine.get(key)

    def scan(self, start_key: bytes, end_key: bytes) -> List[tuple]:
        with self._lock:
            if self._state != NodeState.LEADER:
                return []
            
            self._wait_for_apply(self._commit_index)
            
            return self._state_machine.scan(start_key, end_key)

    def shutdown(self) -> None:
        self._cancel_election_timer()
        self._cancel_heartbeat()
        
        if self._grpc_server is not None:
            self._grpc_server.stop(grace=0.5)
        
        if self._network_client is not None:
            self._network_client.shutdown()
        
        with self._lock:
            self._state = NodeState.FOLLOWER
            self._shutdown_flag = True
    
    def wait_for_shutdown(self):
        if self._grpc_server is not None:
            self._grpc_server.wait_for_termination(timeout=5)


class RaftCluster:
    def __init__(self, num_nodes: int = 3):
        self._nodes: Dict[int, MemoryRaftNode] = {}
        self._state_machines: Dict[int, StateMachine] = {}
        self._storages: Dict[int, 'RaftStorage'] = {}
        self._network_clients: Dict[int, 'RaftNetworkClient'] = {}
        self._grpc_servers: Dict[int, any] = {}
        self._num_nodes = num_nodes

    def start(self, state_machine_factory: Callable[[], StateMachine], storage_factory: Optional[Callable[[int], 'RaftStorage']] = None):
        for node_id in range(1, self._num_nodes + 1):
            state_machine = state_machine_factory()
            self._state_machines[node_id] = state_machine
            
            storage = None
            if storage_factory is not None:
                storage = storage_factory(node_id)
                self._storages[node_id] = storage
            
            peers = [nid for nid in range(1, self._num_nodes + 1) if nid != node_id]
            
            node = MemoryRaftNode(
                node_id=node_id,
                peers=peers,
                state_machine=state_machine,
                get_peer_node=self._get_node,
                storage=storage,
            )
            self._nodes[node_id] = node
        
        print(f"Raft cluster started with {self._num_nodes} nodes")

    def start_network(self, state_machine_factory: Callable[[], StateMachine], 
                      peer_addresses: Dict[int, str], 
                      storage_factory: Optional[Callable[[int], 'RaftStorage']] = None):
        import grpc
        from .raft_servicer import RaftServicer
        from .network_client import RaftNetworkClient
        from oxidedb.proto.raft_pb2_grpc import add_RaftServiceServicer_to_server
        
        for node_id in range(1, self._num_nodes + 1):
            state_machine = state_machine_factory()
            self._state_machines[node_id] = state_machine
            
            storage = None
            if storage_factory is not None:
                storage = storage_factory(node_id)
                self._storages[node_id] = storage
            
            peers = [nid for nid in range(1, self._num_nodes + 1) if nid != node_id]
            
            peer_addrs = {pid: peer_addresses[pid] for pid in peers}
            network_client = RaftNetworkClient(peer_addrs)
            self._network_clients[node_id] = network_client
            
            node = MemoryRaftNode(
                node_id=node_id,
                peers=peers,
                state_machine=state_machine,
                storage=storage,
                network_client=network_client,
            )
            self._nodes[node_id] = node
            
            server = grpc.server(ThreadPoolExecutor(max_workers=10))
            servicer = RaftServicer(node)
            add_RaftServiceServicer_to_server(servicer, server)
            
            address = peer_addresses[node_id]
            try:
                server.add_insecure_port(address)
            except RuntimeError:
                import time
                time.sleep(0.5)
                server.add_insecure_port(address)
            server.start()
            self._grpc_servers[node_id] = server
        
        print(f"Raft cluster started with {self._num_nodes} nodes (network mode)")

    def _get_node(self, node_id: int) -> Optional[MemoryRaftNode]:
        return self._nodes.get(node_id)

    def get_node(self, node_id: int) -> Optional[MemoryRaftNode]:
        return self._nodes.get(node_id)

    def get_leader(self) -> Optional[int]:
        for node_id, node in self._nodes.items():
            if node.state == NodeState.LEADER:
                return node_id
        return None

    def shutdown(self):
        for node in self._nodes.values():
            node.shutdown()
        for node in self._nodes.values():
            node.wait_for_shutdown()