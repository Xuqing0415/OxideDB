import time
import random
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Callable
from enum import Enum
from .state_machine import StateMachine, ApplyResult, ReadResult, ErrorCode


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


#: Command of the empty entry a new leader appends, one per term (Raft 8).
#: It carries no state-machine work: its only job is to let the leader commit
#: the entries it inherited from previous terms, which Raft otherwise refuses
#: to do, and to prove the new leader's term for ReadIndex.
NOOP_COMMAND = b""


#: How many recent apply results a node keeps.  ``propose`` records the result
#: of the index it waited for and reads exactly that one back; nothing reads
#: older entries.  Keeping one per applied command forever is a leak, because a
#: node that has compacted its log still pays for every command it ever applied,
#: so the map is a window instead.  It only has to outlive a proposal timeout.
APPLY_RESULTS_WINDOW = 1024


class _ApplyResults(OrderedDict):
    """Apply results for the most recent indices, oldest evicted first."""

    def __init__(self, capacity: int = APPLY_RESULTS_WINDOW):
        super().__init__()
        self._capacity = max(1, capacity)

    def record(self, index: int, result: ApplyResult) -> None:
        self[index] = result
        while len(self) > self._capacity:
            self.popitem(last=False)


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


class InstallSnapshotRequest:
    def __init__(self, term: int, leader_id: int, last_included_index: int,
                 last_included_term: int, data: bytes):
        self.term = term
        self.leader_id = leader_id
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self.data = data


class InstallSnapshotResponse:
    def __init__(self, term: int, success: bool):
        self.term = term
        self.success = success


def _entry_in(log: List[LogEntry], log_base: int, index: int) -> Optional[LogEntry]:
    """Look up ``index`` in a snapshot of the log whose first entry is ``log_base + 1``.

    ``self._log`` no longer starts at index 1 once compaction dropped a prefix,
    so ``log[index - 1]`` is off by exactly ``log_base``.  Every lookup of an
    entry by index goes through here.
    """
    offset = index - log_base - 1
    if 0 <= offset < len(log):
        return log[offset]
    return None


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
        snapshot_interval: int = 100,
        apply_results_window: int = APPLY_RESULTS_WINDOW,
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

        # Log compaction.  ``_last_included_index`` is the index of the newest
        # entry folded into the state-machine snapshot; the entries up to it were
        # dropped from memory *and* storage, so it is the log's base index rather
        # than a bookmark.  ``_last_included_term`` is that entry's term, which
        # the log-freshness check needs once the log itself is empty.
        self._last_included_index = 0
        self._last_included_term = 0
        self._snapshot_interval = snapshot_interval
        self._snapshot_in_progress = False
        
        self._next_index: Dict[int, int] = {}
        self._match_index: Dict[int, int] = {}
        
        self._election_timeout_min = election_timeout_min
        self._election_timeout_max = election_timeout_max
        self._heartbeat_interval = heartbeat_interval
        
        # Deadlines (``time.monotonic``) rather than ``threading.Timer``
        # objects: one ticker thread per node drives both, see _tick().
        self._election_deadline = float("inf")
        self._next_heartbeat = float("inf")
        self._tick_interval = 0.05
        
        self._lock = threading.RLock()
        
        self._votes_received: Dict[int, bool] = {}
        
        self._apply_cond = threading.Condition(self._lock)
        
        self._shutdown_flag = False
        
        self._apply_results = _ApplyResults(apply_results_window)

        # One bounded pool per node instead of a thread per RPC.  Heartbeats
        # fire every 50 ms and the old code started a thread per peer for each
        # one, which on an idle three-node cluster meant ~80 new threads per
        # second; the scheduling churn showed up as heartbeats arriving late
        # and followers starting elections nobody needed.  ``_rpc_inflight``
        # drops a heartbeat whose previous RPC to the same peer has not
        # answered yet, so a slow peer cannot build a backlog either.
        self._rpc_pool = ThreadPoolExecutor(
            max_workers=max(4, 2 * len(peers)),
            thread_name_prefix=f"raft-{node_id}",
        )
        self._rpc_gate = threading.Lock()
        self._rpc_inflight: set = set()
        
        self._load_from_storage()
        
        self._start_election_timer()

        self._ticker = threading.Thread(
            target=self._tick, daemon=True, name=f"raft-{node_id}-ticker"
        )
        self._ticker.start()
    
    def _load_from_storage(self) -> None:
        if self._storage is None:
            return
        
        current_term, voted_for = self._storage.load_meta()
        self._current_term = current_term
        self._voted_for = voted_for
        
        self._log = self._storage.load_log()

        # A snapshot covers a prefix that is no longer in the log at all, so it
        # must be restored *before* any entry is replayed: those entries are gone
        # from storage, and the snapshot is the only copy of their effect.
        snapshot = self._storage.load_snapshot()
        if snapshot is not None:
            index, term, data = snapshot
            self._state_machine.restore(data)
            self._last_included_index = index
            self._last_included_term = term
            self._last_applied = index
            self._log = [e for e in self._log if e.index > index]

        # Only *committed* entries may be handed to the state machine.  Entries
        # past commit_index are merely replicated: they stay in the log for the
        # next leader to re-drive, and replaying them here would expose writes
        # that were never acknowledged as committed.
        self._commit_index = max(
            min(self._storage.load_commit_index(), self._last_log_index()),
            self._last_applied,
        )
        self._apply_committed_entries()

    def _last_log_index(self) -> int:
        """Index of the newest entry in the log, or the snapshot's index when the
        log has been compacted empty."""
        return self._log[-1].index if self._log else self._last_included_index

    def _last_log_term(self) -> int:
        return self._log[-1].term if self._log else self._last_included_term

    def _entry_at(self, index: int) -> Optional[LogEntry]:
        """The entry at ``index``, or ``None`` when it is no longer in the log."""
        return _entry_in(self._log, self._last_included_index, index)
    
    def _save_meta(self) -> None:
        if self._storage is not None:
            self._storage.save_meta(self._current_term, self._voted_for)
    
    def _save_log_entry(self, entry: LogEntry) -> None:
        if self._storage is not None:
            self._storage.append_log_entry(entry)

    def _save_log_truncate(self, from_index: int) -> None:
        if self._storage is not None:
            self._storage.truncate_log(from_index)

    def _save_commit_index(self) -> None:
        if self._storage is not None:
            self._storage.save_commit_index(self._commit_index)

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
            # Entries still *retained*: compaction drops the prefix the snapshot
            # covers, so this is not the index of the newest entry (use
            # ``_last_log_index`` for that).
            return len(self._log)

    def _reset_election_timer(self) -> None:
        with self._lock:
            self._arm_election_deadline()

    def _start_election_timer(self) -> None:
        with self._lock:
            self._arm_election_deadline()

    def _arm_election_deadline(self) -> None:
        timeout = random.randint(self._election_timeout_min, self._election_timeout_max) / 1000.0
        self._election_deadline = time.monotonic() + timeout

    def _cancel_election_timer(self) -> None:
        with self._lock:
            self._election_deadline = float("inf")

    def _start_heartbeat(self) -> None:
        with self._lock:
            self._next_heartbeat = time.monotonic() + self._heartbeat_interval / 1000.0

    def _cancel_heartbeat(self) -> None:
        with self._lock:
            self._next_heartbeat = float("inf")

    def _tick(self) -> None:
        """Drive the election and heartbeat deadlines from one thread.

        ``threading.Timer`` allocates a thread per schedule, and a node
        reschedules on every heartbeat it sends *and* every one it receives -
        measured at 50-80 new threads per second on an idle three-node cluster.
        That churn delayed heartbeats enough for followers to start elections
        nobody needed.  Comparing two deadlines on a single thread removes it.
        """
        while not self._shutdown_flag:
            try:
                now = time.monotonic()
                if self._state == NodeState.LEADER:
                    if now >= self._next_heartbeat:
                        self._next_heartbeat = now + self._heartbeat_interval / 1000.0
                        self._send_heartbeats()
                    sleep_for = self._next_heartbeat - time.monotonic()
                elif now >= self._election_deadline:
                    # _start_election arms the next deadline for us.
                    self._election_deadline = float("inf")
                    self._start_election()
                    sleep_for = self._tick_interval
                else:
                    sleep_for = self._election_deadline - now
            except Exception:
                # A dead ticker would silently stop elections, so a broken tick
                # must not kill the loop.
                sleep_for = self._tick_interval
            time.sleep(min(max(sleep_for, 0.001), self._tick_interval))

    def _start_election(self) -> None:
        with self._lock:
            if self._shutdown_flag:
                return

            if self._state == NodeState.LEADER:
                return
            
            self._state = NodeState.CANDIDATE
            self._current_term += 1
            self._voted_for = self._node_id
            self._votes_received = {self._node_id: True}
            
            self._save_meta()
            
            self._reset_election_timer()

            # A single-node cluster has no peers to ask, so its own vote is
            # already a quorum.  Without this the node stayed a candidate
            # forever, which made a one-node deployment unable to serve anything.
            self._check_vote_count()
        
        for peer_id in self._peers:
            self._submit_rpc(self._request_vote, peer_id)

    def _submit_rpc(self, rpc: Callable, *args) -> bool:
        """Run ``rpc`` on the node's RPC pool, or drop it if the node is gone."""
        with self._rpc_gate:
            if self._shutdown_flag:
                return False
        try:
            self._rpc_pool.submit(rpc, *args)
        except RuntimeError:
            # The pool is already shut down; the result no longer matters.
            return False
        return True

    def _dispatch_heartbeat(self, peer_id: int, *args) -> None:
        """Send one heartbeat, unless the previous one to ``peer_id`` is pending."""
        with self._rpc_gate:
            if self._shutdown_flag or peer_id in self._rpc_inflight:
                return
            self._rpc_inflight.add(peer_id)

        try:
            self._rpc_pool.submit(self._run_gated_rpc, peer_id, *args)
        except RuntimeError:
            with self._rpc_gate:
                self._rpc_inflight.discard(peer_id)

    def _run_gated_rpc(self, peer_id: int, *args) -> None:
        try:
            # `_replicate` picks the message this peer actually needs: the
            # entries it is missing, or the snapshot once it is behind them.
            self._replicate(*args)
        finally:
            with self._rpc_gate:
                self._rpc_inflight.discard(peer_id)

    def _request_vote(self, peer_id: int) -> None:
        try:
            with self._lock:
                if self._shutdown_flag:
                    return
                last_log_index = self._last_log_index()
                last_log_term = self._last_log_term()
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
                    # `_last_log_index`, not `len(self._log)`: compaction
                    # drops entries from the front, so the list length is no
                    # longer the index of the newest entry.
                    self._next_index[peer_id] = self._last_log_index() + 1
                    self._match_index[peer_id] = 0
                
                self._append_noop_entry()
                self._send_heartbeats()
                self._start_heartbeat()

    def _append_noop_entry(self) -> None:
        """Append the current term's empty entry, as Raft 8 requires.

        Raft only commits an entry once one from the *current* term is stored on
        a majority, so without this a freshly elected leader sat on the entries
        it inherited from the previous term until some client happened to write
        something.  A restarted cluster therefore could not serve the last few
        writes it had already replicated.  One empty entry per term closes that
        window; ``_update_commit_index`` lets a single-node cluster commit it
        immediately.
        """
        entry = LogEntry(term=self._current_term, index=self._last_log_index() + 1, command=NOOP_COMMAND)
        self._log.append(entry)
        self._save_log_entry(entry)
        self._update_commit_index()

    def _send_heartbeats(self) -> None:
        with self._lock:
            if self._shutdown_flag:
                return

            if self._state != NodeState.LEADER:
                return
            
            peers = list(self._peers)
            term = self._current_term
            next_index = dict(self._next_index)
            log = list(self._log)
            # These two describe the *copy* of the log taken above.  The live
            # values move on as entries and compaction land, and reading them
            # again on the RPC thread would slice the copy with the wrong base.
            log_base = self._last_included_index
            last_log_index = self._last_log_index()
            log_base_term = self._last_included_term
            leader_commit = self._commit_index
        
        for peer_id in peers:
            self._dispatch_heartbeat(
                peer_id, peer_id, term, next_index.get(peer_id), log,
                leader_commit, log_base, last_log_index, log_base_term,
            )

    def _replicate(self, peer_id: int, term: int, next_index: Optional[int],
                   log: List[LogEntry], leader_commit: int,
                   log_base: int = 0, last_log_index: Optional[int] = None,
                   log_base_term: int = 0) -> Optional[object]:
        """Send ``peer_id`` the message it needs: entries, or a snapshot.

        A follower whose ``next_index`` has fallen at or below the compacted
        prefix cannot be caught up with ``AppendEntries`` at all - the entries it
        is missing do not exist anywhere any more - so it gets the snapshot.
        """
        if log_base > 0 and (next_index is None or next_index <= log_base):
            return self._install_snapshot(peer_id, term)
        return self._append_entries(peer_id, term, next_index, log, leader_commit,
                                    log_base, last_log_index, log_base_term)

    def _append_entries(self, peer_id: int, term: int, next_index: Optional[int],
                        log: List[LogEntry], leader_commit: int,
                        log_base: int = 0, last_log_index: Optional[int] = None,
                        log_base_term: int = 0) -> Optional[AppendEntriesResponse]:
        try:
            if self._shutdown_flag:
                return None

            if last_log_index is None:
                last_log_index = log[-1].index if log else log_base

            if next_index is None:
                next_index = last_log_index + 1

            if next_index <= log_base:
                # Everything this peer is behind on was compacted away.  There is
                # nothing left to append, so leave the decrement to the next
                # heartbeat, which will ship the snapshot instead.
                return None

            prev_log_index = next_index - 1
            prev_entry = _entry_in(log, log_base, prev_log_index)
            if prev_entry is not None:
                prev_log_term = prev_entry.term
            elif prev_log_index == log_base:
                # This entry was compacted away on our side too, but the snapshot
                # kept its term.  Sending 0 instead would make every follower
                # reject the request for ever, since their copy of that entry has
                # the real term.
                prev_log_term = log_base_term
            else:
                prev_log_term = 0
            
            entries = []
            if prev_log_index < last_log_index:
                entries = [e for e in log if e.index > prev_log_index]
            
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
                    return None
                response = peer_node.append_entries(request)
            
            if response is None:
                return None
            
            with self._lock:
                if response.term > self._current_term:
                    self._current_term = response.term
                    self._state = NodeState.FOLLOWER
                    self._voted_for = None
                    self._cancel_heartbeat()
                    self._reset_election_timer()
                    return None
                
                if self._state != NodeState.LEADER:
                    return None
                
                if response.success:
                    self._next_index[peer_id] = response.match_index + 1
                    self._match_index[peer_id] = response.match_index
                    self._update_commit_index()
                else:
                    self._next_index[peer_id] = max(1, self._next_index.get(peer_id, 1) - 1)
                
                return response
        except Exception:
            return None

    def _install_snapshot(self, peer_id: int, term: int) -> Optional[InstallSnapshotResponse]:
        """Ship the stored snapshot to a follower that has fallen behind it.

        Used instead of ``AppendEntries`` once the entries that follower needs
        are no longer in the leader's log: the snapshot is then the only thing
        that can bring it to a known state.  Anything it already has above the
        snapshot index is left alone.
        """
        try:
            if self._shutdown_flag or self._storage is None:
                return None

            snapshot = self._storage.load_snapshot()
            if snapshot is None:
                return None
            index, snapshot_term, data = snapshot

            request = InstallSnapshotRequest(
                term=term,
                leader_id=self._node_id,
                last_included_index=index,
                last_included_term=snapshot_term,
                data=data,
            )

            if self._network_client is not None:
                response = self._network_client.install_snapshot(peer_id, request)
            else:
                peer_node = self._get_peer_node(peer_id)
                if peer_node is None:
                    return None
                response = peer_node.install_snapshot(request)

            if response is None:
                return None

            with self._lock:
                if response.term > self._current_term:
                    self._current_term = response.term
                    self._state = NodeState.FOLLOWER
                    self._voted_for = None
                    self._cancel_heartbeat()
                    self._reset_election_timer()
                    return None

                if self._state != NodeState.LEADER:
                    return None

                if response.success:
                    # The follower now holds everything up to ``index``, and the
                    # server tells us it did not mind the snapshot, so
                    # replication resumes at the entry after it.
                    self._next_index[peer_id] = index + 1
                    self._match_index[peer_id] = index
                    self._update_commit_index()
                else:
                    self._next_index[peer_id] = max(1, self._next_index.get(peer_id, 1) - 1)

                return response
        except Exception:
            return None
    def _update_commit_index(self) -> None:
        with self._lock:
            match_indices = sorted(self._match_index.values())
            match_indices.append(self._last_log_index())
            
            n = len(self._peers) + 1
            majority = (n // 2) + 1
            
            if len(match_indices) >= majority:
                new_commit_index = match_indices[-majority]
                if new_commit_index > self._commit_index:
                    # Only an entry of the *current* term may be committed by
                    # counting replicas (Raft 5.4.2).  An index the snapshot
                    # already covers has no entry left to inspect - and needs
                    # none, it was committed long ago.
                    entry = self._entry_at(new_commit_index)
                    if entry is not None and entry.term == self._current_term:
                        self._commit_index = new_commit_index
                        self._save_commit_index()
                        self._apply_committed_entries()

    def _apply_committed_entries(self) -> None:
        with self._lock:
            while self._last_applied < self._commit_index:
                self._last_applied += 1
                entry = self._entry_at(self._last_applied)
                if entry is None:
                    # Folded into a snapshot and dropped from the log: its effect
                    # is already in the state machine, so there is nothing left
                    # to apply.
                    continue
                if entry.command == NOOP_COMMAND:
                    # A no-op belongs to the log, not to the state machine:
                    # handing it an empty command would report an apply
                    # error on every election.
                    self._apply_results.record(self._last_applied, ApplyResult.success())
                else:
                    self._apply_results.record(
                        self._last_applied, self._state_machine.apply(entry.command)
                    )
            
            self._maybe_snapshot()
            
            with self._apply_cond:
                self._apply_cond.notify_all()

    def _maybe_snapshot(self) -> None:
        """Fold the applied prefix into a snapshot and drop it from the log.

        Called from :meth:`_apply_committed_entries`, holding the node lock.  The
        snapshot is written *before* the log is compacted: a crash between the
        two leaves redundant entries, which the next replay skips because
        ``_last_applied`` already covers them, whereas the other order would
        throw away the only copy of that state.
        """
        if self._snapshot_interval <= 0 or self._storage is None:
            return
        if self._snapshot_in_progress:
            return
        if self._last_applied - self._last_included_index < self._snapshot_interval:
            return
        entry = self._entry_at(self._last_applied)
        if entry is None:
            return

        self._snapshot_in_progress = True
        try:
            try:
                data = self._state_machine.snapshot()
            except NotImplementedError:
                # State that cannot be serialised cannot be compacted away, so
                # stop asking rather than discard it.
                self._snapshot_interval = 0
                return

            index, term = self._last_applied, entry.term
            self._storage.save_snapshot(index, term, data)
            self._storage.compact_log(index)
        except Exception:
            # A snapshot that cannot be written must not stop the apply loop;
            # the applied counter is untouched, so the next interval retries.
            return
        finally:
            self._snapshot_in_progress = False

        self._log = [e for e in self._log if e.index > index]
        self._last_included_index = index
        self._last_included_term = term

    def _wait_for_apply(self, index: int) -> None:
        with self._lock:
            while self._last_applied < index:
                self._apply_cond.wait()

    def request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        with self._lock:
            # For election purposes the snapshot *is* part of this node's log:
            # once the entries underneath it are compacted away, comparing
            # candidates by "last entry in the list" would make an empty log look
            # like a node that had never seen a write.
            last_log_index = self._last_log_index()
            last_log_term = self._last_log_term()
            
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
            
            if self._state != NodeState.FOLLOWER:
                # Raft 5.2: a valid AppendEntries *is* the proof that this term
                # already has a leader, so a candidate has to concede.  Without
                # this a node that campaigned in the winner's term stayed a
                # candidate for ever - every heartbeat kept resetting its
                # election timer, so it never even retried at a higher term,
                # and the cluster looked like it had never finished electing.
                self._state = NodeState.FOLLOWER
                self._votes_received.clear()
            
            self._reset_election_timer()
            
            if request.prev_log_index > self._last_log_index():
                if term_changed:
                    self._save_meta()
                return AppendEntriesResponse(term=self._current_term, success=False, match_index=0)
            
            # Below the snapshot there is nothing left to compare against: those
            # entries are exactly what the snapshot replaced.  Only the boundary
            # entry, whose term we kept, can still be checked.
            prev_entry = self._entry_at(request.prev_log_index)
            if prev_entry is None:
                if (request.prev_log_index == self._last_included_index
                        and request.prev_log_term != self._last_included_term):
                    if term_changed:
                        self._save_meta()
                    return AppendEntriesResponse(term=self._current_term, success=False, match_index=0)
            elif prev_entry.term != request.prev_log_term:
                if term_changed:
                    self._save_meta()
                return AppendEntriesResponse(term=self._current_term, success=False, match_index=0)
            
            for entry in request.entries:
                if entry.index <= self._last_included_index:
                    # Folded into our snapshot already: the leader is simply
                    # behind on our own compaction.
                    continue
                existing = self._entry_at(entry.index)
                if existing is not None:
                    if existing.term != entry.term:
                        del self._log[entry.index - self._last_included_index - 1:]
                        # Make the truncation durable, otherwise the stale suffix
                        # is resurrected by the next restart.
                        self._save_log_truncate(entry.index)
                        self._commit_index = min(self._commit_index, self._last_log_index())
                        self._last_applied = min(self._last_applied, self._commit_index)
                        self._log.append(entry)
                        self._save_log_entry(entry)
                else:
                    self._log.append(entry)
                    self._save_log_entry(entry)
            
            if term_changed:
                self._save_meta()
            
            match_index = self._last_log_index()
            
            if request.leader_commit > self._commit_index:
                self._commit_index = min(request.leader_commit, self._last_log_index())
                self._save_commit_index()
                self._apply_committed_entries()
            
            return AppendEntriesResponse(term=self._current_term, success=True, match_index=match_index)

    def install_snapshot(self, request: InstallSnapshotRequest) -> InstallSnapshotResponse:
        """Replace local state with a snapshot sent by the leader.

        This is what a follower that fell behind the leader's compaction needs:
        the entries it is missing no longer exist, so it is handed the state
        itself.  Entries it has *above* the snapshot index are kept, so a
        follower that was merely missing a prefix does not replay the suffix.
        """
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
                return InstallSnapshotResponse(term=self._current_term, success=False)
            
            if self._state != NodeState.FOLLOWER:
                # Same rule as AppendEntries: a valid message for this term is
                # the proof that the term already has a leader.
                self._state = NodeState.FOLLOWER
                self._votes_received.clear()
            
            self._reset_election_timer()
            if term_changed:
                self._save_meta()
            
            if request.last_included_index <= self._last_applied:
                # Everything in the snapshot is applied already, so installing it
                # would only move this node backwards.  Reporting success is
                # still correct: the leader may resume replication after it.
                return InstallSnapshotResponse(term=self._current_term, success=True)
            
            # ``restore`` swaps the whole state in one go.  The node lock is held
            # across it so that a concurrent apply cannot land part of its work
            # in the old state and part in the restored one.
            self._state_machine.restore(request.data)
            
            self._log = [e for e in self._log if e.index > request.last_included_index]
            if self._storage is not None:
                self._storage.save_snapshot(
                    request.last_included_index, request.last_included_term, request.data
                )
                self._storage.compact_log(request.last_included_index)
            
            self._last_included_index = request.last_included_index
            self._last_included_term = request.last_included_term
            self._last_applied = request.last_included_index
            self._commit_index = max(self._commit_index, self._last_applied)
            self._save_commit_index()
            
            return InstallSnapshotResponse(term=self._current_term, success=True)

    def propose(self, command: bytes, timeout: float = 5.0) -> ApplyResult:
        with self._lock:
            if self._state != NodeState.LEADER:
                return ApplyResult.failure(1, "Not leader")
            
            entry = LogEntry(term=self._current_term, index=self._last_log_index() + 1, command=command)
            self._log.append(entry)
            self._save_log_entry(entry)
            entry_term = self._current_term
            entry_index = entry.index

            # A single-node cluster has no peers to acknowledge the entry, so
            # advance the commit index from our own match index.  With peers this
            # is a no-op until a majority has acknowledged.
            self._update_commit_index()
            
            peers = list(self._peers)
            term = self._current_term
            next_index = dict(self._next_index)
            log = list(self._log)
            log_base = self._last_included_index
            last_log_index = self._last_log_index()
            log_base_term = self._last_included_term
            leader_commit = self._commit_index
        
        for peer_id in peers:
            # A thread of its own rather than the heartbeat pool: this call is
            # driven by a client waiting on the result, so it must not be dropped
            # by the per-peer in-flight gate.
            threading.Thread(
                target=self._replicate,
                args=(peer_id, term, next_index.get(peer_id), log, leader_commit,
                      log_base, last_log_index, log_base_term),
                daemon=True,
            ).start()
        
        with self._lock:
            deadline = time.time() + timeout
            while self._commit_index < entry_index:
                if self._state != NodeState.LEADER or self._current_term != entry_term:
                    return ApplyResult.failure(2, "Lost leadership")
                remaining = deadline - time.time()
                if remaining <= 0:
                    return ApplyResult.failure(3, "Proposal timeout")
                self._apply_cond.wait(timeout=remaining)
            
            applied_entry = self._entry_at(entry_index)
            if applied_entry is not None and applied_entry.term != entry_term:
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
            log_base = self._last_included_index
            last_log_index = self._last_log_index()
            log_base_term = self._last_included_term
            leader_commit = self._commit_index
        
        if peers:
            # ReadIndex, step 1: confirm we are still the leader by collecting a
            # quorum of AppendEntries acknowledgements.  The previous version
            # gathered the responses and then never inspected them, so a deposed
            # or partitioned leader served reads it could not justify.
            acks = 1  # our own acknowledgement
            for peer_id in peers:
                try:
                    response = self._replicate(peer_id, current_term, next_index.get(peer_id, 1), log,
                                               leader_commit, log_base, last_log_index, log_base_term)
                except Exception:
                    response = None
                
                if response is not None and response.success and response.term == current_term:
                    acks += 1
            
            with self._lock:
                if self._state != NodeState.LEADER or self._current_term != current_term:
                    return ReadResult.failure(ErrorCode.ERR_NOT_LEADER, "Lost leadership during read")
                
                majority = (len(self._peers) + 1) // 2 + 1
                if acks < majority:
                    return ReadResult.failure(
                        ErrorCode.ERR_NOT_LEADER,
                        f"ReadIndex failed: {acks} acks, {majority} required",
                    )
                
                # ReadIndex, step 2: those acks may have moved match_index (and
                # therefore commit_index) forward, so re-read the commit point
                # before deciding what "fresh enough" means for this read.
                self._update_commit_index()
                read_index = self._commit_index
        
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

        # In-flight RPCs finish on their own (every call is deadline-bounded);
        # queued ones are dropped.  Without this the node kept a pool of
        # worker threads alive for every cluster the tests started.
        self._rpc_pool.shutdown(wait=False, cancel_futures=True)
        
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

    def start(self, state_machine_factory: Callable[[], StateMachine],
              storage_factory: Optional[Callable[[int], 'RaftStorage']] = None,
              snapshot_interval: int = 100,
              apply_results_window: int = APPLY_RESULTS_WINDOW):
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
                snapshot_interval=snapshot_interval,
                apply_results_window=apply_results_window,
            )
            self._nodes[node_id] = node
        
        print(f"Raft cluster started with {self._num_nodes} nodes")

    def start_network(self, state_machine_factory: Callable[[], StateMachine], 
                      peer_addresses: Dict[int, str], 
                      storage_factory: Optional[Callable[[int], 'RaftStorage']] = None,
                      snapshot_interval: int = 100,
                      apply_results_window: int = APPLY_RESULTS_WINDOW):
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
                snapshot_interval=snapshot_interval,
                apply_results_window=apply_results_window,
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
            # Hand the server to the node as well: ``node.shutdown()`` is what
            # stops it, and keeping it only in this dict leaked a listening
            # socket plus its thread pool for the lifetime of the process.
            node._grpc_server = server
        
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
