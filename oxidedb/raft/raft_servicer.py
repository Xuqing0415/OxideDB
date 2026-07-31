import grpc
from oxidedb.proto.raft_pb2_grpc import RaftServiceServicer
from oxidedb.proto import raft_pb2
from .node import MemoryRaftNode, RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, LogEntry


class RaftServicer(RaftServiceServicer):
    def __init__(self, node: MemoryRaftNode):
        self._node = node
    
    def RequestVote(self, request: raft_pb2.RequestVoteRequest, context):
        internal_request = RequestVoteRequest(
            term=request.term,
            candidate_id=request.candidate_id,
            last_log_index=request.last_log_index,
            last_log_term=request.last_log_term,
        )
        
        response = self._node.request_vote(internal_request)
        
        return raft_pb2.RequestVoteResponse(
            term=response.term,
            vote_granted=response.vote_granted,
        )
    
    def AppendEntries(self, request: raft_pb2.AppendEntriesRequest, context):
        entries = []
        for entry in request.entries:
            entries.append(LogEntry(
                term=entry.term,
                index=entry.index,
                command=entry.command,
            ))
        
        internal_request = AppendEntriesRequest(
            term=request.term,
            leader_id=request.leader_id,
            prev_log_index=request.prev_log_index,
            prev_log_term=request.prev_log_term,
            entries=entries,
            leader_commit=request.leader_commit,
        )
        
        response = self._node.append_entries(internal_request)
        
        return raft_pb2.AppendEntriesResponse(
            term=response.term,
            success=response.success,
            match_index=response.match_index,
        )