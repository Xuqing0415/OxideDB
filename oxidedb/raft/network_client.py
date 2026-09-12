import grpc
from typing import Dict, Optional
from oxidedb.proto.raft_pb2_grpc import RaftServiceStub
from oxidedb.proto import raft_pb2
from .node import (
    RequestVoteRequest,
    RequestVoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
)


class RaftNetworkClient:
    def __init__(self, peer_addresses: Dict[int, str], timeout: float = 0.1):
        self._peer_addresses = peer_addresses
        self._timeout = timeout
        self._channels: Dict[int, grpc.Channel] = {}
        self._stubs: Dict[int, RaftServiceStub] = {}
        
        self._init_channels()
    
    def _init_channels(self):
        for peer_id, address in self._peer_addresses.items():
            channel = grpc.insecure_channel(address)
            self._channels[peer_id] = channel
            self._stubs[peer_id] = RaftServiceStub(channel)
    
    def request_vote(self, peer_id: int, request: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        stub = self._stubs.get(peer_id)
        if stub is None:
            return None
        
        try:
            grpc_request = raft_pb2.RequestVoteRequest(
                term=request.term,
                candidate_id=request.candidate_id,
                last_log_index=request.last_log_index,
                last_log_term=request.last_log_term,
            )
            
            grpc_response = stub.RequestVote(grpc_request, timeout=self._timeout)
            
            return RequestVoteResponse(
                term=grpc_response.term,
                vote_granted=grpc_response.vote_granted,
            )
        except grpc.RpcError:
            return None
    
    def append_entries(self, peer_id: int, request: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        stub = self._stubs.get(peer_id)
        if stub is None:
            return None
        
        try:
            grpc_entries = []
            for entry in request.entries:
                grpc_entries.append(raft_pb2.Entry(
                    term=entry.term,
                    index=entry.index,
                    command=entry.command,
                ))
            
            grpc_request = raft_pb2.AppendEntriesRequest(
                term=request.term,
                leader_id=request.leader_id,
                prev_log_index=request.prev_log_index,
                prev_log_term=request.prev_log_term,
                entries=grpc_entries,
                leader_commit=request.leader_commit,
            )
            
            grpc_response = stub.AppendEntries(grpc_request, timeout=self._timeout)
            
            return AppendEntriesResponse(
                term=grpc_response.term,
                success=grpc_response.success,
                match_index=grpc_response.match_index,
            )
        except grpc.RpcError:
            return None

    def install_snapshot(self, peer_id: int, request: InstallSnapshotRequest) -> Optional[InstallSnapshotResponse]:
        stub = self._stubs.get(peer_id)
        if stub is None:
            return None
        
        try:
            # A snapshot is much larger than a heartbeat, so it gets more room
            # than ``self._timeout`` before the peer is written off.  A peer that
            # times out here is simply retried with the next heartbeat.
            grpc_request = raft_pb2.InstallSnapshotRequest(
                term=request.term,
                leader_id=request.leader_id,
                last_included_index=request.last_included_index,
                last_included_term=request.last_included_term,
                data=request.data,
            )
            
            grpc_response = stub.InstallSnapshot(grpc_request, timeout=max(self._timeout, 1.0))
            
            return InstallSnapshotResponse(
                term=grpc_response.term,
                success=grpc_response.success,
            )
        except grpc.RpcError:
            return None
    
    def shutdown(self):
        for channel in self._channels.values():
            channel.close()
