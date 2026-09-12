"""InstallSnapshot end to end, over real gRPC.

``test_snapshot.py`` drives the protocol objects directly.  This file goes
through the wire instead: a replica that lost its disk state rejoins a cluster
whose leader has compacted past everything it was missing, so the entries it
needs do not exist any more and the snapshot is the only way back.  It is the one
path in the snapshot work that the in-process tests cannot reach - the proto
mapping in ``RaftServicer``/``RaftNetworkClient`` and the payload surviving a real
message.
"""

import time
from concurrent.futures import ThreadPoolExecutor

import grpc

from _ports import free_addresses
from _wait import wait_for_single_leader, wait_until

from oxidedb.proto import raft_pb2
from oxidedb.proto.raft_pb2_grpc import RaftServiceStub, add_RaftServiceServicer_to_server
from oxidedb.raft.network_client import RaftNetworkClient
from oxidedb.raft.node import MemoryRaftNode, RaftCluster
from oxidedb.raft.raft_servicer import RaftServicer
from oxidedb.raft.state_machine import CommandType, MVCCStateMachine
from oxidedb.raft.storage import EngineRaftStorage


#: Small enough that a handful of writes compacts the log several times over.
SNAPSHOT_INTERVAL = 3
EARLY_WRITES = 6
LATE_WRITES = 9


def _set_command(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp
    )


def _bind(server, address):
    """Bind ``address``, retrying while the previous owner releases the port."""
    for _ in range(10):
        try:
            if server.add_insecure_port(address):
                return
        except RuntimeError:
            pass
        time.sleep(0.2)
    raise AssertionError(f"could not bind {address}")


def _start_node(node_id, address, peer_addresses, state_machine, storage,
                election_timeout_ms=60000):
    """Bring one node up on ``address`` the way ``RaftCluster.start_network`` does.

    ``RaftCluster`` has no "restart a single node" API, and a node coming back
    after losing its disk is exactly what these tests need, so the same pieces
    are assembled here: a network client, the node, and the gRPC server that
    serves it.

    The election timer is deliberately long.  A fresh replica that started an
    election while it caught up would bump the term and make the leader step
    down, which is not what is under test here.
    """
    peer_ids = [peer for peer in sorted(peer_addresses) if peer != node_id]
    node = MemoryRaftNode(
        node_id=node_id,
        peers=peer_ids,
        state_machine=state_machine,
        storage=storage,
        network_client=RaftNetworkClient({peer: peer_addresses[peer] for peer in peer_ids}),
        election_timeout_min=election_timeout_ms,
        election_timeout_max=election_timeout_ms,
        snapshot_interval=SNAPSHOT_INTERVAL,
    )
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    add_RaftServiceServicer_to_server(RaftServicer(node), server)
    _bind(server, address)
    server.start()
    node._grpc_server = server
    return node


class TestInstallSnapshotOverGrpc:
    def test_replica_that_lost_its_disk_is_restored_from_a_snapshot(self, tmp_path):
        addresses = free_addresses(num_nodes=3)
        node_dirs = {node_id: str(tmp_path / f"node{node_id}") for node_id in (1, 2, 3)}

        def storage_factory(node_id):
            return EngineRaftStorage(data_dir=node_dirs[node_id])

        cluster = RaftCluster(num_nodes=3)
        cluster.start_network(
            state_machine_factory=lambda: MVCCStateMachine(),
            peer_addresses=addresses,
            storage_factory=storage_factory,
            snapshot_interval=SNAPSHOT_INTERVAL,
        )
        restarted = None
        try:
            leader_id = wait_for_single_leader(cluster)
            leader = cluster.get_node(leader_id)
            replica_id = 3 if leader_id != 3 else 2

            wait_until(
                lambda: all(node.commit_index >= 1 for node in cluster._nodes.values()),
                message="the leader's no-op never reached every node",
            )
            for i in range(EARLY_WRITES):
                result = leader.propose(_set_command(
                    leader._state_machine, f"early{i}".encode(), b"v", i + 1
                ))
                assert result.success, f"early proposal {i} failed: {result.error_msg}"
            wait_until(lambda: leader._last_included_index > 0,
                       message="the leader never compacted its log")

            # Take the replica down, then write well past what it had: the
            # entries it is now missing are the ones compaction folded away.
            cluster.get_node(replica_id).shutdown()
            for i in range(LATE_WRITES):
                result = leader.propose(_set_command(
                    leader._state_machine, f"late{i}".encode(), b"v", 100 + i
                ))
                assert result.success, f"late proposal {i} failed: {result.error_msg}"

            base = leader._last_included_index
            assert leader._next_index[replica_id] <= base, (
                "the test only exercises InstallSnapshot if the leader compacted past "
                f"the replica's next_index ({leader._next_index[replica_id]} > base {base})"
            )

            # Wipe its disk before it comes back: a replica that lost everything
            # is the one that needs a snapshot rather than a log prefix.
            state_machine = MVCCStateMachine()
            restarted = _start_node(
                replica_id, addresses[replica_id], addresses, state_machine,
                EngineRaftStorage(data_dir=node_dirs[replica_id]),
            )

            wait_until(
                lambda: restarted._last_included_index >= base,
                message="the leader never installed its snapshot on the restarted replica",
            )
            wait_until(
                lambda: state_machine.get(b"late8").value == b"v",
                message="the restarted replica never applied the writes it had missed",
            )

            installed = EngineRaftStorage(data_dir=node_dirs[replica_id]).load_snapshot()
            assert installed is not None, "the replica must persist what it installed"
            assert installed[0] == base, (
                "the replica should be holding the leader's snapshot (index "
                f"{base}), not one of its own: got {installed[0]}"
            )
            assert state_machine.get(b"early0").value == b"v", (
                "the snapshot has to carry the writes that were compacted away"
            )
            assert restarted._log, (
                "the entry above the snapshot arrives by replication, not by snapshot"
            )
        finally:
            cluster.shutdown()
            if restarted is not None:
                restarted.shutdown()

    def test_the_rpc_maps_the_snapshot_payload_both_ways(self, tmp_path):
        """The servicer is hand-written, so the wire form is checked directly.

        Nothing else validates that ``data`` survives as the same bytes, or that a
        stale term is refused with the node's own term - a field mismatch here
        would only show up as a corruption much later.
        """
        addresses = free_addresses(num_nodes=2)
        node_dir = str(tmp_path / "node1")
        storage = EngineRaftStorage(data_dir=node_dir)
        node = _start_node(1, addresses[1], addresses, MVCCStateMachine(), storage)
        channel = grpc.insecure_channel(addresses[1])
        try:
            stub = RaftServiceStub(channel)
            payload = MVCCStateMachine().snapshot()

            response = stub.InstallSnapshot(raft_pb2.InstallSnapshotRequest(
                term=1, leader_id=2, last_included_index=4, last_included_term=1, data=payload,
            ), timeout=5)

            assert response.success
            assert response.term == 1, "the follower adopts the sender's term"
            assert EngineRaftStorage(data_dir=node_dir).load_snapshot() == (4, 1, payload), (
                "the payload must arrive byte for byte and be stored with its index and term"
            )

            stale = stub.InstallSnapshot(raft_pb2.InstallSnapshotRequest(
                term=0, leader_id=2, last_included_index=9, last_included_term=9, data=payload,
            ), timeout=5)

            assert not stale.success, "a snapshot from an older term must be refused"
            assert stale.term == 1
            assert EngineRaftStorage(data_dir=node_dir).load_snapshot() == (4, 1, payload), (
                "and it must leave the stored snapshot alone"
            )
        finally:
            channel.close()
            node.shutdown()
            storage.close()
