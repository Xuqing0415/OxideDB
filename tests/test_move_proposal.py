"""A move's last two steps, driven by hand: the table told, and the group it left let go.

``move_shard`` has the half of a move that has to be right before anything else matters -
freeze the source, read its rows at one moment, copy them into the group that will own them
- and stops there on purpose.  What finishes a move is two calls that are not wired into it
yet, and this file drives both of them the way the rest of the move will: ``_propose_move``
tells the routing table what the shard's replica set is now, and ``_commit_move`` makes this
cluster's own answers follow it and lets the group the shard left go.

They are called rather than wired in, because wiring them in changes what ``move_shard``
means for every test around it, and because each has an answer worth pinning on its own:

* the set being replaced is read out of the table and not out of this process.  The machine
  refuses a move whose expectation of the current set is wrong (code 14), so a caller
  proposing this process's own view would be refused the moment two moves were computed
  from one table - which is why the table is read on every attempt, and why one of these
  tests turns the table onto a set the cluster itself would not have said;
* a table that does not hold the shard at all is a refusal and not a silence, because the
  two send a caller to different places;
* a cluster with no table has nothing to propose, which is what an in-process cluster is;
* finishing a move closes the group the shard left on every node that is not serving it
  now, keeps that group answering for a window first, puts its directory aside as
  ``orphan-shard-<id>-<when>`` instead of deleting it, and drops the move's note on the way;
* finishing a move twice is finishing it once - which is what a cluster that comes back to
  a move it had already finished will do.
"""

import os
import socket
import threading
import time

import pytest

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_metadata_client, wait_until
from oxidedb.client import LocalNodeClientFactory
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import MetadataCluster, RoutingTable, ShardPlacement
from oxidedb.raft.shard_server import (MigrationPhase, ProposalOutcome, ShardedRaftCluster)
from oxidedb.raft.state_machine import ApplyResult, CommandType, MVCCStateMachine
from oxidedb.raft.storage import EngineRaftStorage

COUNT = 100
KEYS = [b"key%03d" % index for index in range(COUNT)]
VALUES = [b"value%03d" % index for index in range(COUNT)]

#: Shard 0 of a one-shard map owns the whole keyspace, so where a key is is not a question
#: these tests have to ask.  Five nodes and three of them serving the shard: a move needs
#: somewhere to go that is not where the shard is, and one node is not a replica set.
SERVING = [1, 2, 3]
MOVE_TO = [4, 5]
NOTE = "migrate/0"


def _set(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp)


def _host_port(address):
    host, port = address.split(":")
    return (host, int(port))


def _cluster_with_metadata(tmp_path, shard_nodes=None, num_nodes=5):
    metadata = MetadataCluster(num_nodes=3)
    metadata.start()

    cluster = ShardedRaftCluster(num_nodes=num_nodes, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=free_addresses(num_nodes=num_nodes),
        storage_factory=lambda node_id, shard_id: EngineRaftStorage(
            data_dir=str(tmp_path / f"shard{shard_id}_node{node_id}")),
        shard_nodes=shard_nodes,
        lock_cleaner_interval=None,
        metadata=metadata,
    )
    return metadata, cluster


def _write_rows(cluster, count=COUNT):
    """``count`` committed rows in the shard about to move, and the leader holding them."""
    wait_for_keys_leader(cluster, KEYS[:1])
    leader = cluster.get_leader_for_key(KEYS[0])[1]
    for index in range(count):
        assert leader.propose(
            _set(leader._state_machine, KEYS[index], VALUES[index], index + 1)).success
    return leader


def _published(client, shard_ids):
    """The table, once the publisher has filled in every one of ``shard_ids``."""
    def ready():
        table = client.table(refresh=True)
        for shard_id in shard_ids:
            placement = table.shard(shard_id)
            if placement is None or placement.leader_id is None:
                return None
        return table

    return wait_until(ready, message="the cluster never published a leader for every shard")


def _copy_but_do_not_propose(cluster):
    """The half of a move ``move_shard`` does, and the state it deliberately leaves."""
    with pytest.raises(NotImplementedError):
        cluster.move_shard(0, MOVE_TO)
    assert cluster.migration_state(0).phase == MigrationPhase.PROPOSING


def _finish_the_move(cluster):
    """The two steps that are not wired in: tell the table, then let the old group go."""
    result = cluster._propose_move(0, MOVE_TO)
    assert result.ok, result.message
    return cluster._commit_move(0, MOVE_TO, drain=0)


class _ToldClient:
    """The cluster's metadata client, with the table replaced and the call remembered.

    It answers the question the cluster cannot ask itself: which replica set ended up in
    the proposal, and whether it came from the table or from this process.
    """

    def __init__(self, table):
        self._table = table
        self.calls = []

    def table(self, refresh=False):
        return self._table

    def move_shard(self, shard_id, from_nodes, nodes, addresses):
        self.calls.append({"shard": shard_id, "from_nodes": list(from_nodes),
                           "nodes": list(nodes), "addresses": dict(addresses)})
        return ApplyResult.success()


def test_the_set_a_move_replaces_is_the_tables_and_not_this_processes(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path)
    try:
        # Every node serves the shard here, so this process's own answer for it is all
        # five.  The table is pointed at the three the move is really replacing.
        assert cluster._serving_nodes(0) == [1, 2, 3, 4, 5]
        table = RoutingTable(version=1, shards={
            0: ShardPlacement(0, b"", b"\xff", nodes=SERVING,
                              addresses=cluster._addresses_on(SERVING, 0)),
        })
        told = _ToldClient(table)
        cluster._metadata_client = told

        result = cluster._propose_move(0, MOVE_TO)

        assert result.ok, result.message
        assert len(told.calls) == 1, "one proposal, and the table it was made against"
        call = told.calls[0]
        assert call["from_nodes"] == SERVING, "the set to replace is the table's"
        assert call["from_nodes"] != cluster._serving_nodes(0), "and not this process's"
        assert call["nodes"] == MOVE_TO
        assert call["addresses"] == cluster._addresses_on(MOVE_TO, 0), \
            "the addresses of the nodes it is going to, as those servers bound them"
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_table_that_does_not_hold_the_shard_refuses_rather_than_says_nothing(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path)
    try:
        told = _ToldClient(RoutingTable(version=1, shards={}))
        cluster._metadata_client = told

        result = cluster._propose_move(0, MOVE_TO)

        assert result.outcome == ProposalOutcome.REJECTED, \
            "a table that answered is not a table that was silent"
        assert "not in the routing table" in result.message
        assert told.calls == [], "a shard the table does not have is never proposed"
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_a_cluster_with_no_table_has_nothing_to_propose():
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=1)
    cluster.start(state_machine_factory=lambda: MVCCStateMachine(),
                  lock_cleaner_interval=None)
    try:
        assert cluster.metadata_client() is None

        result = cluster._propose_move(0, [1, 2])

        assert result.ok, "an in-process cluster has no client to disagree with"
        assert "no routing table" in result.message
    finally:
        cluster.shutdown()


def test_a_move_ends_with_the_table_told_and_the_group_it_left_gone(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        source_addresses = dict(cluster.shard_addresses(0))
        assert sorted(source_addresses) == SERVING, "the shard is where it was put"

        _write_rows(cluster)
        before = _published(client, (0,))
        assert before.shard(0).nodes == SERVING

        _copy_but_do_not_propose(cluster)
        assert client.table(refresh=True).shard(0).nodes == SERVING, "the table was not told"
        assert cluster._serving_nodes(0) == SERVING, "and neither was the cluster"
        assert cluster._load_migration_record(0) is not None, "the move is written down"

        retired = _finish_the_move(cluster)

        assert retired == SERVING, "the group it left went, on every node holding it"
        after = client.table(refresh=True)
        assert after.shard(0).nodes == MOVE_TO
        assert after.shard(0).addresses == cluster._addresses_on(MOVE_TO, 0)
        assert (after.shard(0).start, after.shard(0).end) == \
            (before.shard(0).start, before.shard(0).end), "a move is not a re-range"

        # Every question the cluster answers about the shard is about the new group now,
        # and the old one is gone from every node that held it - group, port and storage.
        assert cluster._serving_nodes(0) == MOVE_TO
        assert cluster.shard_replica_ids(0) == MOVE_TO
        for node_id in SERVING:
            assert cluster.get_shard_server(node_id).get_shard_node(0) is None
            with pytest.raises(OSError):
                socket.create_connection(_host_port(source_addresses[node_id]), timeout=1)

        # Renamed and not deleted: the rows are still on disk under a name nothing reads,
        # the old name is free, and the note that said a move was in flight has gone.
        orphans = cluster.orphan_dirs()
        assert len(orphans) == len(SERVING), "one directory aside per node that held it"
        for orphan in orphans:
            assert os.path.basename(orphan).startswith("orphan-shard-0-")
            abandoned = EngineRaftStorage(data_dir=orphan)
            try:
                assert abandoned.load_admin(NOTE) is None, "the note goes with the move"
            finally:
                abandoned.close()
        for node_id in SERVING:
            assert not os.path.isdir(str(tmp_path / f"shard0_node{node_id}")), \
                "the name the shard was stored under is free"

        # A client that routes by the table reads the moved rows out of the group the
        # table names, which is the whole point of telling it.
        _published(client, (0,))
        leader = RoutingCache(cluster, client,
                              factory=LocalNodeClientFactory(cluster)).leader_for_key(KEYS[0])
        assert leader is not None
        assert leader.get(KEYS[0]).value == VALUES[0]
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_the_group_the_shard_left_keeps_answering_until_the_window_closes(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        source_address = cluster.shard_addresses(0)[SERVING[0]]
        _write_rows(cluster)
        _published(client, (0,))
        _copy_but_do_not_propose(cluster)
        assert cluster._propose_move(0, MOVE_TO).ok

        # A window long enough to be sampled inside, and a sample taken a tenth of the way
        # in, so that what is asserted is about the window rather than about how busy the
        # machine was.
        window = 3.0
        thread = threading.Thread(target=cluster._commit_move, args=(0, MOVE_TO),
                                  kwargs={"drain": window}, daemon=True)
        thread.start()
        try:
            time.sleep(0.3)
            assert cluster.get_shard_server(SERVING[0]).get_shard_node(0) is not None, \
                "the group is still up while the window is open"
            with socket.create_connection(_host_port(source_address), timeout=2):
                pass
            assert cluster._load_migration_record(0) is not None, \
                "and the note is still on disk until the move is finished"
            assert cluster._serving_nodes(0) == MOVE_TO, \
                "while the cluster already routes to the new group"
        finally:
            thread.join(timeout=15)

        assert not thread.is_alive(), "the window is a wait, not a hang"
        assert cluster.get_shard_server(SERVING[0]).get_shard_node(0) is None
        assert cluster._load_migration_record(0) is None
    finally:
        cluster.shutdown()
        metadata.shutdown()


def test_finishing_a_move_twice_is_finishing_it_once(tmp_path):
    metadata, cluster = _cluster_with_metadata(tmp_path, shard_nodes={0: SERVING})
    try:
        client = wait_for_metadata_client(metadata)
        _write_rows(cluster)
        _published(client, (0,))
        _copy_but_do_not_propose(cluster)
        assert _finish_the_move(cluster) == SERVING
        after = client.table(refresh=True)

        # The cluster that comes back to a finished move: the proposal is made again
        # because the answer may have been lost, and the machine recognises the move it
        # already applied rather than applying a second one.
        assert cluster._propose_move(0, MOVE_TO).ok, "a lost answer is proposed again"
        orphans = cluster.orphan_dirs()
        assert cluster._commit_move(0, MOVE_TO, drain=0) == [], "nothing left to close"
        assert cluster.orphan_dirs() == orphans, "and nothing left to move aside"

        assert client.table(refresh=True).version == after.version, "one move, one write"
        assert cluster._serving_nodes(0) == MOVE_TO
        assert cluster.shard_replica_ids(0) == MOVE_TO
    finally:
        cluster.shutdown()
        metadata.shutdown()
