"""A shard that is being copied out of must not take new rows while it is copied.

A split reads a shard's rows at one moment and copies them into the shard that will own
them.  If a row can be written after that moment, there are two ways to lose it, and the
old code took both: the row is written into the source shard after the copy was taken,
so the new owner never has it; or it is written into the new shard before the copy, and
whatever the copy then does is beside the point.  So the source shard is frozen for the
duration - new rows refused, work already under way allowed to finish - and thawed only
once the shard that will own the range has the rows.

What these tests pin: a frozen shard refuses the commands that add rows and still lands
the commit of a transaction that prewrote before the freeze; a split freezes the source
while the rows are being copied, and the write it refuses there is nowhere at all
afterwards; and a split that refuses for a reason of its own thaws the shard it froze, so
the caller can come back.
"""

import threading

from _ports import free_addresses
from _wait import wait_for_keys_leader, wait_for_leader, wait_until
from oxidedb.raft.node import RaftCluster
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import CommandType, ErrorCode, MVCCStateMachine
from oxidedb.shard.router import locate

KEPT_KEY = b"a_key"    # below the split point, stays in shard 0
MOVED_KEY = b"z_key"   # at or above it, copied into the new shard


def _set(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp)


def _prewrite(state_machine, key, value, start_ts, primary_key=None):
    return state_machine.serialize_command(
        CommandType.PREWRITE, key=key, value=value, start_ts=start_ts,
        primary_key=primary_key or key)


def _commit(state_machine, key, start_ts, commit_ts):
    return state_machine.serialize_command(
        CommandType.COMMIT, key=key, start_ts=start_ts, commit_ts=commit_ts)


def _started_cluster(num_shards=1):
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=num_shards)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        # A node's ports are a block of its own, so the nodes are a whole block apart:
        # the port a split binds for the shard it creates is this node's, not a
        # neighbour's.
        peer_addresses=free_addresses(),
        lock_cleaner_interval=None,
    )
    return cluster


def test_a_frozen_shard_refuses_new_rows_until_it_is_thawed():
    """The commands that add rows are refused; the shard is otherwise untouched."""
    raft = RaftCluster(num_nodes=3)
    raft.start(lambda: MVCCStateMachine())
    try:
        leader = raft.get_node(wait_for_leader(raft))
        machine = leader._state_machine
        assert leader.propose(_set(machine, b"k", b"v1", 1)).success

        leader.freeze_writes()
        refusals = [
            leader.propose(_set(machine, b"k2", b"v2", 2)),
            leader.propose(_prewrite(machine, b"k3", b"v3", 3)),
            leader.propose(machine.serialize_command(CommandType.DELETE, key=b"k", timestamp=4)),
        ]
        for refusal in refusals:
            assert not refusal.success
            assert refusal.error_code == ErrorCode.ERR_SPLIT_IN_PROGRESS, refusal.error_msg

        assert leader.get(b"k2").value is None, "a refused row is nowhere"
        assert leader.get(b"k3").value is None
        assert leader.get(b"k").value == b"v1", "the row that was there is still there"

        leader.resume_writes()
        assert leader.propose(_set(machine, b"k2", b"v2", 2)).success
        assert leader.get(b"k2").value == b"v2"
    finally:
        raft.shutdown()


def test_a_transaction_that_prewrote_before_the_freeze_still_commits():
    """The freeze is about rows that do not exist yet, not about work in flight.

    A commit is how a transaction that already prewrote finishes.  Dropping it would
    drop a write that was promised before the copy had a reason to care, and leave a
    lock behind on a shard whose rows are about to be copied with that lock in them.
    """
    raft = RaftCluster(num_nodes=3)
    raft.start(lambda: MVCCStateMachine())
    try:
        leader = raft.get_node(wait_for_leader(raft))
        machine = leader._state_machine
        assert leader.propose(_prewrite(machine, b"k", b"v", 10)).success

        leader.freeze_writes()
        blocked = leader.propose(_prewrite(machine, b"other", b"v", 11))
        assert blocked.error_code == ErrorCode.ERR_SPLIT_IN_PROGRESS

        assert leader.propose(_commit(machine, b"k", 10, 20)).success, "the commit has to land"
        assert leader.get(b"k").value == b"v"
        assert leader._state_machine._storage.get_newest_lock(b"k") is None

        leader.resume_writes()
        assert not leader.writes_frozen
    finally:
        raft.shutdown()


def test_the_source_shard_is_frozen_while_its_rows_are_copied(monkeypatch):
    """The freeze is not a flag the split sets and forgets: it is on during the copy.

    Observed from inside the copy, which is the only place that can see it: the row the
    split is about to copy is read from a shard that is refusing new rows at that
    moment, and the row it refuses is in neither shard once the split is done.
    """
    cluster = _started_cluster()
    seen = {}
    original = ShardedRaftCluster._move_row

    def spy_move_row(self, source, target, key, value, version):
        if not seen:
            # The copy is handed a client; the freeze it has to be seen on is the node
            # the client wraps, which in this process the wrapper holds.
            node = source._node
            seen["frozen"] = node.writes_frozen
            seen["refused"] = node.propose(
                _set(node._state_machine, b"x_new", b"v", 999))
        return original(self, source, target, key, value, version)

    monkeypatch.setattr(ShardedRaftCluster, "_move_row", spy_move_row)
    try:
        wait_for_keys_leader(cluster, [KEPT_KEY, MOVED_KEY])
        source = cluster.get_leader_for_key(MOVED_KEY)[1]
        assert source.propose(_set(source._state_machine, KEPT_KEY, b"kept", 1)).success
        assert source.propose(_set(source._state_machine, MOVED_KEY, b"moved", 2)).success

        assert cluster.split_shard(0, b"n"), "the split has to happen"
    finally:
        cluster.shutdown()

    assert seen, "the split copied no rows, so nothing was observed"
    assert seen["frozen"] is True, "the source shard was open while its rows were copied"
    assert not seen["refused"].success
    assert seen["refused"].error_code == ErrorCode.ERR_SPLIT_IN_PROGRESS
    assert seen["refused"].error_msg


def test_a_freeze_waits_out_a_proposal_that_was_already_admitted():
    """Freezing is a boundary, not a flag: the writes already in the air land first.

    The proposal here is left unanswered by both peers, so it sits in flight until its
    own timeout.  The freeze must not hand the caller a quiet shard to read while a
    write it admitted could still be applied - the copy is taken after such a write,
    not around it.
    """
    raft = RaftCluster(num_nodes=3)
    raft.start(lambda: MVCCStateMachine())
    try:
        leader = raft.get_node(wait_for_leader(raft))
        machine = leader._state_machine
        # Nobody can reach anybody, so the proposal is never acknowledged and the
        # leader is never told about a newer term: it sits in flight until its own
        # timeout.  Cutting only the leader's links would let the other two elect a
        # successor and abort the proposal, which is a different test.
        for node in raft._nodes.values():
            node._get_peer_node = lambda _peer_id: None

        result = {}

        def slow_write():
            result["applied"] = leader.propose(_set(machine, b"k", b"v", 1), timeout=1.0)

        thread = threading.Thread(target=slow_write, daemon=True)
        thread.start()
        wait_until(lambda: leader._proposes > 0, message="the proposal never started")

        leader.freeze_writes()
        assert not leader.wait_for_writes_to_drain(timeout=0.2), (
            "the freeze returned while a write it had admitted was still in flight")

        thread.join(timeout=5)
        assert not thread.is_alive()
        assert not result["applied"].success, "both peers were cut off"
        assert leader.wait_for_writes_to_drain(timeout=1.0), "the drain never finished"
        assert leader.get(b"k").value is None
    finally:
        raft.shutdown()


def test_a_split_that_refuses_thaws_the_shard_it_froze():
    """A caller that is told no has to be able to come back.

    The refusal here is a lock in the range, which is a reason to wait rather than a
    reason to stop answering writes: the shard is exactly as it was before the split
    was attempted, and the next attempt - once the transaction has settled - goes
    through.
    """
    cluster = _started_cluster()
    try:
        wait_for_keys_leader(cluster, [KEPT_KEY, MOVED_KEY])
        leader = cluster.get_leader_for_key(MOVED_KEY)[1]
        machine = leader._state_machine
        assert leader.propose(_prewrite(machine, MOVED_KEY, b"v", 10)).success

        assert not cluster.split_shard(0, b"n"), "a lock in the range stops the split"
        assert not leader.writes_frozen, "a refused split has to thaw the shard"
        assert locate(cluster.range_map(), MOVED_KEY) == 0

        assert leader.propose(machine.serialize_command(
            CommandType.ROLLBACK, key=MOVED_KEY, start_ts=10)).success
        assert cluster.split_shard(0, b"n"), "with nothing in flight, the split happens"
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    test_a_frozen_shard_refuses_new_rows_until_it_is_thawed()
    test_a_transaction_that_prewrote_before_the_freeze_still_commits()
    test_a_split_that_refuses_thaws_the_shard_it_froze()
