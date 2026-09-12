"""Apply results are kept in a bounded window, not for the life of the node.

``_apply_results`` exists so a waiting ``propose`` can read back the result of
the index it waited for, and nothing reads older entries.  Keeping one result per
applied command forever is a leak: a node that has compacted its log still pays
for every command it ever applied.  These tests pin both halves of the fix - the
map stays bounded, and the result a proposal just waited for is still there,
failure included.
"""

from _wait import wait_for_single_leader

from oxidedb.raft.node import _ApplyResults, RaftCluster
from oxidedb.raft.state_machine import CommandType, ErrorCode, MVCCStateMachine


WINDOW = 4
WRITES = 12


def _set_command(state_machine, key, value, timestamp):
    return state_machine.serialize_command(
        CommandType.SET, key=key, value=value, timestamp=timestamp
    )


def test_the_window_evicts_the_oldest_result_first():
    results = _ApplyResults(capacity=3)
    for index in range(1, 6):
        results.record(index, f"result-{index}")

    assert list(results) == [3, 4, 5]
    assert results.get(5) == "result-5"
    assert results.get(2) is None


class TestApplyResultsWindow:
    def test_a_node_keeps_only_the_most_recent_results(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine(), apply_results_window=WINDOW)

        try:
            leader = cluster.get_node(wait_for_single_leader(cluster))

            for i in range(WRITES):
                command = _set_command(
                    leader._state_machine, f"k{i}".encode(), b"v", i + 1
                )
                assert leader.propose(command).success

            assert len(leader._apply_results) == WINDOW
            assert list(leader._apply_results) == list(
                range(leader._last_applied - WINDOW + 1, leader._last_applied + 1)
            )

            # The window bounds the bookkeeping, not the data: everything the
            # node applied is still readable from the state machine.
            assert leader._state_machine.get(b"k0").value == b"v"
            assert leader._state_machine.get(b"k11").value == b"v"
        finally:
            cluster.shutdown()

    def test_the_result_of_a_rejected_command_survives_the_window(self):
        cluster = RaftCluster(num_nodes=3)
        cluster.start(lambda: MVCCStateMachine(), apply_results_window=WINDOW)

        try:
            leader = cluster.get_node(wait_for_single_leader(cluster))
            state_machine = leader._state_machine

            assert leader.propose(
                _set_command(state_machine, b"k", b"newer", 100)
            ).success

            # A prewrite below the version already written is a write conflict,
            # so the state machine rejects it.  ``propose`` must hand that
            # failure back rather than falling through to its default success.
            prewrite = state_machine.serialize_command(
                CommandType.PREWRITE,
                key=b"k",
                value=b"older",
                start_ts=5,
                primary_key=b"k",
            )
            result = leader.propose(prewrite)

            assert not result.success
            assert result.error_code == ErrorCode.ERR_WRITE_CONFLICT
        finally:
            cluster.shutdown()
