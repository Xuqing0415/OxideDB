"""A client that read the routing table, and a shard that moves out from under it.

A move leaves a window behind itself - the group the shard left goes on answering after the
table has moved on (``RecoveryRunner._commit_move``) - and that window exists for
exactly
one caller: a client that routes by a table it read before the move and asks the group the
table named.  ``tests/test_move_proposal.py`` checks the window from the cluster's side: that
the group is still up and its port still bound while it is open.  These tests put a client on
the other side of it, and pin the three answers it gets:

* a read is answered, and that is the whole point of the window: the rows are the ones the
  copy carried away, unchanged, so a client that arrived a moment too late is answered
  instead of meeting a closed port;
* a write is refused, because the group was frozen before its rows were read.  Nothing is
  written anywhere - the command is refused and not forwarded to the group that will own the
  range - and what the client is told is that the shard is moving;
* after the window the group is gone, and nobody tells the client anything: it asks the
  addresses it holds one at a time and then reads the table again, which is the same walk a
  leader change takes.  That walk costs one timeout per address of the set that left, which
  is what the timeout below is sized for.

The client here is a real one - ``RemoteNodeClientFactory``, over a socket to the port a shard
listens on - because half of what a client does with a refusal happens at the seam: the
shard's own code for a frozen range (306, ``ERR_MIGRATING``) does not cross the wire, and a
caller is told the one code the classification has for "the shard said no", with the shard's
words left in the message.  A test holding the node object would not see that, and the
difference is what a caller can act on.

The cluster is in this process, and its table client with it: a move is started by the cluster
and not by anything a client service offers, so there is no moving a shard over a wire here.
The table the client holds is one a publisher wrote either way.
"""

import threading

from _ports import free_addresses
from _wait import wait_for_metadata_client, wait_until
from oxidedb.client import RemoteNodeClientFactory
from oxidedb.client.routing import ShardLeaders, ask_shard
from oxidedb.metadata.cache import RoutingCache
from oxidedb.metadata.service import MetadataCluster
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import (CommandType, ErrorCode, MVCCStateMachine,
                                        serialize_command)
from oxidedb.raft.storage import EngineRaftStorage

COUNT = 20
KEYS = [b"key%03d" % index for index in range(COUNT)]
VALUES = [b"value%03d" % index for index in range(COUNT)]

#: Five nodes with the shard on three of them: a move needs somewhere to go that is not where
#: the shard is, and one node is not a replica set.
SERVING = [1, 2, 3]
MOVE_TO = [4, 5]

#: How long the group the shard leaves goes on answering.  Long enough that a few calls over a
#: socket land inside it, short enough that waiting it out is not what these tests cost.
WINDOW = 2.0

#: How long one call to a node is given.  Short, because the addresses a closed group leaves
#: behind are asked one at a time and each of them spends this much before the table is read
#: again.  A node of this cluster, which is in this process and listening on localhost,
#: answers in about a millisecond.
WIRE_TIMEOUT = 0.5


def _start_metadata():
    metadata = MetadataCluster(num_nodes=3)
    metadata.start()
    return metadata


def _start_cluster(tmp_path, addresses, metadata, shard_nodes=None, storages=None):
    """A five-node cluster over ``addresses``, with shard 0 on ``shard_nodes``.

    ``storages`` collects the storages the cluster was built with, so that a test can release
    them itself: on Windows a directory cannot be renamed while a file inside it is open, and
    renaming one is how a move puts the group it left aside.
    """
    def storage(node_id, shard_id):
        opened = EngineRaftStorage(data_dir=str(tmp_path / f"shard{shard_id}_node{node_id}"))
        if storages is not None:
            storages.append(opened)
        return opened

    cluster = ShardedRaftCluster(num_nodes=5, num_shards=1)
    cluster.start_network(
        state_machine_factory=lambda: MVCCStateMachine(),
        peer_addresses=addresses,
        storage_factory=storage,
        shard_nodes=shard_nodes,
        lock_cleaner_interval=None,
        metadata=metadata,
    )
    return cluster


def _close(storages):
    """Release the storages a cluster was built with, so their directories can be renamed."""
    for storage in storages:
        storage.close()


def _write_rows(cluster, count=COUNT):
    """``count`` committed rows in the shard about to move, and the leader holding them."""
    wait_until(lambda: cluster.get_leader_for_key(KEYS[0]),
               message="shard 0 never elected a leader")
    leader = cluster.get_leader_for_key(KEYS[0])[1]
    for index in range(count):
        assert leader.propose(
            serialize_command(CommandType.SET, key=KEYS[index], value=VALUES[index],
                              timestamp=index + 1)).success
    return leader


def _published(client, shard_ids):
    """The table, once the publisher has filled in a leader for every one of ``shard_ids``."""
    def ready():
        table = client.table(refresh=True)
        for shard_id in shard_ids:
            placement = table.shard(shard_id)
            if placement is None or placement.leader_id is None:
                return None
        return table

    return wait_until(ready, message="the cluster never published a leader for the shard")


def _a_client_holding_the_table(cluster, table_client, factory):
    """A client that has read the table once, and the lookup that routes by it.

    The cache is the client's own copy of the placement - it reads once and keeps the answer
    until something says it is old - and ``ShardLeaders`` is the object the coordinator, the
    lock resolver and the SQL executor all route through, so what is asked below is asked the
    way this project's clients ask it.
    """
    cache = RoutingCache(cluster, table_client, factory=factory)
    return cache, ShardLeaders(cluster, router=cache)


class _AMoveAndTheClientThatMissedIt:
    """A cluster moving shard 0, and a client that read the routing table before it did.

    Holding the two together is the whole of what these tests are about: nobody tells the
    client that the shard moved, so everything it learns, it learns from the group it asks.
    This sets up the rows, the table, the client over real sockets and the move itself, so
    that each test says only which moment it wants and what the client does in it.
    """

    def __init__(self, tmp_path, drain: float = WINDOW):
        self._storages = []
        self._metadata = _start_metadata()
        self._cluster = _start_cluster(tmp_path, free_addresses(num_nodes=5), self._metadata,
                                       shard_nodes={0: SERVING}, storages=self._storages)
        self._factory = RemoteNodeClientFactory(timeout=WIRE_TIMEOUT)
        self._move = None
        self._drain = drain
        _write_rows(self._cluster)
        self.table_client = wait_for_metadata_client(self._metadata)
        _published(self.table_client, (0,))
        self.cache, self.leaders = _a_client_holding_the_table(
            self._cluster, self.table_client, self._factory)
        assert self.cache.table().shard(0).nodes == SERVING, \
            "the client read the table before the move, which is the whole setup"

    @property
    def cluster(self):
        return self._cluster

    def start_the_move(self) -> None:
        """Start the move, and wait until both groups are up - the inside of the window."""
        self._move = threading.Thread(target=self._cluster.move_shard, args=(0, MOVE_TO),
                                      kwargs={"drain": self._drain}, daemon=True)
        self._move.start()
        _the_window_is_open(self._cluster)

    def wait_for_the_move(self) -> None:
        """Wait until the window is over and the group the shard left has been put aside."""
        assert self._move is not None, "nothing was started"
        self._move.join(timeout=30)
        assert not self._move.is_alive(), "the window is a wait, not a hang"
        self._move = None

    def ask(self, question):
        """``question`` asked of shard 0 through the client that is holding the old table."""
        return ask_shard(self.leaders, 0, question)

    def close(self) -> None:
        if self._move is not None:
            self._move.join(timeout=30)
        self._factory.close()
        self._cluster.shutdown()
        self._metadata.shutdown()
        _close(self._storages)

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.close()
        return False


def _the_window_is_open(cluster, timeout: float = 30.0):
    """Wait until the shard is the new group's and the group it left is still up.

    That conjunction is the window and not a sleep.  A commit makes the new set this
    cluster's answer for the shard first and lets the old group go only after the drain, so
    the moment both are true is the moment a client can be routed by the placement it read
    before the move and still be answered.  A cluster where that never happens fails here
    rather than passing an assertion by luck.
    """
    return wait_until(
        lambda: (cluster._serving_nodes(0) == MOVE_TO
                 and cluster.get_shard_server(SERVING[0]).get_shard_node(0) is not None),
        timeout=timeout,
        message="the move never reached the window in which the shard is the new group's "
                "and the group it left is still serving")


def test_a_read_the_client_arrived_with_is_answered_by_the_group_the_shard_left(tmp_path):
    """The window, used by the client it exists for.

    The client's table is read and kept; the move commits underneath it; the client asks the
    group the table named and is answered, because that group is still up and its rows are
    what the copy carried away rather than anything that changed.  What says the answer came
    from there and not from a table read is that the table has already moved on: the cluster
    has the shard on the new nodes while the client's own copy still names the old ones.
    """
    with _AMoveAndTheClientThatMissedIt(tmp_path) as scenario:
        scenario.start_the_move()

        assert scenario.table_client.table(refresh=True).shard(0).nodes == MOVE_TO, \
            "the table has moved on"
        assert scenario.cache.table().shard(0).nodes == SERVING, \
            "and the copy this client holds has not"

        answer = scenario.ask(lambda client: client.get(KEYS[3]))

        assert answer.success, answer.error_msg
        assert answer.value == VALUES[3]
        assert scenario.cache.refreshes == 0, \
            "answered out of the placement the client was already holding"


def test_a_write_the_client_arrived_with_is_refused_rather_than_forwarded(tmp_path):
    """A frozen group refuses a write, and the client is told what the shard is doing.

    The old group has been frozen since before its rows were read, so the row a client sends
    it is refused - across the wire as the one code the seam has for "the shard said no",
    because the shard's own code does not cross it, with the shard's words left in the
    message.  What a caller can act on here is the prose and a table it can read again, which
    is why the next move is the caller's and not this client's.

    And nothing is written anywhere: the refusal is not a forwarding, so the group that is
    about to own the range does not have the row either.
    """
    with _AMoveAndTheClientThatMissedIt(tmp_path) as scenario:
        scenario.start_the_move()
        command = serialize_command(CommandType.SET, key=b"late", value=b"late")

        refused = scenario.ask(lambda client: client.propose(command))

        assert not refused.success
        assert refused.error_code == ErrorCode.ERR_APPLY_ERROR, \
            "the move's own code does not survive the wire's classification"
        assert "moving to another group" in refused.error_msg, \
            "the shard's own words do"
        assert refused.leader_address is None, \
            "a group that is moving knows nowhere to send the client"
        assert scenario.cache.refreshes == 0, "a refusal did not send the client to the table"

        new_group = scenario.cluster.get_leader_for_key(b"late")[1]
        assert new_group.get(b"late").value is None, "a refused write is not a write anywhere"


def test_a_client_still_holding_the_old_table_finds_the_new_group_once_the_window_closes(
        tmp_path):
    """What recovers the client is the walk, and it costs one timeout per address.

    The group the shard left is gone, and the client's table still names it - so the address
    it asks does not answer, which is the same evidence a refusal is: not the node to ask.
    The rest of the set the table named is walked the same way, one call each, and only then
    is the table read again and the group the shard moved to named.  Nothing else tells the
    client anything, and that is the recovery the window is sized against.

    The price is the timeout on the factory, spent once per address of the set that left:
    three of them here, which is why the constant is small.  It is not a retry budget and it
    does not grow with how long the shard is gone - the set is finite and the table is read
    once - so a caller that wants it cheaper wants the set smaller, not the client cleverer.
    """
    with _AMoveAndTheClientThatMissedIt(tmp_path) as scenario:
        scenario.start_the_move()
        scenario.wait_for_the_move()

        assert [scenario.cluster.get_shard_server(node).get_shard_node(0) for node in SERVING] \
            == [None, None, None], "the group the shard left is gone"

        answer = scenario.ask(lambda client: client.get(KEYS[3]))

        assert answer.success, answer.error_msg
        assert answer.value == VALUES[3]
        assert scenario.cache.refreshes == 1, "the walk ends at one read of the table"

        written = scenario.ask(lambda client: client.propose(
            serialize_command(CommandType.SET, key=b"late", value=b"late")))

        assert written.success, written.error_msg
        new_group = scenario.cluster.get_leader_for_key(b"late")[1]
        assert new_group.get(b"late").value == b"late"
