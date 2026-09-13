"""Keeping the routing table in step with the cluster.

The table is only worth having if it is current, and the only process that knows where
the shards are is the cluster itself: which nodes serve a shard, and which of them won
the last election.  This is the thread that tells it - the same shape as the lock
cleaner, and for the same reason: a protocol that only tests run is a protocol that
does not run.

It publishes on change rather than on a timer.  The ranges and each shard's replica set
are proposed once, and a leader report only when the leader or its term moved, because
the table is read by every client that routes a key: a pass that rewrote it every poll
would give every one of those caches a reason to refresh.  A poll that finds nothing
costs one comparison.

A report that loses is normal - two reports racing is what terms are for - so a refused
report is not an error here.  A table that already holds *different* ranges is, and the
publisher stops rather than overwriting it: two clusters publishing placement for one
keyspace is a mistake an operator has to see, not one for the table to hide.
"""

import threading
from typing import Any, Dict, Optional, Tuple

from ..raft.node import NodeState
from ..raft.state_machine import ErrorCode
from .service import MetadataClient

#: How often the publisher looks for something to say.  The table is on nobody's hot
#: path, so this only decides how long a leader change takes to become visible.
DEFAULT_PUBLISH_INTERVAL = 0.5


class MetadataPublisher:
    """Publishes this cluster's placement into the metadata group.

    ``shard_cluster`` is used through five methods - ``range_map``, ``shard_ids``,
    ``shard_replica_ids``, ``shard_addresses`` and ``shard_leader`` - which is the whole
    of what the table needs to know about a cluster.
    """

    def __init__(self, client: MetadataClient, shard_cluster,
                 poll_interval: float = DEFAULT_PUBLISH_INTERVAL):
        self._client = client
        self._cluster = shard_cluster
        self._poll_interval = poll_interval
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._routes_published = False
        self._nodes_published: set = set()
        self._leaders: Dict[int, Tuple[int, int]] = {}
        #: Set when the table disagrees with this cluster.  Publishing stops for good:
        #: there is nothing this thread could write that would be right.
        self.error: Optional[str] = None
        #: The last transient failure - a metadata group mid-election, a shard with no
        #: leader - which is retried and therefore not worth stopping over.
        self.last_error: Optional[str] = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="metadata-publisher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.publish_once()
            except Exception as error:
                with self._lock:
                    self.last_error = str(error)
            self._stop.wait(self._poll_interval)

    # -- one pass ----------------------------------------------------------

    def publish_once(self) -> None:
        """Say everything that has changed since the last pass, and nothing else."""
        if self.error is not None:
            return

        self._publish_routes()
        if not self._routes_published:
            return

        for shard_id in sorted(self._cluster.shard_ids()):
            self._publish_nodes(shard_id)
            self._publish_leader(shard_id)

    def _publish_routes(self) -> None:
        if self._routes_published:
            return

        ranges = self._cluster.range_map()
        result = self._client.init_routes(ranges)
        if result.success:
            self._routes_published = True
            return
        if result.error_code == ErrorCode.ERR_NOT_LEADER:
            return  # the group is still electing; the next pass tries again

        # The command was refused as a command, which for this one means the table has
        # ranges already.  If they are ours the cluster has simply restarted; if they
        # are not, this cluster is not the one that owns this keyspace.
        table = self._client.table(refresh=True)
        if table.routes() == ranges:
            self._routes_published = True
            return
        self.error = (f"the metadata table already holds different ranges "
                      f"{table.routes()!r}; refusing to overwrite them with {ranges!r}")

    def _publish_nodes(self, shard_id: int) -> None:
        if shard_id in self._nodes_published:
            return

        nodes = self._cluster.shard_replica_ids(shard_id)
        if not nodes:
            return

        result = self._client.set_shard_nodes(
            shard_id, nodes, self._cluster.shard_addresses(shard_id))
        if result.success:
            self._nodes_published.add(shard_id)
            return
        if result.error_code == ErrorCode.ERR_NOT_LEADER:
            return
        # A command-level refusal here means the table has no such range, and a replica
        # set will not talk it into having one: ranges come from a split, and a split
        # publishes its own.  Retrying a proposal that cannot apply would be a log entry
        # every poll, so this shard is left until the ranges change.
        self._nodes_published.add(shard_id)

    def _publish_leader(self, shard_id: int) -> None:
        if shard_id not in self._nodes_published:
            return

        observed = self._cluster.shard_leader(shard_id)
        if observed is None:
            return
        if self._leaders.get(shard_id) == observed:
            return

        reported = self._leaders.get(shard_id)
        if reported is not None and observed[1] <= reported[1]:
            # We have already named a leader at least as new as this one.  Nothing to
            # report, and reporting it would be a proposal the table would refuse.
            return

        result = self._client.report_leader(shard_id, observed[0], observed[1])
        if result.success:
            with self._lock:
                self._leaders[shard_id] = observed
            return
        if result.error_code == ErrorCode.ERR_NOT_LEADER:
            return
        # Refused as a command: the table already has a claim at least as new (so there
        # is nothing to fix), or this node is not in the replica set (so there is
        # nothing this thread can do).  Either way, remember it rather than proposing
        # the same losing report every poll.
        with self._lock:
            self._leaders[shard_id] = observed

    def leaders(self) -> Dict[int, Tuple[int, int]]:
        """What this publisher has told the table, for callers that want to look."""
        with self._lock:
            return dict(self._leaders)