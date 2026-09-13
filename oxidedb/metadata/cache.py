"""The routing table as a client holds it: read once, kept, re-read when a shard says so.

A client cannot afford a linearizable read of the metadata group per key, so it reads the
table once and routes from that decision.  That is safe for the reason section 7 of the
design notes gives - the cached table was a decision when it was read, not a guess - so a
key routed the wrong way is refused by the shard it lands on, and the refusal is a retry
rather than a wrong answer.  What this file adds is the retry: a lookup that meets a
refusal goes back to the table and hands out the new answer.

Being out of date is the normal case, not an error.  A shard's leader moves when its node
dies, and the table learns about it on the publisher's next poll; in between, every client
holding the old table is routing at a node that has stopped leading.  So a refresh is
deliberately narrow - it happens when a shard refuses a read, and when the node the table
names has stopped claiming leadership, which is the one kind of staleness a cache can see
for itself - and never on a timer, because the table is on the hot path and a fetch per
key is exactly what the cache exists to avoid.

It says nothing about how the client reaches a shard once it knows where that shard is.
Here it is an object in the same process; a networked client would take the address the
table publishes for the same node, which is why the table carries addresses at all.
"""

import threading
from typing import Optional

from ..raft.node import MemoryRaftNode, NodeState
from .service import MetadataClient, RoutingTable, ShardPlacement


class RoutingCache:
    """The cluster's placement as one client sees it, with a way to be told it is old."""

    def __init__(self, cluster, client: MetadataClient):
        self._cluster = cluster
        self._client = client
        self._lock = threading.RLock()
        #: The client's own copy of the decision.  It is kept here rather than left
        #: to ``MetadataClient``'s cache because that one is dropped by the writes
        #: this client makes - which for the cluster's publishing client is every
        #: write it makes - and a cache that is dropped by somebody else's write is
        #: not a cache a reader can reason about.
        self._table: Optional[RoutingTable] = None
        #: How many times this client has gone back to the table.  A client that
        #: refreshes constantly is a client whose shards keep moving; one that never
        #: does is a client that has not been around for an election.
        self.refreshes = 0

    @classmethod
    def from_cluster(cls, cluster) -> Optional["RoutingCache"]:
        """A cache for a cluster that publishes to a metadata group, or None.

        None is the honest answer for a cluster started without a metadata service:
        there is no table to read, and the caller keeps the older behaviour of looking
        at the cluster's own nodes.
        """
        client = cluster.metadata_client()
        if client is None:
            return None
        return cls(cluster, client)

    def table(self) -> RoutingTable:
        """The cached table, read from the group the first time.

        The first read is a linearizable one - the group's ReadIndex path, the
        same read any other key would take - and after that this is a local
        object until something says it is wrong.
        """
        with self._lock:
            if self._table is None:
                self._table = self._client.table(refresh=True)
            return self._table

    def refresh(self) -> RoutingTable:
        """Read the table again, after a shard said the cached one was old."""
        with self._lock:
            self.refreshes += 1
            self._table = self._client.table(refresh=True)
            return self._table

    def shard_for(self, key: bytes) -> Optional[ShardPlacement]:
        """Where the table says ``key`` lives."""
        return self.table().shard_for(key)

    def leader_for_shard(self, shard_id: int) -> Optional[MemoryRaftNode]:
        """The node the table names as ``shard_id``'s leader, if it is still leading.

        A table that names a node which has since stepped down is stale in the one way
        this layer can detect by itself, so the lookup re-reads the table rather than
        handing back a node it can see is not the leader.  A table that names nobody is
        not stale - it is the table saying the shard has no leader - so that answer is
        returned as it stands.
        """
        node = self._node_for(shard_id)
        if node is not None and node.state != NodeState.LEADER:
            self.refresh()
            return self._node_for(shard_id)
        return node

    def leader_for_key(self, key: bytes) -> Optional[MemoryRaftNode]:
        """The node the table says should answer for ``key``."""
        placement = self.shard_for(key)
        if placement is None:
            return None
        return self.leader_for_shard(placement.shard_id)

    def _node_for(self, shard_id: int) -> Optional[MemoryRaftNode]:
        placement = self.table().shard(shard_id)
        if placement is None or placement.leader_id is None:
            return None
        server = self._cluster.get_shard_server(placement.leader_id)
        if server is None:
            return None
        return server.get_shard_node(shard_id)
