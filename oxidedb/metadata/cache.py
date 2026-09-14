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
deliberately narrow - when a shard refuses a read, and when the table names nobody at all,
which is also what a table read between the range and the leader report looks like.  It is
never per key: the table is on the hot path, and a fetch per key is exactly what the cache
exists to avoid.  The one repeated read is the nameless one, and it is rate-limited to the
publisher's own poll period - see :data:`MISSING_LEADER_REFRESH_INTERVAL`.

The refusal is the caller's to notice, and that is why what this hands out is a client and
not a node.  A node the table names is exactly the node whose own belief about leading is
in question - one cut off from its peers goes on answering as if nothing had happened - so
this file does not ask it, and does not read its state.  ``refresh_for_shard`` is what a
caller that met a refusal calls; ``ask_shard`` in ``client/routing.py`` is that caller.

It says nothing about how a client reaches a shard once it knows where that shard is: the
factory does, and what this file decides is which node the factory is asked for.
"""

import threading
import time
from typing import Optional

from ..client.node_client import (LocalNodeClientFactory, NodeClient,
                                  NodeClientFactory)
from .service import MetadataClient, RoutingTable, ShardPlacement

#: How long a client waits before reading a table again because it named no leader for a
#: shard.  Placement is published a command at a time - the ranges, then a shard's replica
#: set, then its leader - so a table read in between names a range nobody leads yet, and a
#: client that took that for a final answer would route to nothing for the rest of its
#: life.  Re-reading faster than the publisher writes learns nothing, so the wait is the
#: publisher's own poll period (``publisher.DEFAULT_PUBLISH_INTERVAL``) and a shard that
#: really has no leader costs one read per interval rather than one per read.
MISSING_LEADER_REFRESH_INTERVAL = 0.5


class RoutingCache:
    """The cluster's placement as one client sees it, with a way to be told it is old."""

    def __init__(self, cluster, client: MetadataClient,
                 factory: Optional[NodeClientFactory] = None,
                 missing_leader_refresh_interval: float = MISSING_LEADER_REFRESH_INTERVAL):
        self._client = client
        #: How this client reaches a node the table names.  The cluster is enough to
        #: build the in-process one, which is the only kind there is so far; a client
        #: outside the process hands in the factory it reaches nodes through.
        self._factory = factory if factory is not None else LocalNodeClientFactory(cluster)
        self._lock = threading.RLock()
        #: How long a nameless leader is believed for.  A caller that knows the publisher
        #: writes faster than this can pass a shorter one; nothing should pass a longer
        #: one, which would only delay the client's recovery from a table caught
        #: mid-publication.
        self._missing_leader_refresh_interval = missing_leader_refresh_interval
        #: When the table this cache holds was read, for the rate limit above.
        self._last_read = 0.0
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
        there is no table to read, and the caller routes by the cluster's own nodes
        instead - which is what ``ShardLeaders`` does when it is given no router.
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
                self._table = self._read()
            return self._table

    def refresh(self) -> RoutingTable:
        """Read the table again, after a shard said the cached one was old."""
        with self._lock:
            self.refreshes += 1
            self._table = self._read()
            return self._table

    def _read(self) -> RoutingTable:
        """Read the table from the group, and remember when it was read."""
        table = self._client.table(refresh=True)
        self._last_read = time.monotonic()
        return table

    def shard_for(self, key: bytes) -> Optional[ShardPlacement]:
        """Where the table says ``key`` lives."""
        return self.table().shard_for(key)

    def leader_for_shard(self, shard_id: int) -> Optional[NodeClient]:
        """The client for the node the table names as ``shard_id``'s leader.

        The table is the answer, and this lookup does not also ask the node whether it
        still leads.  The node it reached is the one whose belief is in question, so a
        leader that has been cut off from its peers goes on answering reads as if
        nothing had happened - and that is an answer a client cannot tell from the right
        one.  What it can tell is a refusal, and a refusal is the shard's to give, not
        this file's: ``ask_shard`` is what acts on it.

        A table that names nobody is answered as it stands, but only for as long as that
        answer could still be current.  It has two causes that look identical from here:
        a shard that has no leader - a fact about the shard, not staleness in the table -
        and a table read after the range was published and before the leader was, which
        is a table that is simply behind.  A cache that could not tell them apart would
        answer ``None`` for ever for the second one, so it reads again once the publisher
        has had time to write; see :data:`MISSING_LEADER_REFRESH_INTERVAL`.
        """
        client = self._client_for(shard_id)
        if client is None:
            self._refresh_if_it_could_have_changed()
            return self._client_for(shard_id)
        return client

    def refresh_for_shard(self, shard_id: int) -> None:
        """Read the table again, because ``shard_id`` refused this client.

        The shard id is what the caller has - it is what the caller was asking about -
        and the read it causes is of the whole table, because the table is whole: a
        placement is not per key, so a client that refreshed one shard's entry would be
        holding one decision split into two.
        """
        self.refresh()

    def invalidate(self, shard_id: int) -> None:
        """Forget this client's handle on the node the table names for ``shard_id``.

        For a handle that has stopped working - a connection that dropped - which is not
        a table that has gone old: the placement is not in doubt, only the way to reach
        it.  Nothing calls this yet, because every handle so far is an object in this
        process, and an object in this process does not break; it is the hook a
        networked client needs.
        """
        placement = self.table().shard(shard_id)
        if placement is None or placement.leader_id is None:
            return
        self._factory.forget_client(shard_id, placement.leader_id)

    def _refresh_if_it_could_have_changed(self) -> None:
        """Read the table again, unless the read being held is too recent to be improved on.

        Placement arrives as separate commands, so a nameless leader has to be re-read
        eventually; the rate limit is what keeps a shard that is genuinely leaderless
        from turning every lookup into a metadata round trip.
        """
        with self._lock:
            if time.monotonic() - self._last_read < self._missing_leader_refresh_interval:
                return
        self.refresh()

    def leader_for_key(self, key: bytes) -> Optional[NodeClient]:
        """The client for the node the table says should answer for ``key``."""
        placement = self.shard_for(key)
        if placement is None:
            return None
        return self.leader_for_shard(placement.shard_id)

    def _client_for(self, shard_id: int) -> Optional[NodeClient]:
        """The factory's client for the node the table names for ``shard_id``."""
        placement = self.table().shard(shard_id)
        if placement is None or placement.leader_id is None:
            return None
        return self._factory.get_client(shard_id, placement.leader_id)
