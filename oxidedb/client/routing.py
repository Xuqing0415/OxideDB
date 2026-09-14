"""Which client leads a shard, and what a refusal to answer means.

A caller outside a cluster can only know where a shard's leader is by reading the table
the cluster publishes, which is what ``RoutingCache`` holds.  A caller inside one - which
is what an in-process test is - can ask the cluster itself, and asking it for a node *id*
rather than for a node object is what keeps this on the client side of the seam: an id
goes into a factory and a ``NodeClient`` comes back either way, so no caller learns which
of the two answered it.

Having both sources here is also what makes the retry honest.  A shard that says it is
not the leader is the only evidence a client can get that the placement it holds is old:
the node it reached is exactly the node whose own belief is in question, and a node cut
off from its peers believes it leads right up until something refuses it.  So the way
back to the table sits next to the way to the shard, and ``ask_shard`` is the one place
that walks it - a second refusal right after a table read is not staleness, and asking a
third time would only turn a wrong answer into a slow one.

Nothing here keeps a node.  What is kept is a table read (in the cache) and the handles
the factory builds, and those are the two places a stale answer is already expected and
already recovered from.
"""

from typing import Callable, Optional

from ..raft.state_machine import ErrorCode, ScanRefused
from ..shard.router import locate
from .node_client import LocalNodeClientFactory, NodeClient, NodeClientFactory


class ShardLeaders:
    """The client that leads a shard, from the table when there is one.

    One object for both answers so that no caller has to know which one it got: the
    coordinator, the resolver and the SQL executor all ask the same question, and the
    answer they act on is a client either way.
    """

    def __init__(self, cluster, factory: Optional[NodeClientFactory] = None, router=None):
        self._cluster = cluster
        #: None is a client of a cluster that publishes nowhere - an in-process test -
        #: and the cluster's own nodes are then the only placement there is.
        self._router = router
        self._factory = factory if factory is not None else LocalNodeClientFactory(cluster)

    def leader_for_shard(self, shard_id: int) -> Optional[NodeClient]:
        """The client for the node that leads ``shard_id``, or None when nobody does.

        With a table, this is the table's answer and nothing else: the lookup does not
        ask the node whether it still leads, because the node it reached is the one whose
        belief is in question.  What a client trusts instead is the shard's refusal -
        ``ask_shard`` acts on it - which is a fact about the shard rather than about one
        node's memory of itself.

        Without a table the cluster is asked, which is the same question put to the only
        thing that knows, and what comes back is the node's id: the id goes to the
        factory, so a caller outside the process would be handed a client over a channel
        instead of an object, and nothing above this line would change.
        """
        if self._router is not None:
            return self._router.leader_for_shard(shard_id)

        leader = self._cluster.shard_leader(shard_id)
        if leader is None:
            return None
        return self._factory.get_client(shard_id, leader[0])

    def leader_for_key(self, key: bytes) -> Optional[NodeClient]:
        """The client for the node that leads whatever shard owns ``key``."""
        shard_id = self.shard_for_key(key)
        if shard_id is None:
            return None
        return self.leader_for_shard(shard_id)

    def shard_for_key(self, key: bytes) -> Optional[int]:
        """Which shard owns ``key``: the table's answer if there is one, the cluster's if not.

        None means the placement this client holds covers no such key, which a caller
        should treat the way it treats a shard with no leader: there is nowhere to send
        this request.
        """
        if self._router is not None:
            placement = self._router.shard_for(key)
            return None if placement is None else placement.shard_id
        return locate(self._cluster.range_map(), key)

    def refresh_for_shard(self, shard_id: int) -> None:
        """``shard_id`` refused this client: read the table again.

        Nothing to do without a table, and that is not a shortcut: a client that can see
        the cluster's leaders directly is holding nothing that can go old, so it has
        nothing to re-read.
        """
        if self._router is not None:
            self._router.refresh_for_shard(shard_id)

    def invalidate(self, shard_id: int) -> None:
        """Forget the handle on the shard's leader, so the next ask builds a new one.

        For a handle that has stopped working - a connection that dropped - which is not
        a table that has gone old: the placement is not in doubt here, only the way to
        reach it.  Nothing calls this yet, because every handle so far is an object in
        this process and an object in this process does not break; it is the hook a
        networked client needs, and it lives here so that the callers who will use it do
        not have to know where handles are kept.
        """
        if self._router is not None:
            self._router.invalidate(shard_id)
            return

        leader = self._cluster.shard_leader(shard_id)
        if leader is not None:
            self._factory.forget_client(shard_id, leader[0])


def ask_shard(leaders: ShardLeaders, shard_id: int,
              question: Callable[[NodeClient], object],
              attempts: int = 2):
    """Ask ``shard_id``'s leader ``question``, going back to the table once if refused.

    A refusal here is ``ERR_NOT_LEADER``, and it is answered by reading the table again
    and asking whoever it names now: what the caller reached was the node the table
    named, and the only thing that has changed is which node that is.  One retry and not
    a loop, because a second refusal right after a fresh read is not staleness, and a
    caller that kept asking would be turning a wrong answer into a slow one.

    ``get``, ``propose``, ``get_lock`` and ``get_write_record`` refuse in their result;
    ``scan`` raises ``ScanRefused`` instead, since rows are not a place to put a refusal.
    Both are handled here, so that no caller has to remember which of the six primitives
    refuses in which way.

    Returns whatever ``question`` returned, or None when the shard has no leader this
    client can reach.  A refusal that outlives the retry is handed back the way it
    arrived: the result, for the calls that refuse in one, and a raised ``ScanRefused``
    for a range read, because a caller that has to keep rows apart from refusals cannot
    be given a refusal in the place where rows go.
    """
    last_refusal: Optional[ScanRefused] = None
    last_answer = None

    for attempt in range(attempts):
        client = leaders.leader_for_shard(shard_id)
        if client is None:
            break

        try:
            answer = question(client)
        except ScanRefused as refusal:
            if refusal.error_code != ErrorCode.ERR_NOT_LEADER:
                raise
            last_refusal = refusal
            answer = None

        if (answer is not None
                and getattr(answer, "error_code", None) != ErrorCode.ERR_NOT_LEADER):
            return answer

        last_answer = answer
        if attempt + 1 < attempts:
            leaders.refresh_for_shard(shard_id)

    if last_refusal is not None:
        raise last_refusal
    return last_answer
