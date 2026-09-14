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

import threading
from typing import Callable, Dict, Optional

from ..raft.state_machine import ErrorCode, ScanRefused
from ..shard.router import locate
from .node_client import (LocalNodeClientFactory, NodeClient, NodeClientFactory,
                          NodeUnreachable)


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
        self._factory = factory if factory is not None else self._factory_of(router, cluster)
        #: Leaders a shard named in a refusal, by shard, waiting for the one retry they were
        #: given for.  Taken when they are used and dropped when the table is read, because a
        #: hint is one shard's answer to one refusal and the table is the record: a hint kept
        #: beyond that would be a placement nothing maintains.
        self._hints: Dict[int, str] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _factory_of(router, cluster) -> NodeClientFactory:
        """How this lookup reaches a node: the table's own way, when there is a table.

        A client that routes by a table reaches nodes the way that table's cache does, and
        taking the factory from there rather than defaulting to the cluster's own nodes is
        what keeps one client from having two reachabilities - the table naming an address
        while the factory looked for an object in this process, with the read going one way
        and the write the other.  A router with no factory of its own, which is what a test
        stand-in for one is, leaves the cluster's own nodes as the answer.
        """
        factory = getattr(router, "factory", None)
        if factory is not None:
            return factory
        return LocalNodeClientFactory(cluster)

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

        A hint comes first, and only once: it is a leader change that happened after the
        table was read, told to this client by the shard that refused it, and it is spent
        on the retry it was given for.  A client that kept it would stop reading the table
        for that shard, and nothing would ever correct it again.
        """
        hint = self._take_hint(shard_id)
        if hint is not None:
            client = self._factory.get_client_at(shard_id, hint)
            if client is not None:
                return client

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

    def refused_with(self, shard_id: int, hint: Optional[str] = None) -> None:
        """``shard_id`` refused this client: follow the name it gave, or read the table.

        The hint is the shard's own claim about where its leader is, made after the table
        answered and therefore newer than the table is, and following it costs one more
        call instead of a read of the metadata group.  A refusal that names nowhere - a
        node that does not know who leads, a factory whose handles are objects in this
        process - falls back to reading the table, which is the only other thing a client
        can do about a placement it holds.
        """
        if hint is not None and self._remember_hint(shard_id, hint):
            return
        self.refresh_for_shard(shard_id)

    def refresh_for_shard(self, shard_id: int) -> None:
        """Read the table again, because ``shard_id`` refused this client.

        Nothing to do without a table, and that is not a shortcut: a client that can see
        the cluster's leaders directly is holding nothing that can go old, so it has
        nothing to re-read.  Any hint for that shard goes in the same step: the table is
        about to answer the question the hint stood in for, and a claim that is about to be
        replaced is not worth keeping.
        """
        with self._lock:
            self._hints.pop(shard_id, None)

        if self._router is not None:
            self._router.refresh_for_shard(shard_id)

    def invalidate(self, shard_id: int) -> None:
        """Forget the handle on the shard's leader, so the next ask builds a new one.

        For a handle that has stopped working - a connection that dropped - which is not
        a table that has gone old: the placement is not in doubt here, only the way to
        reach it.  A hint for the shard goes the same way, since it names a handle of the
        same kind and would otherwise be the one handle this call forgot to drop.
        """
        with self._lock:
            self._hints.pop(shard_id, None)

        if self._router is not None:
            self._router.invalidate(shard_id)
            return

        leader = self._cluster.shard_leader(shard_id)
        if leader is not None:
            self._factory.forget_client(shard_id, leader[0])

    # -- hints -------------------------------------------------------------

    def _take_hint(self, shard_id: int) -> Optional[str]:
        """The address a shard named for that shard's leader, if one is still waiting."""
        with self._lock:
            return self._hints.pop(shard_id, None)

    def _remember_hint(self, shard_id: int, address: str) -> bool:
        """Keep an address a shard named, if this factory can reach one at all.

        False is not a failure: a factory of in-process handles has nowhere to send an
        address, and a caller holding the cluster can see for itself which node leads.
        """
        if self._factory.get_client_at(shard_id, address) is None:
            return False

        with self._lock:
            self._hints[shard_id] = address
        return True


def ask_shard(leaders: ShardLeaders, shard_id: int,
              question: Callable[[NodeClient], object],
              attempts: int = 2):
    """Ask ``shard_id``'s leader ``question``, going back to the table once if refused.

    A refusal here is ``ERR_NOT_LEADER``, and it is answered twice over: a shard that knows
    where its leader is names an address, which is followed directly, and one that does not
    is answered by reading the table again and asking whoever it names now.  Either way the
    caller reached a node that is not the node to ask, and the only thing that has changed
    is which node that is.  One retry and not a loop, because a second refusal right after
    a fresh answer is not staleness, and a caller that kept asking would be turning a wrong
    answer into a slow one.

    A node that does not answer at all is the same situation with no answer in it: the
    address the table named is not accepting calls, which is what a leader that has been
    killed looks like until the publisher catches up.  So that is retried here too, and
    raised on the way out if the retry does not rescue it - a caller that ends up with
    nothing has to be told what happened, and a silent ``None`` would look like a shard
    with no leader.

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
    last_answer = None
    #: Only the last attempt's outcome is kept, so that an attempt which raised and a
    #: later one which answered cannot leave the raise behind to be raised again.
    last_refusal: Optional[ScanRefused] = None
    last_silence: Optional[NodeUnreachable] = None

    for attempt in range(attempts):
        last_refusal = None
        last_silence = None

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
        except NodeUnreachable as silence:
            # Nothing answered, which means what a refusal means: the node this caller was
            # sent to is not the node to ask.  Kept rather than swallowed, so that a caller
            # left with nothing is told why instead of being handed None.
            last_silence = silence
            answer = None

        if (answer is not None
                and getattr(answer, "error_code", None) != ErrorCode.ERR_NOT_LEADER):
            return answer

        last_answer = answer
        if attempt + 1 < attempts:
            hint = getattr(answer, "leader_address", None)
            if hint is None and last_refusal is not None:
                hint = last_refusal.leader_address
            leaders.refused_with(shard_id, hint)

    if last_refusal is not None:
        raise last_refusal
    if last_silence is not None:
        raise last_silence
    return last_answer
