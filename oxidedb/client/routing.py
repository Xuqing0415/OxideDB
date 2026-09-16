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
that walks it.  A refusal is followed in the order the evidence is worth: the address the
shard named first, because it was said after the table was read; then the shard's other
replicas, one at a time, because the table names a whole set and only one of them leads;
and the table last, because re-reading it is the only thing left that can name a node
nobody has tried.  The walk is bounded by the set, which is finite, and by one re-read of
the table: a second refusal right after a table read is not staleness, and asking on would
only turn a wrong answer into a slow one.

Nothing here keeps a node.  What is kept is a table read (in the cache) and the handles
the factory builds, and those are the two places a stale answer is already expected and
already recovered from.
"""

import threading
from typing import Callable, Dict, List, Optional, Set

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
        #: The shard's other replicas, in the order a refusal left them to try, by shard.
        #: Built from the table when a refusal arrives and not before, and dropped when the
        #: table is read, for the same reason a hint is: a replica set is the table's
        #: placement, so a walk over one is only as good as the read it came from.
        self._queued: Dict[int, List[str]] = {}
        #: The addresses a request has already gone to, by shard.  What keeps the walk from
        #: asking one node twice while the table still names it, and what ends the walk:
        #: when every address the table knows has been asked, there is nothing left here.
        self._asked: Dict[int, Set[str]] = {}
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

        The replicas a refusal queued come next, in the order that refusal left them, and
        the table comes last.  They are the same walk in the same order ``refused_with``
        built, and what this lookup adds is that every address it hands out is remembered:
        a shard that keeps refusing cannot send this client round the same set of nodes
        twice, so the walk ends when the addresses run out rather than when a counter does.
        """
        hint = self._take_hint(shard_id)
        if hint is not None:
            client = self._factory.get_client_at(shard_id, hint)
            if client is not None:
                self._note_asked(shard_id, hint)
                return client

        client = self._take_queued(shard_id)
        if client is not None:
            return client

        if self._router is not None:
            client = self._router.leader_for_shard(shard_id)
            return None if client is None else self._hand_out(shard_id, client)

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

    def refused_with(self, shard_id: int, hint: Optional[str] = None) -> bool:
        """``shard_id`` refused this client: take up whatever else the refusal leaves.

        True when this client now has somewhere else to ask - the address the shard named,
        or one of the shard's other replicas - and False when the refusal spent everything
        but the table.  Whether that table is worth reading is the caller's decision and
        not this one's, which is why no read is made here: a walk over three replicas costs
        three calls where a read of the metadata group is one budgeted thing, and only the
        caller knows what of that budget it has spent.

        The hint wins over the replicas because it is newer.  The table was read before the
        shard was asked and the shard's answer is about now, so an address the shard names
        is the one piece of placement a client can hold that nothing has had to publish.
        A refusal that names nowhere - a node that does not know who leads, a factory whose
        handles are objects in this process - falls through to the replicas, and a replica
        set this client cannot reach falls through to nothing, which is the caller's cue.
        """
        if hint is not None and self._remember_hint(shard_id, hint):
            return True
        return self._queue_replicas(shard_id)

    def refresh_for_shard(self, shard_id: int) -> None:
        """Read the table again, because ``shard_id`` refused this client.

        Nothing to do without a table, and that is not a shortcut: a client that can see
        the cluster's leaders directly is holding nothing that can go old, so it has
        nothing to re-read.  Everything else this client holds about that shard goes in the
        same step - the hint, the replicas queued behind it, the addresses already asked -
        because the table is about to answer all of it: a claim that is about to be replaced
        is not worth keeping, and a walk that began at the placement being replaced is not
        one to carry on with.
        """
        with self._lock:
            self._forget_shard(shard_id)

        if self._router is not None:
            self._router.refresh_for_shard(shard_id)

    def invalidate(self, shard_id: int) -> None:
        """Forget the handle on the shard's leader, so the next ask builds a new one.

        For a handle that has stopped working - a connection that dropped - which is not
        a table that has gone old: the placement is not in doubt here, only the way to
        reach it.  A hint for the shard goes the same way, since it names a handle of the
        same kind and would otherwise be the one handle this call forgot to drop, and so
        does the walk behind it: an address this client could not open a channel to is not
        one to skip for the rest of its life.
        """
        with self._lock:
            self._forget_shard(shard_id)

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
        An address this client has already been to is not kept either, and that is not a
        failure: a shard naming the node it has just refused has said nothing this client
        can act on, and the walk over the other replicas is what is left of its answer.
        """
        if self._was_asked(shard_id, address):
            return False
        if self._factory.get_client_at(shard_id, address) is None:
            return False

        with self._lock:
            self._hints[shard_id] = address
        return True

    # -- the round a refusal leaves ----------------------------------------

    def answered(self, shard_id: int) -> None:
        """A shard answered this client, so nothing this client held is in doubt any more.

        The round the walk was keeping - the addresses it had been to, the replicas still
        queued behind it - is over, because what a round is for is finding a shard that
        answers and one has.  A round kept past that would go on excluding nodes for a
        doubt that has been settled, and the second write of a session would walk a queue
        the first one had already drained.
        """
        with self._lock:
            self._forget_shard(shard_id)

    def _queue_replicas(self, shard_id: int) -> bool:
        """Queue the shard's other replicas, and say whether one is left to be asked.

        The addresses come from the table, because a replica set is placement and placement
        is what a table publishes; a factory knows only the addresses it has been handed,
        and while the table names a leader the leader is the only one it is handed.  An
        address this factory cannot open at all is not queued: it is not somewhere else to
        ask, and counting it as one would spend the caller's retry on nothing.

        What is answered is whether the queue holds a replica - the queue and not this call.
        A refusal that arrives while one of the set is still queued - the second node of a
        walk, whose successor was queued by the first - is told there is somewhere to go,
        because there is; only an empty queue is the caller's cue to go back to the table.
        """
        addresses = [address for address in self._replica_addresses(shard_id)
                     if self._factory.get_client_at(shard_id, address) is not None]

        with self._lock:
            asked = self._asked.setdefault(shard_id, set())
            queue = self._queued.setdefault(shard_id, [])
            waiting = [address for address in addresses
                       if address not in asked and address not in queue]
            queue.extend(waiting)
            return bool(queue)

    def _replica_addresses(self, shard_id: int) -> List[str]:
        """Every address the table says serves ``shard_id``, or none when there is no table.

        Asked of the router rather than of the factory, because a replica set is the
        table's to name and the factory only knows what it has been handed.  A router that
        cannot answer - a stand-in for one, a cache with no table behind it - leaves this
        client the table's own answer and nothing else, which is the walk it had before
        there was one.
        """
        if self._router is None:
            return []
        addresses = getattr(self._router, "replica_addresses", None)
        if addresses is None:
            return []
        return list(addresses(shard_id))

    def _take_queued(self, shard_id: int) -> Optional[NodeClient]:
        """The next queued address as a client, or None when the queue is dry.

        An address is spent when it is asked rather than when it answers: what the queue is
        for is asking each of a shard's replicas once, and an address this factory cannot
        open has had its turn.
        """
        while True:
            with self._lock:
                queue = self._queued.get(shard_id)
                if not queue:
                    return None
                address = queue.pop(0)
            self._note_asked(shard_id, address)

            client = self._factory.get_client_at(shard_id, address)
            if client is not None:
                return client

    def _hand_out(self, shard_id: int, client: NodeClient) -> NodeClient:
        """Note where a handle that knows an address was reached, and hand it back.

        This is how the address the table named gets into the round: it was not popped from
        the queue, so nothing else would remember it, and a shard that refused it would put
        it back among the replicas to try.  A handle in this process knows no address -
        there is none in it - and keeps none, which is the case where there is nothing on
        the other side of the walk to reach anyway.
        """
        address = getattr(client, "address", None)
        if address is not None:
            self._note_asked(shard_id, address)
        return client

    def _note_asked(self, shard_id: int, address: str) -> None:
        with self._lock:
            self._asked.setdefault(shard_id, set()).add(address)

    def _was_asked(self, shard_id: int, address: str) -> bool:
        with self._lock:
            return address in self._asked.get(shard_id, ())

    def _forget_shard(self, shard_id: int) -> None:
        """Drop everything held about one shard's placement.  Called with the lock held."""
        self._hints.pop(shard_id, None)
        self._queued.pop(shard_id, None)
        self._asked.pop(shard_id, None)


def ask_shard(leaders: ShardLeaders, shard_id: int,
              question: Callable[[NodeClient], object],
              table_reads: int = 2):
    """Ask ``shard_id``'s leader ``question``, following a refusal to the next place.

    A refusal here is ``ERR_NOT_LEADER``, and the node that gave it is evidence: it is not
    the node to ask, and something else is.  Which one is what ``ShardLeaders`` works out -
    the address the shard named, or the next replica of its set - and this asks there until
    there is nowhere left to ask.  That is a walk and not a loop: a replica set has as many
    addresses as it has, each is asked at most once, and the table is what bounds the rest.

    ``table_reads`` is what a fresh table is worth, counted in reads of the table rather
    than in questions asked, because that is what the alternative costs: the first read is
    the placement this call started with and the second is the one a refusal buys, since a
    table read after a leader change is a different answer while a refusal that survives a
    table read is the shard's current one.  Replicas are not counted against it - walking to
    an address the table already named is not asking twice - so the budget stays the number
    of times a client is willing to go back to the metadata group, whatever the set is.

    A node that does not answer at all is the same situation with no answer in it: the
    address the table named is not accepting calls, which is what a leader that has been
    killed looks like until the publisher catches up.  So that is retried here too, and
    raised on the way out if the walk does not rescue it - a caller that ends up with
    nothing has to be told what happened, and a silent ``None`` would look like a shard
    with no leader.

    ``get``, ``propose``, ``get_lock`` and ``get_write_record`` refuse in their result;
    ``scan`` raises ``ScanRefused`` instead, since rows are not a place to put a refusal.
    Both are handled here, so that no caller has to remember which of the six primitives
    refuses in which way.

    Returns whatever ``question`` returned, or None when the shard has no leader this
    client can reach.  A refusal that outlives the walk is handed back the way it
    arrived: the result, for the calls that refuse in one, and a raised ``ScanRefused``
    for a range read, because a caller that has to keep rows apart from refusals cannot
    be given a refusal in the place where rows go.
    """
    last_answer = None
    #: Only the last attempt's outcome is kept, so that an attempt which raised and a
    #: later one which answered cannot leave the raise behind to be raised again.
    last_refusal: Optional[ScanRefused] = None
    last_silence: Optional[NodeUnreachable] = None
    #: What is left of the caller's budget for the table, the read it started with
    #: included.  The walk is not charged against it: one address asked is not two
    #: answers, and what a caller is rationing is the metadata group.
    reads_left = max(table_reads - 1, 0)

    while True:
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
            leaders.answered(shard_id)
            return answer

        last_answer = answer
        hint = getattr(answer, "leader_address", None)
        if hint is None and last_refusal is not None:
            hint = last_refusal.leader_address

        if leaders.refused_with(shard_id, hint):
            continue
        if reads_left == 0:
            break
        reads_left -= 1
        leaders.refresh_for_shard(shard_id)

    if last_refusal is not None:
        raise last_refusal
    if last_silence is not None:
        raise last_silence
    return last_answer
