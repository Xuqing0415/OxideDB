"""What a recovery is handed, on whichever side of a process boundary it runs.

A split and a move both write down what they are doing before they do it, and both are
finished by a cluster that comes back and finds the note.  The work is the same work on
both sides - freeze the source, read the rows it holds, copy them into the group that is
to own them, tell the routing table - and the difference is which objects the side has in
its hand: a ``ShardedRaftCluster`` holds every replica of every group it serves, and a node
in a process of its own holds itself.

So the work is written once, against this protocol, and each side implements it with the
objects it has.  Three rules decide what can be in here at all.

**Every argument and every answer crosses a process boundary.**  Values - ids, ranges,
notes, addresses - and handles that have an implementation on both sides: ``NodeClient``,
and the routing table's client, which is not named as a type here for the reason
``MetadataPublisher`` gives.  A method that answers with a node object cannot be
implemented by a process, and not because it would be awkward there: the leader of a shard
may be inside another process, so there is no object to answer with.  That is why the two
leader lookups hand back clients, and why ``_client_for_node`` is not here - its argument
is a node object, which is the one thing a process does not have.

**Every method means the nodes this side holds.**  A process holds itself, so "freeze this
shard" is one node; a cluster holds the whole group, so it is all of them.  That is the
only difference between the two implementations, and it is why nothing here takes a set of
nodes to act on: a side knows which nodes are its to act on, and a caller that named a set
would be describing a shape only one of the two has.

**Nothing here is an action.**  The protocol reads what is written down, freezes, builds,
reads rows, proposes and applies; it does not say "do a split" or "do a move".  Which of
the two a note describes is the note's business, and the flow that acts on it is the
recovery, written once, so that two start-up paths cannot drift into two protocols.

Absent for the same three rules: ``get_shard_server`` answers with a server, which is an
object only a process has; ``_shard_leader_node``, ``_leader_on``, ``_wait_for_shard_leader``
and ``_wait_for_leader_on`` answer with a node object.  ``_close_group_on`` is not missed
either, and that one is worth saying: closing the group a refused move built is
:meth:`ensure_serving` with the set the routing table names, which is a call that already
means "make what I hold match the table".
"""

from typing import Dict, List, Optional, Protocol, runtime_checkable

from ..client.node_client import NodeClient
from ..shard.router import RangeMap
from .recovery_notes import PendingNote



@runtime_checkable
class RecoveryView(Protocol):
    """What a recovery needs from the side it is running on.

    A protocol rather than a base class, because neither side is written *as* a recovery
    view: a cluster is a cluster and a node is a node, and what makes their methods the
    implementations of this is the question each one answers.  ``isinstance`` is how the
    two are held to it - see ``tests/test_recovery_view.py`` - so a call that goes missing
    is a failure there rather than a surprise in the middle of a recovery.
    """

    # -- what this side is ----------------------------------------------------

    def shard_ids(self) -> List[int]:
        """The shards this side looks after: the keys of its range map.

        What the notes are looked for in, and never the routing table's list - a process
        does not learn its shards from the table, and the shard a split creates is one it
        has to build rather than one it was started with.  A read.
        """

    def node_ids(self) -> List[int]:
        """Every node of the cluster this side is part of, in a fixed order.

        Not "the nodes this side holds": a split's new shard is served by the whole
        cluster, and a process that answered with itself would build a group of one and
        then publish itself as the shard's replica set.  Every side answers the same set,
        so the members they each build are the members of one group.  A read.
        """

    def range_map(self) -> RangeMap:
        """The ranges this side routes by, the source shard's own among them.

        What a split is splitting and what a move is copying: a shard's range is a fact
        about this map and not about the routing table, which while a split is in flight
        still holds the range before it.  A read.
        """

    # -- what was left written down -------------------------------------------

    def pending_notes(self, shard_id: int) -> List[PendingNote]:
        """The notes this side holds for ``shard_id``: what the shard was in the middle of.

        One on every replica of the shard they are about, and which kind each one is, is
        the note's own answer rather than the caller's question.  A read, so a caller that
        asks twice gets the same notes: a note goes when :meth:`forget_note` drops it and
        at no other time.  At most one today, and a second would be a state neither flow
        can have been in - which a recovery refuses rather than picks between.
        """

    def remember_note(self, note: PendingNote) -> None:
        """Write ``note`` on every replica this side holds of the shard it is about.

        Before the first row moves, which is the whole point: between the copy and the
        proposal the work exists nowhere else, and the one place it can be written down is
        the thing that survives a restart.  Writing the same note twice is the same bytes,
        so a retry that has not dropped it yet changes nothing.
        """

    def forget_note(self, shard_id: int) -> None:
        """Drop the notes this side holds for ``shard_id``.

        The last step of a split or a move and the one that ends it: once the routing
        table has the range there is nothing left to pick up.  A replica whose storage has
        been closed is skipped rather than failed on.  Idempotent - dropping a note that
        is not there is what the second run of a commit finds.
        """

    # -- the routing table's answers ------------------------------------------

    def serving_nodes(self, shard_id: int) -> Optional[List[int]]:
        """The set the routing table names for ``shard_id``, or None if it names none.

        How a side that comes back learns how far a move got: the table names the set the
        move was going to, which means the proposal landed; the set it was leaving, which
        means nothing was proposed; or neither - an operator's hand, or a second move
        computed from the same table - which is not something a recovery may guess about,
        since guessing between two live groups is how a range ends up served by one of
        them while its rows are in the other.  A side with no table at all, as an
        in-process cluster is, answers with its own placement.  A read.
        """

    def shard_replica_ids(self, shard_id: int) -> List[int]:
        """Every node serving the shard, from the group *this side* holds for it.

        The set a finished split is published with, and the set the publisher follows.
        Empty when this side holds no group, which is the difference from
        :meth:`serving_nodes`: that is the table's answer and this is the group's, and for
        a shard being moved they are the group it is going to and the group it is leaving.
        A read.
        """

    def addresses_on(self, shard_id: int, nodes: List[int]) -> Dict[int, str]:
        """Where each of ``nodes`` serves ``shard_id``, as those nodes bound it.

        Asked of a set, because the nodes a move is going to are not the nodes the shard
        is served by yet and a proposal that took its addresses from the shard's own
        answer would hand the table the addresses of the group it is leaving.  A node this
        side cannot place is left out rather than guessed at.  A read.
        """

    # -- the groups, as clients -----------------------------------------------

    def leader_client(self, shard_id: int) -> Optional[NodeClient]:
        """A handle on whatever leads ``shard_id``, or None while nobody does.

        The group the routing table names, which for a shard being moved is the one its
        rows are coming from.  None is an answer and not a failure - a shard that has just
        been created has not elected anything - and how long a caller waits for one is the
        caller's business.  A read.
        """

    def leader_client_for_nodes(self, shard_id: int,
                               nodes: List[int]) -> Optional[NodeClient]:
        """A handle on whichever of ``nodes`` leads ``shard_id``, or None.

        The same question asked about a set rather than about the shard, and a call of its
        own rather than a set defaulted on the other one: the group a move copies *into*
        is exactly the group the table does not name yet, so the set has to come from the
        caller that built it, and a signature that let a caller forget it would hand back
        the group the rows are leaving.  A read.
        """

    # -- changing this side ---------------------------------------------------

    def freeze(self, shard_id: int, reason: str) -> None:
        """Refuse commands that add rows on the replicas this side holds for the shard.

        ``reason`` is what the refusal quotes, so a caller can tell a split from a move.
        Commits and rollbacks still go through: they are the end of a transaction that
        prewrote before this, and dropping one would leave a lock on a shard whose rows
        are about to be copied with that lock in them.  Idempotent.
        """

    def unfreeze(self, shard_id: int) -> None:
        """The reverse, for the source of a split that is done and a move that was refused.

        It waits for nothing: the writes admitted before the freeze are the caller's to
        drain, and a recovery that comes back and finds a note freezes the shard again
        anyway.  Idempotent.
        """

    def ensure_serving(self, shard_id: int, nodes: List[int]) -> List[int]:
        """Make every node of this side that serves ``shard_id`` one of ``nodes``.

        One call for both directions, because on a side that holds one replica they are
        one event: a member that is in the set and holds nothing is built, a member that
        holds a group and is not in the set is closed and its storage put aside for the
        operator, and a member whose group has different members is closed and built again
        - a ``MemoryRaftNode`` takes its peers once and keeps them.  What the routing table
        holds is not this call's to change: a placement moves when the table moves.  What
        it returns is the nodes whose group it closed, so a caller can tell a call that did
        something from one that found the work already done.  Idempotent.
        """

    def ensure_group_on(self, shard_id: int, nodes: List[int]) -> None:
        """Build this side's member of the group for ``shard_id``, and close nothing.

        The build half of :meth:`ensure_serving` on its own, and the one asymmetry between
        the two sides.  A move builds the group its rows are going into while the source is
        still the group the table names, so the nodes holding that shard are not a set any
        one replica set describes: a cluster holds both groups, and this is how it builds
        the second one; a process holds one, and this is the whole of what it does about
        the group it is being asked to join.  Idempotent.
        """

    def apply_split_locally(self, shard_id: int, split_key: bytes,
                            new_shard_id: int) -> None:
        """Re-range this side: ``shard_id`` now ends at ``split_key``, the new one starts.

        One call and one map, made by the side that holds the map - which is what keeps a
        key from being routed back into the shard that has just given it away: two edits,
        or a caller that reached into the map itself, leave a moment in which a live range
        belongs to neither shard, and the keys in it are routed to shard zero without
        anything failing.  The source keeps what it had below the point and the shard the
        split created owns the rest.  Idempotent: the same split applied twice is the same
        map.
        """

    # -- the table itself -----------------------------------------------------

    def metadata_client(self):
        """The routing table's group as a client: for reading it and for its two writes.

        Deliberately not named as a type, exactly as in ``MetadataPublisher``: the
        in-process client and the one that crosses a wire both answer ``table``,
        ``split_shard`` and ``move_shard``, and which of the two a side holds is the
        difference between a recovery that works only where it leads the table's group and
        one that works anywhere.  None when there is no table to tell, which an in-process
        cluster answers by being the whole world to itself.  A read.
        """
