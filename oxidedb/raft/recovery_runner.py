"""The body of a recovery, written once, run against whichever side of a process.

A split and a move are finished by reading back what they wrote down, and that work is
the same work on both sides of a process boundary; what differs is the objects the side
has in its hand.  ``RecoveryView`` is what a side is asked for and this is what asks,
kept apart on purpose: the flow lives here so that the cluster's two start-up paths and a
node started as a process cannot drift into three slightly different protocols.

Both halves of that flow are here, and a side reaches them through this object: the two
start-up paths call ``recover_splits`` and ``recover_migrations``, and the call that
begins a split or a move uses the entries in between - ``pending_split``,
``remember_split`` and ``finish_split`` for a split, ``remember_migration`` and
``finish_move`` for a move - which write the note down and hand the rest over.  What is
still written twice is what those beginning calls read: the rows of a range and the locks
in it, here against a client, and in ``shard_server`` against the node object a side that
is beginning the work already has in its hand.

The state is the part worth settling before anything moves, because it is the part of the
flow that is not a call.  ``_pending_splits`` and ``_migrations`` are the in-memory copy
of the notes on disk, and the two error strings are why the last attempt could not
finish.  All four belong to the recovery rather than to the side it runs on, and the
reason comes from the process side: a process has nowhere to keep a placement or a
working table, and it does not need one - the note in the shard's own storage is the
durable copy, and what a recovery needs in memory is rebuilt from it on the way up.  A
view asked to hold these would be holding what the disk already carries.

The move's record and the three answers its proposal can come back with -
``MigrationState``, ``MigrationPhase``, ``ProposalOutcome`` and ``ProposalResult`` - are
defined here as well, with the state they describe.  Not for tidiness: this module is
reached from ``shard_server`` the moment either is imported, so a body written here
cannot import what it needs back out of the file that reaches in.
"""

import time

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from ..client.node_client import NodeClient
from ..metadata.service import PROPOSE_ATTEMPTS, RETRY_BACKOFF
from .recovery_notes import MIGRATION_RECORD_PREFIX, PendingNote, SPLIT_RECORD_PREFIX
from .recovery_view import RecoveryView
from .state_machine import (ApplyResult, CommandType, ErrorCode, ScanRefused,
                            serialize_command)

#: How long a wait for a leader lasts, in seconds.  An election takes a few hundred
#: milliseconds; a group that has not held one by now is a group this recovery cannot
#: finish, not something to wait for for ever.  The deadline is the caller's business -
#: the view answers None while nobody leads, and says nothing about how long to look - so
#: it is here, where the looking is.
LEADER_TIMEOUT = 10.0

#: How often a wait looks again, in seconds.  Short enough that a leader appearing is
#: noticed a moment after it does, long enough not to spin.
LEADER_POLL_SECONDS = 0.05

#: How long a recovery waits for the routing table to answer at all, in seconds.
#:
#: A recovery runs when a side comes back, and so does the group that holds the table: a
#: node that reads its notes a moment before that group has elected a leader is told there
#: is nothing there, and a recovery that took that as final would never look again -
#: nothing calls it a second time.  An election takes a few hundred milliseconds, and a
#: group that has not held one in ten seconds is not one this recovery can finish, so the
#: wait is bounded.  It is the process side's wait in practice: a cluster reads the table
#: out of a group in its own process, where a leader that exists answers on the first read.
TABLE_TIMEOUT = 10.0

#: How long a shard that has moved goes on answering on the node it left, in seconds.
#: Clients route by a table they cached, so the node one of them was sent to a moment
#: ago is a node it may still ask: the group stays up for a fixed window after the table
#: has moved on, and then it goes.  A fixed window is the only honest one - "until every
#: client has noticed" is not something a server can know - and it is a parameter of the
#: call that waits it out, so a test does not have to wait it out for real.
MIGRATION_DRAIN_SECONDS = 30.0

class MigrationPhase:
    """Where a move of a shard has got to.

    A shard being moved is a shard in two places at once, and which of these a move is in
    is how a caller - and a cluster that has just come back - tells "nothing has happened
    yet" from "the rows are in the new group and the switch is owed".  The order is the
    protocol: freeze, copy, propose, done.
    """

    FREEZING = "freezing"
    COPYING = "copying"
    PROPOSING = "proposing"
    DONE = "done"


class ProposalOutcome:
    """The three answers a proposal to the routing table comes back with.

    Read as what the caller knows now, because that is what decides the caller's next
    move - and for a move, the next move is either finishing it or giving up on it.
    """

    #: The group applied it.  Whether it applied it just now or the first time it was
    #: asked, the table says what the caller wanted it to say.
    OK = "ok"
    #: The group answered no, and would answer no again: the caller asked for something
    #: the table will not hold, and a retry is the same question.
    REJECTED = "rejected"
    #: Nothing answered.  The proposal may or may not have landed and the caller cannot
    #: tell the difference - which is the one outcome a move may not treat as failure.
    UNREACHABLE = "unreachable"


@dataclass
class ProposalResult:
    """What came of one proposal.  See :class:`ProposalOutcome`."""

    outcome: str
    #: What the group said when it refused, or why nothing answered.  For a human, and
    #: for the refusals the wire flattens into one code: the message is where "the shard
    #: is not in the table" and "the set overlaps the one leaving" are still told apart.
    message: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.outcome == ProposalOutcome.OK


@dataclass
class MigrationState:
    """One move of one shard, as the cluster making it remembers it."""

    shard_id: int
    #: Where the shard is going: a replica set, named by the caller of ``move_shard``.
    target_nodes: List[int]
    #: Where it is coming from, which is the set that answers for it - and the one the
    #: routing table names - until the proposal lands.
    source_nodes: List[int] = field(default_factory=list)
    phase: str = MigrationPhase.FREEZING
    #: The keys this move has copied into the new group so far.
    copied_keys: List[bytes] = field(default_factory=list)
    #: The rows as they were read at the freeze, which is the one moment at which they
    #: are the whole truth about the range.  In this process only: a cluster that comes
    #: back reads them out of the source again rather than out of a note.
    rows: List[Tuple[bytes, bytes]] = field(default_factory=list)


class RecoveryRunner:
    """The flow that finishes a split or a move, over the side it is handed.

    Constructed by a side with itself as the view - ``RecoveryRunner(cluster)`` and
    ``RecoveryRunner(node_view)`` - and called at start-up and on demand after it, since
    a recovery that cannot finish leaves its shard frozen and the note on disk for the
    next call.
    """

    def __init__(self, view: RecoveryView):
        self._view = view
        #: What a split is doing, per shard: the in-memory copy of the note it wrote.
        self._pending_splits: Dict[int, Dict[str, Any]] = {}
        #: What a move is doing, per shard, as a ``MigrationState``.  The in-memory copy
        #: of the note on disk: written by the side that begins a move, and rebuilt out
        #: of the note by a side that comes back - the split's arrangement, one size up.
        self._migrations: Dict[int, MigrationState] = {}
        #: Why the last split or move could not finish, for an operator or a caller.
        self._last_split_error: Optional[str] = None
        self._last_migration_error: Optional[str] = None

    # -- what a side that came back calls --------------------------------------

    def recover_splits(self) -> List[int]:
        """Finish the splits this side was in the middle of when it stopped.

        A split writes down what it is doing before it does it - the shard, the split
        point, the id of the shard it is creating - and the note is dropped only once
        the routing table has the new range.  A side that comes back and finds one is a
        side whose rows may already be in the new shard's group, or may not be, if it
        stopped before the copy had finished; either way the answer is the same one a
        retry gets: freeze the source, copy whatever the new shard is missing, and make
        the proposal.

        The rows are read back out of the source shard's own state rather than out of
        the note, so what gets copied is what the shard would answer with.  A note
        carrying a copy of the rows would be a second copy of the shard, taken at some
        earlier moment, and a copy of a copy is how a split loses a row.

        Returns the shards whose split it finished.  A split it could not finish - the
        shard has not elected a leader yet, the table cannot be reached - is left
        frozen and still remembered, and the next call picks it up.
        """
        self.load_pending_splits()
        return self.finish_pending_splits()

    def load_pending_splits(self) -> None:
        """Read the split notes this side's shards left behind, without acting.

        The half of :meth:`recover_splits` that comes before anything publishes: a
        cluster's two start-up paths read the notes of both kinds first and finish them
        after, because a publisher that started in between could see a table a step
        ahead of this side and take this side's own split for somebody else's keyspace.
        """
        for shard_id in sorted(self._view.shard_ids()):
            if shard_id in self._pending_splits:
                continue
            pending = self._load_split_record(shard_id)
            if pending is not None:
                self._pending_splits[shard_id] = pending

    def finish_pending_splits(self) -> List[int]:
        """Finish every split this side knows it was in the middle of."""
        return [shard_id for shard_id in sorted(self._pending_splits)
                if self._resume_split(shard_id)]

    def possible_ranges(self) -> List[Dict[int, tuple]]:
        """Both range maps this side could be in, while a split is in flight.

        A side that is splitting has two answers for a moment: the map its servers route
        by, and the map the table gets once the split lands.  Both are this side's own
        placement, which is what a publisher needs to tell apart from somebody else's -
        so it is given both rather than one and left to guess.

        One entry when nothing is in flight, which is the usual case.
        """
        now = self._view.range_map()
        settled = dict(now)
        for pending in self._pending_splits.values():
            start, end = settled.get(pending["shard_id"], (None, None))
            if start is None:
                continue
            settled[pending["shard_id"]] = (start, pending["split_key"])
            settled[pending["new_shard_id"]] = (pending["split_key"], end)
        return [now] if settled == now else [now, settled]

    def recover_migrations(self) -> List[int]:
        """Finish the moves this side was in the middle of when it stopped.

        A move writes down what it is doing before it does it - the shard, the set it is
        leaving, the set it is going to - and the note is dropped only once the routing
        table names the new group and the one it left has gone.  A side that comes back
        and finds one is a side that was moving a shard when the process stopped, and the
        routing table is what says how far it got:

        * the table names the set the move was going to, so the proposal landed and what
          is left is the half after it: the note goes and the group the shard left is let
          go, which is :meth:`_commit_move` and nothing else.  Nothing is copied again -
          the rows are already in the group the table names, which is the group the copy
          was made for.
        * the table still names the set the move was leaving, so nothing was proposed,
          and the move is picked up where a retry of it is: freeze, copy what the new
          group is missing, propose - which is :meth:`finish_move`.
        * the table names neither, which is an operator who moved the shard by hand or a
          second move computed from the same table.  There is no answer here that is not
          a guess, and guessing between two live groups is how a range ends up served by
          one of them while its rows are in the other, so the shard stays frozen and the
          caller is told what the table says.

        The rows come back out of the source rather than out of the note, for the split's
        reason: a note carrying a copy of the rows would be a second copy taken at some
        earlier moment, and a copy of a copy is how a move loses a row.

        Returns the shards whose move it finished.  A move it could not finish - the
        source has no leader yet, the table cannot be reached, the table names a set that
        is neither of the two - is left frozen and still remembered, and the next call
        picks it up.  Every step is a no-op the second time, so a caller that is not sure
        whether the last call got there may simply ask again.
        """
        self.load_pending_migrations()
        return self.finish_pending_migrations()

    def load_pending_migrations(self) -> None:
        """Read the move notes this side's shards left behind, and freeze them.

        A note means a move of that shard was in flight when the process stopped: its
        rows may already be in the new group, and the routing table may or may not name
        it.  The one thing that must not happen while that is unknown is the shard taking
        rows - a row written into it now would be one the new group does not have, on a
        shard the table may already have given away - so the source goes back to being
        frozen here, and stays frozen until the move is finished.  Finishing one is
        :meth:`recover_migrations`, which reads the routing table to find out which half
        of it is left.
        """
        for shard_id in sorted(self._view.shard_ids()):
            if self.migration_state(shard_id) is not None:
                continue
            state = self._load_migration_record(shard_id)
            if state is None:
                continue
            self.hold_migration(state)
            self._view.freeze(shard_id, "migration", state.source_nodes)

    def finish_pending_migrations(self) -> List[int]:
        """Finish every move this side knows it was in the middle of."""
        return [shard_id for shard_id in sorted(self._migrations)
                if self._resume_migration(shard_id)]

    def split_error(self) -> Optional[str]:
        """Why the last split could not finish, if it could not."""
        return self._last_split_error

    def pending_splits(self) -> Dict[int, Dict[str, Any]]:
        """The splits that are waiting for the routing table, for callers that look."""
        return dict(self._pending_splits)

    # -- the move's state, which is the recovery's and not the side's ----------------

    def migration_error(self) -> Optional[str]:
        """Why the last move could not be started or copied, if it could not."""
        return self._last_migration_error

    def record_migration_error(self, reason: str) -> None:
        """Say why a move could not be started, or could not be carried on.

        The one call here that a side beginning a move makes, and the reason it exists: the
        beginning of a move is not a recovery - it is ``move_shard``, which refuses for
        reasons of its own (nowhere to move to, a set that already serves the shard, a shard
        with no leader) - and what it has to say about a refusal is the same kind of string a
        recovery leaves when a copy or a proposal cannot be finished.  A caller reads both
        through :meth:`migration_error`, so both land in one place, and it is the recovery's.
        """
        self._last_migration_error = reason

    def migration_state(self, shard_id: int) -> Optional[MigrationState]:
        """The move this side has begun or picked up for ``shard_id``, if there is one.

        A shard with one of these has two groups for a moment - the one the routing table
        names and the one it is about to - so this is also what says which of the two a
        lookup about the shard means, and what a view answers while that is true.
        """
        return self._migrations.get(shard_id)

    def migrations(self) -> Dict[int, MigrationState]:
        """Every move this side has not finished, keyed by the shard being moved."""
        return dict(self._migrations)

    def hold_migration(self, state: MigrationState) -> None:
        """Keep the move a side has just begun, beside the note it has just written.

        The note is the durable half, and the side that begins the move writes it because
        that side is the one that knows the replica set whose storage it belongs in.  This
        is the half that does not survive a restart, which is why it is the recovery's: a
        recovery is what reads the note back and puts an entry here again.
        """
        self._migrations[state.shard_id] = state

    def drop_migration(self, shard_id: int) -> None:
        """Let go of the move this side was in the middle of.

        The note's own going is :meth:`RecoveryView.forget_note` and a call of its own: a
        move is over when the routing table has the range, and the note goes in the same
        breath.  This is the in-memory record and nothing else, dropped by the side that
        was carrying it when it either finishes the move or gives up on it.
        """
        self._migrations.pop(shard_id, None)


    # -- what a side that is beginning a split calls ---------------------------

    def pending_split(self, shard_id: int) -> Optional[Dict[str, Any]]:
        """The split this side has already begun for ``shard_id``, if it has one.

        The question a caller asks before beginning a second split of the same shard:
        the entry is the split whose rows may already be in the new group, and a retry
        has to finish that one rather than ask for a second, which would point the table
        at a group nothing was ever copied into.
        """
        return self._pending_splits.get(shard_id)

    def remember_split(self, pending: Dict[str, Any]) -> None:
        """Write a split down: the note, and the copy of it this side works from.

        Written before the first row moves.  The window between the copy and the
        proposal is the one place the work exists nowhere else - rows in the new
        shard's group, and a freeze nothing has recorded - so the intent goes into the
        source shard's own storage, the thing that does survive a restart, and the same
        note is what :meth:`load_pending_splits` reads back on the way up.

        Every replica this side holds writes its own copy.  The note says which shard is
        being split, and any replica holding that shard can be the one that finds it.
        """
        note = PendingNote.split(pending["shard_id"], bytes(pending["split_key"]),
                                 int(pending["new_shard_id"]))
        self._view.remember_note(note)
        self._pending_splits[pending["shard_id"]] = pending

    def finish_split(self, pending: Dict[str, Any]) -> bool:
        """Copy what the new shard is missing, then tell the routing table.

        Called for the first attempt and for every retry of it.  A retry that finds rows
        already in the new shard - which is what a split whose proposal was refused, or
        whose response was lost, or that died part way through the copy, comes back to -
        copies only the rest: the source has been frozen since they were read, so a row
        that is already there is the row, and copying it again would be work for nothing.
        What is left is the proposal, which the group recognises as the split it already
        applied.
        """
        if not self._copy_what_is_missing(pending):
            return False

        if not self._publish_split(pending):
            return False

        # Re-range this side first: from here on it is the map the table has, and the
        # publisher - which compares the two - sees a split that is finished rather
        # than a side whose ranges disagree with it.
        self._view.apply_split_locally(pending["shard_id"], pending["split_key"],
                                       pending["new_shard_id"])
        self._pending_splits.pop(pending["shard_id"], None)
        # Every note this side holds for the shard, which is the split's and not a
        # move's: a shard cannot be splitting and moving at once, and the interface
        # drops what a shard has rather than what a caller names.
        self._view.forget_note(pending["shard_id"])
        self._view.unfreeze(pending["shard_id"],
                           self._view.shard_replica_ids(pending["shard_id"]))
        return True

    # -- what a side that is beginning a move calls ----------------------------

    def remember_migration(self, state: MigrationState) -> None:
        """Write a move down, on every replica of the shard being moved.

        The window this covers is the split's, one size larger: between the copy and the
        proposal the rows are in a group the routing table has never heard of, and a
        process that dies there comes back with no idea that a shard of its own is in two
        places.  So the intent goes into the source shard's own storage - the thing that
        does survive a restart - before the first row moves.

        What is not in the note is how far the move got.  A phase that said "done" would
        be a lie the moment the process writing it died; what a side that comes back
        knows is that the move did not finish, and what it does about that is freeze the
        source and copy again, which is what a retry does anyway.
        """
        note = PendingNote.move(state.shard_id, state.source_nodes, state.target_nodes)
        self._view.remember_note(note)

    def finish_move(self, state: MigrationState,
                    drain: float = MIGRATION_DRAIN_SECONDS) -> bool:
        """Copy the rows into the new group, tell the table, and let the old group go.

        Called for the first attempt and for every retry of it - by the call that begins
        a move and by :meth:`_resume_migration` - and idempotent for the reason the
        split's is: a row already in the new group at the same timestamp is the row, and
        copying it again would be a second timestamp on a row that already has one - the
        source has been frozen since the rows were read, so nothing it holds has moved on.

        The group is built here rather than before the freeze because the rows were read
        first: a target node that already served the shard is a node the call that begins
        a move refuses to move onto, so building it is adding a group where there was
        none - and the group it replaces nothing of is the one the copy is written into.

        ``drain`` is the window the group it left goes on answering for, handed to
        :meth:`_commit_move`: a caller making the move waits it out, and a side finishing
        one it found in the shard's own storage does not.
        """
        shard_id = state.shard_id
        source = self._wait_for_group(shard_id, state.source_nodes)
        if source is None:
            self.record_migration_error(f"shard {shard_id} has no leader")
            return False

        if not state.rows:
            # A move this process did not start: a side that came back found the note,
            # which says which shard was moving and where to, and nothing else - so the
            # rows are read out of the source, which has been frozen since it started and
            # is the only place they exist, rather than out of the note.
            start, end = self._view.range_map()[shard_id]
            # And the check the caller that begins a move makes, because this is the same
            # read: a lock in the range may be a commit that has not been applied yet, and
            # a copy taken over one is a row read at a moment when it is about to change.
            # The note stays, the shard stays frozen, and the lock clears on its own.
            if self._locks_in_range(source, start, end):
                self.record_migration_error(
                    f"a transaction holds a lock in shard {shard_id}; its rows cannot be "
                    f"read at one moment while one is in flight")
                return False
            # The rows as they stand, which is what the caller that begins a move reads
            # out of the shard's own state: one range read over the group's leader, at the
            # newest committed version, which is the read the copy below is about to make
            # again for the versions it needs.
            state.rows = [(key, value)
                          for key, value, _ in source.scan_versions(start, end)]

        # The target's own members and nothing closed, which is not the placement: the
        # source is still the group the table names, so this is a group built beside it
        # rather than instead of it, and its members are the target nodes alone.
        self._view.ensure_group_on(shard_id, state.target_nodes)

        target = self._wait_for_group(shard_id, state.target_nodes)
        if target is None:
            self.record_migration_error(
                f"the new group for shard {shard_id} on {state.target_nodes} elected no "
                f"leader")
            return False

        state.phase = MigrationPhase.COPYING
        if not self._copy_rows(source, target, state):
            return False

        state.phase = MigrationPhase.PROPOSING
        result = self._propose_move(shard_id, state.target_nodes)
        if result.outcome == ProposalOutcome.REJECTED:
            # The table answered no, and would answer no again.  The shard goes back to
            # serving, because a refusal is not a reason to leave a range unserved, and
            # this is the end of the move rather than a state to retry out of.
            self.record_migration_error(result.message)
            self._abort_move(state)
            return False

        if result.outcome == ProposalOutcome.UNREACHABLE:
            # Nothing answered, so the proposal may or may not have landed, and the one
            # thing that must not happen is the shard taking rows for a range the table
            # may already have given away.  Frozen, written down, and waiting for the
            # caller to ask again - which is where a side that comes back finds it too.
            self.record_migration_error(result.message)
            return False

        self._commit_move(shard_id, state.target_nodes, drain=drain)
        return True

    # -- the split's body -------------------------------------------------------

    def _resume_split(self, shard_id: int) -> bool:
        """Pick a split back up.  See :meth:`recover_splits`."""
        pending = self._pending_splits.get(shard_id)
        if pending is None:
            return False

        # It was copied, or it was about to be.  Either way the shard may not take new
        # rows until the table says where they go, and a restarted node has to be told
        # again: a freeze is a local fact, and it died with the process that held it.
        # The group this side holds for the shard, and the one its rows are about to be
        # read out of: a split has one, so this names the whole of it.
        self._view.freeze(shard_id, "split",
                          self._view.shard_replica_ids(shard_id))
        self._view.ensure_serving(pending["new_shard_id"], self._view.node_ids())

        source = self._wait_for_leader(shard_id)
        if source is None:
            self._last_split_error = f"shard {shard_id} has no leader"
            return False

        # The check the call that begins a split makes, made here by the call that
        # finishes one: this is the same read of the same frozen range, so a lock in it
        # is the same reason to refuse.  The note stays and the shard stays frozen,
        # which is the state a retry starts from - and the lock clears on its own.
        start, end = self._view.range_map()[shard_id]
        if self._locks_in_range(source, start, end):
            self._last_split_error = (
                f"a transaction holds a lock in shard {shard_id}; its rows cannot be "
                f"copied while one is in flight")
            return False

        pending["rows"] = self._rows_above(source, shard_id, pending["split_key"])
        return self.finish_split(pending)

    def _copy_what_is_missing(self, pending: Dict[str, Any]) -> bool:
        """Move the rows of the right half that the new shard does not already hold.

        A retry starts from whatever the process that died had managed to copy, so the
        question is asked per row rather than per split: a split that got one row across
        before it stopped has one row less to move, and one that got none has all of
        them.  Asking it per split would re-copy the rows that did make it, which is the
        same data written twice and a second timestamp on a row that already had one.

        The version already in the new shard, at the same timestamp, is the row - the
        source has been frozen since they were read, so it cannot have moved on.  A row
        the source no longer has is skipped: it was deleted, and there is nothing left to
        move.  A row the new shard has at another timestamp is not the same row, and is
        copied over.

        The new shard is read through a leader that has confirmed an entry of its own
        term with a quorum, because a row being in the group's log and the group's leader
        being able to see it are two different things straight after a restart, and
        believing the second when only the first is true would lose the row.
        """
        new_shard_id = pending["new_shard_id"]
        # The new shard is not a shard the routing table names: for a split it is the one
        # the split's own proposal is about to name, and until that lands the table has no
        # entry for it.  So the group the copy goes into is asked for by node set - which
        # is what ``leader_client_for_nodes`` is for - and not by shard id: asking by id
        # would reach the every-node fallback ``_serving_nodes`` keeps for a shard it has
        # no entry for, which happens to include the group just built, and a process keeps
        # no such fallback and would be answered None.  A caller that broke this would be
        # making a mistake rather than meeting a failure of the shard, so the call is held
        # to it by a test that reads this body (``tests/test_copy_row.py``) rather than by
        # a check here: the four outcomes of a split are about the work, and a bug filed
        # as one more attempt is a bug that gets retried instead of fixed.
        target = self._wait_for_group(new_shard_id, self._view.node_ids())
        source = self._view.leader_client(pending["shard_id"])
        if target is None or source is None:
            self._last_split_error = (f"shard {new_shard_id} or "
                                      f"{pending['shard_id']} has no leader")
            return False

        # The versions are read over the whole source range rather than per key, because a
        # key-by-key read is a quorum round each: a range read is one, and the rows it
        # answers for are the ones this copy is asking about.
        start, end = self._view.range_map()[pending["shard_id"]]
        versions = {key: version
                    for key, _, version in source.scan_versions(start, end)}
        for key, value in pending["rows"]:
            expected = versions.get(key)
            if expected is None:
                continue
            if target.get(key).commit_ts == expected:
                continue
            self._move_row(source, target, key, value, expected)
        return True

    def _publish_split(self, pending: Dict[str, Any]) -> bool:
        """Tell the routing table that the new shard owns the right half.

        A refusal is not the end of the split: the shard stays frozen and the caller
        comes back through :meth:`finish_split`, which is the same proposal again.
        What must not happen is a thaw.  The rows are in a group the table has not
        been told about, and a client routing by the old table would be sent to the
        source shard for keys whose data has already been copied out of it.
        """
        client = self._view.metadata_client()
        if client is None:
            # No table to tell.  An in-process cluster has its own range map and
            # nothing else; that map is the whole world to it.
            return True

        new_shard_id = pending["new_shard_id"]
        # The addresses come from every node of the cluster, because that is the set the
        # shard is served by until the table says otherwise - the same set the split
        # built the new group from - and a node this side cannot place is left out rather
        # than guessed at.
        result = client.split_shard(
            pending["shard_id"], pending["split_key"], new_shard_id,
            self._view.shard_replica_ids(new_shard_id),
            self._view.addresses_on(new_shard_id, self._view.node_ids()),
        )
        if result.success:
            return True

        self._last_split_error = result.error_msg
        return False

    # -- the move's body --------------------------------------------------------

    def _wait_for_table(self, timeout: Optional[float] = None) -> Any:
        """The routing table, asked for until it answers or until ``timeout`` runs out.

        The look a recovery takes at the table before anything is decided, and the one look
        that is waited for.  A side that comes back reads its notes while the metadata group
        is still electing - the two are the same restart - and a read that lands in that
        window is answered "there is no leader", which is true of the moment and not of the
        table.  A recovery that took that as final would stop there for good: nothing calls
        it a second time.  So the read is made again, and the deadline is what keeps that
        bounded.

        None is an answer rather than a failure: it is a side with no table at all, which
        is an in-process cluster with no metadata service, and there is nothing to come
        back.  A table that has still not answered by the deadline is handed on as the
        failure it is - the raise, and not a None that would read as "no table" - so what a
        recovery does with a table it cannot read is what it did before the wait was here.
        ``timeout`` defaults to ``TABLE_TIMEOUT``, read when the wait starts rather than
        bound to this call: a test that cannot wait the ten seconds out for real shortens
        the recovery's patience by patching the constant.
        """
        if timeout is None:
            timeout = TABLE_TIMEOUT
        client = self._view.metadata_client()
        if client is None:
            return None

        deadline = time.time() + timeout
        while True:
            try:
                return client.table(refresh=True)
            except RuntimeError:
                if time.time() >= deadline:
                    raise
                time.sleep(LEADER_POLL_SECONDS)

    def _resume_migration(self, shard_id: int) -> bool:
        """Pick a move back up.  See :meth:`recover_migrations`."""
        state = self.migration_state(shard_id)
        if state is None:
            return False

        # The freeze is a local fact, and it died with the process that set it.  It goes
        # back on before anything else here happens: the shard may already be the new
        # group's, and a row let into the old one now is a row nothing will carry across.
        # This is also what makes the call safe to make twice.
        self._view.freeze(shard_id, "migration", state.source_nodes)

        try:
            table = self._wait_for_table()
        except RuntimeError as nothing_read:
            # The same two shapes the proposal loop folds into one: an in-process group
            # with no leader, and a client that found no address to ask.  Nothing was read,
            # so nothing is known, and the move waits for the next call.
            self.record_migration_error(str(nothing_read))
            return False

        if table is None:
            # No table to disagree with: this side's own range map is the whole world to
            # it, so nothing was proposed anywhere and the move finishes the way a retry
            # of it does.
            return self.finish_move(state, drain=0)

        placement = table.shard(shard_id)
        if placement is None:
            self.record_migration_error(
                f"shard {shard_id} is not in the routing table, and a move of it was in "
                f"flight")
            return False

        if sorted(placement.nodes) == sorted(state.target_nodes):
            # The proposal landed before the process stopped, so the shard is the new
            # group's and the note is all that is left of the move.  The group has to be
            # built here first: a restarted side builds the shards it was told to serve,
            # and the one the table names is not necessarily one of them.
            # Built, but not the placement yet: the note this move wrote into the source
            # is dropped by :meth:`_commit_move` before that group goes, so the source has
            # to still be holding its storage when the cleanup gets there.
            self._view.ensure_group_on(shard_id, state.target_nodes)
            # No drain and no proposal.  A window is what lets a client that cached the old
            # table finish the read it arrived with, and this process has been down long
            # enough that no read is still waiting on it - waiting the window out was the
            # caller's step, in the call that switched the table.
            self._commit_move(shard_id, state.target_nodes, drain=0)
            return True

        if sorted(placement.nodes) == sorted(state.source_nodes):
            # Nothing was proposed, so the copy is where the move stopped - or never
            # started - and the call that finishes it is the one a caller retrying the
            # move would make.
            return self.finish_move(state, drain=0)

        self.record_migration_error(
            f"shard {shard_id} is moving from {state.source_nodes} to "
            f"{state.target_nodes}, and the routing table says {placement.nodes}")
        return False

    def _copy_rows(self, source: NodeClient, target: NodeClient,
                   state: MigrationState) -> bool:
        """Copy the rows the new group does not already have, as the versions they are.

        Written against the seam and nothing else: a row's version comes out of a range
        read, the write record out of ``get_write_record``, and the row goes in through
        ``propose``.  That is what lets one body serve a cluster copying between groups of
        its own nodes and a process copying between groups whose leaders are in other
        processes - and a copy that reached around the client would work in this process
        and nowhere else, and would work here silently, because a local state machine
        always answers.

        ``state.rows`` is what the caller read at the freeze, which is a pair per row:
        which version each one is at is a fact about the shard that holds it, so it is
        asked of the shard.  When the read that fills ``state.rows`` answers with the
        versions as well the rows will carry them, and this becomes one read where it is
        now two.
        """
        start, end = self._view.range_map()[state.shard_id]
        versions = {key: version
                    for key, _, version in source.scan_versions(start, end)}
        for key, value in state.rows:
            expected = versions.get(key)
            if expected is None:
                # The source no longer has it, so there is nothing left to move.
                continue
            if target.get(key).commit_ts == expected:
                # A read that could not answer carries no version, and 0 is what a row
                # that is not there carries too.  Either way the row goes in, which is
                # idempotent for a row that is already at this version.
                continue
            result = self._move_row(source, target, key, value, expected)
            if not result.success:
                # A row the new group refused is a row it does not have, and a move that
                # carried on would leave a shard whose table entry names a group that is
                # missing one of its rows.
                self.record_migration_error(
                    f"the new group refused a row of shard {state.shard_id}: "
                    f"{result.error_msg}")
                return False
            state.copied_keys.append(key)
        return True

    def _propose_move(self, shard_id: int, target_nodes: List[int]) -> ProposalResult:
        """Tell the routing table that ``shard_id`` is served by ``target_nodes`` now.

        The last step of a move and the only one a client can see, so it is written as
        the one thing it may never do: describe a replica set the caller made up.  The set
        being replaced is read out of the table at the attempt that uses it, rather than
        taken from this side's own answer - the table's machine refuses a move whose
        expectation of the current set is wrong (code 14), so a caller that guessed would
        be refused every time two moves were computed from one table, and this side's own
        answer is exactly the stale thing a 14 exists to catch.

        The three outcomes are the three a caller can act on.  A refusal is final: the
        machine would answer the same command the same way, and the loop does not ask it
        twice.  No answer is not final - it means the command may or may not have landed -
        and the second attempt is how the caller finds out, because the machine recognises
        a move it has already applied and answers it as a success rather than as a second
        write.

        What is not here is any reading of *which* rule refused (12 through 16).  Each has
        a code of its own in the machine and the wire flattens them all into one, so the
        reason survives only in the message - see the README.  Nothing below branches on
        it, and a caller that wants to (a 14 is worth re-reading the table for, a 15 is
        worth walking away from) is reading prose, which is the debt and not the design.
        """
        client = self._view.metadata_client()
        if client is None:
            # No table to tell.  A side with no routing table has its own range map and
            # nothing else; that map is the whole world to it.  There is nothing to
            # tell, which is what a split says in the same position.
            return ProposalResult(ProposalOutcome.OK, "there is no routing table to tell")

        addresses = self._view.addresses_on(shard_id, target_nodes)
        last_error = None
        for attempt in range(PROPOSE_ATTEMPTS):
            try:
                table = client.table(refresh=True)
            except RuntimeError as nothing_read:
                # One exception for two shapes on purpose: the in-process client raises a
                # RuntimeError when its group has no leader, and the one across a wire
                # raises ``NodeUnreachable`` - which is one - when no address answered.
                # Both mean the same thing here: nothing was asked, so nothing is known.
                last_error = str(nothing_read)
                time.sleep(RETRY_BACKOFF * (attempt + 1))
                continue

            placement = table.shard(shard_id)
            if placement is None:
                return ProposalResult(
                    ProposalOutcome.REJECTED,
                    f"shard {shard_id} is not in the routing table")

            result = client.move_shard(shard_id, placement.nodes, target_nodes, addresses)
            if result.success:
                return ProposalResult(ProposalOutcome.OK)

            if result.error_code != ErrorCode.ERR_NOT_LEADER:
                return ProposalResult(ProposalOutcome.REJECTED, result.error_msg)

            # Not the leader, and the client has already followed every name it was
            # given - so either the group changed under it or nothing answered at all.
            # Reading the table again is the only way to tell those apart, and it is
            # where the next attempt starts.
            last_error = result.error_msg
            time.sleep(RETRY_BACKOFF * (attempt + 1))

        return ProposalResult(ProposalOutcome.UNREACHABLE, last_error)

    def _commit_move(self, shard_id: int, target_nodes: List[int],
                     drain: float = MIGRATION_DRAIN_SECONDS) -> List[int]:
        """Make the new group the shard's, and let the group it left go.

        Called once the routing table says the shard is served by ``target_nodes`` - by
        the move that proposed it, and by a side that comes back and finds that it is.
        The order is not free:

        1.  This side's own answer for the shard becomes the new set, so every question it
            answers about the shard - who serves it, where its leader is, which address to
            publish - is answered with the group the table names.  That write is
            :meth:`RecoveryView.apply_move_locally`, one call for the placement and for the
            leader this side last published, because from here on the group it left is not
            the answer to anything.  The group itself is still up for the next step.
        2.  That group goes on answering for a fixed window (``drain``).  A client routes
            by a table it cached, so the node it was sent to a moment ago is a node it may
            still ask: the window is what lets a client finish the read it arrived with
            instead of meeting a closed port.  It cannot take new rows - it has been
            frozen since before the copy, and stays frozen through all of this - so what
            it can still answer is a read of what the copy already carried away.
        3.  The move's note goes, while the storage holding it is still open.  The note is
            what a side that comes back reads to find a move in flight; after this there is
            none to find, and a note left inside a storage about to be closed could not be
            deleted at all.
        4.  The group goes: its node, its port and its storage, on every node that is not
            in the new set.
        5.  What is left on disk is renamed rather than deleted -
            ``orphan-shard-<id>-<when>``, in the directory the shard's own storage lived
            in.  A table that has to be put back, after a bug in the machine that holds it
            or an operator's mistake, finds the rows still here; nobody finds them by
            accident, because nothing looks under that name.  A leaked directory costs
            disk and a lost range costs the data.

        Every step is a no-op the second time, because the caller may be a side that died
        in the middle of these and started again: closing a shard that is already closed,
        deleting a note that is already gone and renaming a directory that is no longer
        there are all answers rather than errors.

        What it returns is the nodes whose group it closed, so an empty list is a move
        that was already committed - which is an answer and not a failure, and is the
        difference a caller can see between "I did it" and "it was done".
        """
        # The placement, and the leader this side last published for the shard, in one
        # call: both are this side's answer for the shard, and both belong to the group
        # that has just left.  What the shard is *held* by does not change until the step
        # below, and is meant not to.
        self._view.apply_move_locally(shard_id, target_nodes)
        self.drop_migration(shard_id)

        if drain > 0:
            time.sleep(drain)

        # Every note this side holds for the shard, which is a move's and not a split's:
        # a shard cannot be splitting and moving at once, and the interface drops what a
        # shard has rather than what a caller names.
        self._view.forget_note(shard_id)
        return self._view.ensure_serving(shard_id, target_nodes)

    def _abort_move(self, state: MigrationState) -> List[int]:
        """Give up on a move the routing table refused, and put the shard back.

        A refusal is final - the machine would answer the same command the same way - so
        this is the end of the move and not a state to retry out of.  What it undoes is
        everything except the copy: the source goes back to taking rows, because it is the
        group the table still names and a refusal is not a reason to leave a range
        unserved; the note goes, because there is no move in flight to find; and the group
        that was built to receive the rows is closed and put aside like any other storage
        of a shard that is not served here, so that one shard does not go on having two
        live groups.

        The rows themselves are kept, under the orphan name.  A caller that means to try
        again - with a target set the table will take - does not find them, which is right:
        the new group is built fresh and the copy is the whole of the range rather than
        part of a move that was refused.  An operator who wants to know what was copied
        before it was refused does find them, which is the other half of why nothing here
        deletes anything.
        """
        shard_id = state.shard_id
        self.drop_migration(shard_id)
        self._view.forget_note(shard_id)
        self._view.unfreeze(shard_id, state.source_nodes)
        return self._view.close_group_on(shard_id, state.target_nodes)

    # -- the split's leaves -----------------------------------------------------

    def _load_split_record(self, shard_id: int) -> Optional[Dict[str, Any]]:
        """The split this side's shard was in the middle of, as its replicas wrote it.

        Read through the view, which answers with the notes this side holds for the
        shard and lets each note say which kind it is.  At most one is here today: a
        shard cannot be splitting and moving at once.  A second would be a state neither
        flow can have been in, and the interface says a recovery refuses that rather than
        picks between them - which nothing here does yet; this takes the split's note and
        leaves a move's to the move's own reading.
        """
        for note in self._view.pending_notes(shard_id):
            if note.kind == SPLIT_RECORD_PREFIX:
                return {"shard_id": shard_id,
                        "split_key": note.split_key,
                        "new_shard_id": note.new_shard_id,
                        "rows": []}
        return None

    def _wait_for_leader(self, shard_id: int,
                         timeout: float = LEADER_TIMEOUT) -> Optional[NodeClient]:
        """A client for whatever leads ``shard_id``, waiting a bounded while for one.

        A shard that has just been created has no leader yet - the group has to elect one
        before anything can be read out of it - and the view answers None until it has,
        which is an answer and not a failure.  How long to wait for one is the caller's
        business, which is why the deadline is here and not behind the interface.
        """
        deadline = time.time() + timeout
        while True:
            client = self._view.leader_client(shard_id)
            if client is not None:
                return client
            if time.time() >= deadline:
                return None
            time.sleep(LEADER_POLL_SECONDS)

    def _wait_for_group(self, shard_id: int, nodes: List[int],
                        timeout: float = LEADER_TIMEOUT) -> Optional[NodeClient]:
        """A client for whichever of ``nodes`` leads ``shard_id``, waiting for one.

        The wait above, asked about a set rather than about the shard: the group a split
        copies *into* is not one the routing table names until the split's own proposal
        lands, so the set is the one the split built the group from.  What the lookup
        behind it answers for is a leader that has confirmed an entry of its own term
        with a quorum - ``follower_read_index`` collecting acknowledgements, which is the
        same question ``has_committed_in_its_own_term`` answers on a node - so a group
        that has elected a leader it cannot yet read its own log through is not one this
        hands back.
        """
        deadline = time.time() + timeout
        while True:
            client = self._view.leader_client_for_nodes(shard_id, nodes)
            if client is not None:
                return client
            if time.time() >= deadline:
                return None
            time.sleep(LEADER_POLL_SECONDS)

    def _rows_above(self, client: NodeClient, shard_id: int, split_key: bytes
                    ) -> List[Tuple[bytes, bytes]]:
        """The rows of ``shard_id``'s range that belong to the shard above the point.

        The read the split's copy is about to make, over the seam: a side holding the
        cluster reads the source's own storage, and a process has none to read, so the
        range comes back through the group's leader as the versions each row is at.  A
        lock in the range refuses the read rather than being left out of it, which is
        what :meth:`_locks_in_range` asks about first.
        """
        start, end = self._view.range_map()[shard_id]
        return [(key, value) for key, value, _ in client.scan_versions(start, end)
                if key >= split_key]

    def _locks_in_range(self, client: NodeClient, start: bytes, end: bytes) -> bool:
        """Whether a transaction holds a lock anywhere in ``[start, end)``.

        Asked by every caller about to read a whole range out of a shard and put it
        somewhere else: the two that begin a split or a move, and the two that finish one
        out of a note.  A lock in there may be a commit that has not been applied yet, and
        an intent is not in the version space, so a read that went ahead would answer with
        the old version of a row a transaction is in the middle of changing - the copy
        would be of a row that is about to be different, and nothing downstream could
        tell.  A caller that gets a yes refuses and comes back later: the lock belongs to
        a transaction, so it clears by itself, and the shard waits for that frozen.

        The cluster asks a node's own storage, which a process has none of; here the
        question goes to the group instead, as the range read that refuses over a lock.
        A refusal that is not about a lock is not this question's answer: the read that
        follows meets it, and reports it there.
        """
        try:
            client.scan(start, end)
        except ScanRefused as refusal:
            return refusal.error_code == ErrorCode.ERR_LOCKED
        return False

    def _move_row(self, source: NodeClient, target: NodeClient, key: bytes,
                  value: bytes, version: int) -> ApplyResult:
        """Copy one row into the new shard *as the version it already is*.

        The row keeps the timestamp it was committed at, and the write record of
        the transaction that committed it.  A copy stamped with the moment of the
        move - which is what a wall clock gives you, and what this used to do - is
        the newest thing that has ever happened to that key: a snapshot read at
        any timestamp a client can hold does not see it, and every prewrite
        against it is refused as a write conflict, because the copy is newer than
        the start timestamp the TSO has just handed out.  Moving a row is not a
        write to the key, and a shard that answers for the row differently from
        the shard it came from is a shard the row cannot be moved to.

        The version is handed in rather than read here: the caller has just read it, and
        a second read would be a second answer to a question that already has one.  A
        record whose commit is some other moment is a row written after that transaction,
        and it travels without a ``start_ts``.
        """
        record = source.get_write_record(key)
        command = serialize_command(
            CommandType.SET,
            key=key,
            value=value,
            timestamp=version,
            start_ts=(record["start_ts"] if record is not None
                      and record["commit_ts"] == version else None),
        )
        return target.propose(command)

    # -- the move's leaves ------------------------------------------------------

    def _load_migration_record(self, shard_id: int) -> Optional[MigrationState]:
        """The move this side's shard was in the middle of, as its replicas wrote it.

        Read through the view, the same way as the split's note above and for the same
        reason: which notes this side holds, and which kind each one is, is the side's
        answer and the note's own.  At most one is here today: a shard cannot be splitting
        and moving at once.
        """
        for note in self._view.pending_notes(shard_id):
            if note.kind == MIGRATION_RECORD_PREFIX:
                return MigrationState(shard_id=shard_id,
                                      target_nodes=list(note.target_nodes),
                                      source_nodes=list(note.source_nodes))
        return None
