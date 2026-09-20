"""The body of a recovery, written once, run against whichever side of a process.

A split and a move are finished by reading back what they wrote down, and that work is
the same work on both sides of a process boundary; what differs is the objects the side
has in its hand.  ``RecoveryView`` is what a side is asked for and this is what asks,
kept apart on purpose: the flow lives here so that the cluster's two start-up paths and a
node started as a process cannot drift into three slightly different protocols.

The split's half of that flow is here, and the cluster reaches it through this object:
``recover_splits`` for the two start-up paths, and ``pending_split``, ``remember_split``
and ``finish_split`` for a split that cluster begins itself - which writes the note down
and hands the rest over.  The move's half is still written in ``shard_server`` and moves
next, so for now the leaves the two branches share, the rows of a range and the locks in
it, are written twice: here against a client, and there against a node object the side
beginning the work already has in its hand.

The state is the part worth settling before anything moves, because it is the part of the
flow that is not a call.  ``_pending_splits`` and ``_migrations`` are the in-memory copy
of the notes on disk, and the two error strings are why the last attempt could not
finish.  All four belong to the recovery rather than to the side it runs on, and the
reason comes from the process side: a process has nowhere to keep a placement or a
working table, and it does not need one - the note in the shard's own storage is the
durable copy, and what a recovery needs in memory is rebuilt from it on the way up.  A
view asked to hold these would be holding what the disk already carries.
"""

import time

from typing import Any, Dict, List, Optional, Tuple

from ..client.node_client import NodeClient
from .recovery_notes import PendingNote, SPLIT_RECORD_PREFIX
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
        #: What a move is doing, per shard.  The value is a ``MigrationState``, which
        #: lives in ``shard_server`` today; where it lands is the move's work to answer.
        self._migrations: Dict[int, Any] = {}
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

    def split_error(self) -> Optional[str]:
        """Why the last split could not finish, if it could not."""
        return self._last_split_error

    def pending_splits(self) -> Dict[int, Dict[str, Any]]:
        """The splits that are waiting for the routing table, for callers that look."""
        return dict(self._pending_splits)

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
        self._view.unfreeze(pending["shard_id"])
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
        self._view.freeze(shard_id, "split")
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
