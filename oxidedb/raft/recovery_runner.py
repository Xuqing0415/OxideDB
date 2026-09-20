"""The body of a recovery, written once, run against whichever side of a process.

A split and a move are finished by reading back what they wrote down, and that work is
the same work on both sides of a process boundary; what differs is the objects the side
has in its hand.  ``RecoveryView`` is what a side is asked for and this is what asks,
kept apart on purpose: the flow lives here so that the cluster's two start-up paths and a
node started as a process cannot drift into three slightly different protocols.

Nothing uses it yet - the cluster declares the interface and hands its bodies over next,
and the launcher after that - so this holds the state the flow works in and the three
calls that will be moved onto it, and no behaviour.

The state is the part worth settling before anything moves, because it is the part of the
flow that is not a call.  ``_pending_splits`` and ``_migrations`` are the in-memory copy
of the notes on disk, and the two error strings are why the last attempt could not
finish.  All four belong to the recovery rather than to the side it runs on, and the
reason comes from the process side: a process has nowhere to keep a placement or a
working table, and it does not need one - the note in the shard's own storage is the
durable copy, and what a recovery needs in memory is rebuilt from it on the way up.  A
view asked to hold these would be holding what the disk already carries.
"""

from typing import Any, Dict, List, Optional

from .recovery_view import RecoveryView


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

    def recover_splits(self) -> List[int]:
        """Finish every split this side has written down, and answer which ones ended."""
        raise NotImplementedError

    def recover_migrations(self) -> List[int]:
        """Finish every move this side has written down, and answer which ones ended."""
        raise NotImplementedError

    def possible_ranges(self) -> List[Dict[int, tuple]]:
        """Both range maps this side could be in, while a split is in flight."""
        raise NotImplementedError
