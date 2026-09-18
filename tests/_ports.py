"""Port allocation shared by the network tests.

The original helper picked each port with ``bind(('127.0.0.1', 0))`` and then
closed the socket.  That is racy: nothing holds the port between the probe and
the real bind, so the same port can be handed out twice.

What is left is the spacing.  A node's ports are one block - its shard segment,
then the routing table's group and the timestamp group above it, all at fixed
offsets from its base port (``oxidedb/launcher.py``) - so the block is the same
width whatever a node serves, and two nodes' blocks overlap unless their bases
are a whole block apart.  Every allocation therefore claims ``block_width()``
consecutive free ports and returns the first, so the next allocation is at least
that far away, and it is asked for nothing about the cluster being built.

The window stops at ``_BASE_MAX``, which is where Windows starts handing ports
out to outgoing connections.  That boundary is load-bearing rather than tidy:
above it a thousand consecutive ports are almost never all free - the machine
this was written on had 671 of the 5848 ports from 49152 up taken - so a search
that ran into that range ground through every candidate left in the window, a
thousand binds each, and one allocation took half a minute.  Below the boundary
the same machine has 4 ports taken out of the 29152 the window covers.

Candidates sit on a grid of whole blocks, so the search looks at a few dozen
positions rather than at every port, and two allocations cannot overlap even
when it wraps.  A block is probed once and the verdict is remembered: what the
window has to offer does not change while the suite runs, and probing a thousand
ports again for each of the suite's hundreds of allocations cost about half a
minute - more than the probe protects against.  Blocks that are out on loan are
skipped until the grid runs out, at which point every cluster that took one has
been torn down and the grid is handed out from the start again.
"""

import socket
from typing import Dict, FrozenSet, Optional, Set

from oxidedb.launcher import block_width

#: Below the first port Windows hands out to an outgoing connection.
_BASE_MAX = 49152

#: Above the registered ports, and low enough that nothing else is likely there.
_BASE_MIN = 20000

#: What a caller that does not say otherwise gets: room for a whole node.
_BLOCK = block_width()

_cursor = _BASE_MIN

#: What probing said about a block, and which blocks are out on loan.
_known_free: Dict[int, bool] = {}
_handed_out: Set[int] = set()


def _is_free(port: int) -> bool:
    # Deliberately no SO_REUSEADDR: on Windows that option lets a bind succeed
    # against a live listener, which would make this probe report "free" for a
    # port that is very much in use.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(('127.0.0.1', port))
            return True
        except OSError:
            return False


def _grid(port: int, span: int) -> int:
    """Round ``port`` up to the next block boundary of the grid."""
    offset = (port - _BASE_MIN) % span
    return port if offset == 0 else port + span - offset


def _block_is_free(base: int, span: int) -> bool:
    """Whether every port of the block at ``base`` can be bound right now."""
    if span != _BLOCK:
        return all(_is_free(port) for port in range(base, base + span))
    if base not in _known_free:
        _known_free[base] = all(_is_free(port) for port in range(base, base + span))
    return _known_free[base]


def _next_block(span: int, taken: FrozenSet[int]) -> Optional[int]:
    """Return the base of the first usable block, or ``None`` if there is none."""
    global _cursor

    limit = _BASE_MAX - span
    if limit < _BASE_MIN:
        raise RuntimeError(
            f"a block of {span} ports does not fit between {_BASE_MIN} and {_BASE_MAX}")

    # Scan forward from the cursor, then wrap once, so a cursor parked near the
    # end of the window still finds the blocks at the bottom.
    for start, stop in ((_cursor, limit), (_BASE_MIN, min(_cursor, limit))):
        for base in range(_grid(start, span), stop, span):
            if base in taken or base in _handed_out:
                continue
            if _block_is_free(base, span):
                _cursor = base + span
                if span == _BLOCK:
                    _handed_out.add(base)
                return base
    return None


def allocate_port(span: int = _BLOCK, taken: FrozenSet[int] = frozenset()) -> int:
    """Return a free port, leaving the next ``span`` ports unused.

    ``taken`` names the bases this round of allocation has already handed out,
    so a search that wraps cannot give one caller what it just gave another.
    """
    base = _next_block(span, taken)
    if base is None and _handed_out:
        _handed_out.clear()
        base = _next_block(span, taken)
    if base is None:
        raise RuntimeError("no free port block available")
    return base


def free_addresses(num_nodes: int = 3) -> Dict[int, str]:
    """Return ``{node_id: "127.0.0.1:port"}`` with no overlapping node blocks."""
    bases = []
    for _ in range(num_nodes):
        # ``taken`` as well as the blocks already out on loan: a grid that runs out in
        # the middle of this call is handed out from the bottom again, and two nodes of
        # one cluster must not be given the same base by that.
        bases.append(allocate_port(taken=frozenset(bases)))
    return {index + 1: f"127.0.0.1:{base}" for index, base in enumerate(bases)}
