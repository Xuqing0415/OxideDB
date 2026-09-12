"""Port allocation shared by the network tests.

The original helper picked each port with ``bind(('127.0.0.1', 0))`` and then
closed the socket.  That is racy twice over:

* nothing holds the port between the probe and the real bind, so the same port
  can be handed out twice;
* a ``ShardServer`` derives shard ``s`` of a node at ``base + 100 * s``, so two
  bases less than ``100 * num_shards`` apart collide on a derived port even when
  their own ports differ.

Both are avoided by handing out ports with a reserved gap: every allocation
claims ``span`` consecutive free ports and returns the first, so the next
allocation is at least ``span`` away.
"""

import socket
from typing import Dict

_BASE_MIN = 30000
_BASE_MAX = 55000

_cursor = _BASE_MIN


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


def allocate_port(span: int = 400) -> int:
    """Return a free port, leaving the next ``span`` ports unused."""
    global _cursor

    limit = _BASE_MAX - span
    # Scan forward from the cursor, then wrap once, so a cursor parked near the
    # end of the range still finds the ports at the bottom.
    candidates = list(range(_cursor, limit)) + list(range(_BASE_MIN, min(_cursor, limit)))
    for port in candidates:
        if all(_is_free(candidate) for candidate in range(port, port + span)):
            _cursor = port + span
            return port
    raise RuntimeError("no free port block available")


def free_addresses(num_nodes: int = 3, num_shards: int = 1) -> Dict[int, str]:
    """Return ``{node_id: "127.0.0.1:port"}`` with no overlapping shard ports."""
    stride = 100 * max(1, num_shards)
    return {
        index + 1: f"127.0.0.1:{allocate_port(span=stride)}"
        for index in range(num_nodes)
    }
