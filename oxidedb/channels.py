"""Channels to nodes: one per address, opened once, closed together.

Three kinds of client open a channel to a node - the factory that hands out shard handles,
and the two clients that reach the routing table's group and the timestamp group - and all
three want the same three things from it: a channel to an address, one channel per address
however many callers ask, and one place that closes them all.  So it is one class, and a
second copy of it would be a second place to leak a channel from.

The address is the key rather than a node id, because an address is what a channel is opened
to: one node serves each of its shards and each of its groups on ports of its own, so an
address already names exactly one thing to talk to.
"""

import threading
from typing import Dict, List

import grpc


#: How long one call waits before the node at the other end is written off.  Long enough
#: to cover an election happening underneath a request, which is the slow case a client
#: meets in practice, and short enough that a node which is simply gone does not hold a
#: caller up for long.  It is a per-call deadline: a commit that needs several calls is
#: bounded by several of these rather than by one.
DEFAULT_TIMEOUT = 4.0


class ChannelPool:
    """Channels by address, opened when first asked for and kept until they are closed."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._channels: Dict[str, grpc.Channel] = {}

    def channel(self, address: str) -> grpc.Channel:
        """The channel to ``address``, opening it the first time it is asked for."""
        with self._lock:
            channel = self._channels.get(address)
            if channel is None:
                channel = grpc.insecure_channel(address)
                self._channels[address] = channel
            return channel

    def forget(self, address: str) -> None:
        """Close and drop the channel to ``address``, if one is being kept.

        The caller that found a channel broken is the only one that knows, and keeping it
        would hand the same broken channel to the next caller.  Forgetting an address that
        was never asked for is not an error: a caller that found one broken and a caller
        that never had one want the same thing to happen.
        """
        with self._lock:
            channel = self._channels.pop(address, None)
        if channel is not None:
            channel.close()

    def close(self) -> None:
        """Close every channel this pool opened."""
        with self._lock:
            channels: List[grpc.Channel] = list(self._channels.values())
            self._channels.clear()
        for channel in channels:
            channel.close()
