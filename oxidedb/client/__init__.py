"""The client side's view of a node.

``node_client`` holds the six primitives a client may ask one node, and the
in-process implementation of them; ``proto/client.proto`` is the same six on a wire.
"""

from .node_client import LocalNodeClient, NodeClient

__all__ = ["LocalNodeClient", "NodeClient"]