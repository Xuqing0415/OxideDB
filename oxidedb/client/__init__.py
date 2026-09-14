"""The client side's view of a node, and of which node leads which shard.

``node_client`` holds the six primitives a client may ask one node, the in-process
implementation of them, and the factory a caller gets a handle from;
``proto/client.proto`` is the same six on a wire.  ``routing`` turns a placement into
one of those handles, so that the coordinator, the resolver and the SQL executor ask
the same question and get the same kind of answer whether the node is in this process
or across one.
"""

from .node_client import (LocalNodeClient, LocalNodeClientFactory, NodeClient,
                         NodeClientFactory)
from .routing import ShardLeaders, ask_shard

__all__ = ["LocalNodeClient", "LocalNodeClientFactory", "NodeClient",
           "NodeClientFactory", "ShardLeaders", "ask_shard"]