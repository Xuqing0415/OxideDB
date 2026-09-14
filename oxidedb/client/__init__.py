"""The client side's view of a node, and of which node leads which shard.

``node_client`` holds the six primitives a client may ask one node, the protocol a factory
of handles answers, and the in-process implementation of both; ``remote_node_client`` is the
other implementation, over a channel, and ``raft/client_servicer.py`` is the other end of
it.  ``routing`` turns a placement into one of those handles, so that the coordinator, the
resolver and the SQL executor ask the same question and get the same kind of answer whether
the node is in this process or across one.  ``remote_group_client`` is the same idea for the
two groups that are not a shard - the routing table and the clock - which a client reaches
by walking the addresses it was seeded with rather than by looking them up.
"""

from .node_client import (LocalNodeClient, LocalNodeClientFactory, NodeClient,
                          NodeClientFactory, NodeUnreachable)
from .remote_group_client import RemoteMetadataClient, RemoteTSOClient
from .remote_node_client import RemoteNodeClient, RemoteNodeClientFactory
from .routing import ShardLeaders, ask_shard

__all__ = ["LocalNodeClient", "LocalNodeClientFactory", "NodeClient",
           "NodeClientFactory", "NodeUnreachable", "RemoteMetadataClient",
           "RemoteNodeClient", "RemoteNodeClientFactory", "RemoteTSOClient",
           "ShardLeaders", "ask_shard"]