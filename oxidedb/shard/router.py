# EXPERIMENTAL: routing not wired to a metadata service, do not use
"""The one routing rule in the codebase.

Every component that needs to know which shard owns a key - the shard server,
the transaction coordinator and the (scaffolding) client router - goes through
:func:`locate`, so they cannot disagree with each other.  The keyspace is split
into byte ranges, the way TiKV splits it, rather than hashed: a range can be
split later without rehashing every key.
"""

from typing import Dict, List, Optional, Tuple

RangeMap = Dict[int, Tuple[bytes, bytes]]


def default_range_map(num_shards: int) -> RangeMap:
    """Split the keyspace into equal-width ranges by first byte.

    A shard owns the half-open interval ``start <= key < end``.
    """
    if num_shards <= 1:
        return {0: (b"", b"\xff")}

    range_map: RangeMap = {}
    chunk = 256 // num_shards
    for i in range(num_shards):
        start = bytes([i * chunk])
        end = bytes([(i + 1) * chunk]) if i < num_shards - 1 else b"\xff"
        range_map[i] = (start, end)
    return range_map


def locate(range_map: RangeMap, key: bytes) -> int:
    """Return the shard that owns ``key``.

    A key that falls outside every range goes to shard 0.  With the default map
    that covers keys whose first byte is ``0xff``, because the final range ends
    at ``b"\xff"`` and the comparison is exclusive.
    """
    for shard_id, (start, end) in range_map.items():
        if start <= key < end:
            return shard_id
    return 0


class ShardInfo:
    def __init__(self, shard_id: int, nodes: List[int], leader_id: Optional[int] = None):
        self.shard_id = shard_id
        self.nodes = nodes
        self.leader_id = leader_id


class ShardRouter:
    def __init__(self, num_shards: int = 3, range_map: Optional[RangeMap] = None):
        self._num_shards = num_shards
        self._range_map: RangeMap = (
            dict(range_map) if range_map is not None else default_range_map(num_shards)
        )
        self._shard_map: Dict[int, ShardInfo] = {}
        self._node_address_map: Dict[int, str] = {}

    def set_range_map(self, range_map: RangeMap):
        self._range_map = dict(range_map)

    def get_range_map(self) -> RangeMap:
        return dict(self._range_map)

    def add_shard(self, shard_id: int, nodes: List[int], leader_id: Optional[int] = None):
        self._shard_map[shard_id] = ShardInfo(shard_id, nodes, leader_id)

    def add_node_address(self, node_id: int, address: str):
        self._node_address_map[node_id] = address

    def get_node_address(self, node_id: int) -> Optional[str]:
        return self._node_address_map.get(node_id)

    def get_shard_id(self, key: bytes) -> int:
        return locate(self._range_map, key)

    def get_shard_info(self, shard_id: int) -> Optional[ShardInfo]:
        return self._shard_map.get(shard_id)

    def get_leader_for_key(self, key: bytes) -> Optional[int]:
        shard_info = self.get_shard_info(self.get_shard_id(key))
        if shard_info:
            return shard_info.leader_id
        return None

    def get_leader_address_for_key(self, key: bytes) -> Optional[str]:
        leader_id = self.get_leader_for_key(key)
        if leader_id:
            return self.get_node_address(leader_id)
        return None

    def update_shard_leader(self, shard_id: int, leader_id: int):
        if shard_id in self._shard_map:
            self._shard_map[shard_id].leader_id = leader_id

    def get_all_shards(self) -> List[ShardInfo]:
        return list(self._shard_map.values())