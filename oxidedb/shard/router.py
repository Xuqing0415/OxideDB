# EXPERIMENTAL: routing not wired to a metadata service, do not use
import hashlib
from typing import Dict, List, Tuple, Optional


class ShardInfo:
    def __init__(self, shard_id: int, nodes: List[int], leader_id: Optional[int] = None):
        self.shard_id = shard_id
        self.nodes = nodes
        self.leader_id = leader_id


class ShardRouter:
    def __init__(self, num_shards: int = 3):
        self._num_shards = num_shards
        self._shard_map: Dict[int, ShardInfo] = {}
        self._node_address_map: Dict[int, str] = {}

    def add_shard(self, shard_id: int, nodes: List[int], leader_id: Optional[int] = None):
        self._shard_map[shard_id] = ShardInfo(shard_id, nodes, leader_id)

    def add_node_address(self, node_id: int, address: str):
        self._node_address_map[node_id] = address

    def get_node_address(self, node_id: int) -> Optional[str]:
        return self._node_address_map.get(node_id)

    def get_shard_id(self, key: bytes) -> int:
        hash_val = int(hashlib.md5(key).hexdigest(), 16)
        return hash_val % self._num_shards

    def get_shard_info(self, shard_id: int) -> Optional[ShardInfo]:
        return self._shard_map.get(shard_id)

    def get_leader_for_key(self, key: bytes) -> Optional[int]:
        shard_id = self.get_shard_id(key)
        shard_info = self.get_shard_info(shard_id)
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
