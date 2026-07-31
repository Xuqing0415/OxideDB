import grpc
from typing import Optional, List, Tuple
from ..proto import client_pb2, client_pb2_grpc
from ..shard.router import ShardRouter


class OxideDBClient:
    def __init__(self, router: ShardRouter):
        self._router = router
        self._channel_cache: dict = {}

    def _get_channel(self, address: str):
        if address not in self._channel_cache:
            self._channel_cache[address] = grpc.insecure_channel(address)
        return self._channel_cache[address]

    def _get_stub(self, address: str) -> client_pb2_grpc.ClientServiceStub:
        channel = self._get_channel(address)
        return client_pb2_grpc.ClientServiceStub(channel)

    def get(self, key: bytes) -> Optional[bytes]:
        address = self._router.get_leader_address_for_key(key)
        if not address:
            return None

        stub = self._get_stub(address)
        try:
            response = stub.Get(client_pb2.GetRequest(key=key))
            return response.value if response.found else None
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                return None
            raise

    def set(self, key: bytes, value: bytes) -> bool:
        address = self._router.get_leader_address_for_key(key)
        if not address:
            return False

        stub = self._get_stub(address)
        try:
            response = stub.Set(client_pb2.SetRequest(key=key, value=value))
            return response.success
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                return False
            raise

    def delete(self, key: bytes) -> bool:
        address = self._router.get_leader_address_for_key(key)
        if not address:
            return False

        stub = self._get_stub(address)
        try:
            response = stub.Delete(client_pb2.DeleteRequest(key=key))
            return response.success
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                return False
            raise

    def scan(self, start_key: bytes, end_key: bytes) -> List[Tuple[bytes, bytes]]:
        address = self._router.get_leader_address_for_key(start_key)
        if not address:
            return []

        stub = self._get_stub(address)
        try:
            response = stub.Scan(client_pb2.ScanRequest(start_key=start_key, end_key=end_key))
            return [(entry.key, entry.value) for entry in response.entries]
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                return []
            raise

    def close(self):
        for channel in self._channel_cache.values():
            channel.close()
        self._channel_cache.clear()