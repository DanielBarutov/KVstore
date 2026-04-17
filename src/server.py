import threading
import time
from collections import OrderedDict
from concurrent import futures

import grpc

import kvstore_pb2
import kvstore_pb2_grpc


MAX_KEYS = 10
PORT = 8000


class InMemoryKVStore:
    def __init__(self, capacity: int = MAX_KEYS) -> None:
        self.capacity = capacity
        # key -> (value, expires_at | None)
        self._data: OrderedDict[str, tuple[str, float | None]] = OrderedDict()
        self._lock = threading.RLock()

    def _is_expired(self, expires_at: float | None) -> bool:
        return expires_at is not None and time.time() >= expires_at

    def _purge_if_expired(self, key: str) -> bool:
        item = self._data.get(key)
        if item is None:
            return False

        _, expires_at = item
        if self._is_expired(expires_at):
            self._data.pop(key, None)
            return True
        return False

    def _evict_if_needed(self) -> None:
        while len(self._data) > self.capacity:
            self._data.popitem(last=False)

    def put(self, key: str, value: str, ttl_seconds: int) -> None:
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be >= 0")

        with self._lock:
            expires_at = None if ttl_seconds == 0 else time.time() + ttl_seconds

            if key in self._data:
                self._data.pop(key)

            self._data[key] = (value, expires_at)
            self._evict_if_needed()

    def get(self, key: str) -> str | None:
        with self._lock:
            if key not in self._data:
                return None

            if self._purge_if_expired(key):
                return None

            value, expires_at = self._data.pop(key)
            self._data[key] = (value, expires_at)
            return value

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def list_by_prefix(self, prefix: str) -> list[tuple[str, str]]:
        with self._lock:
            expired_keys: list[str] = []
            result: list[tuple[str, str]] = []

            for key, (value, expires_at) in self._data.items():
                if self._is_expired(expires_at):
                    expired_keys.append(key)
                    continue

                if key.startswith(prefix):
                    result.append((key, value))

            for key in expired_keys:
                self._data.pop(key, None)

            return result


class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, store: InMemoryKVStore) -> None:
        self.store = store

    def Put(self, request, context):
        key = request.key
        value = request.value
        ttl_seconds = request.ttl_seconds

        if not key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "key must not be empty")

        if ttl_seconds < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "ttl_seconds must be >= 0")

        self.store.put(key=key, value=value, ttl_seconds=ttl_seconds)
        return kvstore_pb2.PutResponse()

    def Get(self, request, context):
        key = request.key

        if not key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "key must not be empty")

        value = self.store.get(key)
        if value is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"key '{key}' not found")

        return kvstore_pb2.GetResponse(value=value)

    def Delete(self, request, context):
        key = request.key

        if not key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "key must not be empty")

        self.store.delete(key)
        return kvstore_pb2.DeleteResponse()

    def List(self, request, context):
        prefix = request.prefix
        items = self.store.list_by_prefix(prefix)

        return kvstore_pb2.ListResponse(
            items=[kvstore_pb2.KeyValue(key=key, value=value) for key, value in items]
        )


def serv() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store = InMemoryKVStore(capacity=MAX_KEYS)

    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(store),
        server,
    )

    server.add_insecure_port(f"[::]:{PORT}")
    print(f"gRPC server started on port {PORT}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serv()
