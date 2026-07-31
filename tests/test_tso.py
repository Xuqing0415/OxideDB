import time
import threading
import socket
from oxidedb.tso.tso import TSOCluster, TSOClient


def get_free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def test_tso_single_client():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    client = tso_cluster.get_client()
    assert client is not None, "TSO client should be available"
    
    timestamps = []
    for i in range(100):
        ts = client.get_timestamp()
        timestamps.append(ts)
    
    for i in range(1, len(timestamps)):
        assert timestamps[i] == timestamps[i-1] + 1, f"Timestamps should be consecutive: {timestamps[i-1]} -> {timestamps[i]}"
    
    print(f"TSO test: Got {len(timestamps)} consecutive timestamps from {timestamps[0]} to {timestamps[-1]}")
    
    tso_cluster.shutdown()
    print("TSO single client test passed!")


def test_tso_concurrent_requests():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    client = tso_cluster.get_client()
    assert client is not None
    
    timestamps = []
    lock = threading.Lock()
    
    def get_ts():
        ts = client.get_timestamp()
        with lock:
            timestamps.append(ts)
    
    threads = []
    for _ in range(10):
        t = threading.Thread(target=get_ts, daemon=True)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    assert len(timestamps) == 10, "Should have 10 timestamps"
    
    unique_timestamps = set(timestamps)
    assert len(unique_timestamps) == 10, "All timestamps should be unique"
    
    print(f"Concurrent TSO test: Got {len(timestamps)} unique timestamps")
    
    tso_cluster.shutdown()
    print("TSO concurrent requests test passed!")


def test_tso_batch_allocation():
    peer_addresses = {
        1: f'127.0.0.1:{get_free_port()}',
        2: f'127.0.0.1:{get_free_port()}',
        3: f'127.0.0.1:{get_free_port()}',
    }
    
    tso_cluster = TSOCluster(num_nodes=3)
    tso_cluster.start(peer_addresses=peer_addresses)
    
    time.sleep(5)
    
    client = tso_cluster.get_client()
    assert client is not None
    
    client._batch_size = 10
    
    timestamps = []
    for i in range(25):
        ts = client.get_timestamp()
        timestamps.append(ts)
    
    assert len(timestamps) == 25
    
    for i in range(1, len(timestamps)):
        assert timestamps[i] == timestamps[i-1] + 1
    
    print(f"Batch TSO test: Got {len(timestamps)} timestamps from {timestamps[0]} to {timestamps[-1]}")
    
    tso_cluster.shutdown()
    print("TSO batch allocation test passed!")


if __name__ == "__main__":
    test_tso_single_client()
    print("\n" + "="*60 + "\n")
    test_tso_concurrent_requests()
    print("\n" + "="*60 + "\n")
    test_tso_batch_allocation()