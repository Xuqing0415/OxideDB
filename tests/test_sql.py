import time
from oxidedb.raft.shard_server import ShardedRaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine, CommandType
from oxidedb.sql.parser import SQLParser
from oxidedb.sql.executor import SQLExecutor


def test_sql_parser():
    parser = SQLParser()
    
    select_stmt = parser.parse("SELECT * FROM users WHERE id = '1'")
    assert select_stmt is not None
    assert select_stmt.table == "USERS"
    assert select_stmt.columns == ["*"]
    assert select_stmt.where_clause == "ID = '1'"
    
    insert_stmt = parser.parse("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    assert insert_stmt is not None
    assert insert_stmt.table == "USERS"
    assert insert_stmt.columns == ["NAME", "AGE"]
    assert insert_stmt.values == ["ALICE", 30]
    
    where_condition = parser.parse_where_condition("id > 10")
    assert where_condition == ("id", ">", 10)
    
    print("SQL parser test passed!")


def test_sql_executor():
    peer_addresses = {
        1: '127.0.0.1:22001',
        2: '127.0.0.1:22002',
        3: '127.0.0.1:22003',
    }
    
    cluster = ShardedRaftCluster(num_nodes=3, num_shards=2)
    cluster.start_network(state_machine_factory=lambda: MVCCStateMachine(), peer_addresses=peer_addresses)
    time.sleep(3)
    
    for i in range(1, 6):
        key = f"users:{i}".encode()
        leader_info = cluster.get_leader_for_key(key)
        assert leader_info is not None
        _, leader = leader_info
        
        command = leader._state_machine.serialize_command(
            CommandType.SET,
            key=key,
            value=f"user_{i}".encode(),
            timestamp=100 + i,
        )
        leader.propose(command)
    
    time.sleep(0.5)
    
    executor = SQLExecutor(cluster)
    
    result = executor.execute("SELECT * FROM users WHERE id = '1'")
    print(f"SELECT WHERE id='1': {result}")
    assert len(result) == 1
    
    result = executor.execute("SELECT * FROM users WHERE id > '2'")
    print(f"SELECT WHERE id>'2': {result}")
    assert len(result) >= 1
    
    result = executor.execute("INSERT INTO products (id, name) VALUES ('p1', 'Apple')")
    print(f"INSERT result: {result}")
    assert len(result) == 1
    
    cluster.shutdown()
    print("SQL executor test passed!")


if __name__ == "__main__":
    test_sql_parser()
    print("\n" + "="*60 + "\n")
    test_sql_executor()