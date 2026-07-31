import heapq
import time
from typing import Dict, List, Optional, Tuple

from ..raft.node import NodeState
from .parser import SQLParser, SelectStatement, InsertStatement


class SQLExecutor:
    def __init__(self, shard_cluster):
        self._shard_cluster = shard_cluster
        self._parser = SQLParser()
    
    def execute(self, sql: str) -> List[Dict[str, any]]:
        statement = self._parser.parse(sql)
        
        if isinstance(statement, SelectStatement):
            return self._execute_select(statement)
        elif isinstance(statement, InsertStatement):
            return self._execute_insert(statement)
        
        return []
    
    def _execute_select(self, stmt: SelectStatement) -> List[Dict[str, any]]:
        where_condition = None
        if stmt.where_clause:
            where_condition = self._parser.parse_where_condition(stmt.where_clause)
        
        if where_condition and where_condition[1] == "=":
            column, _, value = where_condition
            key = f"{stmt.table.lower()}:{value}".encode()
            
            leader_info = self._shard_cluster.get_leader_for_key(key)
            if leader_info is None:
                return []
            
            _, leader = leader_info
            result = leader.get(key)
            
            if result.success and result.value:
                return [{column: result.value.decode()}]
            return []
        
        elif where_condition and where_condition[1] in (">", "<", ">=", "<="):
            return self._execute_range_scan(stmt, where_condition)
        
        else:
            return self._execute_full_scan(stmt)
    
    def _execute_full_scan(self, stmt: SelectStatement) -> List[Dict[str, any]]:
        all_results = []
        
        for server in self._shard_cluster._shard_servers.values():
            for shard_id in range(len(server._shards)):
                node = server.get_shard_node(shard_id)
                if node and node.state == NodeState.LEADER:
                    scan_result = node._state_machine.scan(b"", b"\xff")
                    for key, value in scan_result:
                        if key.decode().startswith(f"{stmt.table}:"):
                            _, column_value = key.decode().split(":", 1)
                            all_results.append({column_value: value.decode()})
        
        return all_results
    
    def _execute_range_scan(self, stmt: SelectStatement, where_condition: Tuple) -> List[Dict[str, any]]:
        column, operator, value = where_condition
        
        start_key = b""
        end_key = b"\xff"
        
        table_name = stmt.table.lower()
        
        if isinstance(value, str):
            value_bytes = f"{table_name}:{value}".encode()
        else:
            value_bytes = f"{table_name}:{value}".encode()
        
        if operator == ">":
            start_key = value_bytes
        elif operator == ">=":
            start_key = value_bytes
        elif operator == "<":
            end_key = value_bytes
        elif operator == "<=":
            end_key = value_bytes
        
        all_results = []
        shard_results = []
        
        for server in self._shard_cluster._shard_servers.values():
            for shard_id in range(len(server._shards)):
                node = server.get_shard_node(shard_id)
                if node and node.state == NodeState.LEADER:
                    scan_result = node._state_machine.scan(start_key, end_key)
                    for key, value in scan_result:
                        if key.decode().startswith(f"{table_name}:"):
                            shard_results.append((key, value))
        
        sorted_results = sorted(shard_results, key=lambda x: x[0])
        
        for key, value in sorted_results:
            _, column_value = key.decode().split(":", 1)
            all_results.append({column_value: value.decode()})
        
        return all_results
    
    def _execute_insert(self, stmt: InsertStatement) -> List[Dict[str, any]]:
        for i, column in enumerate(stmt.columns):
            value = stmt.values[i]
            key = f"{stmt.table}:{column}".encode()
            value_bytes = str(value).encode()
            
            leader_info = self._shard_cluster.get_leader_for_key(key)
            if leader_info is None:
                continue
            
            _, leader = leader_info
            
            command = leader._state_machine.serialize_command(
                b"SET",
                key=key,
                value=value_bytes,
                timestamp=int(time.time() * 1000000),
            )
            leader.propose(command)
        
        return [{"result": "inserted"}]