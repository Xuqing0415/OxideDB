import heapq
import time
from typing import Dict, List, Optional, Tuple

import msgpack

from ..raft.node import NodeState
from ..raft.state_machine import CommandType
from ..storage.engine import next_key
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
            # key schema: "{table}:{id_value}"，与 INSERT 写入路径保持一致
            key = f"{stmt.table.lower()}:{value}".encode()

            leader_info = self._shard_cluster.get_leader_for_key(key)
            if leader_info is None:
                return []

            _, leader = leader_info
            result = leader.get(key)

            if result.success and result.value:
                # 优先按 msgpack dict 反序列化整行；失败则降级为原始 bytes（兼容直接 SET 的旧数据）
                try:
                    row = msgpack.unpackb(result.value, raw=False)
                    if isinstance(row, dict):
                        return [row]
                except Exception:
                    pass
                return [{column.lower(): result.value.decode()}]
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

        # scan 区间为半开 [start_key, end_key)，按操作符语义构造边界：
        #   > : 排除 value 自身 -> start = value 的后继键
        #   >=: 包含 value 自身 -> start = value
        #   < : 排除 value 自身 -> end = value
        #   <=: 包含 value 自身 -> end = value 的后继键
        # 使用 next_key() 而非 value + \x00：0x00 现在是 MVCC 键空间的分隔符，
        # 不允许出现在用户键里。
        if operator == ">":
            start_key = next_key(value_bytes)
        elif operator == ">=":
            start_key = value_bytes
        elif operator == "<":
            end_key = value_bytes
        elif operator == "<=":
            end_key = next_key(value_bytes)

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
            # 优先按 msgpack dict 反序列化整行；失败则按原始 bytes 返回
            try:
                row = msgpack.unpackb(value, raw=False)
                if isinstance(row, dict):
                    all_results.append(row)
                    continue
            except Exception:
                pass
            all_results.append({column_value: value.decode()})

        return all_results
    
    def _execute_insert(self, stmt: InsertStatement) -> List[Dict[str, any]]:
        if not stmt.columns or not stmt.values:
            return []

        # key schema: "{table}:{first_column_value}"，与 SELECT WHERE id=... 读取路径对齐
        # 否则之前 INSERT 用 "{table}:{column_name}" 作 key、SELECT 用 "{table}:{id_value}" 作 key，
        # 两套 schema 完全错位 -> INSERT 写入的数据 SELECT 永远读不到。
        row_id = stmt.values[0]
        row_key = f"{stmt.table.lower()}:{row_id}".encode()

        leader_info = self._shard_cluster.get_leader_for_key(row_key)
        if leader_info is None:
            return []

        _, leader = leader_info

        # 整行序列化为 msgpack dict，列名统一小写以保持大小写无关
        row = {col.lower(): val for col, val in zip(stmt.columns, stmt.values)}
        value_bytes = msgpack.packb(row, use_bin_type=True)

        command = leader._state_machine.serialize_command(
            CommandType.SET,
            key=row_key,
            value=value_bytes,
            timestamp=int(time.time() * 1000000),
        )
        leader.propose(command)

        return [{"result": "inserted"}]
