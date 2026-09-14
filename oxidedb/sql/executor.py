import heapq
import time
from typing import Dict, List, Optional, Tuple

import msgpack

from ..client.node_client import LocalNodeClientFactory, NodeClientFactory
from ..client.routing import ShardLeaders
from ..raft.state_machine import CommandType, ErrorCode, ScanRefused, serialize_command
from ..storage.engine import next_key
from .parser import SQLParser, SelectStatement, InsertStatement


class SQLExecutor:
    def __init__(self, shard_cluster, factory: Optional[NodeClientFactory] = None):
        self._shard_cluster = shard_cluster
        #: One factory for the whole executor: a scan of every shard asks it for a
        #: client per replica, and building one per range read would be building one
        #: per key of the range in any implementation where a handle is a connection.
        self._factory = factory if factory is not None else LocalNodeClientFactory(shard_cluster)
        self._leaders = ShardLeaders(shard_cluster, factory=self._factory)
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

            client = self._leaders.leader_for_key(key)
            if client is None:
                return []

            result = client.get(key)

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

        for key, value in self._rows_from_every_shard(b"", b"\xff"):
            if key.decode().startswith(f"{stmt.table}:"):
                _, column_value = key.decode().split(":", 1)
                all_results.append({column_value: value.decode()})

        return all_results

    def _rows_from_every_shard(self, start_key: bytes,
                               end_key: bytes) -> List[Tuple[bytes, bytes]]:
        """Every row in the range, from whichever replica of each shard leads it.

        A range read goes through a client, and a replica that is not its shard's
        leader refuses the read instead of answering it - which is what makes it safe
        to ask every replica and keep the answers: a follower's rows are rows that
        shard has not promised, and a follower says so rather than handing them over.
        A refusal that is not "not the leader" - a lock in the way - is raised, since
        for that one there is nothing to keep and nobody else to ask.
        """
        rows: List[Tuple[bytes, bytes]] = []

        for shard_id in self._shard_cluster.shard_ids():
            for node_id in self._shard_cluster.shard_replica_ids(shard_id):
                client = self._factory.get_client(shard_id, node_id)
                if client is None:
                    continue
                try:
                    rows.extend(client.scan(start_key, end_key))
                except ScanRefused as refusal:
                    if refusal.error_code == ErrorCode.ERR_NOT_LEADER:
                        continue
                    raise

        return rows
    
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

        for key, value in self._rows_from_every_shard(start_key, end_key):
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

        client = self._leaders.leader_for_key(row_key)
        if client is None:
            return []

        # 整行序列化为 msgpack dict，列名统一小写以保持大小写无关
        row = {col.lower(): val for col, val in zip(stmt.columns, stmt.values)}
        value_bytes = msgpack.packb(row, use_bin_type=True)

        command = serialize_command(
            CommandType.SET,
            key=row_key,
            value=value_bytes,
            timestamp=int(time.time() * 1000000),
        )
        client.propose(command)

        return [{"result": "inserted"}]
