import re
from typing import Dict, List, Optional, Tuple


class SQLStatement:
    pass


class SelectStatement(SQLStatement):
    def __init__(self, table: str, columns: List[str], where_clause: Optional[str] = None):
        self.table = table
        self.columns = columns
        self.where_clause = where_clause
    
    def __repr__(self):
        return f"SelectStatement(table={self.table}, columns={self.columns}, where={self.where_clause})"


class InsertStatement(SQLStatement):
    def __init__(self, table: str, columns: List[str], values: List):
        self.table = table
        self.columns = columns
        self.values = values
    
    def __repr__(self):
        return f"InsertStatement(table={self.table}, columns={self.columns}, values={self.values})"


class SQLParser:
    def parse(self, sql: str) -> Optional[SQLStatement]:
        sql = sql.strip().upper()
        
        if sql.startswith("SELECT"):
            return self._parse_select(sql)
        elif sql.startswith("INSERT"):
            return self._parse_insert(sql)
        
        return None
    
    def _parse_select(self, sql: str) -> Optional[SelectStatement]:
        match = re.match(r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?", sql, re.DOTALL)
        if not match:
            return None
        
        columns_str, table, where_clause = match.groups()
        
        columns = [c.strip() for c in columns_str.split(",")]
        
        return SelectStatement(table=table, columns=columns, where_clause=where_clause)
    
    def _parse_insert(self, sql: str) -> Optional[InsertStatement]:
        match = re.match(r"INSERT\s+INTO\s+(\w+)\s*\((.+?)\)\s*VALUES\s*\((.+)\)", sql, re.DOTALL)
        if not match:
            return None
        
        table, columns_str, values_str = match.groups()
        
        columns = [c.strip() for c in columns_str.split(",")]
        
        values = []
        for v in values_str.split(","):
            v = v.strip()
            if v.startswith("'") and v.endswith("'"):
                values.append(v[1:-1])
            elif v.startswith('"') and v.endswith('"'):
                values.append(v[1:-1])
            elif v.isdigit():
                values.append(int(v))
            elif v.lower() == "null":
                values.append(None)
            else:
                values.append(v)
        
        return InsertStatement(table=table, columns=columns, values=values)
    
    def parse_where_condition(self, where_clause: str) -> Optional[Tuple[str, str, str]]:
        match = re.match(r"(\w+)\s*(=|>|<|>=|<=)\s*('.*?'|\d+)", where_clause)
        if not match:
            return None
        
        column, operator, value = match.groups()
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        elif value.isdigit():
            value = int(value)
        
        return (column, operator, value)