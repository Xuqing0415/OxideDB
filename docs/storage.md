# Storage engines (plan E)

> Moved out of the README, which links here instead.  The text is the README's, with the
> references between these notes updated to name the file each one now lives in, and each
> heading moved up a level.

Persistence is delegated to a pluggable engine rather than hand-rolled.  All the
database keeps is the *encoding* of its logical keyspaces; the engine guarantees
durability and byte-ordered iteration.  The contract is deliberately small:
`put`, `get`, `delete`, `put_batch`, `delete_range`, `scan([start, end))`,
`flush`, `close`, with keys ordered by plain `memcmp`.

| Engine | Durability | Use |
| --- | --- | --- |
| `MemoryEngine` | none | embedded clusters, tests |
| `SQLiteEngine` | WAL, crash-safe | durable single-node and `data_dir`-backed clusters |

`SQLiteEngine` uses only the standard library and stores keys as `BLOB`s in a
`WITHOUT ROWID` table, so the primary key *is* the clustered index and a range
scan is a B-tree walk in exactly the order the callers expect.  Swapping in
RocksDB or LMDB later means implementing one class; no caller changes.

## Keyspace layout

Because timestamps are appended as big-endian suffixes, "all versions of a key,
oldest first" is a contiguous range scan, and a read at `t` is that range
truncated at `t`:

```
MVCC versions    \x01 || key || \x00 || ts(8B BE)          -> flag(1B) || value
write record     \x03 || key || \x00 || commit_ts(8B BE)   -> start_ts(8B BE)
lock record      \x04 || key || \x00 || start_ts(8B BE)    -> msgpack{status,
                                                              primary_key,
                                                              lock_time, value}

Raft log entry   \x01 || index(8B BE)                      -> msgpack{term, command}
Raft meta        \x02                                      -> msgpack{current_term, voted_for}
Raft commit      \x03                                      -> commit_index(8B BE)
Raft snapshot    \x04                                      -> msgpack{index, term, data}
```

The snapshot record carries its own `index`/`term`, so it is written before the
log prefix it covers is dropped: a crash in between leaves entries that replay
skips (they are at or below the snapshot index) rather than state with no entry
to rebuild it from.

`\x02` in the MVCC keyspace used to hold a bare write intent.  A lock record
carries the same value plus the primary key, the status and the TTL, so the
intent namespace was retired and `set_write_intent` is now a thin wrapper over
`put_lock`.

The MVCC namespaces and the Raft namespaces never meet: they live in separate
engine instances (`<data_dir>/data.sqlite3`, `<data_dir>/raft.sqlite3`).

`\x00` is the field separator inside the MVCC keyspace, so **user keys must not
contain `0x00`** (the same constraint TiKV-style encoders impose).  It is
validated on the way in rather than silently corrupting the index; use
`oxidedb.storage.next_key` to build exclusive range bounds instead of appending
`b"\x00"`.

```python
from oxidedb.raft.node import RaftCluster
from oxidedb.raft.state_machine import MVCCStateMachine
from oxidedb.raft.storage import EngineRaftStorage

def storage_factory(node_id):
    return EngineRaftStorage(data_dir=f"/var/lib/oxidedb/node{node_id}")

cluster = RaftCluster(num_nodes=3)
cluster.start(lambda: MVCCStateMachine(), storage_factory)
```

For a single process holding the whole keyspace on disk:

```python
from oxidedb.storage.engine import SQLiteEngine
from oxidedb.storage.mvcc import MVCCStorage

store = MVCCStorage(SQLiteEngine("/var/lib/oxidedb/data.sqlite3"))
store.set(b"user:1", b"alice", timestamp=1)
assert store.get(b"user:1", timestamp=2) == b"alice"
store.close()
```

Omitting the engine keeps the original in-memory behaviour, which is what the
embedded tests rely on.
