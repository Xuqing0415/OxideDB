# Design boundaries

> Moved out of the README, which links here instead.  The text is the README's
> `## Design boundaries`, carried into `docs/known-gaps.md` whole when the README was
> split, and left there without its heading; this file is that section with the heading
> back.

Where the design stops.  Nothing here is waiting to be done: each is a choice this
project makes, and what would move it is a different design rather than the next piece
of work.

* **No membership change.**  Cluster size is fixed at construction; there is no
  joint-consensus configuration change.
* **Serializable isolation is validated, not SSI.**  A transaction is refused when any
  key it read was committed over after its snapshot.  That prevents write skew, but it
  also refuses read-write overlaps that a conflict graph would allow, so it aborts more
  than SSI would.  The validation and the primary commit are ordered by one lock on the
  coordinator, which is what makes the check atomic with the commit - and bounds the
  guarantee to the transactions that commit through one coordinator; two committing at
  the same time would need the graph.  The read set covers keys, not ranges: `scan` is
  not part of the transaction path - it can be read at a timestamp, but it records
  nothing - so a phantom is not detected, and a transaction with a large read set pays
  one lookup per key with no batching or Bloom filter.
* **Timing is a thread per node, not an event loop.**  Elections and heartbeats
  are driven by one long-lived ticker thread per node (`MemoryRaftNode._tick`)
  and RPCs run on a small bounded pool.  That replaced a `threading.Timer`
  schedule that created 50-80 threads per second on an idle three-node cluster.
  An `asyncio` event loop per node is the intended end state.
* **`synchronous=NORMAL` trades power-loss durability for throughput.**  A
  process crash loses nothing, because every write is its own committed SQLite
  transaction, but a machine crash can drop the tail of the WAL because commits
  are not fsynced.  Use `SQLiteEngine(path, synchronous="FULL")` when that
  matters.
