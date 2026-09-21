# Consistency levels

> Moved out of the README, which links here instead.  The text is the README's, with the
> references between these notes updated to name the file each one now lives in, and each
> heading moved up a level.

A read is answered at one of three levels, named on `get` and `scan` with
`--consistency`; the level says which copy of a shard may answer and where the index the
answer is promised at comes from.  The definitions are the ones
`oxidedb/transaction/smart_client.py` writes - the flag's help prints the same three in
fewer words - so there is no third wording here to keep in step:

* **`strong`** (the default) - *a quorum: the leader confirms an index with one and
  answers at that index, so the basis is established after the read began and no answer
  older than it can be given.*
* **`follower`** - *that same basis, fetched by the node instead of by the caller: any
  member of the set answers, and the index it answers at is one the leader confirmed for
  it over the wire.*
* **`cached`** - *the client's own memory, and the one source that can be older than the
  read: any member answers at an index this client was given earlier.*

Which to use:

* `strong` when the answer has to contain everything committed before the read began - a
  read after a write in one session, or a decision taken from what was just written.
* `follower` to spread reads over a shard's replica set.  It costs the same quorum
  confirmation, and what moves is where the hop happens: on the node the caller chose, so
  many clients reading together are served by the set rather than by one node.
* `cached` for reads that repeat, where an answer as old as the window can be lived with:
  an index this client was already given is worth `READ_INDEX_TTL` (0.1s), and inside
  that window a read costs nothing beyond the read itself.

In code the level is an argument rather than a mode: `SmartClient.get(key, consistency=...)`
and the same on `scan`, or `read_index=` on the node primitives for a caller that already
has an index of its own to name.  Neither `follower` nor `cached` means anything without a
replica set to draw from: a client that has read a routing table has one, and a client
holding cluster objects in its own process does not - with no member to name, the level
costs what `strong` costs and spreads nothing.

**What `cached` costs.**  It is the only one of the three that can return data older than
the read.  The basis was confirmed before the read began, so the answer is as of
somewhere between now and the window ago, and a write committed inside that window may
not be in it; a member that has not applied the index yet is waited for rather than
refused, so reaching a lagging member makes this level slower than `follower` and not
only cheaper.  Ask for it when a run of reads can tolerate an answer that old - it is not
a level to switch on because it looks inexpensive.
