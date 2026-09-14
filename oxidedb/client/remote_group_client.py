"""The two groups that are not a shard, reached over a channel.

A client in another process has no node objects to ask, so the routing table and the clock
are reached the way a shard is: an address, a stub, and an answer that may be a refusal.
What is different is that neither group is placed anywhere a client can look up - there is
no table of tables - so a client is given seed addresses and walks them: ask one, follow the
leader it names, and move to the next when an address does not answer at all.  Both groups
are walked the same way, which is why they share this file, and the walk is written once so
that a second implementation cannot walk them two ways.

What a refusal means here is what it means on a shard's port: a caller told NOT_LEADER has
somewhere to go, and a caller told REFUSED does not.  An address that answers nothing is
``NodeUnreachable``, which is not a refusal and not a reason to stop - the walk has the other
seeds for that, and only when none of them answers does it become the caller's problem.

The table's client keeps the table the way its in-process twin does, and rebuilds it through
the table's own parse: what arrives on the wire is turned back into the payload
``_parse_table`` reads, so a record and a table field cannot come to mean two things in two
parsers.
"""

import threading
from typing import Any, Dict, List, Optional, Sequence

import grpc

from oxidedb.proto import groups_pb2
from oxidedb.proto.groups_pb2_grpc import MetadataServiceStub, TSOServiceStub

from ..channels import ChannelPool, DEFAULT_TIMEOUT
from ..metadata.service import RoutingTable, ShardPlacement, _parse_table
from ..tso.tso import DEFAULT_BATCH_SIZE
from .node_client import NodeUnreachable


class _GroupClient:
    """The seeds, the channels and the walk both group clients do.

    One address is asked at a time: the one that last answered, then the leader it named,
    then the seeds in the order they were given.  The last one to answer is remembered
    because an election moves a group once and a client that re-walked its seeds per call
    would pay for the walk every time; an address that has stopped working is skipped by
    the call that fails on it, not by a health check nobody asked for.

    A refusal that names a leader is followed inside the walk, because that is what the
    name is for.  A refusal that names nobody, or names somebody who does not answer
    either, is handed back: it is an answer about the group, and what to do about it is the
    caller's business.  Nothing answering at all is ``NodeUnreachable``.

    The channels are handed in when a caller already has a pool - a factory that hands out
    shard handles opens channels of its own, and two pools would be two places a socket can
    be leaked from - and a client that is handed none opens one it closes itself.
    """

    def __init__(self, seeds: Sequence[str],
                 channels: Optional[ChannelPool] = None,
                 timeout: float = DEFAULT_TIMEOUT):
        if not seeds:
            raise ValueError("a group client needs at least one address to ask")
        self._seeds = list(seeds)
        self._channels = channels if channels is not None else ChannelPool()
        self._owns_channels = channels is None
        self._timeout = timeout
        self._lock = threading.RLock()
        self._preferred: Optional[str] = None

    @property
    def seeds(self) -> List[str]:
        """The addresses this client was given, in the order it walks them."""
        return list(self._seeds)

    def close(self) -> None:
        """Let the channels go, if this client opened them.  A closed client is not usable.

        A client built over somebody else's pool closes nothing: the pool outlives it, and
        closing a channel another client is still using is a way to break that client.
        """
        if self._owns_channels:
            self._channels.close()

    # -- the walk ----------------------------------------------------------

    def _ask(self, stub_type, method_name: str, request):
        """One call, at whichever address answers it.  Raises if none of them does.

        The answer is the first one that is not a refusal, and a refusal is the last one
        seen if no address answered at all - which is what a group with no leader looks
        like from outside: every member says ask the leader, and none of them is it.
        """
        answered = None
        unanswered: List[str] = []
        pending = self._candidates()
        asked = set()
        while pending:
            address = pending.pop(0)
            if address in asked:
                continue
            asked.add(address)
            try:
                stub = stub_type(self._channels.channel(address))
                response = getattr(stub, method_name)(request, timeout=self._timeout)
            except grpc.RpcError:
                # No answer from there.  Which is what the next seed is for, and what
                # the caller hears about only if no seed answers.
                unanswered.append(address)
                continue
            if response.error_code == groups_pb2.OK:
                with self._lock:
                    self._preferred = address
                return response
            answered = response
            hint = response.leader_address if response.HasField("leader_address") else None
            if response.error_code == groups_pb2.NOT_LEADER and hint and hint not in asked:
                pending.insert(0, hint)

        if answered is not None:
            return answered
        raise NodeUnreachable(
            "no address answered: " + (", ".join(unanswered) or ", ".join(self._seeds)))

    def _candidates(self) -> List[str]:
        """Where the group might be, the address that answered last first."""
        with self._lock:
            preferred = self._preferred
        ordered: List[str] = []
        for address in ([preferred] if preferred else []) + self._seeds:
            if address not in ordered:
                ordered.append(address)
        return ordered


def _payload(response) -> Dict[str, Any]:
    """A ``ListShardsResponse`` as the payload ``_parse_table`` reads.

    The same shape the group packs for its own snapshot, field for field, so that the table
    has one parser rather than one for a snapshot and one for a socket.  The addresses come
    back as a map and are turned into the sorted pairs the state machine writes, which is
    what the parser takes.
    """
    return {
        "version": response.version,
        "shards": [
            {
                "shard_id": record.shard_id,
                "start": record.start_key,
                "end": record.end_key,
                "nodes": list(record.nodes),
                "addresses": sorted(record.addresses.items()),
                "leader_id": record.leader_id if record.HasField("leader_id") else None,
                "leader_term": record.leader_term,
            }
            for record in response.shards
        ],
    }


class RemoteMetadataClient(_GroupClient):
    """The routing table, read from whichever node of its group answers.

    Reads only, and that is deliberate: a client routes by the table, and what writes the
    table is the cluster.  The table is cached the way its in-process twin caches it, so a
    lookup costs nothing until a refresh and ``table`` reads it the first time.

    What it does not have is a refresh loop.  Its in-process twin is held by a caller that
    already knows when to re-read - a shard refused this client, so the placement is in
    doubt - and this one is read by the caller on the same terms.
    """

    def __init__(self, seeds: Sequence[str],
                 channels: Optional[ChannelPool] = None,
                 timeout: float = DEFAULT_TIMEOUT):
        super().__init__(seeds, channels, timeout)
        self._table: Optional[RoutingTable] = None

    def refresh(self) -> RoutingTable:
        """Read the whole table from the group, and cache it."""
        response = self._ask(MetadataServiceStub, "ListShards",
                             groups_pb2.ListShardsRequest())
        if response.error_code != groups_pb2.OK:
            raise RuntimeError(f"the routing table cannot be read: {response.message}")

        table = _parse_table(_payload(response))
        with self._lock:
            self._table = table
        return table

    def table(self, refresh: bool = False) -> RoutingTable:
        """The cached table, reading it the first time and when asked to."""
        with self._lock:
            cached = self._table
        if cached is not None and not refresh:
            return cached
        return self.refresh()

    def get_shard_for(self, key: bytes) -> int:
        """Which shard owns ``key``, from the cached table."""
        return self.table().route_for(key)

    def get_shard_leader(self, shard_id: int) -> Optional[int]:
        """Which node the table names as ``shard_id``'s leader, if it names one."""
        placement = self.table().shard(shard_id)
        return None if placement is None else placement.leader_id

    def get_leader_for_key(self, key: bytes) -> Optional[int]:
        placement = self.table().shard_for(key)
        return None if placement is None else placement.leader_id

    def list_shards(self) -> List[ShardPlacement]:
        """Every shard in the table, for a caller that wants all of it."""
        return list(self.table().shards.values())


class RemoteTSOClient(_GroupClient):
    """The clock, and the run of timestamps it handed out last, kept for the next call.

    A timestamp costs a Raft entry, so the group allocates them in runs and this consumes
    one run at a time - the same bargain its in-process twin makes, with the run arriving
    in a message.  Runs are what ``GetTimestampBatch`` is for, which is why this asks for
    one rather than reading single timestamps that each cost a proposal.

    Both ends of a run come back inclusive, exactly as the allocate command answers them,
    and are kept as the half-open range this client hands out of: ``_local_end`` is one
    past the last number it may use, so a run of ``[1, 1000]`` is offered as 1 through
    1000 and then refilled.  Getting the off-by-one wrong drops a timestamp per run, and
    the timestamps that are dropped are the ones a transaction would have committed at.
    """

    def __init__(self, seeds: Sequence[str],
                 channels: Optional[ChannelPool] = None,
                 timeout: float = DEFAULT_TIMEOUT,
                 batch_size: int = DEFAULT_BATCH_SIZE):
        super().__init__(seeds, channels, timeout)
        self._batch_size = batch_size
        self._local_start = 0
        self._local_end = 0

    def get_timestamp(self) -> int:
        """One timestamp, taking a run from the group when the last one is used up."""
        with self._lock:
            if self._local_start >= self._local_end:
                self._fetch_batch()
            timestamp = self._local_start
            self._local_start += 1
            return timestamp

    def batch_get_timestamps(self, count: int) -> List[int]:
        """``count`` timestamps, in one increasing run, out of whatever is in hand."""
        return [self.get_timestamp() for _ in range(count)]

    def _fetch_batch(self) -> None:
        """Take one run from the group, as the half-open range this client hands out."""
        response = self._ask(TSOServiceStub, "GetTimestampBatch",
                             groups_pb2.GetTimestampBatchRequest(count=self._batch_size))
        if response.error_code != groups_pb2.OK:
            raise RuntimeError(f"the clock cannot allocate a timestamp: {response.message}")

        start = response.start_ts if response.HasField("start_ts") else 0
        end = response.end_ts if response.HasField("end_ts") else 0
        self._local_start = start
        self._local_end = end + 1

