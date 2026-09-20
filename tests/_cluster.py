"""Node processes of a cluster, started and stopped by a test.

Every other test builds its cluster out of node objects inside the test process: that is
what ``RaftCluster`` and ``ShardedRaftCluster`` are for, and it is the right way to test
a node.  What that cannot test is the half of the code which only exists between a client
and a node - serialisation, a socket, a leader hint arriving from somewhere else - because
none of it happens when both ends are objects in one interpreter.

This is the scaffolding for that other kind of test.  ``start_cluster`` runs real
``python -m oxidedb.launcher`` processes, waits for each of them to say ``READY``, and hands
back the addresses they listen on.  Leaving the ``with`` block asks each of them to stop
and fails if one of them left without saying ``STOPPED``.

Three things here are deliberate:

* nothing waits for a number of seconds.  A node says ``READY`` once every port is bound and
  ``STOPPED`` once nothing is listening, and those two lines are what this waits for: a fixed
  wait would be a guess about how loaded the machine is;
* every port comes from ``oxidedb.launcher.ports_for``, the same function a node uses to work
  out where its own shards, metadata group and TSO group listen.  A second copy of that
  arithmetic here would be a second answer, and the two would drift apart;
* a node's stdout is drained on a thread rather than read where it is waited for.  A pipe
  has a fixed size, and a node whose output nobody reads blocks on its next print - which
  would be the ``READY`` line being waited for.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from typing import Dict, List, Optional, Sequence

from _ports import allocate_port
from oxidedb.launcher import (DEFAULT_HOST, DEFAULT_NUM_SHARDS, READY, STOPPED,
                              ClusterConfig, Peer, block_width, ports_for)
from oxidedb.raft.state_machine import ApplyResult, ErrorCode, ReadResult

#: Where the package under test is.  A node process is started from here, so the package it
#: imports is this working tree rather than whatever a stray install put on its path.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: How long the whole cluster is given to say ``READY``, in seconds.  Shared rather than
#: per node: a cluster whose nodes are still coming up is one cluster coming up, and a node
#: that needs twenty seconds of its own is a node that is not going to make it.
READY_TIMEOUT = 20.0

#: How long one node is given to stop after being asked, in seconds.
STOP_TIMEOUT = 10.0

#: How long the reader thread is given to see the last line a node printed, in seconds.
READER_TIMEOUT = 2.0

#: How long a client keeps asking a shard that has no leader yet, in seconds.
CLIENT_TIMEOUT = 20.0

#: How long to wait between two asks, in seconds.  Short, because the wait is for an
#: election, which is fast when it happens at all.
CLIENT_INTERVAL = 0.05


class NodeProcess:
    """One node's process, the command line it was given, and everything it has said."""

    def __init__(self, config: ClusterConfig, process: subprocess.Popen,
                 stderr_path: str, stderr_file) -> None:
        self.config = config
        self._process = process
        self._stderr_path = stderr_path
        self._stderr_file = stderr_file
        self._lines: List[str] = []
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._stopped = threading.Event()
        self._reader = threading.Thread(target=self._read_stdout,
                                        name=f"node-{config.node_id}-stdout", daemon=True)
        self._reader.start()

    # -- what it was told ---------------------------------------------------

    @property
    def node_id(self) -> int:
        return self.config.node_id

    @property
    def address(self) -> str:
        """Where this node serves its shard 0: the address its other ports derive from."""
        return self.config.address

    @property
    def data_dir(self) -> str:
        return str(self.config.data_dir)

    @property
    def ports(self):
        """Every port this node binds, by the arithmetic this node itself used."""
        return ports_for(self.config.port)

    # -- what it has said ---------------------------------------------------

    def _read_stdout(self) -> None:
        """Collect every line this node prints, and notice the two that are a contract."""
        assert self._process.stdout is not None
        for line in self._process.stdout:
            text = line.strip()
            with self._lock:
                self._lines.append(text)
            if text.startswith(READY):
                self._ready.set()
            elif text == STOPPED:
                self._stopped.set()

    @property
    def lines(self) -> List[str]:
        """Everything this node has printed so far, oldest first."""
        with self._lock:
            return list(self._lines)

    @property
    def output(self) -> str:
        return "\n".join(self.lines)

    @property
    def stderr(self) -> str:
        """What this node wrote to stderr, read from its file so that it survives a crash."""
        try:
            self._stderr_file.flush()
            with open(self._stderr_path, "r", encoding="utf-8", errors="replace") as handle:
                return handle.read()
        except (OSError, ValueError):
            return ""

    @property
    def process(self) -> subprocess.Popen:
        """The process itself, for a test that wants to wait on it or read its exit code."""
        return self._process

    @property
    def running(self) -> bool:
        return self._process.poll() is None

    @property
    def returncode(self) -> Optional[int]:
        return self._process.poll()

    @property
    def stopped_cleanly(self) -> bool:
        """Whether this node said ``STOPPED``, which is the only proof it stopped itself."""
        return self._stopped.is_set()

    def report(self) -> str:
        """Everything this node has said, for a failure that has to explain itself."""
        return (f"node {self.node_id} at {self.address} (exit {self.returncode})\n"
                f"  stdout:\n{_indent(self.output)}\n"
                f"  stderr:\n{_indent(self.stderr)}")

    # -- the two things a test does to it -----------------------------------

    def wait_until_ready(self, timeout: float) -> None:
        """Block until this node has said ``READY``, or fail with everything it said."""
        waited = max(0.0, timeout)
        if self._ready.wait(waited):
            return
        raise AssertionError(
            f"node {self.node_id} said no READY in the {waited:.1f}s it had left:\n"
            f"{self.report()}")

    def ask_to_stop(self) -> None:
        """Ask this node to stop, without waiting for it to have done so.

        The ask is a line on stdin, because on Windows one process cannot send another a
        signal: ``terminate`` ends a process without running any of its own code, so the pipe
        it was started with is the only channel a graceful stop has.
        """
        if not self.running:
            return
        try:
            assert self._process.stdin is not None
            self._process.stdin.write("stop\n")
            self._process.stdin.flush()
        except (OSError, ValueError):
            # The node has already gone, or its pipe is not there: nothing left to ask.
            pass

    def wait_for_stop(self, timeout: float = STOP_TIMEOUT) -> bool:
        """Wait for this node to go.  True if it said ``STOPPED`` before it did."""
        try:
            self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # It was asked nicely and did not go: take it down, and report that it was not
            # a graceful stop by returning what it did say, which is not STOPPED.
            self.kill()
            return False
        self._close()
        return self.stopped_cleanly

    def kill(self) -> None:
        """Take this node down without asking, for cleanup after a failure."""
        if self.running:
            self._process.kill()
            try:
                self._process.wait(timeout=STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                pass
        self._close()

    def _close(self) -> None:
        """Let go of the pipes, once the reader has seen everything that was printed."""
        self._reader.join(timeout=READER_TIMEOUT)
        for handle in (self._process.stdout, self._process.stdin):
            if handle is not None:
                try:
                    handle.close()
                except OSError:
                    pass
        try:
            self._stderr_file.close()
        except OSError:
            pass


class Cluster:
    """The nodes ``start_cluster`` started, and everything they were given.

    Held as a context manager, because the interesting part is the way out: leaving it
    stops every node and checks that each of them stopped itself.
    """

    def __init__(self, nodes: Sequence[NodeProcess], num_shards: int, root: str,
                 owned: bool) -> None:
        self._nodes = list(nodes)
        self.num_shards = num_shards
        self._root = root
        self._owned = owned

    # -- what a test asks it ------------------------------------------------

    @property
    def nodes(self) -> List[NodeProcess]:
        """The nodes, in node id order."""
        return list(self._nodes)

    @property
    def data_dirs(self) -> Dict[int, str]:
        """``{node_id: directory}``, which is where a node's logs and rows are."""
        return {node.node_id: node.data_dir for node in self._nodes}

    @property
    def bootstrap_address(self) -> str:
        """Node 1's shard 0: the address a client starts from.

        Any node would do - each of them serves the same shards and can be asked for the
        routing table - and node 1 is the one every process was told about, so it is the
        one that is certainly there.
        """
        return self._nodes[0].address

    @property
    def processes(self) -> List[subprocess.Popen]:
        """The node processes themselves, in node id order."""
        return [node.process for node in self._nodes]

    def node(self, node_id: int) -> NodeProcess:
        """The node with this id, or a KeyError naming what this cluster has."""
        for node in self._nodes:
            if node.node_id == node_id:
                return node
        raise KeyError(f"no node {node_id} in this cluster, which has "
                       f"{[node.node_id for node in self._nodes]}")

    def address(self, node_id: int) -> str:
        """Where this node serves its shard 0."""
        return self.node(node_id).address

    def shard_address(self, shard_id: int, node_id: int = 1) -> str:
        """Where ``node_id`` serves ``shard_id``, by that node's own arithmetic.

        Defaults to node 1 because that is the node whose base port every other node was
        told, so it is the one an address can be worked out for without asking the routing
        table - which is not something a test has before it has a client.
        """
        return self.node(node_id).config.shard_address(shard_id)

    def metadata_address(self, node_id: int = 1) -> str:
        """Where ``node_id``'s member of the routing table's group listens."""
        return self.node(node_id).config.metadata_address()

    def tso_address(self, node_id: int = 1) -> str:
        """Where ``node_id``'s member of the timestamp group listens."""
        return self.node(node_id).config.tso_address()

    @property
    def metadata_seeds(self) -> List[str]:
        """Every node's table port, in node id order: what a client outside is seeded with.

        All of them rather than one, because no client knows which member leads, and a
        client that is given one address it cannot use has no way to find another.
        """
        return [self.metadata_address(node.node_id) for node in self._nodes]

    @property
    def tso_seeds(self) -> List[str]:
        """Every node's timestamp port, in node id order, on the same terms."""
        return [self.tso_address(node.node_id) for node in self._nodes]

    # -- the way out --------------------------------------------------------

    def stop(self) -> None:
        """Ask every node to stop, then fail if one of them left without saying STOPPED.

        Asked all at once and only then waited for, so that the cluster stops in about the
        time one node takes rather than the time all of them take together.

        A node that exits without that line stopped for a reason nobody asked about - a
        crash, or a shutdown path that did not finish - and a test that took that for an
        ordinary ending would hide the failure it exists to catch.
        """
        for node in self._nodes:
            node.ask_to_stop()
        for node in self._nodes:
            node.wait_for_stop()

        unwanted = [node for node in self._nodes if not node.stopped_cleanly]
        if unwanted:
            raise AssertionError(
                "these nodes stopped without saying STOPPED:\n\n"
                + "\n\n".join(node.report() for node in unwanted))

    def kill(self) -> None:
        """Take every node down without asking, for cleanup when a test has failed."""
        for node in self._nodes:
            node.kill()

    def _discard_data(self) -> None:
        """Remove the directory this object made, and leave one it was handed alone."""
        if self._owned:
            shutil.rmtree(self._root, ignore_errors=True)

    def __enter__(self) -> "Cluster":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if exc_type is not None:
            # The test has already failed, and its failure is the one worth reporting: the
            # nodes are taken down without waiting for a graceful stop, which would only
            # produce a second, less interesting error.
            self.kill()
            self._discard_data()
            return False
        try:
            self.stop()
        finally:
            self._discard_data()
        return False


def start_cluster(num_nodes: int = 1, num_shards: int = DEFAULT_NUM_SHARDS,
                  base_dir: Optional[str] = None,
                  host: str = DEFAULT_HOST, bootstrap: bool = True) -> Cluster:
    """Start ``num_nodes`` node processes and return them once each has said ``READY``.

    ``base_dir`` is where the nodes keep their logs and rows.  Given none, a directory is
    made under the system's temporary directory and removed again on the way out; a test
    that wants to look at what a node wrote, or to restart one, passes its own.

    READY is a promise about ports, not about elections: it is printed once every port is
    bound and the groups have begun electing, so a first write can arrive before a shard has
    a leader and come back NOT_LEADER.  A test that writes should retry that answer, which is
    what a real client does with it as well.

    Bootstrap is on for every node unless it is turned off here.  The first node to reach
    the metadata group installs the starting ranges and the others are refused, which is
    the ordinary case rather than an error - so no node has to know whether it started
    first.  ``bootstrap=False`` starts nodes that publish nothing, for a test about a
    node whose table somebody else is keeping.
    """
    if num_nodes < 1:
        raise ValueError(f"a cluster has at least one node, not {num_nodes}")

    root = base_dir if base_dir is not None else tempfile.mkdtemp(prefix="oxidedb-cluster-")
    owned = base_dir is None
    os.makedirs(root, exist_ok=True)

    # One block per node, wide enough that no node's shards, metadata port or TSO port can
    # land on another node's: the base ports below are what the nodes themselves derive
    # their other ports from, so they have to be a whole block apart - and a block is the
    # same width whatever the nodes were told to serve.
    bases = [allocate_port(span=block_width()) for _ in range(num_nodes)]
    configs = [_config(node_id, bases, num_shards, host, root, bootstrap)
               for node_id in range(1, num_nodes + 1)]

    nodes: List[NodeProcess] = []
    try:
        for config in configs:
            nodes.append(_spawn(config, root))
        deadline = time.monotonic() + READY_TIMEOUT
        for node in nodes:
            node.wait_until_ready(deadline - time.monotonic())
    except BaseException:
        for node in nodes:
            node.kill()
        if owned:
            shutil.rmtree(root, ignore_errors=True)
        raise

    return Cluster(nodes, num_shards, root, owned)


def _config(node_id: int, bases: Sequence[int], num_shards: int, host: str,
            root: str, bootstrap: bool = True) -> ClusterConfig:
    """One node's configuration: what it would have been handed on a command line."""
    peers = tuple(Peer(other_id, host, bases[other_id - 1])
                  for other_id in range(1, len(bases) + 1) if other_id != node_id)
    return ClusterConfig(node_id=node_id, port=bases[node_id - 1], host=host, peers=peers,
                         data_dir=os.path.join(root, f"node{node_id}"),
                         num_shards=num_shards, bootstrap=bootstrap)


def _command(config: ClusterConfig) -> List[str]:
    """The command line that runs this node, spelled as the launcher's parser reads it."""
    command = [sys.executable, "-m", "oxidedb.launcher",
               "--node-id", str(config.node_id),
               "--host", config.host,
               "--port", str(config.port),
               "--num-shards", str(config.num_shards),
               "--data-dir", str(config.data_dir)]
    if not config.bootstrap:
        command.append("--no-bootstrap")
    if config.peers:
        command += ["--peers", ",".join(peer.text for peer in config.peers)]
    return command


def _spawn(config: ClusterConfig, root: str) -> NodeProcess:
    """Run one node's process, with its output going where a failure can read it."""
    stderr_path = os.path.join(root, f"node{config.node_id}.stderr.log")
    stderr_file = open(stderr_path, "wb")
    environment = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1")
    try:
        process = subprocess.Popen(
            _command(config),
            cwd=REPO_ROOT,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr_file,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
    except BaseException:
        stderr_file.close()
        raise
    return NodeProcess(config, process, stderr_path, stderr_file)


def _indent(text: str) -> str:
    lines = text.splitlines()
    return "\n".join(f"    {line}" for line in lines) if lines else "    (nothing)"
def write_when_ready(client, command: bytes, timeout: float = CLIENT_TIMEOUT) -> ApplyResult:
    """Propose ``command`` until a shard has a leader, and return the last answer.

    A node prints READY once its ports are bound and its groups have begun electing, so the
    first proposal of a test can arrive before there is a leader to take it.  The answer
    then is NOT_LEADER rather than a failure, and retrying it is what a client does with it
    - so a test that goes over the wire retries it too, rather than being a test about how
    fast one machine elects.
    """
    deadline = time.monotonic() + timeout
    while True:
        result = client.propose(command)
        if result.success or result.error_code != ErrorCode.ERR_NOT_LEADER:
            return result
        if time.monotonic() >= deadline:
            return result
        time.sleep(CLIENT_INTERVAL)


def read_when_ready(client, key: bytes, timeout: float = CLIENT_TIMEOUT) -> ReadResult:
    """Read ``key``, retrying the same NOT_LEADER a write would, and return the answer.

    Only that one answer is retried.  A key that is not there comes back as a successful
    read of nothing, and a test that treated that as "not ready yet" would pass by waiting.
    """
    deadline = time.monotonic() + timeout
    while True:
        result = client.get(key)
        if result.success or result.error_code != ErrorCode.ERR_NOT_LEADER:
            return result
        if time.monotonic() >= deadline:
            return result
        time.sleep(CLIENT_INTERVAL)

