> **发布说明（发布前删掉这一段）**
> 标题（后台标题栏）：收集了不检查，等于没做：一个躲过 96 个测试的 Raft 读 bug
> 摘要（分享卡片用，≤120 字）：一段收集了所有 AppendEntries 响应却从不检查它们的读路径，96 个测试全绿，直到一场网络分区让它给出了一个没有资格给出的答案。
> 封面建议：深色底 + 一行等宽字 response.success && response.term == current_term
> 本文按 Markdown 撰写，建议先粘到 mdnice（墨滴）之类的工具转换，再贴进公众号后台；直接粘进后台会原样保留 ` 反引号 ` 和 ``` 代码围栏。
> 为适配手机屏，时间线由对齐的 ASCII 图改成了列表（对齐图在手机上会被折行毁掉）；代码块仍可横向滑动。
> 正文第一行的一级标题，在后台单独填好标题后可删。

96 个测试全绿，读却是错的。这篇文章讲一个我自己写出来的 bug：一段收集了所有 RPC 响应、却从不检查它们的读路径，以及它如何在一场网络分区里，给出了一个没有资格给出的答案。

# 收集了不检查，等于没做：一个躲过 96 个测试的 Raft 读 bug

三个节点的 Raft 集群：选举正常、日志复制正常、96 个测试全绿。然后我把另外两个节点的服务端停掉，向留下来那个「领导者」发一次读：

```
propose committed: True
read with a healthy quorum: True b'v'
peer servers stopped; the isolated node still says: NodeState.LEADER
read with the quorum gone: success=False code=301 msg='ReadIndex failed: 1 acks, 2 required' (0.217s)
```

最后一行是修好之后的样子。修好之前，同样的场景会返回 `True b'v'`——一个它没有资格给出的答案。

这篇文章讲这个 bug：它为什么能躲过所有测试、为什么在分布式数据库里属于致命缺陷、以及正确的 ReadIndex 究竟在证明什么。

## 一、为什么「领导者读自己的状态机」是错的

Raft 里读操作比写操作更容易被想当然。写要走一次多数派复制，读看起来只是查本地状态机——领导者手里就有最新数据，为什么还要问别人？

因为下面三句话，没有一句是本地能自证的：

- 你可能已经不是领导者了。
- 你的 commit_index 可能不是真正的提交点。
- 你的状态机可能还没追上自己的 commit_index。

展开说：

1. **你可能已经不是领导者了。** 网络分区时，被切走的那一小撮节点里，老领导者不会自动降级——它只是收不到心跳回应，而 `state == LEADER` 这个字段是它自己上次写的，凭本地信息它无从知道多数派已经选出了新领导者。
2. **你的 commit_index 可能不是真正的提交点。** commit_index 只能通过「当前任期的条目被多数派确认」推进（Raft 5.4.2）。刚上任的领导者手里可能有一堆上个任期留下的条目，它不知道这些条目到底提交了没有。
3. **你的状态机可能还没追上自己的 commit_index。** 应用日志是异步的，`_last_applied < commit_index` 是完全正常的状态。

一个正确的线性一致读必须同时解决这三件事。Raft 论文给的方案就是 ReadIndex。

## 二、我写的那段代码

这是我最初的实现（`git show b519fd0^:oxidedb/raft/node.py`，修复前的真实代码）：

```python
def get(self, key: bytes) -> ReadResult:
    with self._lock:
        if self._state != NodeState.LEADER:
            return ReadResult.failure(ErrorCode.ERR_NOT_LEADER, "Not leader")

        read_index = self._commit_index
        current_term = self._current_term

        peers = list(self._peers)
        next_index = dict(self._next_index)
        log = list(self._log)
        leader_commit = self._commit_index

    if peers:
        responses = []
        for peer_id in peers:
            try:
                response = self._append_entries(peer_id, current_term, next_index.get(peer_id, 1), log, leader_commit)
                responses.append(response)
            except Exception:
                responses.append(None)

        with self._lock:
            if self._state != NodeState.LEADER or self._current_term != current_term:
                return ReadResult.failure(ErrorCode.ERR_NOT_LEADER, "Lost leadership during read")

    with self._lock:
        self._wait_for_apply(read_index)

        if self._state != NodeState.LEADER:
            return ReadResult.failure(ErrorCode.ERR_NOT_LEADER, "Not leader")

        return self._state_machine.get(key)
```

请盯着 `responses` 这个变量看三秒。

它被创建、被 `append`、然后……再也没有被读过一次。**这一段里唯一需要检查的东西，就是它。**

### 它为什么看起来是对的

这才是可怕的地方。这段代码做对了几乎所有「看起来像 ReadIndex」的事：

- 它确实向所有 peer 发了 AppendEntries（RPC 发出去了，代码看起来在通信）；
- 它确实在 RPC 之后重新检查了自己是不是还是领导者、任期有没有变（有并发意识）；
- 它确实调用了 `_wait_for_apply(read_index)`（考虑了本地应用延迟）；
- 它有完整的错误处理，`except` 分支把失败的响应记成 `None`（看起来考虑了失败）。

唯一缺的那一件事是：**没有人检查响应**。`acks` 没有统计，多数派没有判定，`responses` 收集完就扔了。

它甚至能通过 code review——reviewer 看到 `responses` 被 append，会默认「统计逻辑在下面某处」。而下面某处并没有。

把这段代码读一遍就能推断出后果：哪怕两个 peer 全都不可达，`responses` 里全是 `None`，它也不会因此返回失败——它会直接往下走，等本地 apply 追平，然后返回本地的值。**它没有任何一个时刻在验证「我还是多数派认可的领导者」。**

## 三、代价：这不是性能问题，是一致性问题

用具体的时序说明。三个节点 A、B、C，A 是领导者：

- **t0** — A 是领导者，写入 `k=v` 已提交（A、B、C 都有）
- **t1** — 网络分区：A 与 B、C 断开
- **t2** — B、C 收不到心跳，超时，B 当选新领导者（任期 +1），写入 `k=w` 并提交
- **t3** — A 仍然认为自己是领导者，收到客户端读 `k`
- **t4** — 旧实现：A 返回本地的 `v`（陈旧读，客户端无法察觉）；正确实现：A 拿不到多数派确认，拒绝这次读
- **t5** — A 恢复连接，收到更高任期，降级为 follower，日志向新领导者对齐（未提交的冲突后缀被截断，本地 `commit_index` 与 `last_applied` 随之收回）——但 t3 已经发给客户端的那个 `v`，收不回来了

关键在于**客户端无法分辨**。返回 `k=v` 时它是 `success=True`，和一次正常的读完全一样，没有任何字段告诉你「这个答案来自一个已经被取代的领导者」。而线性一致性要求：一次读至少要看到读开始之前已经提交的全部写入。B、C 那边 `k=w` 已经提交，这里却返回 `v`——同一个问题在两个时刻给出两个不同的答案，且中间没有任何写入发生。

注意 t3 到 t5 这段窗口：旧实现不是「错了一下」，而是**一直错到 A 恢复连接为止**。而窗口结束时，那个已经发出去的错答案依然躺在客户端的日志里。

分布式系统里「看起来成功但其实无权回答」是最危险的一类错误：它不会报错、不会超时、不会让测试变红——它只是悄悄给你一个错误的答案，然后你在几个月后从业务数据里发现它。

## 四、正确的 ReadIndex

修复后的实现，只保留与本文有关的三处（省略了快照相关的参数准备）：

```python
if peers:
    acks = 1  # our own acknowledgement
    for peer_id in peers:
        try:
            response = self._replicate(...)      # 参数省略
        except Exception:
            response = None

        if response is not None and response.success and response.term == current_term:
            acks += 1

    with self._lock:
        if self._state != NodeState.LEADER or self._current_term != current_term:
            return ReadResult.failure(ErrorCode.ERR_NOT_LEADER, "Lost leadership during read")

        majority = (len(self._peers) + 1) // 2 + 1
        if acks < majority:
            return ReadResult.failure(
                ErrorCode.ERR_NOT_LEADER,
                f"ReadIndex failed: {acks} acks, {majority} required",
            )

        self._update_commit_index()
        read_index = self._commit_index
```

三处关键改动，正好对应第一节的三个问题：

1. **统计 ACK 并判定多数派**（`acks`、`majority`）。这解决「我是不是还是领导者」。
2. **在拿到多数派之后重读 commit_index**（`_update_commit_index()`）。这解决「我的 commit_index 是不是真正的提交点」——刚才那些 ACK 可能把 `match_index` 往前推了，新的 commit_index 才是这次读应该用的位置。
3. **每条响应都要求 `response.term == current_term`**。任期更高的响应意味着集群里已经有人开始了新一轮选举，这时必须放弃。

最后仍然是 `_wait_for_apply(read_index)` 再读状态机，解决第三个问题。

（真实代码里 `_replicate` 的参数包含 `log_base`、`last_log_index`、`log_base_term` 这些快照相关的量，与本文无关，已在上面省略。）

### 为什么多数派的 ACK 就够了

这是 ReadIndex 最漂亮的一步推理，值得展开：

- **两个多数派必然相交。** 这是纯粹的集合事实：任何两个多数派集合的交集非空。选举需要多数派投票，提交也需要多数派保存，所以「确认到多数派」这件事本身就意味着你的集合里包含了某些关键节点。
- **一个节点一旦在更大任期投过票，就绝不可能再支持任期 T。** 所以如果某个节点在更高任期投过票，它给我们的回复里带的是更高的任期，我们收到的是 `term > T` 而不是 ACK——代码里 `response.term == current_term` 检查的正是这件事。因此，拿到当前任期 T 的多数派 ACK，就证明了**此刻不存在比 T 更大的任期**：你没有被罢免。
- **提交点有了下界。** 提交需要多数派保存条目，而上一条确认到的多数派与任何「已经提交的多数派」相交，所以这个多数派里至少有一个节点持有**此前所有已提交的条目**。因此领导者此时的 commit_index，一定不小于「这次读开始之前已经被提交的任何位置」。

两条合起来：多数派 ACK + 重新读取的 commit_index，给出的是一个**有资格提供线性一致读的位置**。然后再等本地 apply 追平，就可以放心读状态机了。这也是为什么那两步的**先后顺序**不能换：先确认领导权，再取提交点。

到这里 ReadIndex 就完整了。但还有一个必须同时处理的姊妹问题：一个刚上任的领导者，它的 commit_index 可能永远推不动。

## 五、姊妹坑：新领导者为什么必须写一条空日志

这一节和 ReadIndex 是同一类问题——**新任期不能凭旧信息下结论**——所以我把它们放在一起修。

Raft 5.4.2 规定：领导者只能通过统计副本数来提交**当前任期**的条目。为什么？因为一个刚上任的领导者无法判断「上个任期的某个条目到底提交了没有」——它可能被复制到了多数派，也可能没有。数人头只在当前任期有意义。

后果是：如果新领导者上任后不写入任何东西，它的 commit_index 就永远停在旧位置，`_update_commit_index()` 也推不动它。**集群看起来选出了领导者，却读不到任何东西。**

标准解法（Raft 论文第 8 节）：新领导者上任时立刻追加一条**空条目**（no-op），提交它。之后 commit_index 就能继续推进，读也就能做了。这个项目里它就是 `NOOP_COMMAND = b""`：

```python
def _append_noop_entry(self) -> None:
    """Append the current term's empty entry, as Raft 8 requires.  ..."""
    entry = LogEntry(term=self._current_term, index=self._last_log_index() + 1, command=NOOP_COMMAND)
    self._log.append(entry)
    self._save_log_entry(entry)
    self._update_commit_index()
```

它不携带任何业务数据，唯一的作用是**让新任期有一个能提交的条目**。我在同一次修复里加上它之后，「重启整个集群后读不到最后几条写入」的问题也一起消失了。

## 六、怎么让这种 bug 不再回来

一个断言 `assert read.success` 的测试永远发现不了这个 bug——它测的是「读能不能成功」，而这个 bug 的表现恰恰是**太成功了**。

换句话说，测试要断言的是「什么情况下读必须失败」。这是现在仓库里的回归测试（`tests/test_durability.py`）：

```python
def test_read_is_refused_when_leader_loses_quorum(self):
    cluster = RaftCluster(num_nodes=3)
    cluster.start(lambda: MVCCStateMachine())
    try:
        leader = _wait_for_leader(cluster)
        assert leader.propose(_set_command(leader._state_machine, b"k", b"v", 1)).success
        assert leader.get(b"k").value == b"v"

        # Cut the leader off from both peers.  It still believes it leads,
        # which is exactly when a read must be refused instead of served.
        leader._get_peer_node = lambda _peer_id: None

        read = leader.get(b"k")
        assert not read.success
        assert read.error_code == ErrorCode.ERR_NOT_LEADER
    finally:
        cluster.shutdown()
```

先断言有 quorum 时能读到正确值，再切断连接，断言这次读被**拒绝**而不是返回旧值。两条断言成对出现——只有第二条能抓住这类 bug。

关于 `leader._get_peer_node = lambda _peer_id: None` 这一行：`_get_peer_node` 是 `MemoryRaftNode` 的构造参数，进程内集群里 leader 靠它找到 peer 节点（走网络时换用 `_network_client`，两者在 `_replicate` 里二选一）。所以把它换成一个永远返回 `None` 的 lambda，在进程内集群里就等价于「这条线路被剪断」——注意 `_replicate` 拿到 `None` 后直接返回失败，不会计入 ACK。文末第七节的真 gRPC 版本做法更直观：直接停掉对方的 server。

另外这个测试套件里有一条纪律值得一提：**网络测试不 sleep，只等待它们真正在意的条件**。早期的测试用 `time.sleep(5)` 然后断言选举完成——在一台负载高的机器上，5 秒可能还不够选出一个领导者，测试就会因为和被测代码无关的原因随机失败。改成轮询等待「恰好一个 LEADER 且其余都是 FOLLOWER」之后，测试既更快也更诚实：它明确说出了自己依赖什么。

## 七、自己跑一遍

仓库是 github.com/Xuqing0415/OxideDB（Python，存储层只用标准库；安装：pip install -e ".[test]"）。公众号正文放不了外部超链接，需要的话把地址复制到浏览器打开。。最小复现就是本文开头那段输出：

1. 起一个 3 节点的 gRPC 集群，等选出唯一领导者；
2. 提交一次写入；
3. 停掉另外两个节点的 gRPC 服务端；
4. 向剩下的那个节点读——它还自称 `LEADER`，但读会被拒绝：

```
read with the quorum gone: success=False code=301 msg='ReadIndex failed: 1 acks, 2 required'
```

顺带一个细节：这次被拒绝的读花了 0.217 秒，时间基本都花在两次连接失败上。拒绝不是免费的——但比返回一个错误答案便宜得多。

## 八、几条教训

- **收集了不检查，等于没做。** `responses = []` 加 `append`，再加一个从没被读过的变量名，构成了一种非常有说服力的「我已经处理了」。写代码时的意图不会自动变成行为。
- **一致性协议里，「看起来像」是最危险的状态。** 这段代码有 RPC、有锁、有错误处理、有并发复查，长得和 ReadIndex 一模一样。它能骗过代码审查、骗过测试，因为它缺的不是代码量，而是一个判断。
- **测试要能说出「它证明了什么」。** 「读返回了正确的值」证明不了线性一致性；「失去多数派时读必须失败」才能。
- **正确性 bug 不一定表现为失败。** 最贵的一类 bug 是让不该成功的事情成功。

这次修复让我明白一件事：在分布式系统里，**「能跑」和「正确」之间的距离，比「能跑」和「跑不起来」之间的距离大得多**。后者有测试、有日志、有报错；前者什么都没有，只有一个看起来一切正常的答案。

## 附：这个项目还剩下什么

OxideDB 是个教学向的原型，不是生产数据库。README 里有一份诚实的 Known gaps 清单：快照是整库一个 blob、没有成员变更、没有多版本 GC、分片模块是冻结的实验性代码。`docs/design.md` 记录了键空间编码、快照、Percolator 2PC 和本文的 ReadIndex 这些设计决策的来龙去脉——写那份文档的过程，正是我发现「我以为我实现了 ReadIndex，其实我只实现了它的形状」的过程。

---

如果这篇文章让你对 Raft 的读路径多了一点警惕，欢迎点个「在看」，或者在评论区说说你踩过的类似坑。

项目地址（复制到浏览器打开）：github.com/Xuqing0415/OxideDB