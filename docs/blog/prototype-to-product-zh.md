# 从原型到产品：三次我改了做法

## 二、从「修 bug」到「发现 bug 为什么可能」

有些 bug 不会以失败的形式出现。它不抛异常、不超时、也不违反任何断言——它让**不该成功的事情成功了**。

我原来的节奏是：发现 bug、修掉、补一条回归测试。这个节奏默认 bug 会自己暴露——异常、超时、断言变红，总有一个会响。但只要 bug 的表现是「一个看起来完全正常的答案」，这一整套都不会响：测试是绿的，代码审查是过的，日志里没有一行异常，而答案是错的。

那天我盯着 `responses` 看了几秒。这是修复前的真实代码（`git show b519fd0^:oxidedb/raft/node.py`）：

```python
responses = []
for peer_id in peers:
    try:
        response = self._append_entries(peer_id, current_term, next_index.get(peer_id, 1),
                                        log, leader_commit)
        responses.append(response)
    except Exception:
        responses.append(None)

with self._lock:
    if self._state != NodeState.LEADER or self._current_term != current_term:
        return ReadResult.failure(ErrorCode.ERR_NOT_LEADER, "Lost leadership during read")
```

它被创建、被填充、再也没被读过。而在整条读路径里，唯一需要被检查的东西就是它——`responses` 里到底有几个节点回应了我。收集了多数派响应却不数，等于没有多数派：一个已经被废黜的领导者照样会回答，而这个答案**和正确的那个长得一模一样**。这段代码有 RPC、有锁、有错误处理、有并发复查，看起来和 ReadIndex 没有任何区别；它能骗过代码审查、骗过 96 个测试，因为它缺的不是代码量，是一个判断。

修好之后，同一个循环里只多做了一件事：`acks = 1`，每收到一个响应 `acks += 1`，然后 `if acks < majority` 就直接拒绝——从「收集」到「检查」，差的只是这一行。

后来我在另外两处看到了同一个形状：`freeze` 冻结对了组，靠的是两条语句碰巧的顺序，换个顺序就会冻结整个集群（`abc5306`）；一条 move 记录同时回答两个生命周期不同的问题，删早了一步，publisher 就把刚落地提案写的集合覆盖回了路由表（`456cd37`）。

做法上改的只有一件：**断言的目标从「能不能成功」换成「什么情况下必须失败」**，并开始把「一旦不成立就会静默地错」的东西写成清单（`de01143` 的五条不变量）。这个转折的边界很清楚——它只对表现为「太成功」的 bug 有用；报错、超时、挂死的 bug，老办法照旧抓得到。