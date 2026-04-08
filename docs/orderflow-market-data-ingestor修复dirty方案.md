# `orderflow-market-data-ingestor` 修复 dirty 方案

更新时间：2026-04-08  
适用范围：`orderflow-market-data-ingestor -> orderflow-indicator-engine` 的 `canonical 1m` 链路  
目标：在不要求上游停机的前提下，以指标计算准确性为第一目标，用最小复杂度修复当前 `dirty` 长期饿死问题。

---

## 1. 版本定位

本版不是“完备版版本化架构”，而是“极简第一性方案”。

本版只回答一个问题：

如果 `orderflow-market-data-ingestor` 会持续改写过去的 `1m bucket`，而 `orderflow-indicator-engine` 又必须保证指标计算准确，那么最简单、最稳、最少增加服务器压力的修法是什么。

本版明确不追求：

- 保留每一次输入修订历史
- 对外发布 `provisional` 指标
- 给每个输入和输出都做正式 revision 化
- 在 live 热路径上持续跑 dirty suffix replay

因为这些都不是“准确性第一”下的最小必要条件。

---

## 2. 当前事实

### 2.1 当前上游是谁

当前 `canonical 1m` 的直接生产者就是 `orderflow-market-data-ingestor`。

它会生成并发布：

- `md.agg.trade.1m`
- `md.agg.orderbook.1m`
- `md.agg.liq.1m`
- `md.agg.funding_mark.1m`

对应代码：

- `systems/market_data_ingestor/src/aggregate/minute_agg.rs:1363`
- `systems/market_data_ingestor/src/aggregate/minute_agg.rs:1419`
- `systems/market_data_ingestor/src/aggregate/minute_agg.rs:1453`
- `systems/market_data_ingestor/src/aggregate/minute_agg.rs:1513`

它也会把这些 `1m` canonical 数据写入：

- `md.agg_trade_1m`
- `md.agg_orderbook_1m`
- `md.agg_liq_1m`
- `md.agg_funding_mark_1m`

对应代码：

- `systems/market_data_ingestor/src/sinks/md_db_writer.rs:1203`
- `systems/market_data_ingestor/src/sinks/md_db_writer.rs:1267`
- `systems/market_data_ingestor/src/sinks/md_db_writer.rs:1346`
- `systems/market_data_ingestor/src/sinks/md_db_writer.rs:1385`

### 2.2 当前下游怎么使用这些数据

`orderflow-indicator-engine`：

- live 模式直接消费 `md.agg.*.1m`
- 启动 replay 也直接从 `md.agg_*_1m` 表回放

对应代码：

- `systems/indicator_engine/src/ingest/decoder.rs:374-377`
- `systems/indicator_engine/src/app/runtime.rs:5532-5599`

### 2.3 当前 dirty 是怎么被点亮的

只要同一个 `ts_bucket` 的 canonical `1m` 数据后来又发生“实质变化”，系统就会把该分钟标记为 dirty。

对应代码：

- `systems/indicator_engine/src/runtime/state_store.rs:2197-2215`
- `systems/indicator_engine/src/runtime/state_store.rs:2220-2246`
- `systems/indicator_engine/src/runtime/state_store.rs:2251-2265`
- `systems/indicator_engine/src/runtime/state_store.rs:2270-2289`

### 2.4 当前 dirty 为什么会长期饿死

当前 runtime 只有在 live 完全空闲时才允许 enqueue dirty 任务：

- `live_windows_enqueued == 0`
- `live_queue_pending == 0`
- `next_live_minute > ready_through_ts`

对应代码：

- `systems/indicator_engine/src/app/runtime.rs:4001-4014`

所以 dirty 在 steady live 模式下天然会被饿死。

### 2.5 当前系统其实已经有“进度游标”概念

现有库里已经有 `feat.indicator_progress`：

- 表定义：`sql/rebuild_common_base.sql:1392-1396`
- runtime 读取它：`systems/indicator_engine/src/app/runtime.rs:5510-5521`
- 写快照时会推进它：`systems/indicator_engine/src/storage/snapshot_writer.rs:996-1010`

另外，现有修复工具也已经有“回退 indicator progress 再重算”的思路：

- `systems/market_data_ingestor/src/bin/rebuild_trade_1m_from_raw.rs:166-170`
- `systems/market_data_ingestor/src/bin/rebuild_trade_1m_from_raw.rs:650-670`

这说明系统本身已经具备“用一个小游标控制最终计算进度”的基础，不需要先引入三张大表。

---

## 3. 第一性原理结论

### 3.1 准确性的本质要求

如果过去的 `1m bucket` 还可能继续变化，那么在它稳定之前，就不应该把基于它的指标结果当成最终事实落库或对外发布。

这句话就是整个问题的根。

### 3.2 最简单的正确解

最简单的正确解不是“dirty 跑快一点”，而是：

**确认前不产出最终指标，确认后只按当前最新 canonical 值算一次最终结果。**

换句话说：

- `md.agg_*_1m` 表负责保存“当前最新值”
- `feat.*` 指标表只保存“确认后的最终值”

这样一来：

- 确认前，上游怎么改都只是更新 canonical 表
- 下游不需要对这些分钟反复重算和反复发布
- dirty 这条 live 热路径上的复杂补丁机制就不再是主路径


## 4. 极简方案总览

### 4.1 一句话方案

把现有 `md.agg_*_1m` 表视为 **mutable pending truth**，把指标表视为 **final published truth**，在两者之间加入一个明确的 `T_confirm` 确认窗口。

### 4.2 核心原则

1. 上游继续正常运行，继续 upsert 现有 canonical `1m` 表  
2. 下游不再对“刚闭合的分钟”立刻产出最终指标  
3. 只有当某分钟跨过 `T_confirm`，且必需输入齐全时，才计算并写最终指标  
4. 确认点之前的任何修正，都只体现在 canonical 表更新，不触发 dirty suffix replay  
5. 确认点之后如果还出现修正，走异常 repair 路径，而不是日常 live 路径

### 4.3 目标状态机

```text
open -> pending_mutable -> confirmed -> published_final
```

语义如下：

- `open`：分钟未结束
- `pending_mutable`：分钟已结束，但仍允许上游修正
- `confirmed`：已跨过确认窗口，可以把当前 canonical 值视为本轮最终输入
- `published_final`：指标已基于 confirmed canonical 值落库

这里的重点是：

**系统只对 `confirmed` 分钟做最终计算。**

---

## 5. 数据模型

### 5.1 不新增输入 revision 表

本方案不新增：

- `ops.indicator_input_revision_log`
- `ops.indicator_minute_frontier`
- `feat.indicator_bundle_revision`

原因：

这些表会带来额外写放大、索引开销和存储压力，不是当前问题的最小解。

### 5.2 现有 canonical 表就是 pending truth

继续使用现有：

- `md.agg_trade_1m`
- `md.agg_orderbook_1m`
- `md.agg_liq_1m`
- `md.agg_funding_mark_1m`

它们的角色重新定义为：

**未确认前的最新真值表。**

也就是说，在确认窗口内：

- 可以继续被 upsert / update
- 下游不把它们对应的分钟当成最终输入

### 5.3 现有指标表就是 final truth

继续使用现有 `feat.*` 指标落库表。

它们的角色重新定义为：

**只承接确认后的最终指标值。**

### 5.4 只保留一个很小的提交游标

最小必要状态只需要一个：

- “当前已经成功写到哪一分钟”

现有 `feat.indicator_progress.last_success_ts` 已经可以承担这个职责：

- 表定义：`sql/rebuild_common_base.sql:1392-1396`
- runtime 查询：`systems/indicator_engine/src/app/runtime.rs:5510-5521`
- 正常推进：`systems/indicator_engine/src/storage/snapshot_writer.rs:996-1010`

因此，理想情况下，本方案甚至不需要新增表。

### 5.5 异常 repair 路径可选一个极小入口

如果后续需要把“确认后仍到达的修正”做成可观察、可重试流程，可以选做一个很小的 repair 入口，例如：

- `ops.indicator_repair_request`

但这不是首要前提。

在最小实现里，甚至可以直接沿用现有“回退 `feat.indicator_progress` 再重算”的机制。

---

## 6. Runtime 修复方案

### 6.1 live ingest 不再把“数据变了”立即转成 dirty 任务

live 收到新的 `md.agg.*.1m` 后：

1. 更新内存里的 canonical 状态  
2. 如果需要，继续 upsert 到 `md.agg_*_1m`  
3. 更新各 source 的最新 frontiers  
4. 不再因为“这个 finalized minute 后来变了”就立刻进入 hot-path dirty replay

从语义上说：

在确认窗口以内，这类变化是正常现象，不是异常。

### 6.2 引入 `T_confirm`

定义一个确认窗口：

`T_confirm`

含义是：

一个分钟只有在“距离当前时间超过 `T_confirm`”之后，才允许进入最终指标计算。

更严格一点，真正可提交的分钟应该是：

```text
commit_through_ts = min(all_required_source_frontiers) - T_confirm
```

并且该分钟的必需输入必须齐全。

### 6.3 runtime 只计算 `commit_through_ts` 以内的连续分钟

主循环不再追逐“最新刚收盘分钟”，而是只处理：

- `last_success_ts + 1m`
- 到
- `commit_through_ts`

之间的连续分钟。

这样系统天然具备三个性质：

1. 处理的是已经更稳定的输入  
2. 对同一分钟通常只算一次  
3. 不再需要在 live 热路径上修过去的 suffix

### 6.4 确认前的修正不再是问题

例如：

- `22:31` 这个 bucket 在 `22:33` 被修一次
- `22:35` 再被修一次
- `T_confirm = 15m`

那么在 `22:46` 之前，`indicator-engine` 都不会对 `22:31` 产出最终指标。

到 `22:46` 真正提交时，它只会读取 `md.agg_*_1m` 当前最新值算一次。

所以：

- 不需要 dirty
- 不需要 provisional
- 不需要一轮轮回放过去

### 6.5 确认后的修正走 repair

如果某分钟已经进入 `published_final`，之后又来了修正，那么这是 **异常晚到**。

正确处理是：

1. 把该分钟视为 repair 起点  
2. 将 `feat.indicator_progress.last_success_ts` 回退到该分钟前一格  
3. 从该分钟重新顺序计算并覆盖既有指标结果

这套思路已经存在于当前工具链里：

- `systems/market_data_ingestor/src/bin/rebuild_trade_1m_from_raw.rs:166-170`
- `systems/market_data_ingestor/src/bin/rebuild_trade_1m_from_raw.rs:650-670`

因此，最小实现完全可以沿用“rewind progress + replay confirmed range”的机制，而不必再发明完整 revision 存储层。

### 6.6 repair 必须和日常 commit 解耦

虽然本方案极简，但仍然要保证一件事：

确认后的异常 repair 不能继续复用当前“live 空闲才跑”的 dirty 调度条件。

也就是说：

- 平时不需要 dirty
- 但一旦进入 repair，repair 必须有独立执行通道或明确优先级

否则同样会再次饿死。

---

## 7. `T_confirm` 如何定义

### 7.1 这是本方案唯一真正需要拍板的参数

极简方案能否成立，关键只在：

`T_confirm` 能不能被合理定义。

### 7.2 正确的定义方式

`T_confirm` 不应该拍脑袋定，而应基于上游各 source 的最坏迟到分布来定。

例如：

- `trade` 最坏晚到 `X_trade`
- `orderbook` 最坏晚到 `X_orderbook`
- `liq` 最坏晚到 `X_liq`
- `funding_mark` 最坏晚到 `X_funding`

那么保守定义可以是：

```text
T_confirm = max(X_trade, X_orderbook, X_liq, X_funding) + safety_margin
```

### 7.3 准确性优先时的选型原则

如果目标是准确性第一，那么：

- `T_confirm` 宁可偏大，不可偏小
- 先牺牲实时性，再追求降低延迟

因为：

- `T_confirm` 偏大，代价只是指标晚一点出来
- `T_confirm` 偏小，代价是又会把会变的数据当成 final

### 7.4 如果根本没有迟到上界

如果实际上根本不存在可接受的迟到上界，那么严格意义上的 final 就不存在。

这时只能二选一：

1. 接受“最终指标有较大延迟”，把 `T_confirm` 取得非常保守  
2. 接受“偶发 late correction 进入 repair”

对于当前系统，第二种更现实。

---

## 8. 为什么这个方案更省服务器

### 8.1 它减少的是重算次数

当前 dirty 方案的问题不是单次计算，而是：

- 一个分钟可能反复被打脏
- dirty 又会回放 suffix
- suffix 重算会构造很重的历史结构

而极简方案把这条路径改成：

- 确认前只更新 canonical 最新值
- 确认后只算一次最终输出

所以最直接减少的是：

- 重复计算
- 重复 materialize
- 重复写指标表

### 8.2 它不新增高频写表

它不需要在 live 热路径新增：

- 输入 revision log 写入
- 输出 revision log 写入
- head 表维护

因此对数据库压力的增加最小。

### 8.3 它更符合现有存储角色分工

在这个方案里：

- `md.agg_*_1m` 负责“最新真值”
- `feat.*` 负责“最终结果”
- `feat.indicator_progress` 负责“已经提交到哪里”

这比“在 indicator-engine 再复制一套版本存储体系”更自然。

---

## 9. 本方案不解决什么

### 9.1 不保留每一次修订历史

如果后续要做审计、追责、逐版 diff，这个方案不够。

### 9.2 不发布 provisional 指标

如果业务强依赖“分钟刚结束就要先看到一个临时指标值”，这个方案不满足。

### 9.3 不消除 repair

如果确认后仍有极晚修正，系统仍然需要 repair。

但 repair 在这个方案里变成少量异常路径，而不是日常热路径。

---

## 10. 不应该采用的伪修复

以下方案都不是根修：

- 只提高 `watermark_lateness_secs`
- 只增大 dirty batch
- 继续维持 “live 优先、dirty 等空闲”
- 直接丢弃 stale event
- 保留当前 dirty suffix replay，只是让它更快

这些办法都没有解决“确认前不该产出 final”这个根问题。

---

## 11. 最终建议

如果当前第一目标是：

**指标计算准确性**

而不是：

- 实时 provisional 输出
- 完整 revision 审计
- 逐版回放

那么最第一性、最简、最省压的方案就是：

1. 继续让 `orderflow-market-data-ingestor` 写现有 `md.agg_*_1m`
2. 把这些表视为 mutable pending truth
3. 给 `indicator-engine` 引入 `T_confirm`
4. 只对跨过 `T_confirm` 的分钟写 final 指标
5. 用现有 `feat.indicator_progress` 维护提交游标
6. 对确认后仍晚到的极少数修正，走 `rewind progress + replay` repair

---

## 12. 评审结论

站在第一性原理上，这个问题的根因不是 dirty 线程不够，也不是 batch 不够，而是：

**系统过早把会变的数据当成了最终输入。**

所以最小正确修复不是“把 dirty 做复杂”，而是：

**把最终指标的写入时机后移到确认点。**

这版方案的核心价值就是：

- 不增加 3 张高频表
- 不增加 live 热路径写放大
- 不牺牲准确性
- 把复杂度从“日常 dirty 重算”降成“确认前等待，确认后一次计算，极少数异常再 repair”

