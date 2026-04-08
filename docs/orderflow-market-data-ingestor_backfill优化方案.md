# orderflow-market-data-ingestor_backfill 优化方案（最终修订版：live repair state-rebuild-only）

## 1. 目标重述

这次优化只服务一个核心目标：

> 在 `orderflow-market-data-ingestor` 持续运行、canonical market data 可从数据库/队列重建的前提下，`orderflow-indicator-engine` 能对丢失分钟进行准确修复，但不再为这些历史 repair 分钟全量存 27 个指标和中间态，从而降低 live repair 阶段的大 payload 写库压力。

同时必须满足三个硬约束：

1. `orderflow-indicator-engine` 指标计算准确性不能下降。
2. 下游 `llm` 层的 live 输入合同不能变化。
3. 本次修改只影响 **live repair**，不改 startup backfill，不改 post-backfill catch-up，不改 steady-state live。

这意味着本次正确方向不是：

- 改 startup backfill；
- 改 catch-up；
- 继续把 repair 分钟当成“完整指标产品”落库，只是少发消息。

本次正确方向是：

> live repair 直接绕过“指标物化管线”，只重建 `StateStore` 的内存最终状态；历史 repair 分钟不再生成完整 snapshot / feature / event / level / minute bundle。

---

## 2. 代码 review 结论

## 2.1 当前 live repair 的重压并不来自 market_data_ingestor

`market_data_ingestor` 当前负责的是 canonical source-of-truth：

- raw / aggregate market data 入库
- direct MQ / outbox 发布
- `backfill_in_progress` 标记透传

关键文件：

- `systems/market_data_ingestor/src/pipelines/persist_async.rs`
- `systems/market_data_ingestor/src/sinks/mq_publisher.rs`
- `systems/market_data_ingestor/src/sinks/outbox_writer.rs`

这层保存的是原始/规范化市场数据，本来就应该保留，不是这次的优化对象。

## 2.2 当前真正重的是 indicator_engine 的 live repair 物化链路

当前 confirmed live repair 入口：

- `maybe_execute_confirmed_repair_replay(...)`
- `maybe_execute_shutdown_confirmed_repair_replay(...)`
- 文件：`systems/indicator_engine/src/app/runtime.rs`

当前逻辑是：

1. `rewind_persisted_tail(...)`
2. `state_store.rewind_finalized_state_from(...)`
3. `process_ready_minutes(..., DispatchMode::RepairReplay, ...)`

而 `RepairReplay` 当前会走完整的物化链路：

- `write_snapshots(...)`
- `write_indicator_levels(...)`
- `write_indicator_events(...)`
- `write_all(...)`
- `advance_progress_with_outbox(...)`

文件：

- `systems/indicator_engine/src/runtime/dispatcher.rs`
- `systems/indicator_engine/src/storage/snapshot_writer.rs`

这就是 live repair 阶段大 payload 写库压力的根因。

## 2.3 `rewind_persisted_tail(...)` 当前删得太多，不适合你的目标

`rewind_persisted_tail(...)` 当前会删掉 repair 起点之后的大量历史持久化尾巴：

- `ops.indicator_bundle_outbox`
- `ops.indicator_bundle_payload_cache`
- `ops.outbox_event`
- `feat.indicator_snapshot`
- 各类 `feature` / `event` / `level` 表
- 还会回卷 `indicator_snapshot_fanout_progress`

文件：

- `systems/indicator_engine/src/storage/snapshot_writer.rs`

如果继续沿用这条思路，就意味着：

- repair 之前先删历史指标产物，
- repair 之后再完整重写一次。

这和你的目标正相反，因为它仍然要求历史 repair 分钟走全量指标落库。

## 2.4 指标未来计算准确性真正依赖的是 `StateStore`，不是历史 snapshot 表

最关键的代码事实在这里：

- `StateStore::finish_finalize_minute_state(...)`
  - 会刷新：
    - `refresh_incremental_indicator_event_caches(ts_bucket)`
    - `refresh_incremental_indicator_outputs(ts_bucket)`
- 文件：`systems/indicator_engine/src/runtime/state_store.rs`

也就是说，每分钟 finalize 之后，后续分钟计算所需的增量状态已经被刷新进 `StateStore`。

同时：

- `advance_finalized_state(ts_bucket)` 会 finalize 两个 market，然后调用 `finish_finalize_minute_state(ts_bucket)`
- `finalize_minute(ts_bucket)` 则是在此基础上额外构造 `WindowBundle`

这说明：

- 如果 live repair 的目标只是“把未来分钟计算所需状态修正回来”，
- 那么 repair 期间**不一定需要**对历史分钟跑完整 `IndicatorContext -> compute_window_artifacts -> persist_window_artifacts` 流程。

从第一性原理讲：

- repair 需要的是修复 `StateStore`；
- snapshot / feature / event / minute bundle 只是对外产物。

## 2.5 `dirty recompute` 也会把 repair 分钟重新物化

当前不只是 confirmed repair replay 有问题，`dirty recompute` 也会重走历史分钟物化：

- dirty range 来源于 live gap repair / tail reconcile / late canonical correction
- `recompute_dirty_finalized_minutes(...)` 现在返回 `WindowBundle`
- 后续会通过 ready-job / materialize worker 继续走 `process_window_bundle(...)`
- 如果 mode 是 `Live`，历史 dirty 分钟就会被再次发布

文件：

- `systems/indicator_engine/src/runtime/state_store.rs`
- `systems/indicator_engine/src/app/runtime.rs`

因此，如果只修 `confirmed repair replay` 而不修 dirty recompute，这次优化仍然不完整。

## 2.6 `oi_ratio_patch` 也会在 repair 后重新发 bundle

当前：

- `process_pending_oi_ratio_patches(...)`
- `spawn_oi_ratio_patch_task(...)`

最终会调用：

- `dispatcher.process_oi_ratio_patch_window(...)`

而这个函数内部会：

- 写 snapshot
- 写 feature
- `enqueue_bundle_repairs(...)`

也就是 repair 期间可能仍然把历史分钟重新推向下游。

文件：

- `systems/indicator_engine/src/app/runtime.rs`
- `systems/indicator_engine/src/runtime/dispatcher.rs`

## 2.7 仅靠 tail reconcile 不能保证 state-only repair 的跨重启恢复

runtime 里的 live tail reconcile 只有：

- `LIVE_CANONICAL_TAIL_RECONCILE_LOOKBACK_MINUTES = 31`

文件：

- `systems/indicator_engine/src/app/runtime.rs`

这意味着：

- 如果某次 confirmed repair 修复的是更早的分钟，
- 进程在 repair 完成后、runtime snapshot 落盘前崩掉，
- 下次启动未必还能靠 tail reconcile 自动补回来。

因此，如果本次采用 live repair state-only，就必须补一条：

> repair 成功后立即保存 runtime state snapshot。

现有可复用能力已经存在：

- `save_state_snapshot_and_clear_startup_checkpoint_on_success(...)`
- 周期性 snapshot loop `run_periodic_runtime_snapshot_loop(...)`

文件：

- `systems/indicator_engine/src/app/runtime.rs`

---

## 3. 结论：上一版“RepairPersistNoPublish”仍然不够

上一版只做到：

- repair 分钟继续完整重写 snapshot / feature / event / level
- 只是少发 `ind.minute_bundle`

这只能减少：

- outbox / payload cache 的压力

但不能减少：

- 历史 repair 分钟的大量 snapshot / feature / event 重写

所以它**不能真正实现**你的目标。

要实现你的目标，必须进一步收缩成：

> live repair 不再走 dispatcher 的完整指标物化管线，而是直接重建 `StateStore`，然后立刻保存 state snapshot。

---

## 4. 最终方案：live repair state-rebuild-only

## 4.1 总体原则

live repair 只做四件事：

1. 读 canonical market data 真相
2. 修正 `StateStore`
3. 保证修正后的 `StateStore` 可跨重启恢复
4. 不重新产品化历史 repair 分钟

明确不再做：

1. 不重写历史 `feat.indicator_snapshot`
2. 不重写历史 `feature` / `event` / `level`
3. 不重建历史 `indicator_bundle_outbox`
4. 不重建历史 `indicator_bundle_payload_cache`
5. 不重新发历史 `ind.minute_bundle`
6. 不重新发历史 snapshot fanout

## 4.2 confirmed repair replay 改成“只重建内存状态”

当前 confirmed repair replay 使用：

- `process_ready_minutes(...)`
- `DispatchMode::RepairReplay`

本次建议彻底绕过这条链路，新增专门的 helper，例如：

```rust
async fn rebuild_repair_state_range(
    ctx: &Arc<AppContext>,
    state_store: &mut StateStore,
    from_ts: DateTime<Utc>,
    to_ts_inclusive: DateTime<Utc>,
) -> Result<RepairRebuildStats>
```

其逻辑应当是：

1. `state_store.rewind_finalized_state_from(repair_start_ts)`
2. `state_store.clear_dirty_recompute_state()`
3. `state_store.clear_oi_ratio_patch_state()`
4. 先为 `[repair_start_ts, repair_ready_through_ts]` 做必要的 heatmap hydration
5. 逐分钟执行：

```rust
state_store.advance_finalized_state(minute)
```

而不是：

```rust
let window = state_store.finalize_minute(minute);
process_window_bundle(...);
```

### 为什么选 `advance_finalized_state(...)`

因为它会：

- finalize futures/spot
- `finish_finalize_minute_state(...)`
- 刷新增量输出和事件缓存

但不会：

- 构造 `WindowBundle`
- 驱动 27 个指标快照计算
- 触发持久化写入

这正好符合目标。

## 4.3 dirty recompute 改成 state-rebuild-only

当前：

- `recompute_dirty_finalized_minutes(max_batch)` 返回 `Vec<WindowBundle>`
- 随后这些 window 会进入 materialize worker，继续走完整指标物化

本次建议新增一个平行 helper，例如：

```rust
pub fn rebuild_dirty_finalized_state(&mut self, max_batch: usize) -> usize
```

语义与现有 dirty recompute 保持一致：

- 同样会截断 finalized suffix
- 同样按 batch 重建 dirty range

但实现改成：

```rust
while minute <= batch_end {
    self.advance_finalized_state(minute);
    rebuilt += 1;
    minute += Duration::minutes(1);
}
```

而不是返回 `WindowBundle` 给后续 worker。

这样 dirty recompute 修复的是：

- 未来分钟计算所需的内存状态

而不是：

- 历史分钟的指标产品化结果

## 4.4 repair 期间禁用 `oi_ratio_patch` republish

这次必须明确：

- repair replay 期间 `allow_oi_ratio_patches = false`
- dirty rebuild 期间不启动 `spawn_oi_ratio_patch_task(...)`

原因很简单：

- `oi_ratio_patch` 当前会 republish 历史 repair 分钟
- 这和“llm live 输入不变、repair 不再产品化历史分钟”相冲突

steady-state live 阶段则保持现状不变。

## 4.5 repair 后立即保存 runtime snapshot

这是本次方案的关键 durable 保证。

### 为什么必须立即保存

如果 live repair 只改内存状态、不改历史指标表，那么 repair 成功后的 durable truth 就只剩：

- `StateSnapshot`

而不是：

- `feat.indicator_snapshot`
- feature/event/level 表

再考虑到 tail reconcile 只回看 31 分钟，所以：

- state-only repair 完成后如果不立即落 snapshot，
- 进程崩掉就可能丢掉这次更早历史修复的结果。

### 推荐实现

不要在主循环里直接同步写文件阻塞太久，建议复用现有 snapshot 能力，增加一个“高优先级即时保存”触发，例如：

```rust
enum SnapshotSaveReason {
    Periodic,
    RepairCompleted,
}
```

新增一个轻量触发通道或原子 flag，由已有 snapshot task 在收到 `RepairCompleted` 后立刻保存：

- `state_store.extract_snapshot()`
- `save_state_snapshot_and_clear_startup_checkpoint_on_success(...)`

如果不想额外起机制，最小可行版本也可以：

- confirmed repair replay 成功后同步保存一次 snapshot
- dirty rebuild 系列完成后 debounce 保存一次 snapshot

## 4.6 不再回卷历史指标持久化尾巴

当前 `rewind_persisted_tail(...)` 的问题是：

- 它把 repair 起点之后的历史指标产物全删掉，
- 然后逼迫 repair 流程必须完整重写。

本次目标下，推荐做法是：

### 不再调用 `rewind_persisted_tail(...)`

即：

- 不删 `feat.indicator_snapshot`
- 不删 feature/event/level
- 不删 snapshot fanout progress

因为这些历史指标表对未来 live 计算不是 source-of-truth。

### 只做必要的 publish-suppression 清理

如果担心 repair 期间存在未发出的历史 bundle，可单独新增一个很窄的 helper，例如：

```rust
async fn suppress_repair_bundle_publish_tail(
    symbol: &str,
    repair_start_ts: DateTime<Utc>,
    exchange_name: &str,
) -> Result<()>
```

它只清理：

- `ops.indicator_bundle_outbox`
- `ops.indicator_bundle_payload_cache`

且只服务于：

- 防止历史 repair 分钟再次向下游发布

但**不动**：

- snapshot / feature / event / level
- snapshot fanout progress
- indicator progress

这样可以把删除范围控制到最小。

## 4.7 不回退 `indicator_progress`

当前 repair replay 会：

- `scheduler.mark_emitted_through(rewind_target_ts)`
- `metrics.set_last_persisted_ts(rewind_target_ts)`
- `set_indicator_progress_exact(...)`

这套动作适用于“删尾巴再重写”的模型，不适用于 state-only repair。

本次 state-only 方案下：

- `indicator_progress` 应继续代表当前 steady-state live 的 persisted frontier
- historical repair 不应把它回退

否则会让 startup 误以为必须从更早点重新 materialize 全部 tail。

因此：

- live repair state-only 不回退 `feat.indicator_progress`
- 也不把 `metrics.last_persisted_ts` 倒拨到旧 repair 分钟

可以新增单独的 repair metrics，例如：

- `last_repair_rebuilt_from_ts`
- `last_repair_rebuilt_to_ts`
- `repair_rebuilt_minutes`

---

## 5. 为什么这能保证指标计算准确性

因为这次修的是“状态”，而不是“历史产品化结果”。

未来分钟的指标计算依赖：

1. canonical market data
2. `StateStore` 中的 finalized history
3. `finish_finalize_minute_state(...)` 刷新的：
   - incremental outputs
   - incremental event caches

这些都属于：

- 内存最终状态

而不是：

- `feat.indicator_snapshot`
- feature/event/level 历史表
- historical minute bundle

所以只要 repair 能正确重建 `StateStore`，未来 live 分钟的指标计算准确性就不会受影响。

---

## 6. 为什么这不会改变 llm 输入

`llm` 当前消费的是 steady-state live 的：

- `ind.minute_bundle`

并直接读取其中的 `payload`：

- `systems/llm/src/app/runtime.rs`
- `systems/llm/src/workflow/stage2_input.rs`

本次方案下：

- steady-state live 新分钟仍按现有流程构造 `ind.minute_bundle`
- 历史 repair 分钟不再 republish

因此对 `llm` 而言：

- 输入结构完全不变
- 输入来源仍然是新的 live 分钟
- 只是少了历史 repair minute 的重复干扰

这正符合你的要求。

---

## 7. 文件级代码设计

## 7.1 `systems/indicator_engine/src/app/runtime.rs`

### 必改点 A：confirmed repair replay

把：

- `maybe_execute_confirmed_repair_replay(...)`
- `maybe_execute_shutdown_confirmed_repair_replay(...)`

改成：

1. 不再调用 `rewind_persisted_tail(...)`
2. 不再调用 `process_ready_minutes(..., DispatchMode::RepairReplay, ...)`
3. 改为：
   - `state_store.rewind_finalized_state_from(repair_start_ts)`
   - `hydrate_futures_orderbook_heatmaps_for_range(...)`
   - `rebuild_repair_state_range(...)`
   - `request_repair_snapshot_save(...)`

### 必改点 B：dirty recompute

当前 dirty recompute 的历史 window 仍会被 materialize。

本次改成：

- 在 live loop / process_ready_minutes 中，dirty range 不再进入 `process_window_bundle(...)`
- 改用 `state_store.rebuild_dirty_finalized_state(...)`

### 必改点 C：repair 期间关闭 `oi_ratio_patch`

repair state rebuild 期间：

- `allow_oi_ratio_patches = false`
- 不启动 `spawn_oi_ratio_patch_task(...)`

### 必改点 D：repair 完成后的 snapshot save

新增一个 repair-completed 触发路径，复用现有 snapshot 保存函数。

## 7.2 `systems/indicator_engine/src/runtime/state_store.rs`

### 必改点 A：新增 `rebuild_dirty_finalized_state`

新函数建议：

```rust
pub fn rebuild_dirty_finalized_state(&mut self, max_batch: usize) -> usize
```

语义：

- 与 `recompute_dirty_finalized_minutes(...)` 共用 dirty range 控制
- 但不返回 `WindowBundle`
- 只更新 finalized state / incremental caches

### 必改点 B：可选新增 range rebuild helper

如果 runtime 层不想手写 loop，也可以在 `StateStore` 中新增：

```rust
pub fn rebuild_finalized_state_range(
    &mut self,
    from_ts: DateTime<Utc>,
    to_ts_inclusive: DateTime<Utc>,
) -> usize
```

内部循环调用：

```rust
self.advance_finalized_state(minute)
```

## 7.3 `systems/indicator_engine/src/storage/snapshot_writer.rs`

### 本次要改的只有一件事

新增一个极窄的 publish suppression helper，例如：

```rust
pub async fn suppress_repair_bundle_publish_tail(...)
```

只清理：

- `ops.indicator_bundle_outbox`
- `ops.indicator_bundle_payload_cache`

### 本次不要再做

- 不要删除 `feat.indicator_snapshot`
- 不要删除 feature/event/level
- 不要回卷 `indicator_snapshot_fanout_progress`
- 不要回退 `indicator_progress`

## 7.4 `systems/indicator_engine/src/runtime/dispatcher.rs`

本次尽量不扩大改动面。

如果 repair 真正绕过物化管线，那么这里可以：

- 完全不加新 `DispatchMode`

也就是说：

- `dispatcher` 仍只服务 startup/backfill/live/shutdown 这些原有物化路径
- repair state rebuild 不进 dispatcher

这是更贴合第一性原理的做法。

## 7.5 `systems/market_data_ingestor`

本次不改。

理由：

- 你的目标是 live repair 减少历史指标写库
- 根因在 `indicator_engine` repair 物化链路
- `market_data_ingestor` 继续作为 canonical source-of-truth 保持不变即可

---

## 8. 本方案的代价

必须明确接受以下结果：

1. repair 起点之后的历史 `feat.indicator_snapshot` / feature/event/level 可能保持旧值。
2. 历史指标表不再代表绝对 truth，truth 转移为：
   - canonical market data
   - 最新 runtime state snapshot
3. 如果没有在 repair 完成后立即保存 snapshot，而进程又在下一次 periodic snapshot 前崩掉，某些超过 tail reconcile lookback 的历史修复可能丢失。

这也是为什么本方案里“repair 后立即 snapshot save”不是可选项，而是必需项。

---

## 9. 为什么这个方案更符合你的原始诉求

你的诉求不是：

- 让历史指标表永远严格对齐

而是：

- `market_data_ingestor` 持续运行时，`indicator_engine` 可以把丢失分钟修回来；
- 能通过数据库/队列重建的，就不要全量存；
- live repair 阶段不要因为大 payload 写库把 DB 压死。

在这个前提下，最佳边界就是：

- **保留 canonical source-of-truth**
- **修复 `StateStore`**
- **放弃历史 repair 分钟的全量指标物化**

这正是这版最终方案做的事情。

---

## 10. 验证方案

## 10.1 准确性验证

1. 构造一个 confirmed late correction，触发 live repair。
2. 对比修改前后：
   - repair 结束后的 `StateStore.last_finalized_minute()` 一致
   - 后续新 live 分钟的关键指标结果一致
3. 重点观察：
   - orderbook depth
   - liquidation density
   - avwap / rvwap
   - event-cached indicators

## 10.2 llm 输入验证

1. steady-state live 新分钟的 `ind.minute_bundle` 完全不变
2. repair 窗口期间不再看到历史 repair minute 的 bundle
3. `oi_ratio_patch` 在 repair 期间不再 republish

## 10.3 压力验证

重点看 repair 期间：

- `feat.indicator_snapshot` 写入量显著下降
- `feature_write_ms` / `snapshot_write_ms` 显著下降
- `ops.indicator_bundle_payload_cache` 几乎不再增长
- DB acquire latency 降低

## 10.4 恢复验证

1. repair 完成后立即强制保存 snapshot
2. 人工重启 `indicator_engine`
3. 确认启动后无需再次依赖历史指标表，也能继续计算未来 live 分钟

---

## 11. 最终建议

如果目标是：

- 指标计算准确性不变
- llm live 输入不变
- 只影响 live repair
- 并且真正降低 live repair 阶段的大 payload 写库压力

那正确方案不是：

- `RepairReplay but no publish`

而是：

- `live repair state-rebuild-only`

也就是：

1. 不再让 repair 分钟走完整指标物化管线
2. 不再删除并重写历史 snapshot / feature / event / level
3. 只修复 `StateStore`
4. repair 完成后立即保存 runtime snapshot

这是当前代码和你的目标之间，最贴合第一性原理的一版方案。
