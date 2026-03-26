# OI / Ratio 晚到数据无阻塞修补方案 v1.0.0

## 1. 背景

当前 `indicator_engine` 已接入：

- `md.open_interest_hist_5m`
- `md.long_short_ratio_5m`
- `i25 open_interest`
- `i26 long_short_ratios`

但线上运行暴露出一个结构性问题：

- `open_interest_hist_5m / long_short_ratio_5m` 这组 `5m` REST 结构数据天然比 `1m trade/orderbook/liq/funding` 晚
- 它们一旦在 finalized minute 之后到达，会把 `indicator_engine` 的整个 finalized suffix 标脏
- 运行时随后执行 `11:25 -> current` 这类整段阻塞式 replay
- 结果是本来已经 ready 的 `11:30` live minute，也要等旧尾巴回补完才能发 bundle

这会直接放大 `llm` 侧看到的 `bundle stale`。

本方案的目标不是“让 OI / ratio 更早发布”，而是：

- 在**不牺牲指标准确性**的前提下
- 让 `OI / ratio` 的晚到修正不再阻塞整个 live minute 链路

---

## 2. 本次结论

根因已经确认：

- `open_interest_hist_5m / long_short_ratio_5m` 的晚到是**设计内行为**
- 但真正把 `11:30` 拖到 `11:32:51` 的，不是这 `20~30s` 晚到本身
- 而是它们晚到后，被 `indicator_engine` 当成了**全局 finalized dirty trigger**
- 进而触发 `11:25 -> 11:30` 的整段阻塞式回补

因此，真正应该优化的是：

- **dirty scope**

而不是：

- 降精度忽略晚到 OI / ratio
- 或者简单缩短 replay lookback

一句话版本：

> `open_interest_hist_5m / long_short_ratio_5m` 应该从“全局尾部重算触发器”降级成“仅修补 i25 / i26 的专项修补触发器”。

---

## 3. 已确认事实

### 3.1 这两个源确实是按设计晚到

`market_data_ingestor` 的 live 抓取逻辑在 [backfill_scheduler.rs](/data/systems/market_data_ingestor/src/state/backfill_scheduler.rs#L511)：

- 先算 `boundary_bucket = floor_to_5m(now)`
- 只有当 `now > boundary_bucket + 20s` 才开始处理
- 处理目标是 `target_bucket = boundary_bucket - 5m`

这意味着：

- 在 `11:30:20` 之后，系统才会尝试抓 `11:25:00` 这个桶
- 再叠加 `15s` 的 polling 节奏和 REST 耗时，最终落库大约会在 `11:30:29.xxx`

库里的真实数据也验证了这点：

- `md.open_interest_hist_5m`
  - `ts_bucket = 2026-03-26 11:25:00+00`
  - `ts_recv = 2026-03-26 11:30:29.953363+00`
- `md.long_short_ratio_5m`
  - `ts_bucket = 2026-03-26 11:25:00+00`
  - `global_account / top_account / top_position`
  - `ts_recv` 都在 `2026-03-26 11:30:29.9534xx+00`

结论：

- 这不是偶发抖动
- 是当前 live scheduler 的稳定行为

### 3.2 当前代码会把它们升级成“全局脏尾巴”

在 [state_store.rs](/data/systems/indicator_engine/src/runtime/state_store.rs#L1816) 和 [state_store.rs](/data/systems/indicator_engine/src/runtime/state_store.rs#L1835)：

- `store_open_interest_hist_5m(...)`
- `store_long_short_ratio_5m(...)`

只要数据有变化，就会调用 [mark_dirty_recompute_if_finalized(...)](/data/systems/indicator_engine/src/runtime/state_store.rs#L1891)。

这个函数的行为是：

- 如果 `ts_bucket <= last_finalized_minute`
- 就把 `dirty_recompute_from` 推到更早的 finalized minute
- 下一轮 `process_ready_minutes()` 会从这个点开始 truncation + replay

### 3.3 live runtime 当前会先 drain dirty replay，再放新分钟

这段逻辑在 [runtime.rs](/data/systems/indicator_engine/src/app/runtime.rs#L1986)。

代码里的注释也写得很明确：

- accuracy first
- dirty recompute 必须先 drain 完 finalized suffix
- 否则 rolling history 类指标可能看到临时 tail gap

也就是说，当前的核心策略是：

- **只要有 dirty pending，先重算旧尾巴，再放 live 新分钟**

### 3.4 OI / ratio 根本不是 minute completeness 的硬前置条件

当前 complete policy 在 [state_store.rs](/data/systems/indicator_engine/src/runtime/state_store.rs#L326)。

ready 依赖的是：

- futures trade
- futures orderbook
- futures funding
- spot trade
- spot orderbook

并**不包含**：

- `open_interest_hist_5m`
- `long_short_ratio_5m`

这点非常重要，因为它说明：

- 系统本来就没把 OI / ratio 当成 “当前 1m ready 的硬门槛”
- 但它们一晚到，却会升级成“卡整条 live 链路”的全局修补触发器

当前设计在这里是前后不对称的。

### 3.5 这组晚到数据只影响 i25 / i26

`OI / ratio` 在 runtime state 中进入 [build_oi_ratio_view_for_minute(...)](/data/systems/indicator_engine/src/runtime/state_store.rs#L2070)。

这组视图最终被：

- [i25_open_interest.rs](/data/systems/indicator_engine/src/indicators/i25_open_interest.rs)
- [i26_long_short_ratios.rs](/data/systems/indicator_engine/src/indicators/i26_long_short_ratios.rs)

消费。

我已经检索过 `indicator_engine/src`，没有发现其他 flow / orderbook / event 指标依赖：

- `open_interest_hist_5m`
- `global_account_ratio_5m`
- `top_account_ratio_5m`
- `top_position_ratio_5m`
- `latest_common_oi_ratio_bucket`

因此它们的 blast radius 是收敛的：

- 主要就是 `i25 / i26`
- 以及它们对应的 `feat.*` 与 snapshot

### 3.6 本次事故时间线

`2026-03-26 11:30` 这次案例可以复原为：

1. `11:30:29.xxx`
   - `11:25` 的 OI / ratio 进入库
2. `11:30:44`
   - tail reconcile 发现 finalized truth 变化
   - 日志：`dirty_recompute_pending=true`
3. `11:31:25 -> 11:32:22`
   - runtime 阻塞式重放 `11:25/26/27/28/29/30`
4. `11:32:39`
   - `llm` 本地才收到 `113000` bundle
5. `11:32:51`
   - `llm` 判定该 bundle stale，跳过

这说明：

- 真正的大头不是 `20~30s` 源头晚到
- 而是后续 `~80-100s` 的 live blocking replay

---

## 4. 设计目标

### 4.1 必须满足

- 不改变 `i25 / i26` 的数学定义
- 不丢弃 late OI / ratio correction
- 不让 `11:25` 的 OI / ratio 修正再阻塞 `11:30` 的 live 发包
- 保证 `i25 / i26` 对 finalized history 最终仍然收敛到正确值
- 保证下游最终看到的 minute bundle 仍然是完整一致的

### 4.2 明确不做

- 不要求 Binance 这 4 个 REST endpoint 改发布时间
- 不把 `5m` 数据和 `1m` 数据混拼写入
- 不把 late OI / ratio 永久忽略
- 不用“缩 replay lookback”来换准确性
- 不改现有 minute completeness policy

---

## 5. 正确的优化方向

### 5.1 核心思想

把 late OI / ratio correction 从：

- `global dirty replay`

改成：

- `oi_ratio scoped patch replay`

也就是：

- 当前 live minute 继续按现有 completeness policy 推进
- OI / ratio 晚到后，只修补受影响的 `i25 / i26`
- 不再拖着整条 flow / orderbook / event 指标链条一起回放

### 5.2 为什么这不伤准确性

因为这里有两个层面：

1. **当前分钟是否能放行**
2. **历史受影响分钟是否最终修正到正确值**

当前代码已经说明：

- OI / ratio 不是第 1 层的硬门槛

所以 live 放行不等它们，本来就是设计允许的。

但第 2 层仍然必须保证：

- `11:25` 的 late OI / ratio 到达后
- `11:25..11:30` 的 `i25 / i26` 最终要修成正确值

因此最安全的方案不是“不修”，而是：

- **只修该修的两类指标**

---

## 6. 目标架构

## 6.1 拆分 dirty domain

当前 runtime 只有一套：

- `dirty_recompute_from`
- `dirty_recompute_end`
- `dirty_recompute_truncated`

它默认代表：

- finalized canonical truth 变化
- 需要整段 truncation + finalize replay

本方案要求新增第二套专项 patch frontier：

- `oi_ratio_patch_from: Option<DateTime<Utc>>`
- `oi_ratio_patch_end: Option<DateTime<Utc>>`

语义是：

- 有 finalized minute 的 OI / ratio 视图发生变化
- 但这个变化只需要修补 `i25 / i26`
- 不需要触发全量 finalized suffix truncate

### 原则

- `dirty_recompute_*` 继续只服务于 `trade/orderbook/liq/funding` 这类 canonical 1m 主输入
- `oi_ratio_patch_*` 只服务于 `open_interest_hist_5m / long_short_ratio_5m`

---

## 6.2 ingest / state_store 层改造

### 当前问题

[store_open_interest_hist_5m(...)](/data/systems/indicator_engine/src/runtime/state_store.rs#L1816) 和 [store_long_short_ratio_5m(...)](/data/systems/indicator_engine/src/runtime/state_store.rs#L1835) 现在直接调用全局 dirty 标记。

### 目标改法

改成两段式：

1. 先正常 upsert 进：
   - `open_interest_hist_5m`
   - `global_account_ratio_5m`
   - `top_account_ratio_5m`
   - `top_position_ratio_5m`
2. 若 `changed == true && ts_bucket <= last_finalized_minute`
   - 不再调用 `mark_dirty_recompute_if_finalized(ts_bucket)`
   - 改为调用新的 `mark_oi_ratio_patch_if_finalized(ts_bucket)`

### `mark_oi_ratio_patch_if_finalized` 规则

- 若第一次触发：
  - `oi_ratio_patch_from = ts_bucket`
  - `oi_ratio_patch_end = last_finalized_minute`
- 若已有 patch pending：
  - `from = min(old_from, ts_bucket)`
  - `end = max(old_end, last_finalized_minute)`

### 关键点

- 这一步**不允许**触发 `truncate_finalized_suffix(...)`
- 因为 OI / ratio 的晚到修正，并不会让 flow/orderbook/event 历史变错

---

## 6.3 live emit 行为

这部分原则上**不改**：

- `next_minute_to_emit`
- `latest_contiguous_complete_canonical_minute_from(...)`
- `complete_under_current_policy()`

原因：

- 当前 live 发包的 ready 判定是对的
- 问题不在“ready 太早”
- 问题在“late OI / ratio 被错误地升级成了全局阻塞”

### 必须保证的一点

如果某个 OI / ratio late bucket 在当前分钟真正 finalize 之前已经进了 state：

- 当前分钟第一次 finalize 时
- [build_oi_ratio_view_for_minute(...)](/data/systems/indicator_engine/src/runtime/state_store.rs#L2070)
- 就应该直接吃到最新 `latest_common_oi_ratio_bucket`

换句话说：

- `11:30` 分钟在 `11:30:29` 之后、真正被处理时
- 应该直接用上 `11:25` 的公共 `5m` 桶
- 不需要先把 `11:25..11:29` 全部回放完，才允许发 `11:30`

---

## 6.4 新增 OI / Ratio 专项 patch worker

这是本方案的核心。

### patch 范围

patch 处理范围：

- `from = oi_ratio_patch_from`
- `to = oi_ratio_patch_end`

一般会是：

- `late_bucket .. last_finalized_minute`

例如：

- `11:25` 的 OI / ratio 晚到
- 当前 `last_finalized_minute = 11:30`
- 那 patch range 就是 `11:25..11:30`

### patch 行为

对每个 minute：

1. 基于当前 runtime state 构造该 minute 的 `IndicatorContext`
2. 只运行：
   - `open_interest`
   - `long_short_ratios`
3. 只重写：
   - `feat.open_interest_feature`
   - `feat.long_short_ratio_feature`
   - `feat.indicator_snapshot` 中 `indicator_code in ('open_interest', 'long_short_ratios')` 的行
4. 不重写：
   - `footprint`
   - `orderbook_depth`
   - `cvd_pack`
   - `divergence`
   - 各类 event 表

### 为什么可行

因为 snapshot 现在本来就是按：

- `(ts_snapshot, symbol, indicator_code, window_code)`

幂等写入，见 [snapshot_writer.rs](/data/systems/indicator_engine/src/storage/snapshot_writer.rs#L17) 一带的 upsert 语义。

这意味着：

- 只修补两类 indicator 的 snapshot 行
- 在数据模型上是完全成立的

---

## 6.5 bundle 对外一致性

仅更新 `feat.*` 和 snapshot 还不够。

因为 minute bundle 是从 snapshot 重建的，见 [outbox_dispatcher.rs](/data/systems/indicator_engine/src/publish/outbox_dispatcher.rs#L384)。

而 payload cache 也是按：

- `(symbol, ts_bucket)`

缓存 bundle 的，见 [snapshot_writer.rs](/data/systems/indicator_engine/src/storage/snapshot_writer.rs#L352)。

所以专项 patch 完成后，还必须做两件事：

1. 为受影响的 minute 重新生成 minute bundle outbox message
2. 覆盖 `ops.indicator_bundle_payload_cache` 里同 `(symbol, ts_bucket)` 的 payload

这一步是必须的，因为：

- 如果只修 snapshot，不补发 bundle
- 下游还会继续拿到旧 bundle

### 推荐策略

对 patch range 中的每个 minute：

- 读取该 minute 全量 snapshots
- 按现有 minute bundle builder 重新组装 payload
- 重新 upsert：
  - `ops.indicator_bundle_payload_cache`
- 再 enqueue：
  - `ops.indicator_bundle_outbox`

### 重要约束

这里不需要像 overlap repair 一样：

- `DELETE feat.indicator_snapshot tail`
- `DELETE payload cache tail`
- `DELETE outbox tail`

专项 patch 只应做：

- **按 minute 定点覆盖**

不应再做“整段尾巴先删再建”。

---

## 6.6 调度优先级

OI / ratio patch 不应阻塞 live 主路径。

推荐优先级：

1. 先处理当前 tick 的 live ready minute
2. 再在剩余 budget 内处理 `oi_ratio_patch`

即：

- `dirty_recompute` 仍然高优先级
- `oi_ratio_patch` 为低优先级、可分批 drain

原因：

- `dirty_recompute` 代表 canonical 1m truth 变化，影响更广
- `oi_ratio_patch` 只影响 `i25 / i26`

### 推荐批次控制

新增：

- `OI_RATIO_PATCH_BATCH_SIZE`
- `OI_RATIO_PATCH_WINDOW_BUDGET_PER_TICK`

这样可以避免：

- 某次补 `11:25..11:55`
- 又把主循环拉成长阻塞

---

## 7. 精度边界与正确性说明

## 7.1 为什么不能直接忽略晚到 OI / ratio

因为 [build_oi_ratio_view_for_minute(...)](/data/systems/indicator_engine/src/runtime/state_store.rs#L2070) 的定义是：

- `as_of_ts = minute + 1m`
- 从四条 `5m` 序列中找到 `latest_common_bucket`
- 所有窗口都只能截到这个公共桶

因此：

- 当 `11:25` 这组桶晚到时
- `11:25..11:30` 这些分钟的 `latest_common_bucket` 都会变化
- `i25 / i26` 的正确输出也会变化

所以不能靠“不修”来换实时性。

## 7.2 为什么“只修 i25 / i26”是安全的

因为从代码依赖上看：

- OI / ratio 只喂给 `i25 / i26`
- 其他指标没有引用这组 sidecar

所以：

- 不让它们触发全量 finalized replay
- 不会让其他指标数学上漂掉

## 7.3 当前分钟首发与历史补发的关系

存在两种情况：

### 情况 A：late bucket 在当前分钟首发前已到达

例如：

- `11:25` 桶在 `11:30:29` 到达
- `11:30` 分钟在 `11:31:05` 才真正 finalize

那：

- `11:30` 第一次发包就应该是正确的

### 情况 B：late bucket 在当前分钟首发后才到达

那：

- 当前分钟第一次发包先按旧 `latest_common_bucket`
- 随后由 `oi_ratio_patch` 补发修正版

这两种情况都不损伤准确性。

---

## 8. 需要改的模块

## 8.1 `indicator_engine/src/runtime/state_store.rs`

需要新增：

- `oi_ratio_patch_from`
- `oi_ratio_patch_end`
- `mark_oi_ratio_patch_if_finalized(...)`
- `has_pending_oi_ratio_patch()`
- `pending_oi_ratio_patch_batch_range(...)`

需要修改：

- `store_open_interest_hist_5m(...)`
- `store_long_short_ratio_5m(...)`

明确不改：

- `complete_under_current_policy()`
- `build_oi_ratio_view_for_minute(...)` 的数学定义

## 8.2 `indicator_engine/src/app/runtime.rs`

需要新增：

- `process_pending_oi_ratio_patch(...)`

处理顺序建议：

1. 先 live ready minute
2. 再 dirty recompute
3. 再 oi_ratio patch

或者：

1. dirty recompute
2. live ready minute
3. oi_ratio patch

二者都可以，但**oi_ratio patch 必须在 live 主路径之后**。

## 8.3 `indicator_engine/src/runtime/dispatcher.rs`

需要支持“只跑某个指标子集”的执行模式，例如：

- `process_indicator_subset(ctx, ["open_interest", "long_short_ratios"])`

不要为了 patch 两个指标，还把：

- `footprint`
- `tpo_market_profile`
- `divergence`

全部再跑一遍。

## 8.4 `indicator_engine/src/storage/feature_writer.rs`

现有：

- [insert_open_interest_feature_windows(...)](/data/systems/indicator_engine/src/storage/feature_writer.rs#L678)
- [insert_long_short_ratio_feature_windows(...)](/data/systems/indicator_engine/src/storage/feature_writer.rs#L756)

这两个函数本身已经适合复用。

需要做的是：

- 给专项 patch 路径暴露可重入调用方式
- 不和完整 `process_window()` 绑定死

## 8.5 `indicator_engine/src/storage/snapshot_writer.rs`

需要支持：

- 定点 upsert 两类 indicator 的 snapshot
- 定点为某个 minute 重新 enqueue minute bundle

明确不应复用当前 overlap repair 的“整段 delete tail 再重建”策略，因为那会重新引入全局阻塞。

## 8.6 `indicator_engine/src/publish/ind_publisher.rs`

建议新增可选 headers：

- `repair_reason = "oi_ratio_patch"`
- `repair_scope = "indicator_subset"`

这样下游看到重复 `ts_bucket` bundle 时，能知道这是修正版，不是重复噪音。

这不是数学正确性的前置，但非常利于排障。

---

## 9. 日志与可观测性

这次问题之所以排查慢，一个重要原因就是：

- 现在只能看到 `dirty_recompute_pending=true`
- 但看不出 dirty 是哪类源触发的

因此专项 patch 落地时，日志必须一起补。

## 9.1 state_store / ingest 日志

新增计数和结构化字段：

- `oi_ratio_patch_mark_total`
- `oi_ratio_patch_mark_minute`
- `oi_ratio_patch_mark_reason`
  - `open_interest_hist_5m_changed`
  - `long_short_ratio_5m_changed`
- `oi_ratio_patch_extends_backward`

## 9.2 runtime 执行日志

每次 patch batch 记录：

- `reason = "oi_ratio_patch"`
- `from_ts`
- `to_ts`
- `windows_processed`
- `elapsed_ms`
- `batch_size`
- `live_windows_skipped_due_to_budget`

## 9.3 区分两类 replay

日志里必须明确分开：

- `dirty_recompute_pending`
- `oi_ratio_patch_pending`

否则后面仍然很难一眼看出：

- 是 canonical 1m truth 在拖
- 还是 OI / ratio sidecar 在拖

## 9.4 关键 SLI

建议新增：

- `indicator_live_ready_to_bundle_ms`
- `indicator_oi_ratio_patch_backlog_minutes`
- `indicator_oi_ratio_patch_total_ms`
- `indicator_oi_ratio_patch_windows_total`
- `indicator_oi_ratio_patch_republish_total`

---

## 10. 验收标准

本方案落地后，至少要满足下面 5 条。

### 10.1 live 不再被 OI / ratio patch 卡住

当 `11:25` 的 OI / ratio 在 `11:30:29` 到达时：

- 不应再看到：
  - `11:25 -> 11:30` 整段阻塞 replay 才允许发 `11:30`

### 10.2 当前分钟首发尽量直接吃到最新公共桶

若 `11:30` 首发时，`11:25` 公共桶已进 state：

- `11:30` 的 `i25 / i26` 第一次发包即应反映 `latest_common_oi_ratio_bucket = 11:25`

### 10.3 历史 minute 最终仍被修正

若某些分钟已经先发了旧版：

- 后续仍应通过专项 patch 收敛到正确值

### 10.4 非 OI / ratio 指标不重复跑

当 only OI / ratio late arrival 发生时：

- 不应再看到：
  - `footprint`
  - `cvd_pack`
  - `divergence`
  - `event` 表

被整段重复回写

### 10.5 下游能收到修正版 bundle

专项 patch 完成后：

- 对应 `ts_bucket` 的 minute bundle 应重新发送
- `temp_indicator/<ts_bucket>.json` 最终应被新 payload 覆盖

---

## 11. 推荐实施顺序

### Phase 1：拆状态与日志

- `state_store` 新增 `oi_ratio_patch_*`
- ingest 改为标记专项 patch，而非全局 dirty
- runtime 日志新增 `oi_ratio_patch_pending`

### Phase 2：做最小可用 patch executor

- 仅支持：
  - `i25`
  - `i26`
- 仅支持：
  - 重写 feature
  - 重写 snapshot

先不补 bundle republish。

目标是先确认：

- live 不再被卡住
- `i25 / i26` 数据库侧会被补正

### Phase 3：补齐 bundle republish

- patch 后重建 minute bundle
- upsert payload cache
- enqueue outbox

这样下游也能最终收到修正版。

### Phase 4：补齐监控与回归验证

- 验证 patch 不再触发全量 replay
- 验证 `llm stale skip` 次数下降
- 验证 `i25 / i26` 历史窗口和全量重算一致

---

## 12. 最终建议

如果只给一句执行建议，就是：

> 不要再让 `open_interest_hist_5m / long_short_ratio_5m` 调用全局 `mark_dirty_recompute_if_finalized()`；把它们改成只修 `i25 / i26` 的专项 patch 流。

这是当前代码结构下：

- 最不伤准确性
- 对 live 延迟改善最大
- 且和 [指标v2.md](/data/docs/指标v2.md) 里“只有 `i25 / i26` 允许晚到，其他指标不等”的既有原则完全一致

---

## 13. 相关代码位置

- [backfill_scheduler.rs](/data/systems/market_data_ingestor/src/state/backfill_scheduler.rs#L511)
- [state_store.rs](/data/systems/indicator_engine/src/runtime/state_store.rs#L1816)
- [state_store.rs](/data/systems/indicator_engine/src/runtime/state_store.rs#L1835)
- [state_store.rs](/data/systems/indicator_engine/src/runtime/state_store.rs#L1891)
- [state_store.rs](/data/systems/indicator_engine/src/runtime/state_store.rs#L2070)
- [state_store.rs](/data/systems/indicator_engine/src/runtime/state_store.rs#L326)
- [runtime.rs](/data/systems/indicator_engine/src/app/runtime.rs#L1986)
- [dispatcher.rs](/data/systems/indicator_engine/src/runtime/dispatcher.rs#L68)
- [i25_open_interest.rs](/data/systems/indicator_engine/src/indicators/i25_open_interest.rs)
- [i26_long_short_ratios.rs](/data/systems/indicator_engine/src/indicators/i26_long_short_ratios.rs)
- [feature_writer.rs](/data/systems/indicator_engine/src/storage/feature_writer.rs#L678)
- [feature_writer.rs](/data/systems/indicator_engine/src/storage/feature_writer.rs#L756)
- [snapshot_writer.rs](/data/systems/indicator_engine/src/storage/snapshot_writer.rs#L352)
- [outbox_dispatcher.rs](/data/systems/indicator_engine/src/publish/outbox_dispatcher.rs#L384)
