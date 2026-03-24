# Backfill优化方案 V2

## 1. 目标

在不改变 [指标.md](/data/docs/指标.md) 指标公式、窗口、阈值、完整性门槛的前提下，解决 `orderflow-indicator-engine` 在 backfill 尾段和追平 live 末段出现的大量慢 SQL 与长期 lag 问题。

本方案只讨论：

- 如何提升 backfill / replay / live catch-up 的吞吐
- 如何避免 `minute_bundle` 大 JSON 拖慢主路径
- 如何在提速的同时保持指标计算的准确性与稳定性

本方案不允许：

- 放松 required source 完整性
- 跳过某一分钟强行推进前沿
- 修改任一指标公式
- 先推 MQ、后慢慢补 DB，制造双写不一致

## 2. 当前问题复盘

最近日志和数据库复核表明，backfill 尾段和接近 live 时的核心瓶颈，已经不再是早期的共享 `ops.outbox_event`，而是新的 `ops.indicator_bundle_outbox` 热路径。

当前现象：

- `progress_commit_ms` 异常拉长
- `snapshot_write_ms` 偶发拉长
- `indicator_bundle_outbox` 的 `INSERT ... payload_json` 很慢
- `claim_batch` 会把 `payload_json` 整条 `RETURNING` 出来，再发 RabbitMQ

已确认事实：

- 当前 `minute_bundle` 单条消息约 `4.8 MB`
- `ops.indicator_bundle_outbox` 本身很小，常常 `0 rows`
- 因此主因不是表膨胀、不是队列堆积、不是 planner 选错
- 主因是：每分钟都在同步写入并同步读回一条超大 JSONB

这会导致：

- `advance_progress_with_outbox(...)` 被 outbox enqueue 拖慢
- `progress_commit_ms` 被同一事务带慢
- 即使计算已经完成，前沿仍然无法及时推进

## 3. 不可妥协的边界

本方案必须满足以下三条硬约束：

1. 必须按照 [指标.md](/data/docs/指标.md) 产出指标
2. 务必保证指标计算准确性
3. 务必保证指标计算稳定性

因此，必须坚持以下正确性边界：

- `feat.indicator_snapshot / level / event / feature` 是数据库中的核心真相结果
- `indicator_progress` 只能在该分钟核心结果已 durable 后推进
- RabbitMQ 发布可以异步，但不能早于正确性边界
- 不允许出现“MQ 已发，DB 还没写完或写失败”的双写不一致

## 4. 方案结论

合理方案不是“计算完立即发 RabbitMQ，DB 用异步线程慢慢补”。

真正合理的方案是：

### 4.1 三阶段解耦

把当前单线程串行主路径拆成三层：

- `Compute`
- `Persist`
- `Publish`

其职责分别为：

#### A. Compute 层

负责：

- 读取已经准备好的 `WindowBundle`
- 计算 24 个指标
- 生成：
  - `snapshots`
  - `levels`
  - `events`
  - `features`
  - `minute_bundle` 所需的中间结构

要求：

- 不做重 DB I/O
- 不等待 RabbitMQ
- 只产生“该分钟完整计算结果”

#### B. Persist 层

负责：

- 将该分钟核心结果 durable 写入数据库：
  - `feat.indicator_snapshot`
  - `feat.indicator_level_value`
  - `feat.liquidation_density_level`
  - `evt.*`
  - `feat.* feature`
- 在核心结果 durable 后推进 `feat.indicator_progress`
- 同时把用于 MQ 发布的轻量 durable 发布记录写入 outbox

要求：

- `indicator_progress` 的推进必须与核心持久化成功绑定
- 不允许只是把结果留在内存队列里就推进 `progress`

#### C. Publish 层

负责：

- 从 durable outbox 异步读取待发布消息
- 推送到 RabbitMQ
- 成功后清理 outbox

要求：

- 发布失败不能影响已经 durable 的分钟结果
- 发布慢不能反向拖死 Compute / Persist 主路径

### 4.2 关键原则

#### 原则一：可以异步的是 Publish，不是核心 Persist

可以异步：

- `minute_bundle` 的 MQ 发布
- `ind.snapshot` 的 fan-out 发布

不能异步到“慢慢补”的是：

- `feat.indicator_snapshot`
- `level`
- `event`
- `feature`
- `indicator_progress`

因为这些属于恢复和正确性边界。

#### 原则二：MQ 必须建立在 durable 基础上

正确顺序应该是：

1. 计算完成
2. 核心 DB 结果 durable
3. `indicator_progress` 推进
4. durable outbox 记录写入
5. RabbitMQ 异步发布

而不是：

1. 计算完成
2. 直接发 MQ
3. DB 之后慢慢补

后者会在进程崩溃时造成：

- LLM 收到分钟 bundle
- 数据库里却没有相应分钟的 `snapshot/feature/event`
- 下次重启 frontier 与下游消费状态分叉

## 5. 根治当前慢点的具体设计

### 5.1 把 `minute_bundle` 从主路径重 JSON 改成轻量 durable 引用

当前 `minute_bundle` 直接内嵌 24 个指标完整 payload，约 `4.8 MB`。

这正是当前慢 SQL 的第一主因。

改造方向：

- durable outbox 中不再保存完整大 payload
- durable outbox 只保存：
  - `symbol`
  - `ts_bucket`
  - `schema_version`
  - 轻量 headers
  - 可重建所需的最小标识
- 完整 `minute_bundle` 以压缩形式写入单独的 durable payload cache

由 Publish 层在发送前，按 `symbol + ts_bucket` 先从 durable payload cache 读取完整 bundle；仅在兼容旧数据时才回退到旧格式 payload 或重建逻辑。

这样做的效果：

- 主路径 insert 的不再是 `4.8 MB` JSONB
- `claim_batch` 也不再需要 `RETURNING payload_json`
- durable 边界仍然存在
- RabbitMQ 最终拿到的 bundle 内容不变

### 5.2 Persist 层和 Publish 层分池

当前主路径与 outbox dispatcher 争同一个数据库池，容易互相拖慢。

改造方向：

- `Persist` 用专用连接池
- `Publish` 用独立连接池

效果：

- outbox relay 慢，不再拖主路径写 snapshot/feature/progress
- 主路径连接不会被 claim/delete/listen 抢占

### 5.3 Publish 层只搬轻量 key，真正 payload 发送前再取回

当前 claim SQL 最大问题之一是：

- `RETURNING payload_json`

改造方向：

- claim 只返回：
  - `outbox_id`
  - `symbol`
  - `ts_bucket`
  - `routing_key`
  - `message_id`
- Publisher 收到后，再从 durable payload cache 按 `symbol + ts_bucket` 取回完整 bundle 并发布

效果：

- claim SQL 大幅变轻
- outbox 表不再承担大 JSON 临时缓存职责

### 5.4 `ind.snapshot` 继续保持异步 projector

当前 `ind.snapshot` 已经是异步 fan-out，这个方向是对的，应保留。

要求：

- projector 只从已持久化 `feat.indicator_snapshot` 构造消息
- overlap repair / rewind 时同步回退 projector checkpoint
- 不允许 projector 自己重算指标

## 6. 为什么该方案不影响指标准确性

本方案不修改：

- `WindowBundle`
- `IndicatorContext`
- 任一指标的 `evaluate()`
- `levels / events / features` 的计算逻辑
- backfill / replay 的 minute finalize 顺序

本方案只修改：

- 结果计算后如何进入 durable 存储
- durable 消息如何进入 RabbitMQ

因此：

- 指标数学语义不变
- 结果字段值不变
- 变的是吞吐和延迟，不是指标定义

## 7. 与“直接写库异步化”的区别

错误方案：

- 计算线程算完
- 先发 MQ
- DB 用后台线程慢慢补

问题：

- 崩溃时双写不一致
- 下游与数据库状态分叉
- 无法保证重启后的 frontier 正确复用

正确方案：

- 计算线程算完
- Persist durable 写入核心结果
- 进度推进
- 轻量 outbox durable
- Publish 异步发 MQ

所以：

- 是“计算 / 发布”解耦
- 不是“正确性边界也丢给异步线程慢慢补”

## 8. 落地顺序

建议顺序：

1. `indicator_bundle_outbox` 改成轻量 durable outbox
2. Publish 层改成按 `symbol + ts_bucket` 动态组装 `minute_bundle`
3. Persist / Publish 拆独立数据库连接池
4. 补回归测试与端到端校验

## 9. 验收标准

落地后应满足：

1. `progress_commit_ms` 不再因为大 JSON enqueue 飙升到 `10s~70s`
2. `claim_batch` 不再 `RETURNING payload_json`
3. `indicator_bundle_outbox` 插入耗时显著下降
4. replay 尾段和接近 live 时，不再出现“只差最后 2 小时反而越来越慢”
5. `minute_bundle` 内容与当前版本对下游保持兼容
6. 抽样校验结果继续满足：
   - 按 [指标.md](/data/docs/指标.md) 产出
   - 计算准确
   - 计算稳定

## 10. 当前判断

这套方案是当前已定位问题上的根修复方向。

原因不是“数据库单纯太慢”，而是：

- 主路径 durable outbox payload 过大
- claim 时又把大 payload 整条读回
- 发布链路反向拖住了 `indicator_progress`

所以，合理方案不是“再调数据库参数”，而是：

- 保留正确性边界
- 把重 payload 从主路径 durable 阶段移走
- 让发布真正异步
