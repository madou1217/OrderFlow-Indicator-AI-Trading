# LLM层工作流修改方案 v3.0.0

## 1. 唯一目标

本版只服务于这一个目标：

`Stage2A` 不再是“默认基于 15m 微结构来审核 path 并顺手给出近止损”的模块；它应当是“以 stage1输出的current_path 为主语的审核 + 执行设计器”，其中 `15m` 只决定 entry price 更适合落在哪个局部位置。


## 2. 第一性原理

- 高时间框 thesis，应该由高时间框审核
- 高时间框执行，应该由高时间框风险锚定
- 低时间框只该优化 timing，不该默认决定生死


## 3. 新职责边界

### 3.1 Stage1

本版对 `Stage1` 不做任何修改。

### 3.2 Stage2A

`Stage2A` 继续负责 path 审核，但审核逻辑要改：

- 它审核的是：`Stage1.current_path` 这条 path 还活不活
- 它输出的是：服务于这条 path 的执行设计
- 它不再默认用 `15m` 微结构去决定 path 的生死


### 3.3 15m

`15m` 在 `Stage2A` 里的职责收缩为：

- 决定 entry price 更适合落在哪个局部位置

`15m` 不再默认负责：

- 定义 path 是否失效
- 定义 `stop_loss` 的主锚点


## 4. 输入重构

`Stage2A` 的输入只保留三层。

### A. Path 主语层

这一层是核心层，权重最高。

必须包含：

- `stage1_output`
- `strategic_context_frozen`
- `Stage1.current_path.tracked_zones`
- `stage1.current_path` 的位置、状态、驱动、结构信息

这一层只回答一个问题：

这条 `stage1.current_path` path 现在是否仍然成立。

### B. 高时间框执行层

这一层用于设计执行，而不是重新找 path。

必须回答：

- 高时间框激活区在哪里
- 高时间框承接区在哪里
- 高时间框失效区在哪里
- 到 `TP1 / TP2` 是否仍有足够 runway

这一层决定：

- `entry_activation_level`
- `entry_zone`
- `entry_invalidation_level`
- `stop_loss`

### C. 15m timing 层

建议保留：

- 最近 `15m` K 线节奏
- 压缩后的 `15m` 位置结论
- 压缩后的 `15m` timing 结论

建议删除：

- 原始长事件流
- 细粒度 footprint 明细
- 会诱导模型把 stop 锚在最近微结构边上的细噪声描述

原则只有一句：

`15m` 决定 entry price 更适合落在哪个局部位置。


## 5. 输出与校验

### 5.1 Stage2A 输出语义

`Stage2A` 仍然输出：

- `entry_activation_level`
- `entry_zone`
- `entry_invalidation_level`
- `stop_loss`

但语义改为：

- `entry_activation_level`
  回答这笔高时间框执行什么时候可以开始

- `entry_zone`
  回答这笔高时间框执行真正的承接区在哪里

- `entry_invalidation_level`
  回答这笔高时间框执行在结构上何时失效

- `stop_loss`
  回答 execution 应该在哪里做真实风险终止

本版不要求 `Stage2A` 额外输出 timeframe。 因为`Stage2A`的timeframe 就是stage1.current_path的timeframe。

`Stage2A` 只需要看懂 `Stage1` 已有 path 的高时间框背景，然后输出价格区与止损设计即可。

parser 不需要校验

### 5.2 默认规则

prompt 与 parser 应共同保证以下规则：

1. `Stage2A` 先按 `Stage1.current_path`  判断正常回撤容忍，再设计 entry和 stop loss。
2. `15m` 作为背景数据来微调 trigger 和 entry price。

## 6. 本版不做的事

本版明确不处理：

- `Stage2B`
- `Stage2C`
- TTL
- watcher 生命周期


## 7. 落地顺序

### 步骤 1

重组 `Stage2A` 输入：

- 新增并强化 `4h / 1d`窗口的输入源，让stage2a的执行所需要的数据与stage1给出path的数据尽量保持在时间窗口一致。
- 保留瘦身后的 `15m`
- 不再让 `15m` 成为默认风险锚定层

### 步骤 2

重写 `workflow_stage2a/base.txt`：

- 明确 `Stage2A` 仍然审核 path
- 明确审核主语是 `Stage1` 给出的Stage1.current_path
- 明确 `15m` 只做 timing

### 步骤 3

修改 parser：

- 不做校验，只做stage2a 给出的交易策略的解析


