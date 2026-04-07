# LLM层工作流的修改方案 v4.0.0

## 1. 版本定位

本版是方案冻结版，不进入实现。

本版只回答一个问题：

在既定架构下，若未来要继续优化 `Stage1`，应如何基于第一性原理扩展 `3d / 7d / 30d` 窗口，使 `Stage1` 更稳定地产出 `4h-1d` 的唯一高质量 path。

本版明确不做：

- 不修改代码
- 不修改 prompt
- 不修改 schema
- 不修改 config
- 不修改 parser
- 不修改 `Stage2A / Stage2B / Stage2C` 的现有实现

原因很简单：

当前长窗口数据准备度不足，尤其是 `7d / 30d` 在不同指标上的稳定性、覆盖率、历史连续性、空值率，还不足以支持一次可靠上线。因此本版先沉淀为设计文档，后续待数据条件满足后再实施。


## 2. 当前分工共识

当前整体分工维持不变：

- `Stage1`：产出 `4h-1d` 的唯一高质量 path
- `Stage2A`：复核 `Stage1` path，并给出 `entry` 与 `sl`
- `Stage2B`：做持仓管理，以最大收益为核心目的
- `Stage2C`：做挂单管理，以最大收益为核心目的

因此，本版讨论的所有窗口扩充，都只能服务于 `Stage1` 的“唯一高质量 path 选择”目标，不能把 `Stage1` 改造成另一个 `Stage2A`，也不能让 `30d` 反客为主变成主交易时间框。


## 3. 当前代码状态

当前代码已经体现出一部分正确方向，但还没有形成统一、闭环、严格约束的窗口哲学。

### 3.1 Stage1 prompt 的当前定位

`Stage1` 目前已经明确写出：

- 使用 `7D / 3D / 4H / 1D` 战略输入
- `7D / 3D` 是外层背景与 regime constraint
- `4H / 1D` 是直接的 path geometry anchor

参考：

- [workflow_stage1/base.txt](/data/systems/llm/src/llm/prompt/workflow_stage1/base.txt#L5)
- [workflow_stage1/base.txt](/data/systems/llm/src/llm/prompt/workflow_stage1/base.txt#L16)

这说明，本版不是从零设计，而是把这套思路系统化、收口化。

### 3.2 已经扩窗的输入

当前 `Stage1` 入口过滤层已经保留了一部分长窗口：

- `avwap`：`30d / 7d / 3d / 1d / 4h`
- `price_volume_structure`：`7d / 3d / 1d / 4h`

参考：

- [code_layer_entry.rs](/data/systems/llm/src/llm/filter/code_layer_entry.rs#L13)
- [code_layer_entry.rs](/data/systems/llm/src/llm/filter/code_layer_entry.rs#L14)

### 3.3 已经被有意识压缩的输入

当前也有一些指标仍然只保留较短的战略窗口，说明系统已经在隐含地区分“结构信息”和“状态/触发信息”：

- `rvwap_sigma_bands`：当前过滤层只保留 `15m / 4h / 1d`
- `funding_rate`：当前过滤层只保留 `15m / 4h / 1d`，同时额外提供 `recent_7d` 聚合摘要
- `liquidation_density`：当前过滤层只保留 `15m / 4h / 1d`，同时额外提供 `recent_7d` 聚合摘要
- `vpin`：当前过滤层保留到 `3d`
- `whale_trades`：当前过滤层保留到 `3d`
- `footprint`：当前过滤层保留 `15m / 4h / 1d`
- `orderbook_depth`：当前过滤层保留 `15m / 1h`

参考：

- [core_shared.rs](/data/systems/llm/src/llm/filter/core_shared.rs#L225)
- [core_shared.rs](/data/systems/llm/src/llm/filter/core_shared.rs#L614)
- [core_shared.rs](/data/systems/llm/src/llm/filter/core_shared.rs#L694)
- [code_layer_entry.rs](/data/systems/llm/src/llm/filter/code_layer_entry.rs#L240)
- [core_shared.rs](/data/systems/llm/src/llm/filter/core_shared.rs#L571)
- [core_shared.rs](/data/systems/llm/src/llm/filter/core_shared.rs#L261)
- [core_shared.rs](/data/systems/llm/src/llm/filter/core_shared.rs#L293)

### 3.4 当前还不够理想的地方

`open_interest` 与 `long_short_ratios` 当前仍是 raw 直通到 `Stage1` 的 `state_layer`，没有被专门裁剪成“战略版摘要”。

参考：

- [code_layer.rs](/data/systems/llm/src/workflow/code_layer.rs#L368)
- [i25_open_interest.rs](/data/systems/indicator_engine/src/indicators/i25_open_interest.rs#L8)
- [i26_long_short_ratios.rs](/data/systems/indicator_engine/src/indicators/i26_long_short_ratios.rs#L9)

这意味着理论上 `5m / 15m / 4h / 1d / 3d / 7d / 30d` 都可能一起进入 `Stage1` 语境。对于“唯一高质量 path 选择器”来说，这比“少几个长窗口”更危险，因为它会引入不必要的短窗噪声和窗口竞争。


## 4. 第一性原理

本版采用以下五条第一性原理。

### 4.1 Stage1 的任务不是解释市场，而是筛掉错误 path

`Stage1` 的核心不是给出最完整的市场描述，而是从多个可能路径中排除大多数低质量路径，只保留一条当前最值得跟踪的 `4h-1d` path。

因此，输入设计的优先级应该是：

1. 提高 path 唯一性
2. 提高 path 稳定性
3. 提高 path 的可辩护性
4. 最后才是增加解释丰富度

### 4.2 不同窗口承担不同职责

窗口不是越多越好，而是必须按职责分层：

- `4h / 1d`：定义 path
- `3d`：连接 path 与外层结构
- `7d`：做 regime filter
- `30d`：做 macro veto 或 anchor

如果一个窗口既参与 path 定义，又参与 veto，又参与 entry 细化，模型就会发生角色混乱。

### 4.3 结构信息和状态信息不能同权扩窗

结构类指标天然更适合长窗口，因为它们描述的是“价格未来最可能遇到的墙和路”。

状态类、驱动类、触发类指标不适合无差别扩到 `30d`，因为它们的半衰期更短，更容易把旧信息误当成当前约束。

### 4.4 30d 不能拥有 path 主导权

`30d` 在本架构下只应该回答：

- 当前 `4h-1d` path 是否逆着更大结构？
- 当前 path 前方是否存在更高一级的宏观压制区或承接区？
- 当前 crowding / vol / leverage 是否已经处于极端区？

`30d` 不应该直接画 entry path，也不应该决定 `Stage1` 的交易时间框。

### 4.5 扩窗不能以 token 堆积为代价

窗口一旦扩张，如果仍然保留原始字段密度，模型很快会被噪声吞掉。

因此，真正可用的扩窗，必须是：

- 扩窗
- 同时压缩字段
- 同时限定语义

否则不是增强，而是稀释。


## 5. 本版核心提案

本版提案不是“所有指标都加 `3d / 7d / 30d`”，而是：

建立一套 `Stage1` 专用的选择性扩窗体系。

这套体系的原则只有一句：

只有那些能提高 `4h-1d` path 唯一性和胜率的长窗口，才允许进入 `Stage1`；其余指标只能保留短窗，或者只保留长窗摘要。


## 6. 窗口角色定义

### 6.1 4h / 1d

`4h / 1d` 是 `Stage1` 的 path 几何主层。

它们负责：

- 定义当前方向
- 定义 activation zone
- 定义 target zone
- 定义 failure level
- 定义 tracked_zones 的主要几何关系

### 6.2 3d

`3d` 是桥接层。

它负责：

- 判断 `4h / 1d` path 是否有更高一级结构支撑
- 解释当前 path 是顺势延续、拥挤反转，还是 value return
- 给 `1d` 提供更外一层参考锚点

### 6.3 7d

`7d` 是 regime filter。

它负责：

- 判断当前 `4h-1d` path 是否顺着中周期结构
- 判断 path 是否落在更宽的价值区内外
- 判断 crowding / leverage / vol 是否处于支持或压制状态

`7d` 不直接画 path，只负责提高或降低 path 的成立概率。

### 6.4 30d

`30d` 是 macro veto / macro anchor。

它负责：

- 提供大级别 AVWAP / POC / value area / regime anchor
- 提供是否存在大级别逆风结构的 veto
- 提供是否已经处于宏观极端状态的提醒

`30d` 在 `Stage1` 中只能以轻量摘要进入，不得以高细节原始结构进入。


## 7. 指标分层提案

### 7.1 应优先扩到 7d，并有条件扩到 30d-lite 的指标

这些指标属于 path 结构定义层，最适合承担长窗口信息。

| 指标 | 未来建议窗口 | 角色 |
| --- | --- | --- |
| `avwap` | `30d / 7d / 3d / 1d / 4h` | 主锚点体系，`30d` 提供大锚，`7d` 提供 regime 锚 |
| `price_volume_structure` | `30d-lite / 7d / 3d / 1d / 4h` | 结构墙与路径几何 |
| `tpo_market_profile` | `30d-lite / 7d / 3d / 1d / 4h` | auction location 与价值接受度 |
| `ema_trend_regime` | `30d-lite / 7d / 3d / 1d / 4h` | 趋势级别与顺逆势过滤 |
| `fvg` | `7d / 3d / 1d / 4h` | 中高时间框结构缺口定位 |

其中：

- `30d-lite` 指只保留极少数字段，如 `poc / vah / val / regime_label / nearest_structure`
- 不保留完整长数组
- 不保留完整事件历史
- 不保留细粒度 dev series

### 7.2 应扩到 7d，但 30d 只允许摘要进入的指标

这些指标属于状态与约束层，适合做 veto 或强化，不适合主导 path。

| 指标 | 未来建议窗口 | 角色 |
| --- | --- | --- |
| `open_interest` | `7d / 3d / 1d / 4h`，外加 `30d` 百分位摘要 | leverage build/unwind 约束 |
| `long_short_ratios` | `7d / 3d / 1d / 4h`，外加 `30d` crowding percentile | crowding 约束 |
| `options_surface` | `7d-lite / 3d / 1d / 4h` | vol/skew regime 约束 |
| `funding_rate` | `7d-summary + 1d / 4h` | 拥挤状态和 squeeze/unwind 风险 |
| `vpin` | `3d / 1d / 4h`，最多加 `7d-summary` | 流动性压力与 toxic flow 背景 |
| `liquidation_density` | `7d-summary + 1d / 4h` | squeeze path 和清算墙分布 |

这里最重要的原则是：

这些指标即使拿到 `30d`，也不应直接把原始 `by_window["30d"]` 整块喂给 `Stage1`，而应该只喂 percentile、extreme_state、compression_state、crowding_state 一类摘要字段。

### 7.3 不建议扩到 7d / 30d 原始窗口的指标

这些指标半衰期短，只适合做局部确认，不适合上升为长期结构约束。

| 指标 | 建议 |
| --- | --- |
| `footprint` | 只保留 `15m / 4h / 1d` 确认摘要 |
| `orderbook_depth` | 只保留 `15m / 1h` 局部流动性信息 |
| `absorption` | 不扩长窗，只保留事件摘要 |
| `initiation` | 不扩长窗，只保留事件摘要 |
| `buying_exhaustion` | 不扩长窗，只保留事件摘要 |
| `selling_exhaustion` | 不扩长窗，只保留事件摘要 |
| `high_volume_pulse` | 不扩长窗原始窗，只保留摘要 |
| `whale_trades` | 最多到 `3d` |
| `cvd_pack` | 最多到 `3d`，不建议扩 `7d/30d` 原始窗 |


## 8. Stage1 未来输入合同方向

未来如果实施，`Stage1` 的输入合同应从“指标各自带自己的窗口”进一步收口为“窗口有明确角色”。

### 8.1 Stage1 的未来结构目标

`Stage1` 最终应显式形成四层：

- `path_geometry_layer`
- `regime_constraint_layer`
- `driver_support_layer`
- `confirmation_layer`

### 8.2 未来窗口映射原则

- `path_geometry_layer` 以 `4h / 1d` 为主，`3d` 为桥接，允许 `7d/30d-lite` 提供外层锚点
- `regime_constraint_layer` 以 `3d / 7d` 为主，`30d` 只允许摘要
- `driver_support_layer` 以 `4h / 1d / 3d` 为主
- `confirmation_layer` 只保留压缩确认摘要，不参与长窗扩张

### 8.3 Stage1 输出语义不变

无论输入未来如何扩窗，`Stage1` 的输出语义不变：

- 仍然只输出一条 path 或 `no_edge`
- 仍然服务于 `4h-1d`
- 仍然要求 target 与 failure 都来自真实 `4h / 1d` 结构

换句话说，扩窗只改变 `Stage1` 的判断质量，不改变 `Stage1` 的角色。


## 9. 对 Stage2A / Stage2B / Stage2C 的影响边界

本版虽然不实施，但需要提前明确边界，避免未来扩窗后职责漂移。

### 9.1 对 Stage2A 的影响

未来如果 `Stage1` 真的引入更严格的 `7d / 30d-lite` 过滤，那么 `Stage2A` 的收益是：

- path 本身会更干净
- `Stage2A` 更少遇到“path 看起来活着，但其实中周期背景已经不支持”的情况

但 `Stage2A` 的职责不变：

- 它仍然审核 `Stage1.current_path`
- 它仍然负责 `entry` 和 `sl`
- 它不负责重新定义 `30d` 结构

当前 `Stage2A` 冻结上下文仍然明显偏 `4h / 1d`，这与本版目标并不冲突。

参考：

- [stage2_input.rs](/data/systems/llm/src/workflow/stage2_input.rs#L863)

### 9.2 对 Stage2B / Stage2C 的影响

`Stage2B / Stage2C` 的核心目标是收益最大化，不是 path 生成。

因此，未来就算 `Stage1` 上了 `7d / 30d-lite`，也不代表 `Stage2B / Stage2C` 必须同步引入同等级长窗细节。管理层更应该关注：

- 当前 path 是否仍有效
- 当前仓位或挂单是否应继续利用这条 path
- 实际收益保护和扩张


## 10. 为什么现在不做

本版不实施，根本原因不是设计不清楚，而是数据条件还不够。

至少存在以下四类现实约束：

### 10.1 长窗口 readiness 不稳定

长窗口越长，对连续历史、缺口修补、数据库回填、样本覆盖率的要求越高。

### 10.2 不同指标的数据质量不对称

结构类指标和状态类指标对 `7d / 30d` 的适配程度并不相同。

如果在数据不齐时统一扩窗，最后进入模型的不是“更多信息”，而是“更多空值和更多不一致语义”。

### 10.3 30d 信息最容易制造伪约束

一旦 `30d` 数据不稳，模型会产生两类坏结果：

- 把本该成立的 `4h-1d` path 错误 veto
- 把无效的长期噪声当成必须尊重的宏观结构

### 10.4 目前缺少扩窗后的回放验证闭环

在没有足够 replay / review 数据之前，很难证明：

- `no_edge` 是否会明显增多
- path 唯一性是否提升
- `Stage2A` 收到的 path 是否更可执行


## 11. 未来实施前的准入条件

后续若要启动实施，至少应满足以下条件。

### 11.1 数据条件

- `7d / 30d` 相关指标具备稳定 backfill
- 长窗口空值率可控
- 长窗口 coverage ratio 可控
- 不同指标的时间对齐机制稳定

### 11.2 结构条件

- `open_interest` 与 `long_short_ratios` 先从 raw 直通改成战略过滤版
- `30d` 输入统一收敛为 lite 摘要，不允许各指标各自放大
- prompt、filter、schema 三处的窗口语义保持一致

### 11.3 验证条件

- 至少完成一轮 replay 级别的 path 对比
- 能比较扩窗前后 `no_edge` 比例变化
- 能比较扩窗前后 `Stage1` path 唯一性与稳定性变化
- 能检查 `Stage2A` 的 entry 质量是否改善而不是恶化


## 12. 未来实施顺序

未来若数据条件满足，建议按以下顺序推进。

### 步骤 1

先重构 `Stage1` 过滤层，而不是先改 prompt。

目标：

- 把 `open_interest` 与 `long_short_ratios` 改成战略版摘要
- 把 `30d` 输入统一收缩为 lite 摘要
- 明确每类指标允许出现的最大窗口

### 步骤 2

再重写 `Stage1 prompt`。

目标：

- 显式写清楚 `4h/1d` 定义 path
- `3d` 负责桥接
- `7d` 负责 regime filter
- `30d` 负责 veto / anchor

### 步骤 3

最后再决定是否同步微调 `Stage2A` 的 frozen context。

目标：

- 不让 `Stage2A` 失去与 `Stage1` 的中周期语义连续性
- 但也不把 `Stage2A` 变成新的大周期 path 生成器


## 13. 本版结论

本版结论可以压缩成四句话：

1. `Stage1` 的未来优化方向是选择性扩窗，而不是全指标全窗口。
2. `7d` 应主要承担 regime filter 职责，`30d` 应主要承担 macro veto / anchor 职责。
3. 最值得优先扩的是结构类指标；最不应该盲目扩的是触发类和微结构类指标。
4. 当前先不实施，等待长窗口数据条件成熟后再推进。

