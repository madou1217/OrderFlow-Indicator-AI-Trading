# RabbitMQ Topology

当前文档记录的是本机 `/orderflow` vhost 的实际 RabbitMQ 业务拓扑，以及代码里的自动声明规则。

复核命令：

```bash
sudo rabbitmqctl list_exchanges -p /orderflow name type durable auto_delete internal arguments
sudo rabbitmqctl list_queues -p /orderflow name durable auto_delete arguments consumers state messages_ready messages_unacknowledged
sudo rabbitmqctl list_bindings --vhost /orderflow source_name destination_name destination_kind routing_key arguments
```

## Exchanges

当前业务 exchange：

- `x.md.live` (`topic`, durable)
- `x.md.replay` (`topic`, durable)
- `x.ind` (`topic`, durable)
- `x.dlx` (`topic`, durable)

这些 exchange 由以下启动逻辑幂等声明：

- `systems/market_data_ingestor/src/app/bootstrap.rs`
- `systems/indicator_engine/src/app/bootstrap.rs`
- `systems/llm/src/app/bootstrap.rs`

## Queues

当前 broker 上实际存在的业务队列与 TTL 如下：

| 队列名 | 来源/用途 | 当前绑定 | TTL | 其他参数 |
| --- | --- | --- | --- | --- |
| `q.ingestor.selfcheck` | `market_data_ingestor` 自检/旁路验证 | `x.md.live -> md.#` | 无 | 无 |
| `q.indicator.grp.ob_heavy` | `indicator_engine` 订单簿/爆仓重队列 | `x.md.live -> md.agg.*.orderbook.1m.ethusdt`, `md.agg.*.liq.1m.ethusdt` | `10800000ms` = 3 小时 | `x-max-length=200000`, `x-overflow=drop-head` |
| `q.indicator.grp.flow` | `indicator_engine` 成交流量队列 | `x.md.live -> md.agg.*.trade.1m.ethusdt` | `10800000ms` = 3 小时 | 无 |
| `q.indicator.grp.deriv` | `indicator_engine` 衍生品数据队列 | `x.md.live -> md.agg.*.funding_mark.1m.ethusdt`, `md.futures.open_interest.current.ethusdt`, `md.futures.open_interest.5m.ethusdt`, `md.futures.long_short_ratio.*.5m.ethusdt` | `10800000ms` = 3 小时 | 无 |
| `q.strategy.ind.minute` | 指标 bundle 下游策略消费 | `x.ind -> bundle.1m.*` | `300000ms` = 5 分钟 | `x-max-length-bytes=209715200`, `x-overflow=drop-head` |
| `q.llm.ind.minute` | `orderflow-llm` 主消费队列 | `x.ind -> bundle.1m.*` | 无 | 无 |
| `q.llm.ind.minute.watcher_fast.ethusdt` | `orderflow-llm` watcher 快速事件队列 | `x.md.live -> md.futures.mark_price.ethusdt`, `md.agg.futures.trade.1s.ethusdt`, `md.futures.kline.1m.ethusdt` | `900000ms` = 15 分钟 | `x-max-length=4096`, `x-overflow=drop-head` |
| `q.monitor.ind.events` | 指标事件监控/观察队列 | `x.ind -> evt.*.ethusdt` | `300000ms` = 5 分钟 | `x-max-length-bytes=209715200`, `x-overflow=drop-head` |

## Dynamic Rules

- 当前 watcher 快速队列名后缀是 `ethusdt`，它来自当前运行 symbol；如果以后切换 symbol，队列名和 binding routing key 也会一起变。
- `q.llm.ind.minute` 当前没有 TTL，配合 `llm.purge_queue_on_start=true` 使用；broker 不会自动过期旧 bundle，是否清 backlog 由 LLM 服务启动时决定。
- replay 消费队列不会长期常驻。`indicator_engine` 在 replay/repair 场景下会按实例动态创建 `q.indicator.replay.{key}.{instance_suffix}.{idx}`，属性是 `durable=false`、`auto_delete=true`、无 TTL。

## Config Sources

静态队列配置主要来自：

- `config/config.yaml -> mq`
- `systems/market_data_ingestor/src/app/bootstrap.rs`
- `systems/indicator_engine/src/app/bootstrap.rs`
- `systems/llm/src/app/bootstrap.rs`

当前和 TTL 直接相关的配置点：

- `config/config.yaml -> mq.queues.indicator_group_ob_heavy.message_ttl_ms`
- `config/config.yaml -> mq.queues.indicator_group_flow.message_ttl_ms`
- `config/config.yaml -> mq.queues.indicator_group_deriv.message_ttl_ms`
- `config/config.yaml -> mq.queues.strategy_indicator_minute.message_ttl_ms`
- `config/config.yaml -> mq.queues.monitor_indicator_events.message_ttl_ms`
- `systems/llm/src/app/bootstrap.rs -> WATCHER_FAST_QUEUE_TTL_MS`
