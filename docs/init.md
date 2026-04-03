
## RabbitMQ 初始化

当前仓库默认 RabbitMQ 配置和现网一致：

- `host=127.0.0.1`
- `port=5672`
- `vhost=/orderflow`
- `user=guest`
- `password=guest`

对应位置：

- `config/config.yaml -> mq`
- `/etc/default/orderflow-common -> ORDERFLOW_MQ_PASSWORD`

新服务器初始化 RabbitMQ：

```bash
sudo apt-get update
sudo apt-get install -y rabbitmq-server
sudo systemctl enable --now rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
```

创建当前默认配置需要的 vhost 和权限：

```bash
sudo rabbitmqctl add_vhost /orderflow
sudo rabbitmqctl set_permissions -p /orderflow guest ".*" ".*" ".*"
sudo rabbitmqctl list_vhosts
sudo rabbitmqctl list_permissions -p /orderflow
```

如果你的新服务器没有 `guest` 用户，补创建：

```bash
sudo rabbitmqctl add_user guest guest
sudo rabbitmqctl set_permissions -p /orderflow guest ".*" ".*" ".*"
```

如果你想改成专用账号，例如 `orderflow`：

```bash
sudo rabbitmqctl add_user orderflow your_password
sudo rabbitmqctl set_permissions -p /orderflow orderflow ".*" ".*" ".*"
```

然后同步修改：

- `config/config.yaml -> mq.user`
- `config/config.yaml -> mq.password_env`
- `/etc/default/orderflow-common`

RabbitMQ 交换机和队列不需要手工在 UI 里创建。服务启动时会自动幂等声明。

当前 `/orderflow` vhost 的交换机、队列、TTL、binding 和动态队列规则已经单独整理到 [mq_topology.md](/data/docs/mq_topology.md)。

如果你想复核新服务器当前 broker 是否和现网一致，直接按 [mq_topology.md](/data/docs/mq_topology.md) 里的 `rabbitmqctl` 命令检查即可。

## 安装 systemd 服务

仓库根目录下的 `system_services/` 已保存当前机器正在使用的 4 个服务 unit 文件：

- `system_services/orderflow-market-data-ingestor.service`
- `system_services/orderflow-indicator-engine.service`
- `system_services/orderflow-llm.service`
- `system_services/orderflow-api.service`
- `system_services/orderflow-common.env.example`

这些 unit 文件保留的是当前机器上的真实路径和用户：

- `User=ubuntu`
- `WorkingDirectory=/data`
- `orderflow-api` 的代码目录是 `/home/tools/codex-proxy`
- `market-data-ingestor` 的启动命令是 `/home/ubuntu/.cargo/bin/cargo run -p market_data_ingestor`

如果你的新服务器用户名或部署路径不同，请先修改 `system_services/` 里的 unit 文件，再复制到 `/etc/systemd/system/`。

新服务器安装方式：

```bash
cd /data
sudo cp system_services/orderflow-market-data-ingestor.service /etc/systemd/system/
sudo cp system_services/orderflow-indicator-engine.service /etc/systemd/system/
sudo cp system_services/orderflow-llm.service /etc/systemd/system/
sudo cp system_services/orderflow-api.service /etc/systemd/system/
sudo cp system_services/orderflow-common.env.example /etc/default/orderflow-common
sudo systemctl daemon-reload
sudo systemctl enable orderflow-market-data-ingestor
sudo systemctl enable orderflow-indicator-engine
sudo systemctl enable orderflow-llm
sudo systemctl enable orderflow-api
```

## 构建与数据库初始化

数据库重建脚本见：

- `sql/rebuild_orderflow.sql`
- `sql/rebuild_orderflow_ops.sql`
- `sql/rebuild_all_databases.sh`
- `sql/README_rebuild.md`

新服务器重建数据库：

```bash
cd /data
PGPASSWORD=your_password bash sql/rebuild_all_databases.sh
```




1.请帮我从这个库https://github.com/uniteonline/OrderFlow-Indicator-AI-Trading下载代码到/data下，并把目录下的所有代码放到/data,不要根目录：OrderFlow-Indicator-AI-Trading

2.我现在需要你按/data/docs/init.md 的说明和/data/docs/mq_topology.md，帮我下载rabbitmq，并创建vhost，队列，以及队列的ttl，要求与config.yaml和配置严格一致

3.我现在需要你按/data/docs/init.md 的说明和/data/sql/rebuild_all_databases.sh以及/data/sql/README_rebuild.md，帮我下载postgresql并安装timescaledb插件，然后按要求创建表，索引，触发器

4.请帮我把/data/codex-proxy-20260402-105732.tar.gz 解包到/home/tools/codex-proxy目录下，把目录下的所有代码放到/home/tools/codex-proxy.

5.帮我把/home/tools/codex-proxy目录下的代码Npm install一下

6.帮我在本机创建一个ssh的key，我要加到github后台，便于后续对仓库直接管理

7.帮我创建一个16GB的swap

8.帮我安装shadowshock，在本机启动1080的转发，配置在/data/docs/init.md里

9.我需要你按init.md的服务配置，帮我在系统内创建好4个服务，并编译和启动以下2个orderflow-market-data-ingestor和orderflow-indicator-engine服务

