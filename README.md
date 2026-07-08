# b8im-im

b8im IM 运行时仓库，承载 register、gateway、business 等通信层进程。

## 目录

```text
im-register/   RegisterWorker 服务注册发现
im-gateway/    GatewayWorker WebSocket 接入层
im-business/   BusinessWorker 消息、鉴权、ACK、撤回等业务层
```

三个服务都通过 Composer path repository 依赖同级子模块仓库：

```text
../b8im-im-shared
```

从单个服务目录执行 Composer 命令时，相对路径是：

```json
{ "type": "path", "url": "../../b8im-im-shared" }
```

## 本机配置

各服务复制自己的示例配置：

```bash
cp im-register/.env.example im-register/.env
cp im-gateway/.env.example im-gateway/.env
cp im-business/.env.example im-business/.env
```

`im-business` 默认连接本机数据库：

```text
DB_NAME = nb8im
DB_USER = root
DB_PASSWORD = root
```

## Docker 构建

Dockerfile 需要同时读取 `b8im-im/` 和 `b8im-im-shared/`，请在
`b8im-dev-workspace` 根目录执行：

```bash
docker build -f b8im-im/im-register/Dockerfile -t b8im/im-register .
docker build -f b8im-im/im-gateway/Dockerfile -t b8im/im-gateway .
docker build -f b8im-im/im-business/Dockerfile -t b8im/im-business .
```
