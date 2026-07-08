# ---- im-register / RegisterWorker ----
# 注意：im-register 通过 composer path 依赖 ../../b8im-im-shared。
# 推荐在 b8im-dev-workspace 根目录执行：
#   docker build -f b8im-im/im-register/Dockerfile -t b8im/im-register .
FROM php:8.3-cli-alpine

# 安装 composer
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /app

# 先拷依赖清单 + 公共包，利用层缓存
COPY b8im-im-shared /b8im-im-shared
COPY b8im-im/im-register/composer.json /app/composer.json

RUN composer install --no-dev --no-interaction --prefer-dist --no-scripts || true

# 再拷源码
COPY b8im-im/im-register/ /app/

# RegisterWorker 监听端口（内部）
EXPOSE 1238

CMD ["php", "start.php", "start"]
