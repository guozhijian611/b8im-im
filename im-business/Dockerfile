# ---- im-business / BusinessWorker ----
# 通过 composer path 依赖 ../../b8im-im-shared。
# 推荐在 b8im-dev-workspace 根目录执行：
#   docker build -f b8im-im/im-business/Dockerfile -t b8im/im-business .
FROM php:8.3-cli-alpine

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /app

COPY b8im-im-shared /b8im-im-shared
COPY b8im-im/im-business/composer.json /app/composer.json

RUN composer install --no-dev --no-interaction --prefer-dist --no-scripts || true

COPY b8im-im/im-business/ /app/

CMD ["php", "start.php", "start"]
