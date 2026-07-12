# syntax=docker/dockerfile:1.7

FROM php:8.3-cli-alpine AS runtime

ENV COMPOSER_ALLOW_SUPERUSER=1 \
    COMPOSER_CACHE_DIR=/tmp/composer-cache

RUN apk add --no-cache libzip tzdata \
    && apk add --no-cache --virtual .build-deps \
        $PHPIZE_DEPS \
        libzip-dev \
        linux-headers \
    && docker-php-ext-install -j"$(nproc)" \
        pcntl \
        pdo_mysql \
        sockets \
        zip \
    && pecl install redis \
    && docker-php-ext-enable redis \
    && apk del .build-deps \
    && rm -rf /tmp/* /var/cache/apk/*

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /im

# b8im-im 在开发工作区中使用 Composer path 依赖。
# docker-bake.hcl 通过 im-shared named context 提供该目录。
COPY --from=im-shared / /b8im-im-shared

ARG IM_SERVICE

WORKDIR /im/${IM_SERVICE}

COPY ${IM_SERVICE}/composer.json ${IM_SERVICE}/composer.lock ./

RUN --mount=type=cache,id=b8im-composer-cache,target=/tmp/composer-cache,sharing=locked \
    composer install \
        --no-dev \
        --no-interaction \
        --no-progress \
        --no-scripts \
        --prefer-dist

COPY phinx.php /im/phinx.php
COPY database/ /im/database/
COPY ${IM_SERVICE}/ /im/${IM_SERVICE}/

RUN composer dump-autoload --no-dev --classmap-authoritative --no-interaction \
    && mkdir -p runtime \
    && chown -R www-data:www-data runtime

CMD ["php", "start.php", "start"]
