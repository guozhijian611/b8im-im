<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

interface FriendRequestRealtimeAuthorizerInterface
{
    /**
     * Invoke the delivery callback only while the authoritative pending
     * request and its current authorization boundary remain locked.
     *
     * Stale, handled, revoked or malformed-authority events return normally so
     * the Redis claim is acknowledged instead of retried forever.
     *
     * @param callable():void $delivery
     */
    public function withCurrentEvent(
        FriendRequestRealtimeEvent $event,
        callable $delivery,
    ): void;
}
