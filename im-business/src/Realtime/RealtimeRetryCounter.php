<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

interface RealtimeRetryCounter
{
    public function increment(RealtimeEvent $event): int;

    public function clear(RealtimeEvent $event): void;
}
