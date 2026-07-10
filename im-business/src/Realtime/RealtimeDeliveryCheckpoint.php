<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

interface RealtimeDeliveryCheckpoint
{
    public function wasDelivered(RealtimeEvent $event, string $clientId): bool;

    public function markDelivered(RealtimeEvent $event, string $clientId): void;
}
