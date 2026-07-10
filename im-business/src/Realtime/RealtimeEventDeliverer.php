<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

interface RealtimeEventDeliverer
{
    public function deliver(RealtimeEvent $event): void;
}
