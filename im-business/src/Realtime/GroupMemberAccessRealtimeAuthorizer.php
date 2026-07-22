<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

interface GroupMemberAccessRealtimeAuthorizer
{
    /** Run delivery only while the current access rows remain locked and equal to the event. */
    public function withCurrentEvent(RealtimeEvent $event, callable $delivery): void;
}
