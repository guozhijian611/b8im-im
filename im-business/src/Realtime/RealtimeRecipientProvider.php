<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

interface RealtimeRecipientProvider
{
    /** @return list<array{organization:int,user_id:string}> */
    public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array;

    /**
     * Run the delivery callback while the current conversation authorization
     * boundary remains locked. A stale/revoked event calls back with no
     * identities so the broker event can converge via ACK instead of retrying.
     *
     * @param callable(list<array{organization:int,user_id:string}>):void $delivery
     */
    public function withDeliverableIdentities(RealtimeEvent $event, callable $delivery): void;
}
