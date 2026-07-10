<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

interface RealtimeRecipientProvider
{
    /** @return list<string> */
    public function activeUserIds(int $organization, string $conversationId, int $messageSeq): array;
}
