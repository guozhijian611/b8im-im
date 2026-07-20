<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImShared\Support\Constants;

final class ConversationReadEventId
{
    public static function generate(
        int $homeOrganization,
        string $conversationId,
        int $readerOrganization,
        string $readerUserId,
        int $lastReadSeq,
        ?string $crossOrgAccessSnapshotId,
    ): string {
        $parts = [
            $homeOrganization,
            Constants::MQ_ROUTING_CONVERSATION_READ,
            $conversationId,
            $readerOrganization,
            $readerUserId,
            $lastReadSeq,
        ];
        if ($crossOrgAccessSnapshotId !== null) {
            if (preg_match('/^[1-9][0-9]{0,19}$/D', $crossOrgAccessSnapshotId) !== 1) {
                throw new \InvalidArgumentException('conversation.read access snapshot is invalid');
            }
            $parts[] = $crossOrgAccessSnapshotId;
        }

        return hash('sha256', implode('|', array_map('strval', $parts)));
    }
}
