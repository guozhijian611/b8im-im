<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImBusiness\Repository\ImRepository;

final class DatabaseRealtimeRecipientProvider implements RealtimeRecipientProvider
{
    public function __construct(private readonly ImRepository $repository)
    {
    }

    public function activeUserIds(int $organization, string $conversationId, int $messageSeq): array
    {
        $rows = $this->repository->fetchAll(
            'SELECT cm.user_id
               FROM im_conversation_member cm
               INNER JOIN im_conversation c
                  ON c.organization = cm.organization
                 AND c.conversation_id = cm.conversation_id
                 AND c.status = 1
                 AND c.delete_time IS NULL
              WHERE cm.organization = ?
                AND c.organization = ?
                AND cm.conversation_id = ?
                AND cm.status = 1
                AND cm.delete_time IS NULL
                AND EXISTS (
                    SELECT 1
                      FROM im_conversation_membership_period mp
                     WHERE mp.organization = cm.organization
                       AND mp.conversation_id = cm.conversation_id
                       AND mp.user_id = cm.user_id
                       AND mp.status = 1
                       AND ? >= mp.visible_from_message_seq
                       AND (mp.visible_until_message_seq IS NULL OR ? <= mp.visible_until_message_seq)
                )
              ORDER BY cm.id ASC',
            [$organization, $organization, $conversationId, $messageSeq, $messageSeq],
        );

        $userIds = [];
        foreach ($rows as $row) {
            $userId = trim((string) ($row['user_id'] ?? ''));
            if ($userId !== '') {
                $userIds[$userId] = $userId;
            }
        }

        return array_values($userIds);
    }
}
