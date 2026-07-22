<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\CrossOrganizationConversationAccess;
use B8im\ImBusiness\Service\CrossOrganizationSocialPolicy;

final class DatabaseRealtimeRecipientProvider implements RealtimeRecipientProvider
{
    private readonly CrossOrganizationConversationAccess $conversationAccess;

    public function __construct(
        private readonly ImRepository $repository,
        ?CrossOrganizationConversationAccess $conversationAccess = null,
    ) {
        $this->conversationAccess = $conversationAccess ?? new CrossOrganizationConversationAccess(
            $repository,
            new CrossOrganizationSocialPolicy($repository),
        );
    }

    public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array
    {
        $identities = [];
        foreach ($this->activeMemberRows($organization, $conversationId, $messageSeq) as $row) {
            $memberOrganization = (int) ($row['member_organization'] ?? 0);
            $userId = trim((string) ($row['user_id'] ?? ''));
            if ($memberOrganization !== $organization) {
                if ((int) ($row['conversation_type'] ?? 0) === 2) {
                    throw new \RuntimeException('group realtime recipient is outside its home projection');
                }
                // A dual-home single projection contains both identities; only
                // the identity belonging to this home may be delivered here.
                continue;
            }
            if ($memberOrganization > 0 && $userId !== '') {
                $identities[$this->identityKey($memberOrganization, $userId)] = [
                    'organization' => $memberOrganization,
                    'user_id' => $userId,
                ];
            }
        }

        return array_values($identities);
    }

    public function withDeliverableIdentities(RealtimeEvent $event, callable $delivery): void
    {
        try {
            $preview = $this->conversationAccess->assertAccessible(
                $event->organization,
                $event->conversationId,
            );
        } catch (ImException) {
            $delivery([]);
            return;
        }

        $this->repository->transaction(function () use ($event, $preview, $delivery): void {
            try {
                $access = $this->conversationAccess->assertAccessible(
                    $event->organization,
                    $event->conversationId,
                    true,
                    $preview['home_organizations'],
                    $preview['participant_identities'],
                );
            } catch (ImException) {
                $delivery([]);
                return;
            }
            if ((int) $access['conversation_type'] !== $event->conversationType) {
                $delivery([]);
                return;
            }
            if ($access['is_cross_organization']) {
                if (
                    $event->crossOrgAccessSnapshotId === null
                    || !hash_equals($access['access_snapshot_id'], $event->crossOrgAccessSnapshotId)
                ) {
                    $delivery([]);
                    return;
                }
            } elseif ($event->crossOrgAccessSnapshotId !== null) {
                // A same-organization event must not smuggle a global cross-org
                // epoch and later be mistaken for a canonical dual-home event.
                $delivery([]);
                return;
            }

            $rows = $this->activeMemberRows(
                $event->organization,
                $event->conversationId,
                $event->messageSeq,
            );
            $allIdentities = [];
            $homeIdentities = [];
            foreach ($rows as $row) {
                if ((int) ($row['conversation_type'] ?? 0) !== $event->conversationType) {
                    $delivery([]);
                    return;
                }
                $memberOrganization = (int) ($row['member_organization'] ?? 0);
                $userId = trim((string) ($row['user_id'] ?? ''));
                if ($memberOrganization <= 0 || $userId === '') {
                    $delivery([]);
                    return;
                }
                if ($event->conversationType === 2 && $memberOrganization !== $event->organization) {
                    $delivery([]);
                    return;
                }
                $identity = [
                    'organization' => $memberOrganization,
                    'user_id' => $userId,
                ];
                $allIdentities[$this->identityKey($memberOrganization, $userId)] = $identity;
                if ($memberOrganization === $event->organization) {
                    $homeIdentities[$this->identityKey($memberOrganization, $userId)] = $identity;
                }
            }
            if (!isset($allIdentities[$this->identityKey($event->actorOrganization, $event->actorUserId)])) {
                $delivery([]);
                return;
            }

            $delivery(array_values($homeIdentities));
        });
    }

    /** @return list<array<string,mixed>> */
    private function activeMemberRows(int $organization, string $conversationId, int $messageSeq): array
    {
        return $this->repository->fetchAll(
            'SELECT cm.member_organization, cm.user_id, c.conversation_type
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
                AND (c.conversation_type <> 2 OR cm.access_state = \'active\')
                AND cm.delete_time IS NULL
                AND EXISTS (
                    SELECT 1
                      FROM im_conversation_membership_period mp
                     WHERE mp.organization = cm.organization
                       AND mp.conversation_id = cm.conversation_id
                       AND mp.member_organization = cm.member_organization
                       AND mp.user_id = cm.user_id
                       AND mp.status = 1
                       AND ? >= mp.visible_from_message_seq
                       AND (mp.visible_until_message_seq IS NULL OR ? <= mp.visible_until_message_seq)
                )
              ORDER BY cm.id ASC',
            [$organization, $organization, $conversationId, $messageSeq, $messageSeq],
        );
    }

    private function identityKey(int $organization, string $userId): string
    {
        return json_encode([$organization, $userId], JSON_THROW_ON_ERROR);
    }
}
