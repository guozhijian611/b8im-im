<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImBusiness\Repository\GroupMemberAccessRepository;
use B8im\ImShared\Support\Constants;

final class DatabaseGroupMemberAccessRealtimeAuthorizer implements GroupMemberAccessRealtimeAuthorizer
{
    public function __construct(private readonly GroupMemberAccessRepository $repository)
    {
    }

    public function withCurrentEvent(RealtimeEvent $event, callable $delivery): void
    {
        if (
            $event->eventType !== Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED
            || $event->conversationType !== 2
            || $event->targetOrganization !== $event->organization
            || $event->targetUserId === null
            || $event->groupAccessSnapshotId === null
            || $event->groupAccessVersion === null
            || $event->groupAccessState === null
            || $event->groupLastMessageSeq === null
            || $event->groupLastChangeSeq === null
        ) {
            return;
        }
        $this->repository->transaction(function () use ($event, $delivery): void {
            // This is the Server writer lock order prefix. Keeping the locks
            // through Gateway delivery closes check-then-send epoch races.
            $conversation = $this->repository->fetchOne(
                'SELECT conversation_id
                   FROM im_conversation
                  WHERE organization = ?
                    AND conversation_id = ?
                    AND conversation_type = 2
                  FOR UPDATE',
                [$event->organization, $event->conversationId],
            );
            if ($conversation === null) {
                return;
            }
            $member = $this->repository->fetchOne(
                'SELECT access_version, access_state
                   FROM im_conversation_member
                  WHERE organization = ?
                    AND conversation_id = ?
                    AND member_organization = ?
                    AND user_id = ?
                  FOR UPDATE',
                [
                    $event->organization,
                    $event->conversationId,
                    $event->organization,
                    $event->targetUserId,
                ],
            );
            if ($member === null) {
                return;
            }
            $state = $this->repository->fetchOne(
                'SELECT access_snapshot_id
                   FROM im_user_group_access_state
                  WHERE organization = ? AND user_id = ?
                  FOR UPDATE',
                [$event->organization, $event->targetUserId],
            );
            if (
                $state === null
                || !hash_equals((string) $state['access_snapshot_id'], $event->groupAccessSnapshotId)
                || !hash_equals((string) $member['access_version'], $event->groupAccessVersion)
                || !hash_equals((string) $member['access_state'], $event->groupAccessState)
            ) {
                return;
            }

            $delivery();
        });
    }
}
