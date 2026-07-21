<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Support\SingleConversationIdentity;

final class DatabaseFriendRequestRealtimeAuthorizer implements FriendRequestRealtimeAuthorizerInterface
{
    public function __construct(
        private readonly ImRepository $repository,
        private readonly CrossOrganizationConversationAccess $conversationAccess,
    ) {
    }

    public function withCurrentEvent(FriendRequestRealtimeEvent $event, callable $delivery): void
    {
        $this->repository->transaction(function () use ($event, $delivery): void {
            $crossOrganization = $event->fromOrganization !== $event->toOrganization;

            if ($crossOrganization) {
                if ($event->crossOrgAccessSnapshotId === null) {
                    return;
                }
                try {
                    $state = $this->conversationAccess->lockCrossOrganizationWriteBoundary([
                        $event->fromOrganization,
                        $event->toOrganization,
                    ]);
                } catch (ImException) {
                    return;
                }
                if (!hash_equals($state['access_snapshot_id'], $event->crossOrgAccessSnapshotId)) {
                    return;
                }
            } else {
                if ($event->crossOrgAccessSnapshotId !== null) {
                    return;
                }
                $organization = $this->repository->fetchOne(
                    'SELECT id FROM sm_system_organization
                      WHERE id = ? AND status = 1 AND delete_time IS NULL
                      LIMIT 1 LOCK IN SHARE MODE',
                    [$event->toOrganization],
                );
                if ($organization === null) {
                    return;
                }
            }

            $identities = self::orderedIdentities(
                $event->fromOrganization,
                $event->fromUserId,
                $event->toOrganization,
                $event->toUserId,
            );
            if (hash_equals($identities[0]['key'], $identities[1]['key'])) {
                return;
            }

            // Lock one identity at a time in the canonical UTF-8 byte order
            // used by Server writers and single-conversation identity hashing.
            // SQL ORDER BY organization would be numeric (2 before 10), which
            // is the opposite of canonical identity order ("10:u" before
            // "2:u") and can deadlock against a Server FOR UPDATE pair.
            foreach ($identities as $identity) {
                $user = $this->repository->fetchOne(
                    'SELECT organization, user_id
                       FROM im_user
                      WHERE organization = ?
                        AND user_id = ?
                        AND status = 1
                        AND delete_time IS NULL
                      LIMIT 1 LOCK IN SHARE MODE',
                    [$identity['organization'], $identity['user_id']],
                );
                if (
                    $user === null
                    || (int) ($user['organization'] ?? 0) !== $identity['organization']
                    || !hash_equals((string) ($user['user_id'] ?? ''), $identity['user_id'])
                ) {
                    return;
                }
            }

            $request = $this->repository->fetchOne(
                'SELECT id, organization, from_organization, to_organization,
                        from_user_id, to_user_id, status, handle_time, create_time, delete_time
                   FROM im_friend_request
                  WHERE id = ?
                  LIMIT 1 LOCK IN SHARE MODE',
                [$event->requestId],
            );
            if (
                $request === null
                || (int) ($request['id'] ?? 0) !== $event->requestId
                || (int) ($request['organization'] ?? 0) !== $event->toOrganization
                || (int) ($request['from_organization'] ?? 0) !== $event->fromOrganization
                || (int) ($request['to_organization'] ?? 0) !== $event->toOrganization
                || !hash_equals((string) ($request['from_user_id'] ?? ''), $event->fromUserId)
                || !hash_equals((string) ($request['to_user_id'] ?? ''), $event->toUserId)
                || (int) ($request['status'] ?? 0) !== $event->status
                || !hash_equals((string) ($request['create_time'] ?? ''), $event->createTime)
                || ($request['delete_time'] ?? null) !== null
            ) {
                return;
            }
            $requestHandleTime = $request['handle_time'] ?? null;
            if (
                ($event->handleTime === null && $requestHandleTime !== null)
                || ($event->handleTime !== null && (
                    !is_string($requestHandleTime)
                    || !hash_equals($requestHandleTime, $event->handleTime)
                ))
            ) {
                return;
            }

            if ($event->event === FriendRequestRealtimeEvent::ACCEPTED) {
                foreach ([
                    [$event->fromOrganization, $event->fromUserId, $event->toOrganization, $event->toUserId],
                    [$event->toOrganization, $event->toUserId, $event->fromOrganization, $event->fromUserId],
                ] as [$ownerOrganization, $ownerUserId, $friendOrganization, $friendUserId]) {
                    $relation = $this->repository->fetchOne(
                        'SELECT organization,user_id,friend_organization,friend_user_id,status,delete_time '
                        . 'FROM im_friend_relation WHERE organization=? AND BINARY user_id=BINARY ? '
                        . 'AND friend_organization=? AND BINARY friend_user_id=BINARY ? AND status=1 '
                        . 'AND delete_time IS NULL LIMIT 1 LOCK IN SHARE MODE',
                        [$ownerOrganization, $ownerUserId, $friendOrganization, $friendUserId],
                    );
                    if (
                        $relation === null
                        || (int) ($relation['organization'] ?? 0) !== $ownerOrganization
                        || !hash_equals((string) ($relation['user_id'] ?? ''), $ownerUserId)
                        || (int) ($relation['friend_organization'] ?? 0) !== $friendOrganization
                        || !hash_equals((string) ($relation['friend_user_id'] ?? ''), $friendUserId)
                        || (int) ($relation['status'] ?? 0) !== 1
                        || ($relation['delete_time'] ?? null) !== null
                    ) {
                        return;
                    }
                }
            }

            // Keep every policy/organization/user/request lock until the
            // Gateway send completes, establishing revoke-before-or-after
            // ordering for this otherwise delayed Redis event.
            $delivery();
        });
    }

    /**
     * @return list<array{key:string,organization:int,user_id:string}>
     */
    private static function orderedIdentities(
        int $fromOrganization,
        string $fromUserId,
        int $toOrganization,
        string $toUserId,
    ): array {
        $identities = [
            [
                'key' => SingleConversationIdentity::identity($fromOrganization, $fromUserId),
                'organization' => $fromOrganization,
                'user_id' => $fromUserId,
            ],
            [
                'key' => SingleConversationIdentity::identity($toOrganization, $toUserId),
                'organization' => $toOrganization,
                'user_id' => $toUserId,
            ],
        ];
        usort(
            $identities,
            static fn (array $left, array $right): int => strcmp($left['key'], $right['key']),
        );

        return $identities;
    }
}
