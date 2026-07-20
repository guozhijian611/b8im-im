<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Support\SingleConversationIdentity;

/**
 * Fail-closed conversation boundary shared by SEND/ACK/SYNC/mutations/typing.
 * Cross-organization singles must always have a valid canonical row and both
 * home projections. Groups are deliberately single-organization only.
 */
final class CrossOrganizationConversationAccess
{
    public function __construct(
        private readonly ImRepository $repository,
        private readonly CrossOrganizationSocialPolicy $policy,
    ) {
    }

    public function snapshotId(bool $fresh = false): string
    {
        return $this->policy->snapshotId($fresh);
    }

    /**
     * Lock the global policy and both organizations before a new cross-home
     * canonical row, projection or message is allowed to be written.
     *
     * @param list<int> $organizations
     * @return array{enabled:bool,access_snapshot_id:string,valid:bool}
     */
    public function lockCrossOrganizationWriteBoundary(array $organizations): array
    {
        $organizations = array_values(array_unique(array_filter(
            array_map('intval', $organizations),
            static fn (int $organization): bool => $organization > 0,
        )));
        sort($organizations, SORT_NUMERIC);
        if (count($organizations) !== 2) {
            throw new ImException('跨机构写入边界无效', 'CROSS_ORG_WRITE_BOUNDARY_INVALID');
        }

        $state = $this->policy->lockStateForWrite();
        if (!$state['valid'] || !$state['enabled']) {
            throw new ImException('跨机构单聊访问已撤销', 'CROSS_ORG_ACCESS_REVOKED');
        }
        $this->assertOrganizationsActive($organizations, true);

        return $state;
    }

    /**
     * Lock and validate the immutable policy snapshot for both durable homes.
     *
     * @param list<int> $homeOrganizations
     * @return array<int,TenantImPolicySnapshot>
     */
    public function lockHomeTenantPolicies(array $homeOrganizations): array
    {
        $homeOrganizations = array_values(array_unique(array_map('intval', $homeOrganizations)));
        sort($homeOrganizations, SORT_NUMERIC);
        if (count($homeOrganizations) !== 2) {
            throw new ImException('跨机构租户策略边界无效', 'TENANT_POLICY_FORBIDDEN');
        }

        $policies = [];
        foreach ($homeOrganizations as $homeOrganization) {
            $row = $this->repository->fetchOne(
                'SELECT organization,
                        allowed_client_families_json,
                        allow_multi_device_online,
                        max_online_devices,
                        same_device_login_policy,
                        cross_device_login_policy,
                        max_message_concurrency,
                        max_message_qps,
                        default_group_display_member_count,
                        message_recall_window_seconds,
                        message_edit_window_seconds,
                        recall_notice_enabled,
                        group_recall_notice_enabled,
                        status,
                        version
                   FROM sm_tenant_im_policy
                  WHERE organization = ?
                  LIMIT 1 LOCK IN SHARE MODE',
                [$homeOrganization],
            );
            if ($row === null) {
                throw new ImException('参与机构 IM 策略缺失', 'TENANT_POLICY_FORBIDDEN');
            }
            try {
                $policy = TenantImPolicySnapshot::fromDatabaseRow($row);
            } catch (\Throwable) {
                throw new ImException('参与机构 IM 策略无效', 'TENANT_POLICY_FORBIDDEN');
            }
            if ($policy->organization !== $homeOrganization || $policy->status !== 'ENABLED') {
                throw new ImException('参与机构 IM 策略已停用', 'TENANT_POLICY_FORBIDDEN');
            }
            $policies[$homeOrganization] = $policy;
        }

        return $policies;
    }

    /**
     * @param list<int>|null $expectedHomeOrganizations Required for locking reads; discovered before the transaction.
     * @param list<array{organization:int,user_id:string}>|null $expectedParticipantIdentities
     * @return array{
     *   conversation_type:int,
     *   home_organizations:list<int>,
     *   participant_identities:list<array{organization:int,user_id:string}>,
     *   is_cross_organization:bool,
     *   access_snapshot_id:string
     * }
     */
    public function assertAccessible(
        int $currentHome,
        string $conversationId,
        bool $forUpdate = false,
        ?array $expectedHomeOrganizations = null,
        ?array $expectedParticipantIdentities = null,
    ): array
    {
        if ($currentHome <= 0 || trim($conversationId) === '') {
            throw new ImException('会话访问身份不完整', 'CONVERSATION_ACCESS_IDENTITY_INVALID');
        }
        $lock = $forUpdate ? ' FOR UPDATE' : '';
        $lockedState = null;
        $canonical = null;
        if ($forUpdate) {
            $expectedHomeOrganizations = array_values(array_unique(array_filter(
                array_map('intval', $expectedHomeOrganizations ?? []),
                static fn (int $organization): bool => $organization > 0,
            )));
            sort($expectedHomeOrganizations, SORT_NUMERIC);
            if (
                $expectedHomeOrganizations === []
                || count($expectedHomeOrganizations) > 2
                || !in_array($currentHome, $expectedHomeOrganizations, true)
            ) {
                throw new ImException('会话写入边界无效', 'CONVERSATION_WRITE_BOUNDARY_INVALID');
            }

            // Expected homes come from a transaction-external authorization
            // preview. Inside the transaction there are no canonical/home
            // reads before the common config -> sorted organizations prefix.
            $lockedState = $this->policy->lockStateForWrite();
            if (
                count($expectedHomeOrganizations) === 2
                && (!$lockedState['valid'] || !$lockedState['enabled'])
            ) {
                throw new ImException('跨机构单聊访问已撤销', 'CROSS_ORG_ACCESS_REVOKED');
            }
            $this->assertOrganizationsActive($expectedHomeOrganizations, true);
            if (count($expectedHomeOrganizations) === 2) {
                $expectedParticipantIdentities = $this->normalizeParticipantIdentities(
                    $expectedParticipantIdentities ?? [],
                );
                $participantHomes = array_values(array_unique(array_column(
                    $expectedParticipantIdentities,
                    'organization',
                )));
                sort($participantHomes, SORT_NUMERIC);
                if (
                    count($expectedParticipantIdentities) !== 2
                    || $participantHomes !== $expectedHomeOrganizations
                ) {
                    throw new ImException(
                        '跨机构会话写入用户边界无效',
                        'CONVERSATION_WRITE_BOUNDARY_INVALID',
                    );
                }
                $this->lockActiveUsers($expectedParticipantIdentities);
            }
            $canonical = $this->repository->fetchOne(
                'SELECT conversation_id, left_organization, left_user_id,
                        right_organization, right_user_id, status
                   FROM im_cross_organization_conversation
                  WHERE conversation_id = ? LIMIT 1 FOR UPDATE',
                [$conversationId],
            );
            $canonicalHomes = $canonical === null ? [$currentHome] : array_values(array_unique([
                (int) ($canonical['left_organization'] ?? 0),
                (int) ($canonical['right_organization'] ?? 0),
            ]));
            sort($canonicalHomes, SORT_NUMERIC);
            if ($canonicalHomes !== $expectedHomeOrganizations) {
                throw new ImException('跨机构单聊权威身份并发变化', 'CROSS_ORG_CANONICAL_INVALID');
            }
        } else {
            $canonical = $this->repository->fetchOne(
                'SELECT conversation_id, left_organization, left_user_id,
                        right_organization, right_user_id, status
                   FROM im_cross_organization_conversation
                  WHERE conversation_id = ? LIMIT 1',
                [$conversationId],
            );
        }

        if ($forUpdate && $canonical !== null) {
            // The sorted projection loop below performs the first home/member
            // reads and locks. A canonical row can only represent a single.
            $conversationType = 1;
        } else {
            $conversation = $this->repository->fetchOne(
                'SELECT conversation_type, status, delete_time
                   FROM im_conversation
                  WHERE organization = ? AND conversation_id = ?
                  LIMIT 1' . $lock,
                [$currentHome, $conversationId],
            );
            if (
                $conversation === null
                || (int) ($conversation['status'] ?? 0) !== 1
                || ($conversation['delete_time'] ?? null) !== null
            ) {
                throw new ImException('会话不存在或无权访问', 'CONVERSATION_NOT_FOUND');
            }
            $conversationType = (int) $conversation['conversation_type'];
        }

        if ($conversationType === 2) {
            if ($canonical !== null) {
                throw new ImException('群聊误绑定跨机构单聊权威身份', 'CROSS_ORG_CANONICAL_INVALID');
            }
            $members = $this->activeMembers($currentHome, $conversationId, $forUpdate);
            foreach ($members as $member) {
                if ((int) $member['organization'] !== $currentHome) {
                    throw new ImException('群聊不允许包含外机构成员', 'GROUP_MEMBER_ORGANIZATION_FORBIDDEN');
                }
            }

            return [
                'conversation_type' => 2,
                'home_organizations' => [$currentHome],
                'participant_identities' => $members,
                'is_cross_organization' => false,
                'access_snapshot_id' => $lockedState['access_snapshot_id']
                    ?? $this->policy->snapshotId(),
            ];
        }
        if ($conversationType !== 1) {
            throw new ImException('会话类型无效', 'CONVERSATION_TYPE_INVALID');
        }

        if ($canonical === null) {
            $members = $this->activeMembers($currentHome, $conversationId, $forUpdate);
            if ($this->hasForeignMemberRecord($currentHome, $conversationId)) {
                throw new ImException('跨机构单聊权威身份缺失', 'CROSS_ORG_CANONICAL_INVALID');
            }
            foreach ($members as $member) {
                if ((int) $member['organization'] !== $currentHome) {
                    throw new ImException('跨机构单聊权威身份缺失', 'CROSS_ORG_CANONICAL_INVALID');
                }
            }
            $this->assertSingleMemberPair($members, $conversationId);

            return [
                'conversation_type' => 1,
                'home_organizations' => [$currentHome],
                'participant_identities' => $members,
                'is_cross_organization' => false,
                'access_snapshot_id' => $lockedState['access_snapshot_id']
                    ?? $this->policy->snapshotId(),
            ];
        }

        $leftOrganization = (int) ($canonical['left_organization'] ?? 0);
        $rightOrganization = (int) ($canonical['right_organization'] ?? 0);
        $leftUserId = trim((string) ($canonical['left_user_id'] ?? ''));
        $rightUserId = trim((string) ($canonical['right_user_id'] ?? ''));
        $expectedId = '';
        try {
            $expectedId = CrossOrganizationSocialPolicy::singleConversationId(
                $leftOrganization,
                $leftUserId,
                $rightOrganization,
                $rightUserId,
            );
        } catch (\InvalidArgumentException) {
            // Mapped to the canonical corruption error below.
        }
        $homes = [$leftOrganization, $rightOrganization];
        sort($homes, SORT_NUMERIC);
        if (
            (int) ($canonical['status'] ?? 0) !== 1
            || $leftOrganization === $rightOrganization
            || $expectedId !== $conversationId
            || !in_array($currentHome, $homes, true)
        ) {
            throw new ImException('跨机构单聊权威身份损坏', 'CROSS_ORG_CANONICAL_INVALID');
        }
        if (!$forUpdate) {
            $this->assertOrganizationsActive($homes);
        }

        $expectedMembers = [
            $leftOrganization . ':' . $leftUserId,
            $rightOrganization . ':' . $rightUserId,
        ];
        sort($expectedMembers, SORT_STRING);
        $participantIdentities = $this->normalizeParticipantIdentities([
            ['organization' => $leftOrganization, 'user_id' => $leftUserId],
            ['organization' => $rightOrganization, 'user_id' => $rightUserId],
        ]);
        if (
            $forUpdate
            && $expectedParticipantIdentities !== $participantIdentities
        ) {
            throw new ImException(
                '跨机构单聊权威用户身份并发变化',
                'CROSS_ORG_CANONICAL_INVALID',
            );
        }
        foreach ($homes as $home) {
            $projection = $this->repository->fetchOne(
                'SELECT conversation_type, status, delete_time
                   FROM im_conversation
                  WHERE organization = ? AND conversation_id = ?
                  LIMIT 1' . $lock,
                [$home, $conversationId],
            );
            if (
                $projection === null
                || (int) ($projection['conversation_type'] ?? 0) !== 1
                || (int) ($projection['status'] ?? 0) !== 1
                || ($projection['delete_time'] ?? null) !== null
            ) {
                throw new ImException('跨机构单聊home投影缺失', 'CROSS_ORG_HOME_PROJECTION_INVALID');
            }
            $projectionMembers = array_map(
                static fn (array $member): string => (int) $member['organization'] . ':' . (string) $member['user_id'],
                $this->activeMembers($home, $conversationId, $forUpdate),
            );
            sort($projectionMembers, SORT_STRING);
            if ($projectionMembers !== $expectedMembers) {
                throw new ImException('跨机构单聊home成员身份损坏', 'CROSS_ORG_HOME_PROJECTION_INVALID');
            }
        }

        $state = $lockedState ?? $this->policy->state(true);
        if (!$state['valid'] || !$state['enabled']) {
            throw new ImException('跨机构单聊访问已撤销', 'CROSS_ORG_ACCESS_REVOKED');
        }

        return [
            'conversation_type' => 1,
            'home_organizations' => $homes,
            'participant_identities' => $participantIdentities,
            'is_cross_organization' => true,
            'access_snapshot_id' => $state['access_snapshot_id'],
        ];
    }

    /** @param list<int> $organizations */
    private function assertOrganizationsActive(array $organizations, bool $lockingRead = false): void
    {
        $organizations = array_values(array_unique(array_filter(
            array_map('intval', $organizations),
            static fn (int $organization): bool => $organization > 0,
        )));
        sort($organizations, SORT_NUMERIC);
        if ($organizations === []) {
            throw new ImException('跨机构单聊访问已撤销', 'CROSS_ORG_ACCESS_REVOKED');
        }
        $placeholders = implode(',', array_fill(0, count($organizations), '?'));
        $rows = $this->repository->fetchAll(
            'SELECT id FROM sm_system_organization
              WHERE id IN (' . $placeholders . ') AND status = 1 AND delete_time IS NULL
              ORDER BY id' . ($lockingRead ? ' LOCK IN SHARE MODE' : ''),
            $organizations,
        );
        $active = array_map(static fn (array $row): int => (int) $row['id'], $rows);
        if ($active !== $organizations) {
            throw new ImException('跨机构单聊访问已撤销', 'CROSS_ORG_ACCESS_REVOKED');
        }
    }

    /**
     * User rows are the common serialization point with Server friend writers.
     * Lock them one at a time in canonical UTF-8 byte order; numeric SQL order
     * would reverse identities such as "2:u" and "10:u".
     *
     * @param list<array{organization:int,user_id:string}> $identities
     */
    private function lockActiveUsers(array $identities): void
    {
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
                throw new ImException(
                    '跨机构单聊用户不存在或已停用',
                    'CROSS_ORG_ACCESS_REVOKED',
                );
            }
        }
    }

    /**
     * @param list<array{organization:int,user_id:string}> $identities
     * @return list<array{organization:int,user_id:string}>
     */
    private function normalizeParticipantIdentities(array $identities): array
    {
        $normalized = [];
        foreach ($identities as $identity) {
            $organization = (int) ($identity['organization'] ?? 0);
            $userId = trim((string) ($identity['user_id'] ?? ''));
            if ($organization <= 0 || $userId === '') {
                throw new ImException(
                    '跨机构会话用户身份无效',
                    'CONVERSATION_WRITE_BOUNDARY_INVALID',
                );
            }
            $key = SingleConversationIdentity::identity($organization, $userId);
            if (isset($normalized[$key])) {
                throw new ImException(
                    '跨机构会话用户身份重复',
                    'CONVERSATION_WRITE_BOUNDARY_INVALID',
                );
            }
            $normalized[$key] = [
                'organization' => $organization,
                'user_id' => $userId,
            ];
        }
        ksort($normalized, SORT_STRING);

        return array_values($normalized);
    }

    private function hasForeignMemberRecord(int $home, string $conversationId): bool
    {
        return $this->repository->fetchOne(
            'SELECT 1 AS found
               FROM im_conversation_member
              WHERE organization = ? AND conversation_id = ?
                AND member_organization <> ?
              LIMIT 1',
            [$home, $conversationId, $home],
        ) !== null;
    }

    public static function isAccessRevoked(ImException $exception): bool
    {
        return $exception->errorCode() === 'CROSS_ORG_ACCESS_REVOKED';
    }

    /** @return list<array{organization:int,user_id:string}> */
    private function activeMembers(int $home, string $conversationId, bool $forUpdate): array
    {
        $rows = $this->repository->fetchAll(
            'SELECT member_organization, user_id
               FROM im_conversation_member
              WHERE organization = ? AND conversation_id = ?
                AND status = 1 AND delete_time IS NULL
              ORDER BY member_organization, user_id' . ($forUpdate ? ' FOR UPDATE' : ''),
            [$home, $conversationId],
        );

        return array_map(
            static fn (array $row): array => [
                'organization' => (int) $row['member_organization'],
                'user_id' => (string) $row['user_id'],
            ],
            $rows,
        );
    }

    /** @param list<array{organization:int,user_id:string}> $members */
    private function assertSingleMemberPair(array $members, string $conversationId): void
    {
        if (count($members) !== 2) {
            throw new ImException('单聊成员身份损坏', 'SINGLE_CONVERSATION_IDENTITY_INVALID');
        }
        try {
            $expected = CrossOrganizationSocialPolicy::singleConversationId(
                $members[0]['organization'],
                $members[0]['user_id'],
                $members[1]['organization'],
                $members[1]['user_id'],
            );
        } catch (\InvalidArgumentException) {
            throw new ImException('单聊成员身份损坏', 'SINGLE_CONVERSATION_IDENTITY_INVALID');
        }
        if ($expected !== $conversationId) {
            throw new ImException('单聊会话ID与成员身份不一致', 'SINGLE_CONVERSATION_IDENTITY_INVALID');
        }
    }
}
