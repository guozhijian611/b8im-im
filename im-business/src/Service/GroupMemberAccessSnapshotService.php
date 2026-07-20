<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\GroupMemberAccessRepository;
use B8im\ImShared\Protocol\GroupMemberAccessError;
use B8im\ImShared\Protocol\Dto\CanonicalDecimal;
use B8im\ImShared\Protocol\Dto\GroupMemberAccessEntry;
use B8im\ImShared\Protocol\Dto\GroupMemberAccessPeriod;
use B8im\ImShared\Protocol\Dto\GroupMemberAccessSnapshotPage;
use B8im\ImShared\Protocol\Dto\GroupMemberAccessSnapshotRequest;

final class GroupMemberAccessSnapshotService
{
    private readonly string $cursorKey;

    public function __construct(
        private readonly GroupMemberAccessRepository $repository,
        string $cursorSecret,
    ) {
        if (strlen($cursorSecret) < 32) {
            throw new \InvalidArgumentException('group access cursor secret must contain at least 32 bytes');
        }
        $this->cursorKey = hash_hmac(
            'sha256',
            'group-member-access-cursor-v1',
            $cursorSecret,
            true,
        );
    }

    public function currentSnapshotId(int $organization, string $userId): string
    {
        $row = $this->repository->fetchOne(
            'SELECT access_snapshot_id
               FROM im_user_group_access_state
              WHERE organization = ? AND user_id = ?
              LIMIT 1',
            [$organization, $userId],
        );
        if ($row === null) {
            throw new ImException(
                '用户群访问快照未初始化',
                GroupMemberAccessError::STATE_NOT_INITIALIZED,
            );
        }

        try {
            return CanonicalDecimal::positive(
                (string) ($row['access_snapshot_id'] ?? ''),
                'access_snapshot_id',
            );
        } catch (\InvalidArgumentException) {
            throw new ImException('用户群访问快照损坏', GroupMemberAccessError::STATE_INVALID);
        }
    }

    public function assertCurrentSnapshot(int $organization, string $userId, mixed $snapshotId): string
    {
        if (!is_string($snapshotId)) {
            throw new ImException('缺少 access_snapshot_id', GroupMemberAccessError::SNAPSHOT_REQUIRED);
        }
        try {
            $snapshotId = CanonicalDecimal::positive($snapshotId, 'access_snapshot_id');
        } catch (\InvalidArgumentException) {
            throw new ImException('access_snapshot_id 格式错误', GroupMemberAccessError::SNAPSHOT_INVALID);
        }
        $current = $this->currentSnapshotId($organization, $userId);
        if (!hash_equals($current, $snapshotId)) {
            throw new ImException('群成员访问快照已变化', GroupMemberAccessError::SNAPSHOT_STALE);
        }

        return $snapshotId;
    }

    /**
     * Serializes a Gateway snapshot-page commit with user access-epoch changes.
     *
     * The callback may update connection state, but must not acquire conversation,
     * member, or membership-period locks. Server writers and realtime delivery use
     * those locks before this user-state row, so taking them here would invert the
     * global lock order.
     */
    public function commitPageIfCurrent(
        int $organization,
        string $userId,
        mixed $snapshotId,
        callable $commit,
    ): mixed {
        if (!is_string($snapshotId)) {
            throw new ImException('缺少 access_snapshot_id', GroupMemberAccessError::SNAPSHOT_REQUIRED);
        }
        try {
            $snapshotId = CanonicalDecimal::positive($snapshotId, 'access_snapshot_id');
        } catch (\InvalidArgumentException) {
            throw new ImException('access_snapshot_id 格式错误', GroupMemberAccessError::SNAPSHOT_INVALID);
        }

        return $this->repository->transaction(function () use (
            $organization,
            $userId,
            $snapshotId,
            $commit,
        ): mixed {
            $row = $this->repository->fetchOne(
                'SELECT access_snapshot_id
                   FROM im_user_group_access_state
                  WHERE organization = ? AND user_id = ?
                  LIMIT 1
                  FOR UPDATE',
                [$organization, $userId],
            );
            if ($row === null) {
                throw new ImException(
                    '用户群访问快照未初始化',
                    GroupMemberAccessError::STATE_NOT_INITIALIZED,
                );
            }
            try {
                $current = CanonicalDecimal::positive(
                    (string) ($row['access_snapshot_id'] ?? ''),
                    'access_snapshot_id',
                );
            } catch (\InvalidArgumentException) {
                throw new ImException('用户群访问快照损坏', GroupMemberAccessError::STATE_INVALID);
            }
            if (!hash_equals($current, $snapshotId)) {
                throw new ImException('群成员访问快照已变化', GroupMemberAccessError::SNAPSHOT_STALE);
            }

            return $commit();
        });
    }

    /**
     * @return array{
     *   access_snapshot_id:string,
     *   entries:list<array<string,mixed>>,
     *   next_cursor:?string,
     *   has_more:bool
     * }
     */
    public function page(
        int $organization,
        string $userId,
        mixed $requestedSnapshotId,
        mixed $cursor,
        mixed $requestedLimit,
    ): array {
        try {
            $request = GroupMemberAccessSnapshotRequest::fromArray([
                'access_snapshot_id' => $requestedSnapshotId,
                'cursor' => $cursor,
                'limit' => $requestedLimit,
            ]);
        } catch (\InvalidArgumentException $exception) {
            throw new ImException($exception->getMessage(), GroupMemberAccessError::REQUEST_INVALID);
        }
        $continuation = $request->accessSnapshotId !== null;
        if (!$continuation) {
            $snapshotId = $this->currentSnapshotId($organization, $userId);
            $afterConversationId = '';
        } else {
            $snapshotId = $this->assertCurrentSnapshot(
                $organization,
                $userId,
                $request->accessSnapshotId,
            );
            $afterConversationId = $this->decodeCursor(
                (string) $request->cursor,
                $organization,
                $userId,
                $snapshotId,
                $request->limit,
            );
        }

        $rows = $this->repository->fetchAll(
            'SELECT cm.conversation_id, cm.access_version, cm.access_state,
                    c.last_message_seq, c.last_change_seq
               FROM im_conversation_member cm
               INNER JOIN im_conversation c
                  ON c.organization = cm.organization
                 AND c.conversation_id = cm.conversation_id
                 AND c.conversation_type = 2
                 AND c.status = 1
                 AND c.delete_time IS NULL
              WHERE cm.organization = ?
                AND cm.member_organization = ?
                AND cm.user_id = ?
                AND cm.conversation_id COLLATE utf8mb4_bin > ? COLLATE utf8mb4_bin
                AND EXISTS (
                    SELECT 1
                      FROM im_conversation_membership_period mp
                     WHERE mp.organization = cm.organization
                       AND mp.conversation_id = cm.conversation_id
                       AND mp.member_organization = cm.member_organization
                       AND mp.user_id = cm.user_id
                       AND mp.status = 1
                )
              ORDER BY cm.conversation_id COLLATE utf8mb4_bin ASC
              LIMIT ' . ($request->limit + 1),
            [$organization, $organization, $userId, $afterConversationId],
        );
        $hasMore = count($rows) > $request->limit;
        $rows = array_slice($rows, 0, $request->limit);
        $periodsByConversation = $this->periodsByConversation(
            $organization,
            $userId,
            array_map(static fn (array $row): string => (string) $row['conversation_id'], $rows),
        );
        $entries = [];
        foreach ($rows as $row) {
            $conversationId = (string) $row['conversation_id'];
            $periods = $periodsByConversation[$conversationId] ?? [];
            $state = (string) ($row['access_state'] ?? '');
            try {
                $entries[] = (new GroupMemberAccessEntry(
                    $conversationId,
                    2,
                    (string) ($row['access_version'] ?? ''),
                    $state,
                    (string) ($row['last_message_seq'] ?? ''),
                    (string) ($row['last_change_seq'] ?? ''),
                    $periods,
                ))->toArray();
            } catch (\InvalidArgumentException) {
                throw new ImException('群成员访问投影损坏', GroupMemberAccessError::GROUP_STATE_INVALID);
            }
        }

        $this->assertCurrentSnapshot($organization, $userId, $snapshotId);
        $lastRow = $rows === [] ? null : end($rows);

        try {
            $entryDtos = array_map(
                static fn (array $entry): GroupMemberAccessEntry => new GroupMemberAccessEntry(
                    (string) $entry['conversation_id'],
                    2,
                    (string) $entry['access_version'],
                    (string) $entry['access_state'],
                    (string) $entry['last_message_seq'],
                    (string) $entry['last_change_seq'],
                    array_map(
                        static fn (array $period): GroupMemberAccessPeriod => new GroupMemberAccessPeriod(
                            (string) $period['period_no'],
                            (string) $period['from_seq'],
                            $period['to_seq'] === null ? null : (string) $period['to_seq'],
                        ),
                        $entry['periods'],
                    ),
                ),
                $entries,
            );
            return (new GroupMemberAccessSnapshotPage(
                $snapshotId,
                $entryDtos,
                $hasMore && is_array($lastRow)
                    ? $this->encodeCursor(
                        $organization,
                        $userId,
                        $snapshotId,
                        $request->limit,
                        (string) $lastRow['conversation_id'],
                    )
                    : null,
                $hasMore,
            ))->toArray();
        } catch (\InvalidArgumentException) {
            throw new ImException('群成员访问快照响应损坏', GroupMemberAccessError::GROUP_STATE_INVALID);
        }
    }

    public function assertConversationVersion(
        int $organization,
        string $userId,
        string $conversationId,
        mixed $accessVersion,
    ): GroupMemberAccessEntry {
        if (!is_string($accessVersion)) {
            throw new ImException('群会话 SYNC 缺少 access_version', GroupMemberAccessError::VERSION_REQUIRED);
        }
        try {
            $accessVersion = CanonicalDecimal::positive($accessVersion, 'access_version');
        } catch (\InvalidArgumentException) {
            throw new ImException('access_version 格式错误', GroupMemberAccessError::VERSION_INVALID);
        }
        $row = $this->repository->fetchOne(
            'SELECT cm.access_version, cm.access_state,
                    c.last_message_seq, c.last_change_seq
               FROM im_conversation_member cm
               INNER JOIN im_conversation c
                  ON c.organization = cm.organization
                 AND c.conversation_id = cm.conversation_id
                 AND c.conversation_type = 2
                 AND c.status = 1
                 AND c.delete_time IS NULL
              WHERE cm.organization = ?
                AND cm.conversation_id = ?
                AND cm.member_organization = ?
                AND cm.user_id = ?
              LIMIT 1',
            [$organization, $conversationId, $organization, $userId],
        );
        if ($row === null || !hash_equals((string) ($row['access_version'] ?? ''), $accessVersion)) {
            throw new ImException('群会话访问版本已变化', GroupMemberAccessError::VERSION_STALE);
        }
        $periods = $this->periodsByConversation(
            $organization,
            $userId,
            [$conversationId],
        )[$conversationId] ?? [];
        if ($periods === []) {
            throw new ImException('群会话历史访问已撤销', 'CONVERSATION_MEMBER_FORBIDDEN');
        }
        $state = (string) ($row['access_state'] ?? '');
        try {
            return new GroupMemberAccessEntry(
                $conversationId,
                2,
                $accessVersion,
                $state,
                (string) ($row['last_message_seq'] ?? ''),
                (string) ($row['last_change_seq'] ?? ''),
                $periods,
            );
        } catch (\InvalidArgumentException) {
            throw new ImException('群成员访问投影损坏', GroupMemberAccessError::GROUP_STATE_INVALID);
        }
    }

    /** @param list<string> $conversationIds @return array<string,list<GroupMemberAccessPeriod>> */
    private function periodsByConversation(int $organization, string $userId, array $conversationIds): array
    {
        if ($conversationIds === []) {
            return [];
        }
        $placeholders = implode(',', array_fill(0, count($conversationIds), '?'));
        $rows = $this->repository->fetchAll(
            'SELECT conversation_id, period_no, visible_from_message_seq,
                    visible_until_message_seq
               FROM im_conversation_membership_period
              WHERE organization = ?
                AND member_organization = ?
                AND user_id = ?
                AND status = 1
                AND conversation_id IN (' . $placeholders . ')
              ORDER BY conversation_id COLLATE utf8mb4_bin ASC, period_no ASC',
            array_merge([$organization, $organization, $userId], $conversationIds),
        );
        $grouped = [];
        foreach ($rows as $row) {
            $conversationId = (string) ($row['conversation_id'] ?? '');
            try {
                $grouped[$conversationId][] = new GroupMemberAccessPeriod(
                    (string) ($row['period_no'] ?? ''),
                    (string) ($row['visible_from_message_seq'] ?? ''),
                    $row['visible_until_message_seq'] === null
                        ? null
                        : (string) $row['visible_until_message_seq'],
                );
            } catch (\InvalidArgumentException) {
                throw new ImException('群成员历史可见区间损坏', GroupMemberAccessError::PERIOD_INVALID);
            }
        }

        return $grouped;
    }

    private function encodeCursor(
        int $organization,
        string $userId,
        string $snapshotId,
        int $limit,
        string $conversationId,
    ): string
    {
        $json = json_encode(
            [1, $organization, $userId, $snapshotId, $limit, $conversationId],
            JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR,
        );
        $payload = rtrim(strtr(base64_encode($json), '+/', '-_'), '=');

        return $payload . '.' . hash_hmac('sha256', $payload, $this->cursorKey);
    }

    private function decodeCursor(
        string $cursor,
        int $organization,
        string $userId,
        string $snapshotId,
        int $limit,
    ): string {
        if ($cursor === '' || strlen($cursor) > 768 || substr_count($cursor, '.') !== 1) {
            throw new ImException('cursor 格式错误', GroupMemberAccessError::CURSOR_INVALID);
        }
        [$payload, $signature] = explode('.', $cursor, 2);
        if (
            preg_match('/^[A-Za-z0-9_-]+$/D', $payload) !== 1
            || preg_match('/^[a-f0-9]{64}$/D', $signature) !== 1
            || !hash_equals(hash_hmac('sha256', $payload, $this->cursorKey), $signature)
        ) {
            throw new ImException('cursor 格式错误', GroupMemberAccessError::CURSOR_INVALID);
        }
        $padding = (4 - strlen($payload) % 4) % 4;
        $json = base64_decode(strtr($payload, '-_', '+/') . str_repeat('=', $padding), true);
        try {
            $tuple = is_string($json)
                ? json_decode($json, true, flags: JSON_THROW_ON_ERROR)
                : null;
        } catch (\JsonException) {
            $tuple = null;
        }
        if (
            !is_array($tuple)
            || !array_is_list($tuple)
            || count($tuple) !== 6
            || $tuple[0] !== 1
            || $tuple[1] !== $organization
            || $tuple[2] !== $userId
            || $tuple[3] !== $snapshotId
            || $tuple[4] !== $limit
            || !is_string($tuple[5])
            || $tuple[5] === ''
            || strlen($tuple[5]) > 64
            || trim($tuple[5]) !== $tuple[5]
            || str_contains($tuple[5], "\0")
        ) {
            throw new ImException('cursor 与当前身份或快照不匹配', GroupMemberAccessError::CURSOR_INVALID);
        }

        return $tuple[5];
    }
}
