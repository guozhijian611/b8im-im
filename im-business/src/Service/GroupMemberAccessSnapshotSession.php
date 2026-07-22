<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Exception\ImException;
use B8im\ImShared\Protocol\GroupMemberAccessError;

/** Per-Gateway-connection proof that every page in one access snapshot was traversed. */
final class GroupMemberAccessSnapshotSession
{
    public const COMPLETED_KEY = 'access_snapshot_id';
    public const STAGING_KEY = 'group_access_snapshot_staging';

    /** @param array<string,mixed> $session @return array<string,mixed> */
    public static function begin(array $session): array
    {
        unset($session[self::STAGING_KEY]);

        return $session;
    }

    /** @param array<string,mixed> $session */
    public static function assertContinuation(
        array $session,
        string $snapshotId,
        string $cursor,
        int $limit,
    ): void {
        $staging = $session[self::STAGING_KEY] ?? null;
        if (
            !is_array($staging)
            || array_is_list($staging)
            || !is_string($staging['access_snapshot_id'] ?? null)
            || !is_string($staging['next_cursor'] ?? null)
            || !is_int($staging['limit'] ?? null)
            || !hash_equals($staging['access_snapshot_id'], $snapshotId)
            || !hash_equals($staging['next_cursor'], $cursor)
            || $staging['limit'] !== $limit
        ) {
            throw new ImException(
                '当前连接未按顺序完成群访问快照分页',
                GroupMemberAccessError::SNAPSHOT_NOT_COMPLETED,
            );
        }
    }

    /**
     * @param array<string,mixed> $session
     * @param array{access_snapshot_id:string,next_cursor:?string,has_more:bool} $page
     * @return array<string,mixed>
     */
    public static function advance(array $session, array $page, int $limit): array
    {
        $snapshotId = $page['access_snapshot_id'] ?? null;
        $nextCursor = $page['next_cursor'] ?? null;
        $hasMore = $page['has_more'] ?? null;
        if (!is_string($snapshotId) || !is_bool($hasMore)) {
            throw new \RuntimeException('group access snapshot page state is invalid');
        }
        if ($hasMore) {
            if ($nextCursor === null || !is_string($nextCursor) || $nextCursor === '') {
                throw new \RuntimeException('group access snapshot continuation is invalid');
            }
            $completed = $session[self::COMPLETED_KEY] ?? null;
            if (!is_string($completed) || !hash_equals($completed, $snapshotId)) {
                unset($session[self::COMPLETED_KEY]);
            }
            $session[self::STAGING_KEY] = [
                'access_snapshot_id' => $snapshotId,
                'next_cursor' => $nextCursor,
                'limit' => $limit,
            ];

            return $session;
        }
        if ($nextCursor !== null) {
            throw new \RuntimeException('terminal group access snapshot page has a cursor');
        }
        unset($session[self::STAGING_KEY]);
        $session[self::COMPLETED_KEY] = $snapshotId;

        return $session;
    }

    /** @param array<string,mixed> $session @return array<string,mixed> */
    public static function abort(array $session): array
    {
        // Always clear both keys. A non-stale page-chain error can race with a
        // higher realtime epoch after its state lock is released; preserving a
        // completed value from the earlier session copy could resurrect an old
        // authorization pin when the full session is written back.
        unset($session[self::STAGING_KEY], $session[self::COMPLETED_KEY]);

        return $session;
    }

    /** @param array<string,mixed> $session @return array<string,mixed> */
    public static function invalidate(array $session, string $currentSnapshotId): array
    {
        $completed = $session[self::COMPLETED_KEY] ?? null;
        if (!is_string($completed) || !hash_equals($completed, $currentSnapshotId)) {
            unset($session[self::COMPLETED_KEY]);
        }
        $staging = $session[self::STAGING_KEY] ?? null;
        $stagingSnapshotId = is_array($staging)
            ? ($staging['access_snapshot_id'] ?? null)
            : null;
        if (!is_string($stagingSnapshotId) || !hash_equals($stagingSnapshotId, $currentSnapshotId)) {
            unset($session[self::STAGING_KEY]);
        }

        return $session;
    }
}
