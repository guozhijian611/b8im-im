<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Support\SingleConversationIdentity;

/**
 * Platform-level switch for cross-organization friends + single chat.
 * Reads the same sm_system_config key as the control plane.
 */
final class CrossOrganizationSocialPolicy
{
    public const CONFIG_GROUP = 'social_config';
    public const CONFIG_KEY = 'cross_org_social_enabled';
    public const SNAPSHOT_CONFIG_KEY = 'cross_org_access_snapshot_id';

    private ?array $cache = null;

    public function __construct(private readonly ImRepository $repository)
    {
    }

    public function clearCache(): void
    {
        $this->cache = null;
    }

    public function isEnabled(bool $fresh = false): bool
    {
        return $this->state($fresh)['enabled'];
    }

    public function snapshotId(bool $fresh = false): string
    {
        return $this->state($fresh)['access_snapshot_id'];
    }

    /**
     * Acquire a current shared lock on the managed policy rows.
     *
     * Cross-organization writers call this as the first database operation in
     * their transaction. Control-plane mutations take the same rows FOR UPDATE,
     * so a writer is linearized wholly before or wholly after a switch/snapshot
     * transition without serializing concurrent writers with each other.
     *
     * @return array{enabled:bool,access_snapshot_id:string,valid:bool}
     */
    public function lockStateForWrite(): array
    {
        return $this->loadState(' LOCK IN SHARE MODE');
    }

    /** @return array{enabled:bool,access_snapshot_id:string,valid:bool} */
    public function state(bool $fresh = false): array
    {
        $cached = $this->cache;
        if (!$fresh && is_array($cached) && (int) ($cached['expire_at'] ?? 0) > time()) {
            return [
                'enabled' => (bool) ($cached['enabled'] ?? false),
                'access_snapshot_id' => (string) ($cached['access_snapshot_id'] ?? '0'),
                'valid' => (bool) ($cached['valid'] ?? false),
            ];
        }

        return $this->loadState();
    }

    /**
     * @return array{enabled:bool,access_snapshot_id:string,valid:bool}
     */
    private function loadState(string $lockClause = ''): array
    {
        $enabled = false;
        $snapshotId = '0';
        $valid = false;
        $group = $this->repository->fetchOne(
            'SELECT id FROM sm_system_config_group
              WHERE code = ? AND delete_time IS NULL
              LIMIT 1' . $lockClause,
            [self::CONFIG_GROUP],
        );
        if ($group !== null) {
            $rows = $this->repository->fetchAll(
                'SELECT id, `key`, `value` FROM sm_system_config
                  WHERE group_id = ? AND `key` IN (?, ?) AND delete_time IS NULL
                  ORDER BY id' . $lockClause,
                [(int) $group['id'], self::CONFIG_KEY, self::SNAPSHOT_CONFIG_KEY],
            );
            $values = [];
            foreach ($rows as $row) {
                $values[(string) ($row['key'] ?? '')] = (string) ($row['value'] ?? '');
            }
            $candidate = trim((string) ($values[self::SNAPSHOT_CONFIG_KEY] ?? ''));
            $valid = preg_match('/^[1-9][0-9]*$/', $candidate) === 1 && strlen($candidate) <= 20;
            $snapshotId = $valid ? $candidate : '0';
            // A missing/invalid companion snapshot makes cross-organization
            // access fail closed even when the feature switch says enabled.
            $enabled = $valid && self::truthy($values[self::CONFIG_KEY] ?? '0');
        }

        $this->cache = [
            'expire_at' => time() + 15,
            'enabled' => $enabled,
            'access_snapshot_id' => $snapshotId,
            'valid' => $valid,
        ];

        return [
            'enabled' => $enabled,
            'access_snapshot_id' => $snapshotId,
            'valid' => $valid,
        ];
    }

    public static function truthy(mixed $value): bool
    {
        $normalized = strtolower(trim((string) $value));

        return in_array($normalized, ['1', 'true', 'yes', 'on', 'enabled'], true);
    }

    /**
     * Every single chat uses the same global identity rule. The complete
     * organization:user_id identities are byte-sorted before hashing.
     */
    public static function singleConversationId(
        int $leftOrganization,
        string $leftUserId,
        int $rightOrganization,
        string $rightUserId,
    ): string
    {
        return SingleConversationIdentity::conversationId(
            $leftOrganization,
            $leftUserId,
            $rightOrganization,
            $rightUserId,
        );
    }
}
