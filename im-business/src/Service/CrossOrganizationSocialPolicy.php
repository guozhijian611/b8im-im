<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Repository\ImRepository;

/**
 * Platform-level switch for cross-organization friends + single chat.
 * Reads the same sm_system_config key as the control plane.
 */
final class CrossOrganizationSocialPolicy
{
    public const CONFIG_GROUP = 'social_config';
    public const CONFIG_KEY = 'cross_org_social_enabled';

    private ?array $cache = null;

    public function __construct(private readonly ImRepository $repository)
    {
    }

    public function clearCache(): void
    {
        $this->cache = null;
    }

    public function isEnabled(): bool
    {
        $cached = $this->cache;
        if (is_array($cached) && (int) ($cached['expire_at'] ?? 0) > time()) {
            return (bool) ($cached['enabled'] ?? false);
        }

        $enabled = false;
        $group = $this->repository->fetchOne(
            'SELECT id FROM sm_system_config_group WHERE code = ? AND delete_time IS NULL LIMIT 1',
            [self::CONFIG_GROUP],
        );
        if ($group !== null) {
            $row = $this->repository->fetchOne(
                'SELECT `value` FROM sm_system_config
                  WHERE group_id = ? AND `key` = ? AND delete_time IS NULL
                  LIMIT 1',
                [(int) $group['id'], self::CONFIG_KEY],
            );
            $enabled = self::truthy($row['value'] ?? '0');
        }

        $this->cache = [
            'expire_at' => time() + 15,
            'enabled' => $enabled,
        ];

        return $enabled;
    }

    public static function truthy(mixed $value): bool
    {
        $normalized = strtolower(trim((string) $value));

        return in_array($normalized, ['1', 'true', 'yes', 'on', 'enabled'], true);
    }

    /**
     * Org-independent conversation id for cross-tenant single chat.
     * Same-tenant keeps legacy single_{org-hash} via MessageService.
     */
    public static function crossOrgSingleConversationId(string $leftUserId, string $rightUserId): string
    {
        $pair = [$leftUserId, $rightUserId];
        sort($pair, SORT_STRING);

        return 'single_x_' . substr(sha1(implode(':', $pair)), 0, 40);
    }
}
