<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 在线状态查询
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImShared\Support\Constants;
use Redis;

/**
 * 在线状态查询
 *
 * 复用 ConnectionStore 写入的在线态：Redis Set im:{org}:online:{user_id} 存 client_id 列表，
 * Set 非空即在线。仅在当前 organization 范围内查询，天然租户隔离。
 */
final class PresenceService
{
    /** 单次查询最多 user_id 数量，防止超大请求 */
    private const MAX_QUERY = 100;

    public function __construct(
        private readonly Redis $redis,
    ) {
    }

    public static function connect(Config $config): self
    {
        $redis = new Redis();
        $redis->connect($config->redisHost, $config->redisPort, 2.0);
        if ($config->redisPassword !== '') {
            $redis->auth($config->redisPassword);
        }
        if ($config->redisDb > 0) {
            $redis->select($config->redisDb);
        }

        return new self($redis);
    }

    /**
     * 查询一批用户在当前机构的在线状态。
     *
     * @param AuthContext $context 当前鉴权上下文（用于限定 organization）
     * @param array       $userIds 待查询的 user_id 列表
     *
     * @return array{users: list<array{user_id: string, online: bool}>}
     */
    public function query(AuthContext $context, array $userIds): array
    {
        $userIds = $this->normalizeUserIds($userIds);
        if (count($userIds) > self::MAX_QUERY) {
            throw new ImException('查询用户数超过上限', 'PRESENCE_TOO_MANY_USERS');
        }

        $users = [];
        foreach ($userIds as $userId) {
            $key = sprintf(Constants::REDIS_ONLINE, $context->organization, $userId);
            $users[] = [
                'user_id' => $userId,
                'online' => (int) $this->redis->sCard($key) > 0,
            ];
        }

        return ['users' => $users];
    }

    /**
     * 去重、去空、转字符串。
     *
     * @return list<string>
     */
    private function normalizeUserIds(array $userIds): array
    {
        $normalized = [];
        foreach ($userIds as $userId) {
            $userId = trim((string) $userId);
            if ($userId !== '') {
                $normalized[$userId] = true;
            }
        }

        return array_keys($normalized);
    }
}
