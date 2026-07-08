<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 控制面实时事件消费
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Config;
use B8im\ImShared\Protocol\Command;
use B8im\ImShared\Protocol\Packet;
use B8im\ImShared\Support\Constants;
use GatewayWorker\Lib\Gateway;
use Redis;

final class RealtimeEventConsumer
{
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

    public function consume(int $limit = 20): void
    {
        for ($i = 0; $i < $limit; $i++) {
            $raw = $this->redis->lPop(Constants::REDIS_REALTIME_EVENTS);
            if (!is_string($raw) || $raw === '') {
                return;
            }

            $this->dispatch($raw);
        }
    }

    private function dispatch(string $raw): void
    {
        $event = json_decode($raw, true);
        if (!is_array($event)) {
            return;
        }

        if (($event['type'] ?? '') === 'friend_request.created') {
            $this->dispatchFriendRequestCreated($event);
            return;
        }

        if (($event['type'] ?? '') === 'group_message.created') {
            $this->dispatchGroupMessageCreated($event);
        }
    }

    private function dispatchFriendRequestCreated(array $event): void
    {
        $organization = (int) ($event['organization'] ?? 0);
        $data = is_array($event['data'] ?? null) ? $event['data'] : [];
        $toUserId = trim((string) ($data['to_user_id'] ?? ''));
        if ($organization <= 0 || $toUserId === '') {
            return;
        }

        Gateway::sendToUid(
            AuthContext::uidFor($organization, $toUserId),
            Packet::make(Command::FRIEND_REQUEST, [
                'event' => 'created',
                'request_id' => (int) ($data['request_id'] ?? 0),
                'from_user_id' => (string) ($data['from_user_id'] ?? ''),
                'to_user_id' => $toUserId,
                'message' => (string) ($data['message'] ?? ''),
                'pending_count' => (int) ($data['pending_count'] ?? 0),
                'create_time' => (string) ($data['create_time'] ?? ''),
                'from_user' => is_array($data['from_user'] ?? null) ? $data['from_user'] : null,
            ], $organization)->encode()
        );
    }

    private function dispatchGroupMessageCreated(array $event): void
    {
        $organization = (int) ($event['organization'] ?? 0);
        $data = is_array($event['data'] ?? null) ? $event['data'] : [];
        $message = is_array($data['message'] ?? null) ? $data['message'] : [];
        $recipientUserIds = is_array($data['recipient_user_ids'] ?? null) ? $data['recipient_user_ids'] : [];
        if ($organization <= 0 || empty($message) || empty($recipientUserIds)) {
            return;
        }

        $packet = Packet::make(Command::PUSH, ['message' => $message], $organization)->encode();
        foreach ($recipientUserIds as $userId) {
            $userId = trim((string) $userId);
            if ($userId === '') {
                continue;
            }
            Gateway::sendToUid(AuthContext::uidFor($organization, $userId), $packet);
        }
    }
}
