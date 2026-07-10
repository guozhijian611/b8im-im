<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 控制面实时事件消费
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Config;
use B8im\ImShared\Protocol\Command;
use B8im\ImShared\Protocol\Packet;
use JsonException;
use Closure;
use Throwable;

final class RealtimeEventConsumer
{
    /** @var Closure(int): void|null */
    private readonly ?Closure $policyInvalidator;

    private readonly string $workerId;

    public function __construct(
        private readonly RealtimeEventStoreInterface $store,
        private readonly RealtimeEventGatewayInterface $gateway,
        ?callable $policyInvalidator = null,
        ?string $workerId = null,
    ) {
        $this->policyInvalidator = $policyInvalidator === null
            ? null
            : Closure::fromCallable($policyInvalidator);
        $this->workerId = $workerId ?? sprintf(
            '%s:%d:%s',
            gethostname() ?: 'unknown-host',
            getmypid() ?: 0,
            bin2hex(random_bytes(8)),
        );
    }

    public static function connect(Config $config, TenantImPolicyService $policies): self
    {
        return new self(
            RedisRealtimeEventStore::connect($config),
            new GatewayRealtimeEventGateway(),
            static function (int $organization) use ($policies): void {
                $policies->invalidate($organization);
            },
        );
    }

    public function consume(int $limit = 20): void
    {
        $this->store->recoverExpired(min(1000, max(100, $limit)));
        for ($i = 0; $i < $limit; $i++) {
            $claim = $this->store->claim($this->workerId);
            if ($claim === null) {
                return;
            }

            try {
                $this->dispatch($claim->raw);
            } catch (Throwable $throwable) {
                try {
                    $this->store->requeue($claim);
                } catch (Throwable $requeueFailure) {
                    throw new \RuntimeException(
                        sprintf(
                            'Realtime event dispatch failed and requeue failed: %s',
                            $requeueFailure->getMessage(),
                        ),
                        0,
                        $throwable,
                    );
                }
                throw $throwable;
            }

            $this->store->ack($claim);
        }
    }

    private function dispatch(string $raw): void
    {
        try {
            $event = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return;
        }
        if (!is_array($event) || $event === [] || array_is_list($event)) {
            return;
        }
        if (preg_match('/^[a-f0-9]{64}$/', (string) ($event['event_id'] ?? '')) !== 1) {
            // 无唯一 ID 的旧格式或损坏事件明确 ack 丢弃，不进入业务分发。
            return;
        }

        if (RealtimeAuthEvent::supports($event['type'] ?? null)) {
            try {
                $this->dispatchAuthEvent(RealtimeAuthEvent::fromEnvelope($event));
            } catch (\InvalidArgumentException) {
                // 旧格式或界限不完整的控制事件必须丢弃，不能猜测范围后踢线。
            }
            return;
        }

        if (($event['type'] ?? null) === 'tenant.policy.changed') {
            $this->dispatchTenantPolicyChanged($event);
            return;
        }

        if (($event['type'] ?? '') === 'friend_request.created') {
            $this->dispatchFriendRequestCreated($event);
        }
    }

    /** @param array<string, mixed> $event */
    private function dispatchTenantPolicyChanged(array $event): void
    {
        $organization = $event['organization'] ?? null;
        $data = $event['data'] ?? null;
        if (!is_int($organization) || $organization <= 0 || !is_array($data) || array_is_list($data)) {
            return;
        }
        $version = $data['version'] ?? null;
        if (!is_int($version) || $version <= 0 || $this->policyInvalidator === null) {
            return;
        }

        ($this->policyInvalidator)($organization);
        $matchedSessions = $this->organizationSessions($organization);
        $this->store->invalidateCredentialSessions(
            $organization,
            array_values(array_unique(array_column($matchedSessions, 'credential_session_id'))),
        );
        foreach (array_keys($matchedSessions) as $clientId) {
            $this->gateway->close($clientId);
        }
    }

    private function dispatchAuthEvent(RealtimeAuthEvent $event): void
    {
        if ($event->type === RealtimeAuthEvent::ORGANIZATION_ENABLED) {
            $this->store->setOrganizationInactive($event->organization, false);
            return;
        }

        if ($event->type === RealtimeAuthEvent::ORGANIZATION_DISABLED) {
            // 先写强制阻断标记；即使网关遍历中途失败，后续 cmd 也不能借旧正缓存放行。
            $this->store->setOrganizationInactive($event->organization, true);
            $matchedSessions = $this->organizationSessions($event->organization);
            $this->store->invalidateCredentialSessions(
                $event->organization,
                array_values(array_unique(array_column($matchedSessions, 'credential_session_id'))),
            );
            foreach (array_keys($matchedSessions) as $clientId) {
                $this->gateway->close($clientId);
            }
            return;
        }

        $this->store->invalidateCredentialSessions($event->organization, $event->credentialSessionIds);
        foreach ($this->gateway->sessionsForUser($event->organization, $event->userId) as $clientId => $session) {
            if (!is_string($clientId) || !is_array($session) || !$this->matchesTargetSession($clientId, $session, $event)) {
                continue;
            }
            $this->gateway->close($clientId);
        }
    }

    /**
     * @return array<string, array{credential_session_id: string}>
     */
    private function organizationSessions(int $organization): array
    {
        $result = [];
        foreach ($this->gateway->allSessions() as $clientId => $session) {
            if (!is_string($clientId) || !is_array($session) || !$this->isCanonicalSession($clientId, $session, $organization)) {
                continue;
            }
            $result[$clientId] = [
                'credential_session_id' => (string) $session['credential_session_id'],
            ];
        }

        return $result;
    }

    /** @param array<string, mixed> $session */
    private function matchesTargetSession(string $clientId, array $session, RealtimeAuthEvent $event): bool
    {
        if (!$this->isCanonicalSession($clientId, $session, $event->organization)) {
            return false;
        }
        if (
            $session['user_id'] !== $event->userId
            || $session['device_id'] !== $event->deviceId
            || !in_array($session['credential_session_id'], $event->credentialSessionIds, true)
        ) {
            return false;
        }
        if ($event->type === RealtimeAuthEvent::SESSION_REVOKED) {
            if ($event->clientId !== null && $session['client_id'] !== $event->clientId) {
                return false;
            }
            if ($event->connectionSessionId !== null && $session['session_id'] !== $event->connectionSessionId) {
                return false;
            }
        }

        return true;
    }

    /** @param array<string, mixed> $session */
    private function isCanonicalSession(string $clientId, array $session, int $organization): bool
    {
        if (($session['organization'] ?? null) !== $organization) {
            return false;
        }
        foreach (['user_id', 'device_id', 'client_id', 'credential_session_id', 'session_id'] as $field) {
            $value = $session[$field] ?? null;
            if (!is_string($value) || $value === '' || trim($value) !== $value) {
                return false;
            }
        }

        return $session['client_id'] === $clientId
            && preg_match('/^[a-f0-9]{32}$/', $session['session_id']) === 1;
    }

    private function dispatchFriendRequestCreated(array $event): void
    {
        $organization = (int) ($event['organization'] ?? 0);
        $data = is_array($event['data'] ?? null) ? $event['data'] : [];
        $toUserId = trim((string) ($data['to_user_id'] ?? ''));
        if ($organization <= 0 || $toUserId === '') {
            return;
        }

        $this->gateway->sendToUser(
            $organization,
            $toUserId,
            Packet::make(Command::FRIEND_REQUEST, [
                'event' => 'created',
                'event_id' => (string) $event['event_id'],
                'request_id' => (int) ($data['request_id'] ?? 0),
                'from_user_id' => (string) ($data['from_user_id'] ?? ''),
                'to_user_id' => $toUserId,
                'message' => (string) ($data['message'] ?? ''),
                'pending_count' => (int) ($data['pending_count'] ?? 0),
                'create_time' => (string) ($data['create_time'] ?? ''),
                'from_user' => is_array($data['from_user'] ?? null) ? $data['from_user'] : null,
            ], $organization)->encode(),
        );
    }

}
