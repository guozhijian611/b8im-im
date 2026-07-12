<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | BusinessWorker 事件处理 - 连接 / 消息 / 断开
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness;

use GatewayWorker\Lib\Gateway;
use GatewayWorker\Lib\Context;
use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Auth\ConnectionFailurePolicy;
use B8im\ImBusiness\Auth\SessionResolver;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImShared\Protocol\Packet;
use B8im\ImShared\Protocol\Command;
use Workerman\Timer;

/**
 * IM 业务事件处理类
 *
 * Gateway 只负责长连接接入，所有鉴权、在线状态、消息写库和投递都在这里处理。
 */
class Events
{
    /**
     * 进程启动时触发。可在此初始化 DB / Redis / MQ 连接。
     */
    public static function onWorkerStart($businessWorker): void
    {
        Runtime::boot();
        self::startRealtimeEventConsumer();
        echo date('Y-m-d H:i:s') . " ImBusiness worker started\n";
    }

    /** 客户端建立 TCP 连接时 WebSocket 握手尚未完成，不能发送业务帧。 */
    public static function onConnect($clientId): void
    {
    }

    /**
     * WebSocket 握手完成后下发 client_id，等待客户端发送 AUTH 帧。
     *
     * challenge 不能在 onConnect 发送，否则经过 Nginx 等反向代理时，
     * 握手响应之前产生的数据帧可能被丢弃。
     */
    public static function onWebSocketConnect($clientId, $request): void
    {
        Gateway::sendToClient(
            $clientId,
            Packet::make(Command::AUTH, ['client_id' => $clientId])->encode()
        );
    }

    /**
     * 收到客户端消息时触发。
     *
     * readme §6.8 消息链路：校验登录态/租户/会话权限/禁言/黑名单/违禁词/限流
     * -> 生成 message_id -> 写库 -> 更新会话与未读 -> 推送在线方 -> MQ 后处理。
     */
    public static function onMessage($clientId, $message): void
    {
        $packet = Packet::decode((string) $message);
        if ($packet === null) {
            self::sendError($clientId, 'PACKET_INVALID', 'invalid packet');
            return;
        }

        try {
            match ($packet->cmd) {
                Command::AUTH => self::handleAuth($clientId, $packet),
                Command::PING => self::handlePing($clientId, $packet),
                Command::SEND => self::handleSend($clientId, $packet),
                Command::ACK => self::handleAck($clientId, $packet),
                Command::RECALL => self::handleRecall($clientId, $packet),
                Command::EDIT => self::handleEdit($clientId, $packet),
                Command::DELETE => self::handleDelete($clientId, $packet),
                Command::SCREENSHOT => self::handleScreenshot($clientId, $packet),
                Command::SYNC => self::handleSync($clientId, $packet),
                Command::TYPING => self::handleTyping($clientId, $packet),
                Command::PRESENCE => self::handlePresence($clientId, $packet),
                Command::CONVERSATION_READ => self::handleConversationRead($clientId, $packet),
                default => self::handleModuleCmd($clientId, $packet),
            };
        } catch (ImException $exception) {
            $errorCode = $exception->errorCode();
            self::sendError(
                $clientId,
                $errorCode,
                $exception->getMessage(),
                self::responseOrganization($clientId),
                $packet->clientMsgId,
            );
            if (ConnectionFailurePolicy::shouldClose($packet->cmd, $errorCode)) {
                Gateway::closeClient($clientId);
            }
        } catch (\Throwable $throwable) {
            echo date('Y-m-d H:i:s') . ' IM error: ' . $throwable->getMessage() . "\n";
            self::sendError(
                $clientId,
                'SERVER_ERROR',
                'server error',
                self::responseOrganization($clientId),
                $packet->clientMsgId,
            );
            if ($packet->cmd === Command::AUTH) {
                Gateway::closeClient($clientId);
            }
        }
    }

    /**
     * 客户端断开连接时触发。
     *
     * 需清理在线状态、连接映射（readme §6.2 多端连接列表）。
     */
    public static function onClose($clientId): void
    {
        try {
            $connection = Runtime::connections()->unbind($clientId);
            if ($connection !== null) {
                Runtime::devices()->offline($connection);
            }
        } catch (\Throwable $throwable) {
            echo date('Y-m-d H:i:s') . ' IM close error: ' . $throwable->getMessage() . "\n";
        }
    }

    private static function handleAuth(string $clientId, Packet $packet): void
    {
        $existingSession = Gateway::getSession($clientId);
        if (is_array($existingSession) && !empty($existingSession['session_id'])) {
            throw new ImException('当前连接已完成鉴权', 'AUTH_ALREADY_COMPLETED');
        }

        $token = (string) ($packet->data['token'] ?? '');
        $context = Runtime::token()->verify($token, $clientId);
        Runtime::authIdentities()->assertActive($context);
        $context = $context->withSessionId(bin2hex(random_bytes(16)));

        $decision = Runtime::tenantImPolicies()->authorizeAuth($context);

        try {
            foreach ($decision->clientIdsToDisconnect as $oldClientId) {
                if ($oldClientId !== '' && $oldClientId !== $clientId) {
                    Gateway::closeClient($oldClientId);
                }
            }
            $clientIp = long2ip((int) Context::$client_ip) ?: '';
            Runtime::devices()->online($context, $clientIp);
            Runtime::connections()->bind($clientId, $context);
            Gateway::bindUid($clientId, $context->uid());
            Gateway::setSession($clientId, $context->toArray());
        } catch (\Throwable $throwable) {
            Runtime::connections()->unbind($clientId);
            Runtime::devices()->offline($context->toArray());
            throw $throwable;
        } finally {
            $decision->release();
        }

        Gateway::sendToClient(
            $clientId,
            Packet::make(Command::AUTH_ACK, [
                'ok' => true,
                'user_id' => $context->userId,
                'device_id' => $context->deviceId,
                'client_id' => $context->clientId,
                'session_id' => $context->sessionId,
                'credential_session_id' => $context->credentialSessionId,
                'client_family' => $context->clientFamily,
                'os' => $context->os,
                'issuer' => $context->issuer,
                'audience' => $context->audience,
                'not_before' => $context->notBefore,
                'expire_at' => $context->expireAt,
            ], $context->organization)->encode()
        );
    }

    private static function handlePing(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        Runtime::connections()->touch($clientId);
        Gateway::sendToClient($clientId, Packet::make(Command::PONG, [], $context->organization)->encode());
    }

    private static function handleSend(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $permit = Runtime::tenantImPolicies()->acquireSendPermit(
            $context->organization,
            $context->userId,
            $context->clientFamily,
        );
        try {
            $result = Runtime::messages()->send($context, $packet->withServerOrganization($context->organization));
        } finally {
            $permit->release();
        }

        Gateway::sendToClient(
            $clientId,
            Packet::make(Command::SEND_ACK, [
                'ok' => true,
                'duplicated' => $result['duplicated'],
                'organization' => $context->organization,
                'conversation_id' => $result['message']['conversation_id'],
                'message_id' => $result['message']['message_id'],
                'message_seq' => $result['message']['message_seq'],
                'global_seq' => $result['message']['global_seq'],
                'client_msg_id' => $result['message']['client_msg_id'],
                'message' => $result['message'],
            ], $context->organization, $packet->clientMsgId)->encode()
        );
    }

    private static function handleAck(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->ack($context, $packet->data);
        Gateway::sendToClient($clientId, Packet::make(Command::ACK_ACK, $result, $context->organization)->encode());

        if ((string) $result['sender_id'] !== $context->userId) {
            Gateway::sendToUid(
                AuthContext::uidFor($context->organization, (string) $result['sender_id']),
                Packet::make(Command::ACK, $result, $context->organization)->encode()
            );
        }
    }

    private static function handleRecall(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->recall($context, $packet->data);
        Gateway::sendToClient($clientId, Packet::make(Command::RECALL_ACK, $result, $context->organization)->encode());
    }

    private static function handleEdit(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->edit($context, $packet->data);
        Gateway::sendToClient($clientId, Packet::make(Command::EDIT_ACK, $result, $context->organization)->encode());
    }

    private static function handleDelete(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->delete($context, $packet->data);
        Gateway::sendToClient($clientId, Packet::make(Command::DELETE_ACK, $result, $context->organization)->encode());
    }

    private static function handleScreenshot(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->screenshot($context, $packet->data);
        Gateway::sendToClient($clientId, Packet::make(Command::SCREENSHOT_ACK, $result, $context->organization)->encode());
    }

    private static function handleSync(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->sync($context, $packet->data);
        Gateway::sendToClient($clientId, Packet::make(Command::SYNC_ACK, $result, $context->organization)->encode());
    }

    private static function handleTyping(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        Runtime::typing()->relay($context, $clientId, $packet->data);
        // typing 是 fire-and-forget，不回 ack
    }

    private static function handlePresence(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $userIds = $packet->data['user_ids'] ?? [];
        if (!is_array($userIds)) {
            throw new ImException('user_ids 格式错误', 'PRESENCE_USER_IDS_INVALID');
        }

        $result = Runtime::presence()->query($context, $userIds);
        Gateway::sendToClient(
            $clientId,
            Packet::make(Command::PRESENCE_ACK, $result, $context->organization)->encode()
        );
    }

    private static function handleConversationRead(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $conversationId = trim((string) ($packet->data['conversation_id'] ?? ''));
        $lastReadMessageId = trim((string) ($packet->data['last_read_message_id'] ?? ''));

        $result = Runtime::conversationSync()->markRead($context, $clientId, $conversationId, $lastReadMessageId);
        Gateway::sendToClient(
            $clientId,
            Packet::make(Command::CONVERSATION_READ_ACK, $result, $context->organization)->encode()
        );
    }

    /**
     * 商业模块 cmd 分发。
     *
     * 核心 cmd 全在 match 里，走到这里的是模块注册的扩展 cmd（cs_*、rtc_* 等）。
     * 未注册的 cmd 仍返回 CMD_UNKNOWN。
     */
    private static function handleModuleCmd(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        if (!Runtime::cmdDispatcher()->has($packet->cmd)) {
            self::sendError($clientId, 'CMD_UNKNOWN', 'unknown cmd', $context->organization, $packet->clientMsgId);
            return;
        }

        Runtime::cmdDispatcher()->dispatch($clientId, $context->organization, $packet);
    }

    private static function startRealtimeEventConsumer(): void
    {
        Timer::add(0.5, static function (): void {
            try {
                Runtime::realtimeEvents()->consume();
            } catch (\Throwable $throwable) {
                echo date('Y-m-d H:i:s') . ' IM realtime event error: ' . $throwable->getMessage() . "\n";
            }
        });
    }

    private static function requireContext(string $clientId): AuthContext
    {
        $context = SessionResolver::mustResolve($clientId);
        Runtime::activeSessions()->assertActive($context);
        Runtime::tenantImPolicies()->assertConnectionAllowed($context->organization, $context->clientFamily);

        return $context;
    }

    private static function responseOrganization(string $clientId): int
    {
        try {
            return self::requireContext($clientId)->organization;
        } catch (\Throwable) {
            return 0;
        }
    }

    private static function sendError(
        string $clientId,
        string $code,
        string $message,
        int $organization = 0,
        ?string $clientMsgId = null
    ): void
    {
        Gateway::sendToClient(
            $clientId,
            Packet::make(Command::ERROR, ['code' => $code, 'msg' => $message], $organization, $clientMsgId)->encode()
        );
    }
}
