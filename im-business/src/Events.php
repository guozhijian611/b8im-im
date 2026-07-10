<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | BusinessWorker 事件处理 - 连接 / 消息 / 断开
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness;

use GatewayWorker\Lib\Gateway;
use B8im\ImBusiness\Auth\AuthContext;
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

    /**
     * 客户端与 Gateway 建立连接时触发。
     *
     * readme §6.2：连接建立后需用 IM token 鉴权，再把 client_id 与
     * user_id / organization / device_id 绑定。此处先下发 client_id，
     * 等待客户端发 AUTH 帧。
     */
    public static function onConnect($clientId): void
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
            self::sendError($clientId, $exception->errorCode(), $exception->getMessage(), $packet->organization, $packet->clientMsgId);
        } catch (\Throwable $throwable) {
            echo date('Y-m-d H:i:s') . ' IM error: ' . $throwable->getMessage() . "\n";
            self::sendError($clientId, 'SERVER_ERROR', 'server error', $packet->organization, $packet->clientMsgId);
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
        $token = (string) ($packet->data['token'] ?? '');
        $context = Runtime::token()->verify($token, $packet->organization, $packet->data);

        Gateway::bindUid($clientId, $context->uid());
        Gateway::setSession($clientId, $context->toArray());
        Runtime::connections()->bind($clientId, $context);
        Runtime::devices()->online($context, $clientId);

        Gateway::sendToClient(
            $clientId,
            Packet::make(Command::AUTH_ACK, [
                'ok' => true,
                'user_id' => $context->userId,
                'device_id' => $context->deviceId,
                'expire_at' => $context->expireAt,
            ], $context->organization)->encode()
        );
    }

    private static function handlePing(string $clientId, Packet $packet): void
    {
        Runtime::connections()->touch($clientId);
        Gateway::sendToClient($clientId, Packet::make(Command::PONG, [], $packet->organization)->encode());
    }

    private static function handleSend(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->send($context, $packet);

        Gateway::sendToClient(
            $clientId,
            Packet::make(Command::SEND_ACK, [
                'ok' => true,
                'duplicated' => $result['duplicated'],
                'message' => $result['message'],
            ], $context->organization, $packet->clientMsgId)->encode()
        );

        if ($result['duplicated']) {
            return;
        }

        $push = Packet::make(Command::PUSH, ['message' => $result['message']], $context->organization)->encode();
        foreach ($result['recipient_user_ids'] as $userId) {
            Gateway::sendToUid(AuthContext::uidFor($context->organization, (string) $userId), $push);
        }
        self::sendToOtherUserClients($context, $clientId, $push);
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

        $payload = Packet::make(Command::RECALL, [
            'message_id' => $result['message_id'],
            'conversation_id' => $result['conversation_id'],
            'notice_message' => $result['notice_message'] ?? null,
            'last_message_id' => $result['last_message_id'] ?? '',
            'last_message_seq' => $result['last_message_seq'] ?? 0,
            'last_message_time' => $result['last_message_time'] ?? '',
            'last_message_summary' => $result['last_message_summary'] ?? '',
            'time' => $result['time'] ?? date('Y-m-d H:i:s'),
        ], $context->organization)->encode();
        foreach ($result['recipient_user_ids'] as $userId) {
            if ((string) $userId === $context->userId) {
                continue;
            }
            Gateway::sendToUid(AuthContext::uidFor($context->organization, (string) $userId), $payload);
        }
        self::sendToOtherUserClients($context, $clientId, $payload);
    }

    private static function handleEdit(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->edit($context, $packet->data);
        Gateway::sendToClient($clientId, Packet::make(Command::EDIT_ACK, $result, $context->organization)->encode());

        $payload = Packet::make(Command::EDIT, [
            'message_id' => $result['message_id'],
            'conversation_id' => $result['conversation_id'],
            'message' => $result['message'],
            'last_message_id' => $result['last_message_id'] ?? '',
            'last_message_seq' => $result['last_message_seq'] ?? 0,
            'last_message_time' => $result['last_message_time'] ?? '',
            'last_message_summary' => $result['last_message_summary'] ?? '',
            'time' => $result['time'] ?? date('Y-m-d H:i:s'),
        ], $context->organization)->encode();
        foreach ($result['recipient_user_ids'] as $userId) {
            if ((string) $userId === $context->userId) {
                continue;
            }
            Gateway::sendToUid(AuthContext::uidFor($context->organization, (string) $userId), $payload);
        }
        self::sendToOtherUserClients($context, $clientId, $payload);
    }

    private static function handleDelete(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->delete($context, $packet->data);
        Gateway::sendToClient($clientId, Packet::make(Command::DELETE_ACK, $result, $context->organization)->encode());

        $payload = Packet::make(Command::DELETE, [
            'message_id' => $result['message_id'],
            'conversation_id' => $result['conversation_id'],
            'scope' => $result['scope'],
            'last_message_id' => $result['last_message_id'] ?? '',
            'last_message_seq' => $result['last_message_seq'] ?? 0,
            'last_message_time' => $result['last_message_time'] ?? '',
            'last_message_summary' => $result['last_message_summary'] ?? '',
            'time' => $result['time'] ?? date('Y-m-d H:i:s'),
        ], $context->organization)->encode();

        foreach ($result['recipient_user_ids'] as $userId) {
            if ((string) $userId === $context->userId) {
                continue;
            }
            Gateway::sendToUid(AuthContext::uidFor($context->organization, (string) $userId), $payload);
        }
        self::sendToOtherUserClients($context, $clientId, $payload);
    }

    private static function handleScreenshot(string $clientId, Packet $packet): void
    {
        $context = self::requireContext($clientId);
        $result = Runtime::messages()->screenshot($context, $packet->data);
        Gateway::sendToClient($clientId, Packet::make(Command::SCREENSHOT_ACK, $result, $context->organization)->encode());
        if (empty($result['enabled']) || empty($result['notice_message'])) {
            return;
        }

        $payload = Packet::make(Command::SCREENSHOT, [
            'conversation_id' => $result['conversation_id'],
            'notice_message' => $result['notice_message'],
            'last_message_id' => $result['last_message_id'] ?? '',
            'last_message_seq' => $result['last_message_seq'] ?? 0,
            'last_message_time' => $result['last_message_time'] ?? '',
            'last_message_summary' => $result['last_message_summary'] ?? '',
            'time' => $result['time'] ?? date('Y-m-d H:i:s'),
        ], $context->organization)->encode();
        foreach ($result['recipient_user_ids'] as $userId) {
            if ((string) $userId === $context->userId) {
                continue;
            }
            Gateway::sendToUid(AuthContext::uidFor($context->organization, (string) $userId), $payload);
        }
        self::sendToOtherUserClients($context, $clientId, $payload);
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
        if (!Runtime::cmdDispatcher()->has($packet->cmd)) {
            self::sendError($clientId, 'CMD_UNKNOWN', 'unknown cmd', $packet->organization, $packet->clientMsgId);
            return;
        }

        Runtime::cmdDispatcher()->dispatch($clientId, $packet);
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
        return SessionResolver::mustResolve($clientId);
    }

    private static function sendToOtherUserClients(AuthContext $context, string $currentClientId, string $payload): void
    {
        $clientIds = Gateway::getClientIdByUid($context->uid());
        foreach ($clientIds as $clientId) {
            if ($clientId !== $currentClientId) {
                Gateway::sendToClient($clientId, $payload);
            }
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
