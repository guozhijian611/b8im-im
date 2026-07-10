<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 商业模块接入示例 —— 客服模块 cmd Bootstrapper
// +----------------------------------------------------------------------
// | 此文件是参考实现，展示 b8im-module-customer-service 未来接入 IM 的方式。
// | 实际客服模块源码在独立仓库（b8im-module-customer-service），通过
// | Composer path/private package 引入后，在 ModuleRegistry::registerAll()
// | 中取消注释对应行即可激活。
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Module\Example;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Auth\SessionResolver;
use B8im\ImBusiness\CmdDispatcher;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Service\ModuleLicenseChecker;
use B8im\ImShared\Protocol\CmdHandlerInterface;
use B8im\ImShared\Protocol\Command;
use B8im\ImShared\Protocol\Packet;
use GatewayWorker\Lib\Gateway;

// ─────────────────────────────────────────────
// 客服模块 cmd 常量（由模块自己声明，不进核心 Command.php）
// ─────────────────────────────────────────────
final class CustomerServiceCommand
{
    /** 模块标识，对应 sm_module.module_key / sm_tenant_module_license.module_key */
    public const MODULE_KEY = 'customer_service';

    public const CS_QUEUE      = 'cs_queue';       // 客服排队状态推送
    public const CS_ASSIGN     = 'cs_assign';      // 分配坐席（服务端推送）
    public const CS_TRANSFER   = 'cs_transfer';    // 转接
    public const CS_SESSION_END = 'cs_session_end'; // 会话结束
    public const CS_EVALUATE   = 'cs_evaluate';    // 访客发起满意度评价
    public const CS_BOT_HANDOFF = 'cs_bot_handoff'; // 机器人转人工
}

// ─────────────────────────────────────────────
// cmd handler 示例：cs_evaluate
// ─────────────────────────────────────────────
final class CsEvaluateHandler implements CmdHandlerInterface
{
    public function cmd(): string
    {
        return CustomerServiceCommand::CS_EVALUATE;
    }

    /**
     * 访客发送满意度评价。
     *
     * 实际逻辑：写库 → 推送给坐席 → 返回 ack。
     * 此处为参考骨架，真实实现在 b8im-module-customer-service 内。
     */
    public function handle(string $clientId, Packet $packet): void
    {
        $context = SessionResolver::mustResolve($clientId);

        $sessionId = trim((string) ($packet->data['session_id'] ?? ''));
        $score = (int) ($packet->data['score'] ?? 0);
        if ($sessionId === '') {
            throw new ImException('缺少 session_id', 'CS_EVALUATE_SESSION_ID_EMPTY');
        }
        if ($score < 1 || $score > 5) {
            throw new ImException('评分必须在 1~5 之间', 'CS_EVALUATE_SCORE_INVALID');
        }

        // TODO: 写库 im_cs_evaluate，推送给坐席用户
        $agentUserId = ''; // 从会话记录查出坐席 user_id
        if ($agentUserId !== '') {
            Gateway::sendToUid(
                AuthContext::uidFor($context->organization, $agentUserId),
                Packet::make(CustomerServiceCommand::CS_EVALUATE, [
                    'session_id' => $sessionId,
                    'score' => $score,
                    'comment' => (string) ($packet->data['comment'] ?? ''),
                    'visitor_user_id' => $context->userId,
                ], $context->organization)->encode()
            );
        }

        Gateway::sendToClient(
            $clientId,
            Packet::make(Command::ACK_ACK, ['session_id' => $sessionId, 'ok' => true], $context->organization)->encode()
        );
    }
}

// ─────────────────────────────────────────────
// 模块 Bootstrapper
// ─────────────────────────────────────────────

/**
 * 客服模块 IM cmd Bootstrapper
 *
 * 接入方式（在 ModuleRegistry::registerAll 中）：
 *   (new ImCmdBootstrapper($licenseChecker))->register($dispatcher);
 */
final class ImCmdBootstrapper
{
    public function __construct(
        private readonly ModuleLicenseChecker $licenseChecker,
    ) {
    }

    /**
     * 把客服模块所有 cmd handler 注册到分发器，并绑定 license guard。
     *
     * guard 会在每次 cmd dispatch 时执行，查 sm_tenant_module_license 的租户启用状态（带缓存）。
     */
    public function register(CmdDispatcher $dispatcher): void
    {
        $licenseChecker = $this->licenseChecker;
        $guard = static function (int $org) use ($licenseChecker): void {
            $licenseChecker->check($org, CustomerServiceCommand::MODULE_KEY);
        };

        $dispatcher->register(new CsEvaluateHandler(), $guard);

        // 其他客服 cmd：cs_queue / cs_assign / cs_transfer / cs_session_end / cs_bot_handoff
        // 等真实 handler 在 b8im-module-customer-service 里实现后，在此追加注册。
    }
}
