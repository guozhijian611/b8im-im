<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 模块 cmd 注册与分发
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness;

use B8im\ImShared\Protocol\CmdHandlerInterface;
use B8im\ImShared\Protocol\Packet;

/**
 * 模块 cmd 分发器
 *
 * 核心 cmd（auth/send/ack/... 见 Events::onMessage）仍走 match，
 * 商业模块的 cmd（cs_*、rtc_* 等）通过这里注册和分发，实现"基础框架不改、
 * 模块自注册 cmd + license 门控"。
 *
 * 每个 handler 可绑定一个 license guard：dispatch 时先执行 guard 做运行时启用校验，
 * 未启用时 guard 抛 ImException，由 Events 统一转成 error 帧下发。
 */
final class CmdDispatcher
{
    /**
     * @var array<string, array{handler: CmdHandlerInterface, guard: (callable(int): void)|null}>
     */
    private array $registry = [];

    /**
     * 注册一个模块 cmd handler。
     *
     * @param CmdHandlerInterface        $handler      cmd 处理器
     * @param (callable(int): void)|null $licenseGuard 运行时启用校验，入参为 organization；未启用时抛 ImException
     */
    public function register(CmdHandlerInterface $handler, ?callable $licenseGuard = null): void
    {
        $cmd = $handler->cmd();
        if ($cmd === '') {
            throw new \InvalidArgumentException('cmd handler 返回了空 cmd');
        }
        if (isset($this->registry[$cmd])) {
            throw new \InvalidArgumentException("cmd 重复注册: {$cmd}");
        }

        $this->registry[$cmd] = ['handler' => $handler, 'guard' => $licenseGuard];
    }

    /**
     * 是否已注册该 cmd。
     */
    public function has(string $cmd): bool
    {
        return isset($this->registry[$cmd]);
    }

    /**
     * 已注册的全部 cmd 列表（便于自检和文档输出）。
     *
     * @return list<string>
     */
    public function commands(): array
    {
        return array_keys($this->registry);
    }

    /**
     * 分发到模块 handler。
     *
     * 调用方需先用 has() 判断存在性。先执行 license guard，再执行 handler。
     */
    public function dispatch(string $clientId, int $organization, Packet $packet): void
    {
        $entry = $this->registry[$packet->cmd] ?? null;
        if ($entry === null) {
            throw new \RuntimeException("cmd 未注册: {$packet->cmd}");
        }

        if ($entry['guard'] !== null) {
            ($entry['guard'])($organization);
        }

        $entry['handler']->handle($clientId, $packet->withServerOrganization($organization));
    }
}
