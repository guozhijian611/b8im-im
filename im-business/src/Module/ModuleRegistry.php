<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 商业模块 cmd 注册入口
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Module;

use B8im\ImBusiness\CmdDispatcher;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\ModuleLicenseChecker;

/**
 * 模块注册入口
 *
 * Runtime::boot() 时调用 registerAll()，统一把所有商业模块的 cmd handler
 * 注册到 CmdDispatcher，每个 handler 绑定 license guard（通过 ModuleLicenseChecker 校验）。
 *
 * 如何接入新模块（以客服模块为例）：
 *   1. 在 composer.json 引入模块包（path 依赖或 private package）
 *   2. 在下面 registerAll() 中取消对应模块 Bootstrapper 的注释
 *   3. 模块 Bootstrapper 接收 CmdDispatcher 和 ModuleLicenseChecker，自行注册 cmd
 */
final class ModuleRegistry
{
    public static function registerAll(
        CmdDispatcher $dispatcher,
        ImRepository $repository,
        Config $config,
        ModuleLicenseChecker $licenseChecker,
    ): void {
        // —— 在此追加各商业模块的注册 ——

        // 客服模块（b8im-module-customer-service）：
        // (new \B8im\Module\CustomerService\ImCmdBootstrapper($licenseChecker))->register($dispatcher);

        // 音视频模块（b8im-module-rtc，示例）：
        // (new \B8im\Module\Rtc\ImCmdBootstrapper($licenseChecker))->register($dispatcher);
    }
}
