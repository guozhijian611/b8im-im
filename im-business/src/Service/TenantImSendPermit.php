<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use Closure;

final class TenantImSendPermit
{
    private bool $released = false;

    /** @param Closure(): void $release */
    public function __construct(private readonly Closure $release)
    {
    }

    public function release(): void
    {
        if ($this->released) {
            return;
        }
        $this->released = true;
        ($this->release)();
    }
}
