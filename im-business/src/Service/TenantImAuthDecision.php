<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use Closure;

final class TenantImAuthDecision
{
    private bool $released = false;

    /** @param list<string> $clientIdsToDisconnect */
    public function __construct(
        public readonly array $clientIdsToDisconnect = [],
        private readonly ?Closure $releaseReservation = null,
    ) {
    }

    /**
     * Must run after the caller has disconnected replacements and bound the
     * new connection, so concurrent AUTH requests cannot exceed device caps.
     */
    public function release(): void
    {
        if ($this->released) {
            return;
        }
        $this->released = true;
        if ($this->releaseReservation !== null) {
            ($this->releaseReservation)();
        }
    }
}
