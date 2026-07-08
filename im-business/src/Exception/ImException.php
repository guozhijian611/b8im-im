<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 通信层业务异常
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Exception;

use RuntimeException;

final class ImException extends RuntimeException
{
    public function __construct(string $message, private readonly string $errorCode = 'IM_ERROR')
    {
        parent::__construct($message);
    }

    public function errorCode(): string
    {
        return $this->errorCode;
    }
}
