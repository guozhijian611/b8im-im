<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 数据访问封装
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Repository;

use B8im\ImBusiness\Config;
use PDO;
use PDOException;
use Throwable;
use B8im\ImBusiness\Service\ModuleLicenseRepositoryInterface;

final class ImRepository implements MessageShardRepositoryInterface, ModuleLicenseRepositoryInterface
{
    private PDO $pdo;
    private int $transactionDepth = 0;

    public function __construct(private readonly Config $config)
    {
        $this->pdo = $this->createPdo();
    }

    public static function connect(Config $config): self
    {
        return new self($config);
    }

    public function transaction(callable $callback): mixed
    {
        return $this->runTransaction($callback, true);
    }

    public function fetchOne(string $sql, array $params = []): ?array
    {
        return $this->runWithReconnect(function () use ($sql, $params): ?array {
            $statement = $this->pdo->prepare($sql);
            $statement->execute($params);
            $row = $statement->fetch();

            return is_array($row) ? $row : null;
        });
    }

    public function fetchAll(string $sql, array $params = []): array
    {
        return $this->runWithReconnect(function () use ($sql, $params): array {
            $statement = $this->pdo->prepare($sql);
            $statement->execute($params);

            return $statement->fetchAll();
        });
    }

    public function execute(string $sql, array $params = []): int
    {
        return $this->runWithReconnect(function () use ($sql, $params): int {
            $statement = $this->pdo->prepare($sql);
            $statement->execute($params);

            return $statement->rowCount();
        });
    }

    public function lastInsertId(): int
    {
        return (int) $this->pdo->lastInsertId();
    }

    private function createPdo(): PDO
    {
        $dsn = sprintf(
            'mysql:host=%s;port=%d;dbname=%s;charset=%s',
            $this->config->dbHost,
            $this->config->dbPort,
            $this->config->dbName,
            $this->config->dbCharset,
        );
        return new PDO($dsn, $this->config->dbUser, $this->config->dbPassword, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => false,
        ]);
    }

    private function reconnect(): void
    {
        $this->pdo = $this->createPdo();
    }

    private function runTransaction(callable $callback, bool $allowRetry): mixed
    {
        $commitStarted = false;
        try {
            $this->pdo->beginTransaction();
            $this->transactionDepth++;
            try {
                $result = $callback($this);
            } finally {
                $this->transactionDepth--;
            }
            $commitStarted = true;
            $this->pdo->commit();

            return $result;
        } catch (Throwable $throwable) {
            $this->rollBackIfNeeded();

            if ($allowRetry && $throwable instanceof PDOException && $this->isConnectionLost($throwable)) {
                $this->reconnect();

                if ($commitStarted) {
                    // The server may have committed even though the client did
                    // not receive COMMIT OK. Replaying arbitrary callbacks can
                    // duplicate edits/notices or turn success into a conflict.
                    throw new \RuntimeException(
                        'IM transaction commit outcome is unknown; retry with an operation id or SYNC state first.',
                        previous: $throwable,
                    );
                }

                return $this->runTransaction($callback, false);
            }

            throw $throwable;
        }
    }

    private function runWithReconnect(callable $callback): mixed
    {
        if ($this->transactionDepth > 0) {
            return $callback();
        }

        try {
            return $callback();
        } catch (PDOException $exception) {
            if (!$this->isConnectionLost($exception)) {
                throw $exception;
            }

            $this->reconnect();

            return $callback();
        }
    }

    private function rollBackIfNeeded(): void
    {
        try {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
        } catch (PDOException) {
        }
    }

    private function isConnectionLost(PDOException $exception): bool
    {
        $code = (int) ($exception->errorInfo[1] ?? $exception->getCode());
        if (in_array($code, [2006, 2013], true)) {
            return true;
        }

        return str_contains($exception->getMessage(), 'MySQL server has gone away')
            || str_contains($exception->getMessage(), 'Lost connection to MySQL server');
    }
}
