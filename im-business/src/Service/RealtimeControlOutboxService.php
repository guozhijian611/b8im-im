<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Repository\ImRepository;

final class RealtimeControlOutboxService
{
    private const MAX_RETRY = 10;
    private const BASE_RETRY_SECONDS = 30;
    private const MAX_RETRY_SECONDS = 3600;

    public function __construct(
        private readonly ImRepository $repository,
        private readonly int $leaseSeconds = 60,
    ) {
        if ($leaseSeconds < 10 || $leaseSeconds > 3600) {
            throw new \InvalidArgumentException('control outbox lease must be between 10 and 3600 seconds');
        }
    }

    public function claimPending(int $limit, string $workerId): array
    {
        if ($limit < 1 || $limit > 500 || $workerId === '' || strlen($workerId) > 64 || trim($workerId) !== $workerId) {
            throw new \InvalidArgumentException('control outbox claim parameters are invalid');
        }
        return $this->repository->transaction(function () use ($limit, $workerId): array {
            $now = $this->now();
            $candidates = $this->repository->fetchAll(
                'SELECT id FROM im_realtime_control_outbox '
                . 'WHERE (status=1 OR (status=4 AND next_retry_at<=?) OR (status=2 AND locked_until<=?)) '
                . 'AND retry_count < 10 ORDER BY id LIMIT ' . $limit . ' FOR UPDATE SKIP LOCKED',
                [$now, $now],
            );
            $claimed = [];
            foreach ($candidates as $candidate) {
                $id = (int) ($candidate['id'] ?? 0);
                $token = bin2hex(random_bytes(20));
                $lockedUntil = date('Y-m-d H:i:s', time() + $this->leaseSeconds);
                $changed = $this->repository->execute(
                    'UPDATE im_realtime_control_outbox SET status=2,next_retry_at=NULL,locked_until=?,'
                    . 'worker_id=?,claim_token=?,update_time=? WHERE id=? AND '
                    . '(status=1 OR (status=4 AND next_retry_at<=?) OR (status=2 AND locked_until<=?)) '
                    . 'AND retry_count < 10',
                    [$lockedUntil, $workerId, $token, $now, $id, $now, $now],
                );
                if ($changed !== 1) {
                    continue;
                }
                $row = $this->repository->fetchOne(
                    'SELECT * FROM im_realtime_control_outbox WHERE id=? AND status=2 AND claim_token=?',
                    [$id, $token],
                );
                if ($row === null) {
                    throw new \RuntimeException('claimed control outbox row disappeared');
                }
                $claimed[] = $row;
            }
            return $claimed;
        });
    }

    public function renew(int $id, string $token): void
    {
        $now = $this->now();
        $changed = $this->repository->execute(
            'UPDATE im_realtime_control_outbox SET locked_until=?,update_time=? '
            . 'WHERE id=? AND status=2 AND claim_token=? AND locked_until>?',
            [date('Y-m-d H:i:s', time() + $this->leaseSeconds), $now, $id, $token, $now],
        );
        $this->requireFence($changed);
    }

    public function markPublished(int $id, string $token): void
    {
        $now = $this->now();
        $changed = $this->repository->execute(
            'UPDATE im_realtime_control_outbox SET status=3,next_retry_at=NULL,locked_until=NULL,'
            . 'worker_id=NULL,claim_token=NULL,published_at=?,last_error=NULL,update_time=? '
            . 'WHERE id=? AND status=2 AND claim_token=? AND locked_until>?',
            [$now, $now, $id, $token, $now],
        );
        $this->requireFence($changed);
    }

    public function markFailed(int $id, string $token, string $error): void
    {
        $now = $this->now();
        $row = $this->repository->fetchOne(
            'SELECT retry_count FROM im_realtime_control_outbox '
            . 'WHERE id=? AND status=2 AND claim_token=? AND locked_until>? LIMIT 1',
            [$id, $token, $now],
        );
        if ($row === null) {
            $this->requireFence(0);
        }
        $retry = min(self::MAX_RETRY, (int) $row['retry_count'] + 1);
        $dead = $retry >= self::MAX_RETRY;
        $next = $dead ? null : date(
            'Y-m-d H:i:s',
            time() + min(self::MAX_RETRY_SECONDS, self::BASE_RETRY_SECONDS * (2 ** ($retry - 1))),
        );
        $changed = $this->repository->execute(
            'UPDATE im_realtime_control_outbox SET status=?,retry_count=?,next_retry_at=?,'
            . 'locked_until=NULL,worker_id=NULL,claim_token=NULL,last_error=?,update_time=? '
            . 'WHERE id=? AND status=2 AND claim_token=? AND locked_until>?',
            [$dead ? 5 : 4, $retry, $next, substr($error, 0, 500), $now, $id, $token, $now],
        );
        $this->requireFence($changed);
    }

    private function requireFence(int $changed): void
    {
        if ($changed !== 1) {
            throw new \RuntimeException('realtime control outbox claim token is stale');
        }
    }

    private function now(): string
    {
        return date('Y-m-d H:i:s');
    }
}
