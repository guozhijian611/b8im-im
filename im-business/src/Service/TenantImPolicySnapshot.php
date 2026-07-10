<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use UnexpectedValueException;

final class TenantImPolicySnapshot
{
    /**
     * @param list<string> $allowedClientFamilies
     */
    private function __construct(
        public readonly int $organization,
        public readonly array $allowedClientFamilies,
        public readonly bool $allowMultiDeviceOnline,
        public readonly int $maxOnlineDevices,
        public readonly string $sameDeviceLoginPolicy,
        public readonly string $crossDeviceLoginPolicy,
        public readonly int $maxMessageConcurrency,
        public readonly int $maxMessageQps,
        public readonly int $defaultGroupDisplayMemberCount,
        public readonly int $messageRecallWindowSeconds,
        public readonly int $messageEditWindowSeconds,
        public readonly bool $recallNoticeEnabled,
        public readonly bool $groupRecallNoticeEnabled,
        public readonly string $status,
        public readonly int $version,
    ) {
    }

    /** @param array<string, mixed> $row */
    public static function fromDatabaseRow(array $row): self
    {
        if (!array_key_exists('allowed_client_families_json', $row)) {
            throw new UnexpectedValueException('allowed_client_families_json is missing');
        }
        $families = json_decode((string) $row['allowed_client_families_json'], true);

        return self::build($row, $families);
    }

    /** @param array<string, mixed> $snapshot */
    public static function fromCache(array $snapshot): self
    {
        if (!array_key_exists('allowed_client_families', $snapshot)) {
            throw new UnexpectedValueException('allowed_client_families is missing');
        }

        return self::build($snapshot, $snapshot['allowed_client_families']);
    }

    /** @return array<string, mixed> */
    public function toCache(): array
    {
        return [
            'organization' => $this->organization,
            'allowed_client_families' => $this->allowedClientFamilies,
            'allow_multi_device_online' => $this->allowMultiDeviceOnline,
            'max_online_devices' => $this->maxOnlineDevices,
            'same_device_login_policy' => $this->sameDeviceLoginPolicy,
            'cross_device_login_policy' => $this->crossDeviceLoginPolicy,
            'max_message_concurrency' => $this->maxMessageConcurrency,
            'max_message_qps' => $this->maxMessageQps,
            'default_group_display_member_count' => $this->defaultGroupDisplayMemberCount,
            'message_recall_window_seconds' => $this->messageRecallWindowSeconds,
            'message_edit_window_seconds' => $this->messageEditWindowSeconds,
            'recall_notice_enabled' => $this->recallNoticeEnabled,
            'group_recall_notice_enabled' => $this->groupRecallNoticeEnabled,
            'status' => $this->status,
            'version' => $this->version,
        ];
    }

    /** @param array<string, mixed> $data */
    private static function build(array $data, mixed $families): self
    {
        $organization = self::integer($data, 'organization', 1, PHP_INT_MAX);
        if (!is_array($families) || $families === []) {
            throw new UnexpectedValueException('allowed client families must be a non-empty array');
        }
        $families = array_values(array_unique(array_map(
            static fn (mixed $value): string => trim((string) $value),
            $families,
        )));
        if (array_diff($families, ['web', 'app', 'desktop']) !== []) {
            throw new UnexpectedValueException('allowed client family is invalid');
        }

        $allowMulti = self::boolean($data, 'allow_multi_device_online');
        $samePolicy = self::oneOf($data, 'same_device_login_policy', ['replace', 'coexist', 'reject']);
        $crossPolicy = self::oneOf($data, 'cross_device_login_policy', ['allow', 'kick_old', 'reject_new']);
        if (!$allowMulti && $crossPolicy === 'allow') {
            throw new UnexpectedValueException('cross device allow contradicts disabled multi-device policy');
        }

        return new self(
            organization: $organization,
            allowedClientFamilies: $families,
            allowMultiDeviceOnline: $allowMulti,
            maxOnlineDevices: self::integer($data, 'max_online_devices', 1, 100),
            sameDeviceLoginPolicy: $samePolicy,
            crossDeviceLoginPolicy: $crossPolicy,
            maxMessageConcurrency: self::integer($data, 'max_message_concurrency', 1, 1000),
            maxMessageQps: self::integer($data, 'max_message_qps', 1, 10000),
            defaultGroupDisplayMemberCount: self::integer($data, 'default_group_display_member_count', 1, 100000),
            messageRecallWindowSeconds: self::integer($data, 'message_recall_window_seconds', 0, 86400),
            messageEditWindowSeconds: self::integer($data, 'message_edit_window_seconds', 0, 86400),
            recallNoticeEnabled: self::boolean($data, 'recall_notice_enabled'),
            groupRecallNoticeEnabled: self::boolean($data, 'group_recall_notice_enabled'),
            status: self::oneOf($data, 'status', ['ENABLED', 'DISABLED']),
            version: self::integer($data, 'version', 1, PHP_INT_MAX),
        );
    }

    /** @param array<string, mixed> $data */
    private static function integer(array $data, string $field, int $minimum, int $maximum): int
    {
        if (!array_key_exists($field, $data) || !is_numeric($data[$field])) {
            throw new UnexpectedValueException($field . ' is missing or invalid');
        }
        $value = (int) $data[$field];
        if ((string) $value !== (string) $data[$field] || $value < $minimum || $value > $maximum) {
            throw new UnexpectedValueException($field . ' is outside the canonical range');
        }

        return $value;
    }

    /** @param array<string, mixed> $data */
    private static function boolean(array $data, string $field): bool
    {
        if (!array_key_exists($field, $data)) {
            throw new UnexpectedValueException($field . ' is missing');
        }
        if (is_bool($data[$field])) {
            return $data[$field];
        }
        if (is_int($data[$field]) && in_array($data[$field], [0, 1], true)) {
            return $data[$field] === 1;
        }
        if (is_string($data[$field]) && in_array($data[$field], ['0', '1'], true)) {
            return $data[$field] === '1';
        }

        throw new UnexpectedValueException($field . ' is not canonical boolean');
    }

    /** @param array<string, mixed> $data @param list<string> $allowed */
    private static function oneOf(array $data, string $field, array $allowed): string
    {
        $value = $data[$field] ?? null;
        if (!is_string($value) || !in_array($value, $allowed, true)) {
            throw new UnexpectedValueException($field . ' is invalid');
        }

        return $value;
    }
}
