<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
declare(strict_types=1);

use Phinx\Migration\AbstractMigration;

final class AddImTraceContext extends AbstractMigration
{
    public function up(): void
    {
        $table = $this->table('im_message_outbox');
        $table
            ->addColumn('traceparent', 'char', [
                'limit' => 55,
                'null' => true,
                'after' => 'payload_json',
                'comment' => 'W3C Trace Context version 00',
            ])
            ->addColumn('tracestate', 'string', [
                'limit' => 512,
                'null' => true,
                'after' => 'traceparent',
                'comment' => 'W3C tracestate',
            ])
            ->update();
    }

    public function down(): void
    {
        $this->table('im_message_outbox')
            ->removeColumn('tracestate')
            ->removeColumn('traceparent')
            ->update();
    }
}
