import assert from 'node:assert/strict'
import { randomBytes, randomUUID } from 'node:crypto'
import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

const API = process.env.API ?? 'http://127.0.0.1:18888'
const WS_URL = process.env.WS_URL ?? 'ws://127.0.0.1:18787'
const ORIGIN = process.env.ORIGIN ?? 'http://127.0.0.1:16988'
const ORGANIZATION = Number(process.env.ORGANIZATION ?? 1)
const OTHER_ORGANIZATION = Number(process.env.OTHER_ORGANIZATION ?? 2)
const FORGED_ORGANIZATION = 999999
const UINT64_MAX = 18446744073709551615n
const GROUP_ACCESS_PAGE_LIMIT = 100
const RUN_ID = process.env.QA_RUN_ID ?? randomUUID().replaceAll('-', '').slice(0, 16)
const MANIFEST_PATH = resolve(process.env.QA_MANIFEST ?? `/tmp/b8im-im-reliability-${RUN_ID}.json`)
assert.match(RUN_ID, /^[a-z0-9][a-z0-9-]{7,39}$/, 'QA_RUN_ID must be an 8-40 character QA marker')

function newTraceparent() {
  return `00-${randomBytes(16).toString('hex')}-${randomBytes(8).toString('hex')}-01`
}

function traceIdOf(traceparent) {
  const match = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/.exec(traceparent)
  assert.ok(match, 'traceparent must be canonical W3C version 00')
  assert.notEqual(match[1], '0'.repeat(32), 'trace-id must be non-zero')
  assert.notEqual(match[2], '0'.repeat(16), 'parent-id must be non-zero')
  return match[1]
}

function positiveDecimal(value, label) {
  assert.equal(typeof value, 'string', `${label} must be a string`)
  assert.match(value, /^[1-9][0-9]{0,19}$/, `${label} must be a canonical positive decimal`)
  assert.equal(BigInt(value) <= UINT64_MAX, true, `${label} exceeds uint64`)
  return value
}

function nonNegativeDecimal(value, label) {
  assert.equal(typeof value, 'string', label + ' must be a string')
  assert.match(value, /^(0|[1-9][0-9]{0,19})$/, label + ' must be a canonical decimal')
  assert.equal(BigInt(value) <= UINT64_MAX, true, label + ' exceeds uint64')
  return value
}

function assertExactKeys(value, expected, label) {
  assert.ok(value && typeof value === 'object' && !Array.isArray(value), label + ' must be an object')
  assert.deepEqual(Object.keys(value).sort(), [...expected].sort(), label + ' schema changed')
}

function validateGroupAccessEntries(entries, previousConversationId = '') {
  assert.ok(Array.isArray(entries), 'group access snapshot entries must be an array')
  let previous = previousConversationId
  for (const entry of entries) {
    assertExactKeys(
      entry,
      [
        'conversation_id',
        'conversation_type',
        'access_version',
        'access_state',
        'last_message_seq',
        'last_change_seq',
        'periods'
      ],
      'group access entry'
    )
    const conversationId = entry.conversation_id
    assert.equal(typeof conversationId, 'string', 'group conversation_id must be a string')
    assert.equal(conversationId.length > 0, true, 'group conversation_id must not be empty')
    assert.equal(conversationId.trim(), conversationId, 'group conversation_id must be trimmed')
    assert.equal(Buffer.byteLength(conversationId, 'utf8') <= 64, true, 'group conversation_id is too large')
    assert.equal(conversationId.includes('\0'), false, 'group conversation_id contains NUL')
    assert.equal(conversationId.includes('|'), false, 'group conversation_id contains a separator')
    if (previous !== '') {
      assert.equal(
        Buffer.compare(Buffer.from(conversationId), Buffer.from(previous)) > 0,
        true,
        'group access entries must be strictly ordered'
      )
    }
    previous = conversationId
    assert.equal(entry.conversation_type, 2, 'group access entry conversation_type must be 2')
    positiveDecimal(entry.access_version, 'group access_version')
    assert.ok(
      entry.access_state === 'active' || entry.access_state === 'history_only',
      'group access_state must be visible'
    )
    nonNegativeDecimal(entry.last_message_seq, 'group last_message_seq')
    nonNegativeDecimal(entry.last_change_seq, 'group last_change_seq')
    assert.ok(Array.isArray(entry.periods) && entry.periods.length > 0, 'visible group access needs periods')

    let previousPeriodNo = null
    let previousToSeq = null
    let openPeriods = 0
    for (const period of entry.periods) {
      assertExactKeys(period, ['period_no', 'from_seq', 'to_seq'], 'group access period')
      const periodNo = positiveDecimal(period.period_no, 'group period_no')
      const fromSeq = positiveDecimal(period.from_seq, 'group from_seq')
      const toSeq = period.to_seq === null
        ? null
        : positiveDecimal(period.to_seq, 'group to_seq')
      if (toSeq === null) {
        openPeriods += 1
      } else {
        assert.equal(BigInt(toSeq) >= BigInt(fromSeq), true, 'group period ends before it starts')
      }
      if (previousPeriodNo !== null) {
        assert.equal(BigInt(periodNo) > BigInt(previousPeriodNo), true, 'group period_no is not increasing')
        assert.notEqual(previousToSeq, null, 'an open group period must be terminal')
        assert.equal(BigInt(fromSeq) > BigInt(previousToSeq), true, 'group access periods overlap')
      }
      previousPeriodNo = periodNo
      previousToSeq = toSeq
    }
    assert.equal(
      openPeriods,
      entry.access_state === 'active' ? 1 : 0,
      'group access_state differs from its periods'
    )
  }
  return previous
}

function isAccessSnapshotRetryError(packet) {
  return packet.cmd === 'error' && [
    'ACCESS_SNAPSHOT_STALE',
    'ACCESS_SNAPSHOT_NOT_COMPLETED'
  ].includes(packet.data?.code)
}

function isCrossOrgSyncRetryError(packet) {
  return packet.cmd === 'error'
    && packet.data?.code === 'CROSS_ORG_ACCESS_SNAPSHOT_UNSTABLE'
}

const SUCCESS_TRACEPARENT = process.env.B8IM_TRACEPARENT ?? newTraceparent()
const SUCCESS_TRACE_ID = traceIdOf(SUCCESS_TRACEPARENT)
const CROSS_TENANT_TRACEPARENT = newTraceparent()
const CROSS_TENANT_TRACE_ID = traceIdOf(CROSS_TENANT_TRACEPARENT)

const manifest = {
  schema_version: 1,
  qa_run_id: RUN_ID,
  organization: ORGANIZATION,
  other_organization: OTHER_ORGANIZATION,
  started_at: new Date().toISOString(),
  accounts: [],
  messages: [],
  success_trace_id: SUCCESS_TRACE_ID,
  cross_tenant_trace_id: CROSS_TENANT_TRACE_ID
}

function qaClientMessageId(scenario) {
  const suffix = randomBytes(8).toString('hex')
  const prefix = 'qa-im-' + RUN_ID + '-'
  const scenarioLimit = 80 - prefix.length - suffix.length - 1
  assert.equal(scenarioLimit > 0, true, 'QA_RUN_ID leaves no room for a client_msg_id scenario')
  return prefix + scenario.slice(0, scenarioLimit) + '-' + suffix
}

function recordMessage(scenario, acknowledgement, traceId = null) {
  const message = acknowledgement.data.message
  manifest.messages.push({
    scenario,
    message_id: message.message_id,
    conversation_id: message.conversation_id,
    client_msg_id: message.client_msg_id,
    sender_id: message.sender_id,
    message_seq: Number(message.message_seq),
    global_seq: String(message.global_seq),
    ...(traceId === null ? {} : { trace_id: traceId })
  })
  return message
}

function writeManifest(result = {}) {
  mkdirSync(dirname(MANIFEST_PATH), { recursive: true })
  writeFileSync(MANIFEST_PATH, `${JSON.stringify({
    ...manifest,
    ...result,
    finished_at: new Date().toISOString()
  }, null, 2)}\n`, { mode: 0o600 })
}

function required(name) {
  assert.ok(process.env[name], `${name} is required`)
  return process.env[name]
}

function device(name) {
  return `qa-im-${RUN_ID}-${name}-${randomUUID()}`
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds))
}

async function post(organization, path, body, token = '', traceparent = null) {
  const headers = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    'App-Id': String(organization),
    Origin: ORIGIN
  }
  if (token !== '') {
    headers.Authorization = `Bearer ${token}`
  }
  if (traceparent !== null) {
    traceIdOf(traceparent)
    headers.traceparent = traceparent
  }

  const response = await fetch(new URL(path, API), {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10_000)
  })
  const payload = await response.json().catch(() => ({}))
  assert.equal(response.ok, true, `${path}: HTTP ${response.status} ${JSON.stringify(payload)}`)
  assert.equal(payload.code, 200, `${path}: ${JSON.stringify(payload)}`)
  return payload.data
}

async function adminRequest(method, path, body = null) {
  const response = await fetch(new URL(path, API), {
    method,
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      Authorization: `Bearer ${required('ADMIN_TOKEN')}`
    },
    body: body === null ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(10_000)
  })
  const payload = await response.json().catch(() => ({}))
  assert.equal(response.ok, true, `${path}: HTTP ${response.status}`)
  assert.equal(payload.code, 200, `${path}: ${JSON.stringify(payload)}`)
  return payload.data
}

async function login(organization, account, password, deviceId) {
  const data = await post(organization, '/saimulti/web/im/login', {
    account,
    password,
    device_id: deviceId
  })
  assert.equal(Number(data.organization), organization)
  assert.ok(data.token?.access_token)
  assert.ok(data.user?.user_id)
  return { token: data.token.access_token, userId: data.user.user_id }
}

class Peer {
  constructor(name) {
    this.name = name
    this.queue = []
    this.waiters = []
    this.socket = new WebSocket(WS_URL)
    this.socket.addEventListener('message', (event) => {
      assert.equal(typeof event.data, 'string', `${name}: expected a text frame`)
      const packet = JSON.parse(event.data)
      const index = this.waiters.findIndex((waiter) => waiter.test(packet))
      if (index < 0) {
        this.queue.push(packet)
        return
      }
      this.waiters.splice(index, 1)[0].resolve(packet)
    })
  }

  async open() {
    if (this.socket.readyState === WebSocket.OPEN) {
      return this
    }
    await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`${this.name}: open timeout`)),
        10_000
      )
      this.socket.addEventListener('open', () => {
        clearTimeout(timer)
        resolve()
      }, { once: true })
      this.socket.addEventListener('error', () => {
        clearTimeout(timer)
        reject(new Error(`${this.name}: open error`))
      }, { once: true })
    })
    return this
  }

  take(test, label, timeout = 10_000) {
    const queuedIndex = this.queue.findIndex(test)
    if (queuedIndex >= 0) {
      return Promise.resolve(this.queue.splice(queuedIndex, 1)[0])
    }

    return new Promise((resolve, reject) => {
      const waiter = {
        test,
        resolve: (packet) => {
          clearTimeout(timer)
          resolve(packet)
        }
      }
      const timer = setTimeout(() => {
        const index = this.waiters.indexOf(waiter)
        if (index >= 0) {
          this.waiters.splice(index, 1)
        }
        reject(new Error(`${this.name}: timeout waiting for ${label}`))
      }, timeout)
      this.waiters.push(waiter)
    })
  }

  expectNone(test, label, timeout = 1_500) {
    if (this.queue.some(test)) {
      return Promise.reject(new Error(`${this.name}: unexpected ${label}`))
    }

    return new Promise((resolve, reject) => {
      const waiter = {
        test,
        resolve: () => {
          clearTimeout(timer)
          reject(new Error(`${this.name}: unexpected ${label}`))
        }
      }
      const timer = setTimeout(() => {
        const index = this.waiters.indexOf(waiter)
        if (index >= 0) {
          this.waiters.splice(index, 1)
        }
        resolve()
      }, timeout)
      this.waiters.push(waiter)
    })
  }

  send(packet) {
    assert.equal(this.socket.readyState, WebSocket.OPEN)
    this.socket.send(JSON.stringify({ traceparent: newTraceparent(), ...packet, ts: Date.now() }))
  }

  async close() {
    if (this.socket.readyState === WebSocket.CLOSED) {
      return
    }
    const closed = new Promise((resolve) => {
      this.socket.addEventListener('close', resolve, { once: true })
    })
    if (this.socket.readyState === WebSocket.OPEN) {
      this.socket.close(1000, 'phase1 smoke complete')
    }
    await Promise.race([closed, sleep(2_000)])
  }
}

async function completeGroupAccessSnapshot(peer, organization, authSnapshotId) {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    let snapshotId = null
    let cursor = null
    let previousConversationId = ''
    let restart = false
    for (let page = 0; page < 100; page += 1) {
    const requestId = qaClientMessageId('group-access-' + page)
    peer.send({
      cmd: 'group_member_access_snapshot',
      organization: FORGED_ORGANIZATION,
      client_msg_id: requestId,
      data: {
        access_snapshot_id: snapshotId,
        cursor,
        limit: GROUP_ACCESS_PAGE_LIMIT
      }
    })
    const acknowledgement = await peer.take(
      (packet) => (packet.cmd === 'group_member_access_snapshot_ack' || packet.cmd === 'error')
        && packet.client_msg_id === requestId,
      'GROUP_MEMBER_ACCESS_SNAPSHOT_ACK'
    )
    assert.equal(acknowledgement.organization, organization)
    assert.equal(acknowledgement.client_msg_id, requestId)
    if (isAccessSnapshotRetryError(acknowledgement)) {
      restart = true
      break
    }
    assert.equal(
      acknowledgement.cmd,
      'group_member_access_snapshot_ack',
      JSON.stringify(acknowledgement)
    )
    const data = acknowledgement.data ?? {}
    assert.deepEqual(
      Object.keys(data).sort(),
      ['access_snapshot_id', 'entries', 'has_more', 'next_cursor'].sort(),
      'GROUP_MEMBER_ACCESS_SNAPSHOT_ACK schema changed'
    )
    const pageSnapshotId = positiveDecimal(
      data.access_snapshot_id,
      'GROUP_MEMBER_ACCESS_SNAPSHOT_ACK access_snapshot_id'
    )
    if (snapshotId === null) {
      assert.equal(
        BigInt(pageSnapshotId) >= BigInt(authSnapshotId),
        true,
        'group access snapshot regressed behind AUTH_ACK'
      )
      snapshotId = pageSnapshotId
    } else {
      assert.equal(pageSnapshotId, snapshotId, 'group access snapshot changed within a page chain')
    }
    previousConversationId = validateGroupAccessEntries(data.entries, previousConversationId)
    assert.equal(
      data.entries.length <= GROUP_ACCESS_PAGE_LIMIT,
      true,
      'group access snapshot exceeded the requested page limit'
    )
    assert.equal(typeof data.has_more, 'boolean', 'group access snapshot has_more must be boolean')
    if (!data.has_more) {
      assert.equal(data.next_cursor, null, 'terminal group access snapshot page must clear cursor')
      return snapshotId
    }
    assert.equal(
      data.entries.length > 0,
      true,
      'non-terminal group access snapshot page must not be empty'
    )
    assert.equal(typeof data.next_cursor, 'string', 'group access snapshot continuation cursor missing')
    assert.ok(data.next_cursor.length > 0, 'group access snapshot continuation cursor is empty')
    assert.equal(
      Buffer.byteLength(data.next_cursor, 'utf8') <= 512,
      true,
      'group access snapshot continuation cursor is too large'
    )
    assert.equal(data.next_cursor.includes('\0'), false, 'group access snapshot cursor contains NUL')
    cursor = data.next_cursor
    }
    if (!restart) {
      throw new Error('group access snapshot page limit exceeded')
    }
  }
  throw new Error('group access snapshot changed too many times')
}

async function authenticate(peer, organization, webToken, deviceId) {
  const challenge = await peer.take((packet) => packet.cmd === 'auth', 'AUTH challenge')
  assert.equal(Number(challenge.organization), 0)
  const clientId = String(challenge.data?.client_id ?? '')
  assert.ok(clientId)

  const authTraceparent = newTraceparent()
  const authTraceId = traceIdOf(authTraceparent)
  const credential = await post(organization, '/saimulti/web/im/imToken', {
    device_id: deviceId,
    client_id: clientId
  }, webToken, authTraceparent)
  const claims = JSON.parse(
    Buffer.from(credential.token.split('.')[1], 'base64url').toString('utf8')
  )

  peer.send({
    cmd: 'auth',
    organization: FORGED_ORGANIZATION,
    traceparent: authTraceparent,
    data: { token: credential.token, device_id: deviceId, platform: 'web' }
  })
  const acknowledgement = await peer.take(
    (packet) => packet.cmd === 'auth_ack' || packet.cmd === 'error',
    'AUTH_ACK'
  )
  assert.equal(acknowledgement.cmd, 'auth_ack', JSON.stringify(acknowledgement))
  assert.equal(acknowledgement.organization, organization)
  assert.equal(acknowledgement.data?.client_id, clientId)
  assert.equal(acknowledgement.data?.device_id, deviceId)
  assert.equal(acknowledgement.data?.credential_session_id, claims.session_id)
  assert.match(String(acknowledgement.data?.session_id ?? ''), /^[a-f0-9]{32}$/)
  assert.equal(traceIdOf(acknowledgement.traceparent), authTraceId)
  const authSnapshotId = positiveDecimal(
    acknowledgement.data?.access_snapshot_id,
    'AUTH_ACK access_snapshot_id'
  )
  const accessSnapshotId = await completeGroupAccessSnapshot(
    peer,
    organization,
    authSnapshotId
  )
  return { authTraceId, accessSnapshotId }
}

async function takeSendAcknowledgement(peer, clientMessageId, label) {
  const packet = await peer.take(
    (candidate) => (candidate.cmd === 'send_ack' || candidate.cmd === 'error')
      && candidate.client_msg_id === clientMessageId,
    label
  )
  assert.equal(packet.cmd, 'send_ack', JSON.stringify(packet))
  return packet
}

function validateGlobalSyncPage(
  acknowledgement,
  { organization, requestId, cursor, accessSnapshotId, limit }
) {
  assert.equal(acknowledgement.cmd, 'sync_ack', JSON.stringify(acknowledgement))
  assert.equal(acknowledgement.client_msg_id, requestId)
  assert.equal(acknowledgement.organization, organization)
  const data = acknowledgement.data ?? {}
  assertExactKeys(
    data,
    [
      'organization',
      'scope',
      'messages',
      'next_after_global_seq',
      'has_more',
      'cross_org_access_snapshot_id',
      'access_snapshot_id'
    ],
    'SYNC_ACK data'
  )
  assert.equal(data.organization, organization)
  assert.equal(data.scope, 'global')
  assert.equal(data.access_snapshot_id, accessSnapshotId)
  nonNegativeDecimal(data.cross_org_access_snapshot_id, 'SYNC_ACK cross_org_access_snapshot_id')
  const next = nonNegativeDecimal(data.next_after_global_seq, 'SYNC_ACK next_after_global_seq')
  nonNegativeDecimal(cursor, 'SYNC request cursor')
  assert.ok(Array.isArray(data.messages), 'SYNC_ACK messages must be an array')
  assert.equal(data.messages.length <= limit, true, 'SYNC_ACK exceeded the requested page limit')
  assert.equal(typeof data.has_more, 'boolean', 'SYNC_ACK has_more must be boolean')
  assert.equal(BigInt(next) >= BigInt(cursor), true, 'SYNC_ACK cursor regressed')
  if (data.has_more) {
    assert.equal(BigInt(next) > BigInt(cursor), true, 'SYNC_ACK continuation cursor did not advance')
  }

  let previousGlobalSeq = BigInt(cursor)
  for (const message of data.messages) {
    assert.ok(message && typeof message === 'object' && !Array.isArray(message), 'SYNC message must be an object')
    assert.equal(message.organization, organization, 'SYNC message organization changed')
    const globalSeq = positiveDecimal(message.global_seq, 'SYNC message global_seq')
    assert.equal(BigInt(globalSeq) > previousGlobalSeq, true, 'SYNC messages are not strictly ordered')
    assert.equal(BigInt(globalSeq) <= BigInt(next), true, 'SYNC message exceeds the response cursor')
    previousGlobalSeq = BigInt(globalSeq)
  }
  return data
}

async function syncFromZero(peer, organization, accessSnapshotId, messageId) {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    let cursor = '0'
    let crossOrgSnapshotId = null
    let restart = false
    for (let page = 0; page < 100; page += 1) {
      const requestId = qaClientMessageId('sync-' + attempt + '-' + page)
      peer.send({
        cmd: 'sync',
        organization: FORGED_ORGANIZATION,
        client_msg_id: requestId,
        data: {
          after_global_seq: cursor,
          access_snapshot_id: accessSnapshotId,
          limit: 100
        }
      })
      const acknowledgement = await peer.take(
        (packet) => (packet.cmd === 'sync_ack' || packet.cmd === 'error')
          && packet.client_msg_id === requestId,
        'SYNC_ACK'
      )
      assert.equal(acknowledgement.organization, organization)
      assert.equal(acknowledgement.client_msg_id, requestId)
      if (isAccessSnapshotRetryError(acknowledgement)) {
        accessSnapshotId = await completeGroupAccessSnapshot(
          peer,
          organization,
          accessSnapshotId
        )
        restart = true
        break
      }
      if (isCrossOrgSyncRetryError(acknowledgement)) {
        restart = true
        break
      }
      const data = validateGlobalSyncPage(
        acknowledgement,
        { organization, requestId, cursor, accessSnapshotId, limit: 100 }
      )
      if (crossOrgSnapshotId === null) {
        crossOrgSnapshotId = data.cross_org_access_snapshot_id
      } else if (data.cross_org_access_snapshot_id !== crossOrgSnapshotId) {
        restart = true
        break
      }
      const found = data.messages.find((message) => message.message_id === messageId)
      if (found) {
        return { message: found, accessSnapshotId }
      }
      assert.equal(data.has_more, true, 'message ' + messageId + ' not found')
      cursor = data.next_after_global_seq
    }
    if (!restart) {
      throw new Error('SYNC page limit exceeded')
    }
  }
  throw new Error('SYNC snapshot changed too many times')
}

async function expectInvalidGlobalSync(peer, organization, accessSnapshotId) {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const requestId = qaClientMessageId('invalid-sync-' + attempt)
    peer.send({
      cmd: 'sync',
      organization: FORGED_ORGANIZATION,
      client_msg_id: requestId,
      data: {
        after_global_seq: 0,
        access_snapshot_id: accessSnapshotId,
        limit: 20
      }
    })
    const packet = await peer.take(
      (candidate) => (candidate.cmd === 'sync_ack' || candidate.cmd === 'error')
        && candidate.client_msg_id === requestId,
      'SYNC_GLOBAL_SEQ_INVALID'
    )
    assert.equal(packet.organization, organization)
    assert.equal(packet.client_msg_id, requestId)
    if (isAccessSnapshotRetryError(packet)) {
      accessSnapshotId = await completeGroupAccessSnapshot(
        peer,
        organization,
        accessSnapshotId
      )
      continue
    }
    assert.equal(packet.cmd, 'error', JSON.stringify(packet))
    assert.equal(packet.data?.code, 'SYNC_GLOBAL_SEQ_INVALID')
    return { packet, accessSnapshotId }
  }
  throw new Error('invalid SYNC probe snapshot changed too many times')
}

const deviceA = device('alice')
const deviceB = device('bob')
const deviceX = device('alice-other')
const [sessionA, sessionB, otherSession] = await Promise.all([
  login(ORGANIZATION, process.env.A_ACCOUNT ?? 'qa_im_a', required('A_PASSWORD'), deviceA),
  login(ORGANIZATION, process.env.B_ACCOUNT ?? 'qa_im_b', required('B_PASSWORD'), deviceB),
  login(
    OTHER_ORGANIZATION,
    process.env.X_ACCOUNT ?? 'qa_im_x',
    required('X_PASSWORD'),
    deviceX
  )
])
assert.notEqual(sessionA.userId, sessionB.userId)
assert.notEqual(otherSession.userId, sessionA.userId)
assert.notEqual(otherSession.userId, sessionB.userId)
manifest.accounts = [
  { organization: ORGANIZATION, account: process.env.A_ACCOUNT ?? 'qa_im_a', user_id: sessionA.userId },
  { organization: ORGANIZATION, account: process.env.B_ACCOUNT ?? 'qa_im_b', user_id: sessionB.userId },
  { organization: OTHER_ORGANIZATION, account: process.env.X_ACCOUNT ?? 'qa_im_x', user_id: otherSession.userId }
]

const peers = []
try {
  const alice = await new Peer('alice').open()
  const bob = await new Peer('bob').open()
  peers.push(alice, bob)
  const aliceAuth = await authenticate(alice, ORGANIZATION, sessionA.token, deviceA)
  const bobAuth = await authenticate(bob, ORGANIZATION, sessionB.token, deviceB)
  manifest.auth_trace_id = aliceAuth.authTraceId
  manifest.alice_access_snapshot_id = aliceAuth.accessSnapshotId
  manifest.bob_access_snapshot_id = bobAuth.accessSnapshotId

  let sameDeviceCoexist = false
  if (process.env.COEXIST_CHECK === '1') {
    const duplicateAlice = await new Peer('alice-same-device').open()
    peers.push(duplicateAlice)
    await authenticate(duplicateAlice, ORGANIZATION, sessionA.token, deviceA)
    alice.send({ cmd: 'ping', organization: FORGED_ORGANIZATION, data: {} })
    duplicateAlice.send({ cmd: 'ping', organization: FORGED_ORGANIZATION, data: {} })
    const [originalPong, duplicatePong] = await Promise.all([
      alice.take((packet) => packet.cmd === 'pong', 'original same-device PONG'),
      duplicateAlice.take((packet) => packet.cmd === 'pong', 'duplicate same-device PONG')
    ])
    assert.equal(Number(originalPong.organization), ORGANIZATION)
    assert.equal(Number(duplicatePong.organization), ORGANIZATION)
    sameDeviceCoexist = true
  }

  const clientMessageId = qaClientMessageId('online')
  const onlineSend = {
    cmd: 'send',
    organization: FORGED_ORGANIZATION,
    client_msg_id: clientMessageId,
    traceparent: SUCCESS_TRACEPARENT,
    data: {
      conversation_type: 1,
      to_organization: ORGANIZATION,
      to_user_id: sessionB.userId,
      message_type: 1,
      content: { text: `[QA:${RUN_ID}] online delivery ${new Date().toISOString()}` }
    }
  }
  alice.send(onlineSend)
  const sendAcknowledgement = await takeSendAcknowledgement(alice, clientMessageId, 'SEND_ACK')
  assert.equal(Number(sendAcknowledgement.organization), ORGANIZATION)
  assert.equal(sendAcknowledgement.data?.ok, true)
  assert.equal(sendAcknowledgement.data?.duplicated, false)
  assert.equal(traceIdOf(sendAcknowledgement.traceparent), SUCCESS_TRACE_ID)
  const message = recordMessage('online_delivery', sendAcknowledgement, SUCCESS_TRACE_ID)

  const push = await bob.take(
    (packet) => packet.cmd === 'push'
      && packet.data?.message?.message_id === message.message_id,
    'PUSH',
    15_000
  )
  assert.equal(Number(push.organization), ORGANIZATION)
  assert.equal(traceIdOf(push.traceparent), SUCCESS_TRACE_ID)
  assert.match(String(push.data?.event_id ?? ''), /^[a-f0-9]{64}$/)
  await Promise.all([
    bob.expectNone(
      (packet) => packet.cmd === 'push'
        && packet.data?.message?.message_id === message.message_id,
      'duplicate PUSH'
    ),
    alice.expectNone(
      (packet) => packet.cmd === 'push'
        && packet.data?.message?.message_id === message.message_id,
      'origin PUSH echo'
    )
  ])

  alice.send({ ...onlineSend, traceparent: newTraceparent() })
  const duplicateAcknowledgement = await takeSendAcknowledgement(
    alice,
    clientMessageId,
    'duplicate SEND_ACK'
  )
  assert.equal(duplicateAcknowledgement.data?.duplicated, true)
  assert.equal(duplicateAcknowledgement.data?.message?.message_id, message.message_id)
  assert.equal(duplicateAcknowledgement.data?.message?.global_seq, message.global_seq)
  await bob.expectNone(
    (packet) => packet.cmd === 'push'
      && packet.data?.message?.message_id === message.message_id,
    'PUSH after duplicate SEND'
  )

  const deliveredAckClientMessageId = qaClientMessageId('ack-delivered')
  bob.send({
    cmd: 'ack',
    organization: FORGED_ORGANIZATION,
    client_msg_id: deliveredAckClientMessageId,
    data: { message_id: message.message_id, status: 'delivered' }
  })
  const [ackAcknowledgement, senderAck] = await Promise.all([
    bob.take(
      (packet) => (packet.cmd === 'ack_ack' || packet.cmd === 'error')
        && packet.client_msg_id === deliveredAckClientMessageId,
      'ACK_ACK'
    ),
    alice.take(
      (packet) => packet.cmd === 'ack'
        && packet.data?.message_id === message.message_id
        && packet.data?.client_msg_id === deliveredAckClientMessageId
        && packet.data?.request_client_msg_id === deliveredAckClientMessageId,
      'sender ACK'
    )
  ])
  assert.equal(ackAcknowledgement.cmd, 'ack_ack', JSON.stringify(ackAcknowledgement))
  assert.equal(ackAcknowledgement.client_msg_id, deliveredAckClientMessageId)
  assert.equal(ackAcknowledgement.data?.client_msg_id, deliveredAckClientMessageId)
  assert.equal(ackAcknowledgement.data?.request_client_msg_id, deliveredAckClientMessageId)
  assert.equal(ackAcknowledgement.data?.message_id, message.message_id)
  assert.equal(ackAcknowledgement.data.status, 'delivered')
  assert.equal(Number(senderAck.organization), ORGANIZATION)
  assert.equal(senderAck.client_msg_id ?? null, null)
  assert.equal(senderAck.data?.client_msg_id, deliveredAckClientMessageId)
  assert.equal(senderAck.data?.request_client_msg_id, deliveredAckClientMessageId)
  assert.equal(senderAck.data?.status, 'delivered')

  const duplicateAckClientMessageId = qaClientMessageId('ack-duplicate')
  bob.send({
    cmd: 'ack',
    organization: FORGED_ORGANIZATION,
    client_msg_id: duplicateAckClientMessageId,
    data: { message_id: message.message_id, status: 'delivered' }
  })
  const duplicateAck = await bob.take(
    (packet) => (packet.cmd === 'ack_ack' || packet.cmd === 'error')
      && packet.client_msg_id === duplicateAckClientMessageId,
    'duplicate ACK_ACK'
  )
  assert.equal(duplicateAck.cmd, 'ack_ack', JSON.stringify(duplicateAck))
  assert.equal(duplicateAck.client_msg_id, duplicateAckClientMessageId)
  assert.equal(duplicateAck.data?.client_msg_id, duplicateAckClientMessageId)
  assert.equal(duplicateAck.data?.request_client_msg_id, duplicateAckClientMessageId)
  assert.equal(duplicateAck.data?.message_id, message.message_id)
  assert.equal(duplicateAck.data.status, 'delivered')

  let assetResult = null
  const fileId = String(process.env.FILE_ID ?? '')
  if (fileId !== '') {
    assert.match(fileId, /^[a-f0-9]{40}$/)
    const assetClientMessageId = qaClientMessageId('asset')
    alice.send({
      cmd: 'send',
      organization: FORGED_ORGANIZATION,
      client_msg_id: assetClientMessageId,
      data: {
        conversation_type: 1,
        to_organization: ORGANIZATION,
        to_user_id: sessionB.userId,
        message_type: 2,
        content: { file_id: fileId }
      }
    })
    const assetAcknowledgement = await takeSendAcknowledgement(
      alice,
      assetClientMessageId,
      'asset SEND_ACK'
    )
    assert.equal(assetAcknowledgement.data?.message?.content?.file_id, fileId)
    const assetMessage = recordMessage('asset_delivery', assetAcknowledgement)
    await bob.take(
      (packet) => packet.cmd === 'push'
        && packet.data?.message?.message_id === assetMessage.message_id,
      'asset PUSH',
      15_000
    )
    assetResult = {
      asset_message_id: assetMessage.message_id,
      asset_conversation_id: assetMessage.conversation_id,
      asset_file_id: fileId
    }
  }

  const crossTenantId = qaClientMessageId('cross-organization')
  alice.send({
    cmd: 'send',
    organization: OTHER_ORGANIZATION,
    client_msg_id: crossTenantId,
    traceparent: CROSS_TENANT_TRACEPARENT,
    data: {
      conversation_type: 1,
      to_organization: OTHER_ORGANIZATION,
      to_user_id: otherSession.userId,
      message_type: 1,
      content: { text: `[QA:${RUN_ID}] cross-organization rejection probe` }
    }
  })
  const crossTenantError = await alice.take(
    (packet) => packet.cmd === 'error' && packet.client_msg_id === crossTenantId,
    'cross-tenant error'
  )
  assert.equal(Number(crossTenantError.organization), ORGANIZATION)
  assert.equal(crossTenantError.data?.code, 'SEND_SINGLE_RECEIVER_INVALID')
  assert.equal(traceIdOf(crossTenantError.traceparent), CROSS_TENANT_TRACE_ID)

  const invalidSyncResult = await expectInvalidGlobalSync(
    bob,
    ORGANIZATION,
    bobAuth.accessSnapshotId
  )
  const invalidSync = invalidSyncResult.packet
  manifest.bob_access_snapshot_id = invalidSyncResult.accessSnapshotId

  bob.send({ cmd: 'ping', organization: FORGED_ORGANIZATION, data: {} })
  const pong = await bob.take((packet) => packet.cmd === 'pong', 'PONG after error')
  assert.equal(Number(pong.organization), ORGANIZATION)

  await bob.close()
  await sleep(200)

  const offlineMessages = []
  for (const ordinal of [1, 2]) {
    const offlineId = qaClientMessageId(`offline-${ordinal}`)
    alice.send({
      cmd: 'send',
      organization: FORGED_ORGANIZATION,
      client_msg_id: offlineId,
      data: {
        conversation_type: 1,
        to_organization: ORGANIZATION,
        to_user_id: sessionB.userId,
        message_type: 1,
        content: { text: `[QA:${RUN_ID}] offline recovery ${ordinal}` }
      }
    })
    const offlineAck = await takeSendAcknowledgement(
      alice,
      offlineId,
      `offline SEND_ACK ${ordinal}`
    )
    assert.equal(offlineAck.data?.duplicated, false)
    offlineMessages.push(recordMessage(`offline_recovery_${ordinal}`, offlineAck))
  }
  assert.equal(
    Number(offlineMessages[1].message_seq),
    Number(offlineMessages[0].message_seq) + 1,
    'offline messages must have contiguous conversation sequence numbers'
  )
  assert.equal(
    BigInt(offlineMessages[1].global_seq) > BigInt(offlineMessages[0].global_seq),
    true,
    'offline messages must have increasing organization sequence numbers'
  )

  const reconnectedBob = await new Peer('bob-reconnect').open()
  peers.push(reconnectedBob)
  const reconnectedBobAuth = await authenticate(
    reconnectedBob,
    ORGANIZATION,
    sessionB.token,
    deviceB
  )
  let reconnectedBobAccessSnapshotId = reconnectedBobAuth.accessSnapshotId
  let syncResult = await syncFromZero(
    reconnectedBob,
    ORGANIZATION,
    reconnectedBobAccessSnapshotId,
    message.message_id
  )
  reconnectedBobAccessSnapshotId = syncResult.accessSnapshotId
  const synced = syncResult.message
  assert.equal(synced.global_seq, message.global_seq)
  for (const offlineMessage of offlineMessages) {
    syncResult = await syncFromZero(
      reconnectedBob,
      ORGANIZATION,
      reconnectedBobAccessSnapshotId,
      offlineMessage.message_id
    )
    reconnectedBobAccessSnapshotId = syncResult.accessSnapshotId
    const recovered = syncResult.message
    assert.equal(recovered.global_seq, offlineMessage.global_seq)
    assert.equal(recovered.message_seq, offlineMessage.message_seq)
  }
  manifest.reconnected_bob_access_snapshot_id = reconnectedBobAccessSnapshotId

  let adminSessionRevoke = false
  if (process.env.ADMIN_REVOKE_CHECK === '1') {
    const sessions = await adminRequest(
      'GET',
      `/saimulti/admin/im/operations/sessions?organization=${ORGANIZATION}`
        + `&status=1&keyword=${encodeURIComponent(deviceB)}&page=1&limit=100`
    )
    const target = sessions.data.find(
      (session) => session.device_id === deviceB && Number(session.status) === 1
    )
    assert.ok(target?.id, 'active reconnect session was not exposed to Admin IM operations')
    const closed = new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('realtime admin session revoke did not close the socket')),
        10_000
      )
      reconnectedBob.socket.addEventListener('close', (event) => {
        clearTimeout(timer)
        resolve(event)
      }, { once: true })
    })
    const revoked = await adminRequest(
      'POST',
      '/saimulti/admin/im/operations/revokeSession',
      { id: Number(target.id) }
    )
    assert.equal(Number(revoked.id), Number(target.id))
    assert.equal(Number(revoked.organization), ORGANIZATION)
    assert.equal(Number(revoked.status), 2)
    await closed
    adminSessionRevoke = true
  }

  let organizationDisable = false
  if (process.env.ORGANIZATION_DISABLE_CHECK === '1') {
    const otherPeer = await new Peer('other-organization').open()
    peers.push(otherPeer)
    await authenticate(
      otherPeer,
      OTHER_ORGANIZATION,
      otherSession.token,
      deviceX
    )
    const organization = await adminRequest(
      'GET',
      `/saimulti/admin/organization/read?id=${OTHER_ORGANIZATION}`
    )
    const updatePayload = {
      ...organization,
      id: OTHER_ORGANIZATION,
      region: [
        String(organization.province ?? ''),
        String(organization.city ?? ''),
        String(organization.area ?? '')
      ]
    }
    let disabled = false
    try {
      const closed = new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error('organization disable did not close the socket')),
          10_000
        )
        otherPeer.socket.addEventListener('close', (event) => {
          clearTimeout(timer)
          resolve(event)
        }, { once: true })
      })
      await adminRequest('PUT', '/saimulti/admin/organization/update', {
        ...updatePayload,
        status: 2
      })
      disabled = true
      await closed
      organizationDisable = true
    } finally {
      if (disabled) {
        await adminRequest('PUT', '/saimulti/admin/organization/update', {
          ...updatePayload,
          status: 1
        })
      }
    }
  }

  const result = {
    ok: true,
    qa_run_id: RUN_ID,
    manifest: MANIFEST_PATH,
    organization: ORGANIZATION,
    message_id: message.message_id,
    conversation_id: message.conversation_id,
    message_seq: message.message_seq,
    global_seq: message.global_seq,
    trace_id: SUCCESS_TRACE_ID,
    auth_trace_id: aliceAuth.authTraceId,
    cross_tenant_trace_id: CROSS_TENANT_TRACE_ID,
    cross_tenant_error: crossTenantError.data.code,
    invalid_sync_error: invalidSync.data.code,
    sync_found: true,
    duplicate_send_idempotent: true,
    duplicate_ack_monotonic: true,
    offline_recovery_count: offlineMessages.length,
    same_device_coexist: sameDeviceCoexist,
    admin_session_revoke: adminSessionRevoke,
    organization_disable: organizationDisable,
    ...(assetResult ?? {})
  }
  writeManifest({ ok: true, result })
  console.log(JSON.stringify(result))
} catch (error) {
  writeManifest({
    ok: false,
    error: error instanceof Error ? error.message : String(error)
  })
  throw error
} finally {
  await Promise.allSettled(peers.map((peer) => peer.close()))
}
