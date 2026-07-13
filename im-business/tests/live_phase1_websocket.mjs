import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

const API = process.env.API ?? 'http://127.0.0.1:18888'
const WS_URL = process.env.WS_URL ?? 'ws://127.0.0.1:18787'
const ORIGIN = process.env.ORIGIN ?? 'http://127.0.0.1:16988'
const ORGANIZATION = Number(process.env.ORGANIZATION ?? 1)
const OTHER_ORGANIZATION = Number(process.env.OTHER_ORGANIZATION ?? 2)
const FORGED_ORGANIZATION = 999999
const RUN_ID = process.env.QA_RUN_ID ?? randomUUID().replaceAll('-', '').slice(0, 16)
const MANIFEST_PATH = resolve(process.env.QA_MANIFEST ?? `/tmp/b8im-im-reliability-${RUN_ID}.json`)
assert.match(RUN_ID, /^[a-z0-9][a-z0-9-]{7,39}$/, 'QA_RUN_ID must be an 8-40 character QA marker')

const manifest = {
  schema_version: 1,
  qa_run_id: RUN_ID,
  organization: ORGANIZATION,
  other_organization: OTHER_ORGANIZATION,
  started_at: new Date().toISOString(),
  accounts: [],
  messages: []
}

function qaClientMessageId(scenario) {
  return `qa-im-${RUN_ID}-${scenario}-${randomUUID()}`
}

function recordMessage(scenario, acknowledgement) {
  const message = acknowledgement.data.message
  manifest.messages.push({
    scenario,
    message_id: message.message_id,
    conversation_id: message.conversation_id,
    client_msg_id: message.client_msg_id,
    sender_id: message.sender_id,
    message_seq: Number(message.message_seq),
    global_seq: String(message.global_seq)
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

async function post(organization, path, body, token = '') {
  const headers = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    'App-Id': String(organization),
    Origin: ORIGIN
  }
  if (token !== '') {
    headers.Authorization = `Bearer ${token}`
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
    this.socket.send(JSON.stringify({ ...packet, ts: Date.now() }))
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

async function authenticate(peer, organization, webToken, deviceId) {
  const challenge = await peer.take((packet) => packet.cmd === 'auth', 'AUTH challenge')
  assert.equal(Number(challenge.organization), 0)
  const clientId = String(challenge.data?.client_id ?? '')
  assert.ok(clientId)

  const credential = await post(organization, '/saimulti/web/im/imToken', {
    device_id: deviceId,
    client_id: clientId
  }, webToken)
  const claims = JSON.parse(
    Buffer.from(credential.token.split('.')[1], 'base64url').toString('utf8')
  )

  peer.send({
    cmd: 'auth',
    organization: FORGED_ORGANIZATION,
    data: { token: credential.token, device_id: deviceId, platform: 'web' }
  })
  const acknowledgement = await peer.take(
    (packet) => packet.cmd === 'auth_ack' || packet.cmd === 'error',
    'AUTH_ACK'
  )
  assert.equal(acknowledgement.cmd, 'auth_ack', JSON.stringify(acknowledgement))
  assert.equal(Number(acknowledgement.organization), organization)
  assert.equal(acknowledgement.data?.client_id, clientId)
  assert.equal(acknowledgement.data?.device_id, deviceId)
  assert.equal(acknowledgement.data?.credential_session_id, claims.session_id)
  assert.match(String(acknowledgement.data?.session_id ?? ''), /^[a-f0-9]{32}$/)
  return acknowledgement
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

async function syncFromZero(peer, organization, messageId) {
  let cursor = '0'
  for (let page = 0; page < 100; page += 1) {
    peer.send({
      cmd: 'sync',
      organization: FORGED_ORGANIZATION,
      data: { after_global_seq: cursor, limit: 100 }
    })
    const acknowledgement = await peer.take(
      (packet) => packet.cmd === 'sync_ack' || packet.cmd === 'error',
      'SYNC_ACK'
    )
    assert.equal(acknowledgement.cmd, 'sync_ack', JSON.stringify(acknowledgement))
    assert.equal(Number(acknowledgement.organization), organization)
    assert.equal(acknowledgement.data?.scope, 'global')
    const found = acknowledgement.data.messages.find(
      (message) => message.message_id === messageId
    )
    if (found) {
      return found
    }
    assert.equal(acknowledgement.data.has_more, true, `message ${messageId} not found`)
    const next = String(acknowledgement.data.next_after_global_seq)
    assert.notEqual(next, cursor)
    cursor = next
  }
  throw new Error('SYNC page limit exceeded')
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
  await authenticate(alice, ORGANIZATION, sessionA.token, deviceA)
  await authenticate(bob, ORGANIZATION, sessionB.token, deviceB)

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
    data: {
      conversation_type: 1,
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
  const message = recordMessage('online_delivery', sendAcknowledgement)

  const push = await bob.take(
    (packet) => packet.cmd === 'push'
      && packet.data?.message?.message_id === message.message_id,
    'PUSH',
    15_000
  )
  assert.equal(Number(push.organization), ORGANIZATION)
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

  alice.send(onlineSend)
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

  bob.send({
    cmd: 'ack',
    organization: FORGED_ORGANIZATION,
    data: { message_id: message.message_id, status: 'delivered' }
  })
  const [ackAcknowledgement, senderAck] = await Promise.all([
    bob.take(
      (packet) => packet.cmd === 'ack_ack'
        && packet.data?.message_id === message.message_id,
      'ACK_ACK'
    ),
    alice.take(
      (packet) => packet.cmd === 'ack'
        && packet.data?.message_id === message.message_id,
      'sender ACK'
    )
  ])
  assert.equal(ackAcknowledgement.data.status, 2)
  assert.equal(Number(senderAck.organization), ORGANIZATION)

  bob.send({
    cmd: 'ack',
    organization: FORGED_ORGANIZATION,
    data: { message_id: message.message_id, status: 'delivered' }
  })
  const duplicateAck = await bob.take(
    (packet) => packet.cmd === 'ack_ack'
      && packet.data?.message_id === message.message_id,
    'duplicate ACK_ACK'
  )
  assert.equal(duplicateAck.data.status, 2)

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
    data: {
      conversation_type: 1,
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

  const invalidSyncId = `phase1-bad-sync-${randomUUID()}`
  bob.send({
    cmd: 'sync',
    organization: FORGED_ORGANIZATION,
    client_msg_id: invalidSyncId,
    data: { after_global_seq: 0, limit: 20 }
  })
  const invalidSync = await bob.take(
    (packet) => packet.cmd === 'error' && packet.client_msg_id === invalidSyncId,
    'SYNC_GLOBAL_SEQ_INVALID'
  )
  assert.equal(invalidSync.data?.code, 'SYNC_GLOBAL_SEQ_INVALID')

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
  await authenticate(reconnectedBob, ORGANIZATION, sessionB.token, deviceB)
  const synced = await syncFromZero(reconnectedBob, ORGANIZATION, message.message_id)
  assert.equal(synced.global_seq, message.global_seq)
  for (const offlineMessage of offlineMessages) {
    const recovered = await syncFromZero(
      reconnectedBob,
      ORGANIZATION,
      offlineMessage.message_id
    )
    assert.equal(recovered.global_seq, offlineMessage.global_seq)
    assert.equal(recovered.message_seq, offlineMessage.message_seq)
  }

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
