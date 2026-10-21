import { describe, expect, it } from 'vitest'
import type { Status } from './contract/types.js'
import status from './contract/fixtures/status.js'
import narrowed from './contract/fixtures/narrowed.js'
import recent from './contract/fixtures/recent.js'
import { consumersOf, healthOf, publishedByDay, publishedByType, readsContract, recentOf } from './derive.js'
import { parseTimestamp } from './time.js'

const at = parseTimestamp(status.at)!
const MINUTE = 60_000

/** The fixture's status with `change` laid over it. */
function with_(change: (s: Status) => void, from: Status = status): Status {
  const s = structuredClone(from)
  change(s)
  return s
}

/** A status with nothing wrong: running, nothing parked, nothing waiting. */
const calm = with_((s) => {
  s.relay.running = true
  s.outbox.pending = 0
  s.outbox.oldest_pending_at = null
  s.outbox.parked = 0
  s.outbox.first_parked = []
})

describe('healthOf', () => {
  it('is ok when everything leaves and nothing waits', () => {
    expect(healthOf(calm, { now: at })).toEqual({ mood: 'ok', reasons: [] })
  })

  it('is down when no process runs the relay', () => {
    const h = healthOf(status, { now: at })
    expect(h.mood).toBe('down')
    expect(h.reasons[0]).toEqual({ kind: 'not_running' })
  })

  it('is down while the broker fails the relay, and says when it tries again', () => {
    const failing = with_((s) => {
      s.relay.failures = 3
      s.relay.last_error = 'timed out'
      s.relay.retry_after = '2027-01-15T08:01:00Z'
    }, calm)
    expect(healthOf(failing, { now: at })).toEqual({
      mood: 'down',
      reasons: [
        {
          kind: 'broker_failing',
          failures: 3,
          error: 'timed out',
          retryAt: parseTimestamp('2027-01-15T08:01:00Z'),
        },
      ],
    })
    const unreachable = with_((s) => (s.broker_error = 'no responders'), calm)
    expect(healthOf(unreachable, { now: at }).reasons).toEqual([
      { kind: 'broker_unreachable', error: 'no responders' },
    ])
  })

  it('is stuck while a message is set aside', () => {
    const parked = with_((s) => (s.outbox.parked = 2), calm)
    expect(healthOf(parked, { now: at })).toEqual({ mood: 'stuck', reasons: [{ kind: 'parked', n: 2 }] })
  })

  it('lags, then is stuck, as the oldest event waits longer', () => {
    const waiting = with_((s) => {
      s.outbox.pending = 4
      s.outbox.oldest_pending_at = s.at
    }, calm)
    expect(healthOf(waiting, { now: at + 30_000 }).mood).toBe('ok')
    expect(healthOf(waiting, { now: at + MINUTE }).mood).toBe('lagging')
    expect(healthOf(waiting, { now: at + 15 * MINUTE }).reasons).toEqual([
      { kind: 'backlog', n: 4, since: at, mood: 'stuck' },
    ])
    expect(healthOf(waiting, { now: at + 5 * MINUTE, stuckAfter: 5 * MINUTE }).mood).toBe('stuck')
    expect(healthOf(waiting, { now: at + 10_000, lagAfter: 5_000 }).mood).toBe('lagging')
  })

  it('lags while a consumer is far behind, and names it', () => {
    expect(healthOf(calm, { now: at, consumerLag: 3 })).toEqual({
      mood: 'lagging',
      reasons: [{ kind: 'consumer_behind', name: 'billing', n: 3 }],
    })
  })

  it('lists the worst reason first', () => {
    const many = with_((s) => {
      s.relay.running = false
      s.outbox.parked = 1
    }, calm)
    expect(healthOf(many, { now: at, consumerLag: 1 }).reasons.map((r) => r.kind)).toEqual([
      'not_running',
      'parked',
      'consumer_behind',
      'consumer_behind',
    ])
  })
})

describe('the rest of what an answer comes to', () => {
  it('reads only its own contract', () => {
    expect(readsContract(status)).toBe(true)
    expect(readsContract({ contract: 2 })).toBe(false)
  })

  it('lists the consumers furthest behind first', () => {
    expect(consumersOf(status, 3).map((c) => [c.name, c.pending, c.behind, c.lastActive])).toEqual([
      ['billing', 3, true, null],
      ['shipping', 2, false, null],
    ])
    expect(consumersOf(narrowed)).toEqual([])
  })

  it('counts what was published by day and by type', () => {
    expect(publishedByDay(status)).toEqual([{ day: '2027-01-15', count: 3 }])
    expect(publishedByType(status)).toEqual([{ type: 'order.shipped.v1', count: 3 }])
    expect(publishedByDay(narrowed)).toEqual([])
  })

  it('reads the recent messages newest first, their attributes decoded', () => {
    const rows = recentOf(recent)
    expect(rows.map((r) => r.sequence)).toEqual([3, 2, 1])
    expect(rows[0]).toMatchObject({ type: 'order.shipped.v1', time: at })
    expect(rows[0]!.body).toBe('{\n  "store": "south",\n  "n": 3\n}')
    const encoded = recentOf({
      ...recent,
      messages: [{ ...recent.messages[0]!, headers: { 'ce-type': 'a%20b' }, body: 'not json' }],
    })
    expect(encoded[0]).toMatchObject({ type: 'a b', id: '', time: null, body: 'not json' })
  })
})
