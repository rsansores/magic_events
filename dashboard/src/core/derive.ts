// What a dashboard works out from the relay's answers: pure functions of an
// answer and the database's time now, no framework, no words.

import { CONTRACT, type Consumer, type Recent, type Status } from './contract/types.js'
import { timeOf } from './time.js'

/** How the events are doing, in one word, worst last: `ok`, `lagging` (they
 * leave, late), `stuck` (something waits for someone), `down` (nothing
 * leaves). */
export type Mood = 'ok' | 'lagging' | 'stuck' | 'down'

const SEVERITY: Record<Mood, number> = { ok: 0, lagging: 1, stuck: 2, down: 3 }

/** Why the mood is not `ok`: each a fact of the answer. */
export type Reason =
  | { kind: 'not_running' }
  | { kind: 'broker_failing'; failures: number; error: string; retryAt: number | null }
  | { kind: 'broker_unreachable'; error: string }
  | { kind: 'parked'; n: number }
  | { kind: 'backlog'; n: number; since: number; mood: 'lagging' | 'stuck' }
  | { kind: 'consumer_behind'; name: string; n: number }

export interface HealthOptions {
  /** The database's time now, in ms since the epoch (`serverNow`). */
  now: number
  /** How long an event may wait in the outbox before the events count as
   * lagging, in ms. Default one minute. */
  lagAfter?: number
  /** How long before they count as stuck. Default 15 minutes. */
  stuckAfter?: number
  /** How many messages a consumer may have pending before it counts as
   * behind. Default 1,000. */
  consumerLag?: number
}

export interface Health {
  mood: Mood
  /** Worst first. */
  reasons: Reason[]
}

const moodOf = (r: Reason): Mood => {
  switch (r.kind) {
    case 'not_running':
    case 'broker_failing':
    case 'broker_unreachable':
      return 'down'
    case 'parked':
      return 'stuck'
    case 'backlog':
      return r.mood
    case 'consumer_behind':
      return 'lagging'
  }
}

/** Whether this dashboard reads an answer of that contract. */
export function readsContract(answer: { contract: number }): boolean {
  return answer.contract === CONTRACT
}

/** How the events are doing, and why. */
export function healthOf(status: Status, options: HealthOptions): Health {
  const { now, lagAfter = 60_000, stuckAfter = 15 * 60_000, consumerLag = 1_000 } = options
  const reasons: Reason[] = []
  const { relay, outbox } = status
  if (!relay.running) reasons.push({ kind: 'not_running' })
  if (relay.failures > 0)
    reasons.push({
      kind: 'broker_failing',
      failures: relay.failures,
      error: relay.last_error ?? '',
      retryAt: timeOf(relay.retry_after),
    })
  if (status.broker_error != null) reasons.push({ kind: 'broker_unreachable', error: status.broker_error })
  if (outbox.parked > 0) reasons.push({ kind: 'parked', n: outbox.parked })
  const oldest = timeOf(outbox.oldest_pending_at)
  if (outbox.pending > 0 && oldest != null) {
    const waited = now - oldest
    if (waited >= lagAfter)
      reasons.push({
        kind: 'backlog',
        n: outbox.pending,
        since: oldest,
        mood: waited >= stuckAfter ? 'stuck' : 'lagging',
      })
  }
  for (const c of status.consumers ?? [])
    if (c.pending >= consumerLag) reasons.push({ kind: 'consumer_behind', name: c.name, n: c.pending })
  reasons.sort((a, b) => SEVERITY[moodOf(b)] - SEVERITY[moodOf(a)])
  const mood = reasons.reduce<Mood>(
    (worst, r) => (SEVERITY[moodOf(r)] > SEVERITY[worst] ? moodOf(r) : worst),
    'ok',
  )
  return { mood, reasons }
}

/** One consumer, as a row of a table. */
export interface ConsumerRow {
  name: string
  filters: string[]
  pending: number
  ackPending: number
  redelivered: number
  /** When it was last delivered a message, in ms; `null` for never. */
  lastActive: number | null
  /** Whether it is further behind than `consumerLag`. */
  behind: boolean
}

/** The consumers, furthest behind first; none for a narrowed answer. */
export function consumersOf(status: Status, consumerLag = 1_000): ConsumerRow[] {
  return (status.consumers ?? [])
    .map((c: Consumer) => ({
      name: c.name,
      filters: c.filters,
      pending: c.pending,
      ackPending: c.ack_pending,
      redelivered: c.redelivered,
      lastActive: timeOf(c.last_active),
      behind: c.pending >= consumerLag,
    }))
    .sort((a, b) => b.pending - a.pending || a.name.localeCompare(b.name))
}

/** Events published per day, oldest first, every type together. */
export function publishedByDay(status: Status): { day: string; count: number }[] {
  const days = new Map<string, number>()
  for (const p of status.published ?? []) days.set(p.day, (days.get(p.day) ?? 0) + p.count)
  return [...days].map(([day, count]) => ({ day, count })).sort((a, b) => a.day.localeCompare(b.day))
}

/** Events published per type, over the days the answer carries, most first. */
export function publishedByType(status: Status): { type: string; count: number }[] {
  const types = new Map<string, number>()
  for (const p of status.published ?? []) types.set(p.type, (types.get(p.type) ?? 0) + p.count)
  return [...types]
    .map(([type, count]) => ({ type, count }))
    .sort((a, b) => b.count - a.count || a.type.localeCompare(b.type))
}

/** One message of `recent`, its CloudEvents attributes read out. */
export interface RecentRow {
  sequence: number
  subject: string
  id: string
  type: string
  /** `ce-time`, in ms; `null` when it does not read. */
  time: number | null
  /** The body, pretty-printed when it is JSON. */
  body: string
}

/** A header value as the binding writes it, percent-decoded. */
function decoded(value: string | undefined): string {
  if (value == null) return ''
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

/** The messages of `recent`, newest first. */
export function recentOf(recent: Recent): RecentRow[] {
  return recent.messages
    .map((m) => {
      let body = m.body
      try {
        body = JSON.stringify(JSON.parse(m.body), null, 2)
      } catch {
        // Not JSON: shown as it is.
      }
      return {
        sequence: m.sequence,
        subject: m.subject,
        id: decoded(m.headers['ce-id']),
        type: decoded(m.headers['ce-type']),
        time: timeOf(m.headers['ce-time'] ?? null),
        body,
      }
    })
    .reverse()
}
