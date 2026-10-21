import { describe, expect, it } from 'vitest'
import status from './contract/fixtures/status.js'
import { bannerOf, describeLastPublished, describeReason, describeSince, mismatchOf } from './describe.js'
import { healthOf } from './derive.js'
import { translator } from './i18n.js'
import { parseTimestamp } from './time.js'

const now = parseTimestamp(status.at)!
const speech = { t: translator('en'), locale: 'en', now }
const MINUTE = 60_000

describe('the sentences', () => {
  it('say in one line that all is well', () => {
    expect(bannerOf({ mood: 'ok', reasons: [] }, speech)).toEqual({
      mood: 'ok',
      title: 'Events are flowing',
      lines: ['Every event that committed is published, and nothing waits.'],
    })
  })

  it('say each reason the events are not well', () => {
    const later = { ...speech, now: now + 2 * MINUTE }
    const banner = bannerOf(healthOf(status, { now: later.now }), later)
    expect(banner.title).toBe('Events are not leaving')
    expect(banner.lines).toEqual([
      'No process runs the relay: committed events wait in the outbox until one does.',
      '1 event was refused by the broker, and is set aside until it is sent again.',
      '1 event waits to be published; it was emitted 2 min. ago.',
    ])
  })

  it('say when the relay tries the broker again, only while it is to come', () => {
    const failing = {
      kind: 'broker_failing',
      failures: 2,
      error: 'timed out',
      retryAt: now + 30_000,
    } as const
    expect(describeReason(failing, speech)).toEqual([
      'The broker failed the last 2 passes: timed out',
      'Next attempt: in 30 sec.',
    ])
    expect(describeReason({ ...failing, failures: 1, retryAt: now - 1 }, speech)).toEqual([
      'The broker failed the last pass: timed out',
    ])
    expect(describeReason({ kind: 'consumer_behind', name: 'billing', n: 1200 }, speech)).toEqual([
      'billing is 1,200 messages behind.',
    ])
  })

  it('say when the last event left, or that none has', () => {
    expect(describeLastPublished(now - 3 * MINUTE, speech)).toBe('Last published 3 min. ago.')
    expect(describeLastPublished(null, speech)).toBe('Nothing published yet.')
    expect(describeSince(null, speech, 'Never')).toBe('Never')
  })

  it('refuse an answer of another contract, saying both versions', () => {
    expect(mismatchOf(status, speech.t)).toBeNull()
    expect(mismatchOf({ contract: 9 }, speech.t)).toMatch(/version 1, and the server answered version 9/)
  })
})
