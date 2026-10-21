import { describe, expect, it } from 'vitest'
import { CONTRACT } from './contract/types.js'
import status from './contract/fixtures/status.js'
import narrowed from './contract/fixtures/narrowed.js'
import recent from './contract/fixtures/recent.js'
import { parseTimestamp } from './time.js'

// The fixtures are checked field for field against the generated types by
// the type-check (`satisfies`); here, what a type cannot say.

const TIME_FIELDS = /(_at|^at|^time|^last_active)$/

function times(value: unknown, key = ''): [string, unknown][] {
  if (Array.isArray(value)) return value.flatMap((v) => times(v, key))
  if (value && typeof value === 'object') return Object.entries(value).flatMap(([k, v]) => times(v, k))
  return TIME_FIELDS.test(key) && value != null ? [[key, value]] : []
}

describe('the relay’s answers', () => {
  it('are of the contract this dashboard reads', () => {
    for (const answer of [status, narrowed, recent]) expect(answer.contract).toBe(CONTRACT)
  })

  it('write every time as RFC 3339, which the dashboard reads', () => {
    for (const answer of [status, narrowed, recent])
      for (const [key, value] of times(answer))
        expect([key, parseTimestamp(String(value))]).not.toEqual([key, null])
  })

  it('leave out what a narrowed view cannot see', () => {
    expect(status.consumers).not.toBeNull()
    expect(status.published).not.toBeNull()
    expect(narrowed.consumers).toBeNull()
    expect(narrowed.published).toBeNull()
    expect(narrowed.stream?.bytes).toBeNull()
  })
})
