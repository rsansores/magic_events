import { effectScope, shallowRef } from 'vue'
import { describe, expect, it } from 'vitest'
import status from '../core/contract/fixtures/status.js'
import narrowed from '../core/contract/fixtures/narrowed.js'
import recent from '../core/contract/fixtures/recent.js'
import type { Recent, Status } from '../core/contract/types.js'
import { useEventsView } from './useEventsView.js'

/** The composable in an effect scope of its own, over answers the test
 * holds, as a host's data layer would. */
function view(initial: Status | null, recentAnswer: Recent | null = null) {
  const answer = shallowRef<Status | null>(initial)
  const recentRef = shallowRef<Recent | null>(recentAnswer)
  const scope = effectScope()
  const v = scope.run(() => useEventsView(answer, { recent: recentRef, locale: 'en' }))!
  return { v, answer, recentRef, scope }
}

describe('useEventsView', () => {
  it('says nothing, and no error, while there is no answer', () => {
    const { v, scope } = view(null)
    expect(v.status.value).toBeNull()
    expect(v.banner.value).toBeNull()
    expect(v.error.value).toBeNull()
    scope.stop()
  })

  it('works out what the answer it is given comes to, and follows the next one', () => {
    const { v, answer, scope } = view(status)
    expect(v.banner.value?.mood).toBe('down')
    expect(v.consumers.value?.map((c) => c.name)).toEqual(['billing', 'shipping'])
    answer.value = narrowed
    expect(v.consumers.value).toBeNull()
    expect(v.published.value).toBeNull()
    expect(v.status.value?.filter).toBe(narrowed.filter)
    scope.stop()
  })

  it('refuses an answer of another contract', () => {
    const { v, scope } = view({ ...status, contract: 2 })
    expect(v.error.value).toMatch(/version 1, and the server answered version 2/)
    expect(v.status.value).toBeNull()
    expect(v.banner.value).toBeNull()
    scope.stop()
  })

  it('reads the recent events it is given, newest first, and refuses another contract', () => {
    const { v, recentRef, scope } = view(status, recent)
    expect(v.recentError.value).toBeNull()
    expect(v.recent.value?.map((m) => m.sequence)).toEqual(
      [...recent.messages].map((m) => m.sequence).reverse(),
    )
    recentRef.value = { ...recent, contract: 2 }
    expect(v.recent.value).toBeNull()
    expect(v.recentError.value).toMatch(/version 1, and the server answered version 2/)
    scope.stop()
  })
})
