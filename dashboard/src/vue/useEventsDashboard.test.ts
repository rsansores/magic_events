import { effectScope } from 'vue'
import { describe, expect, it } from 'vitest'
import status from '../core/contract/fixtures/status.js'
import type { Status } from '../core/contract/types.js'
import { useEventsDashboard } from './useEventsDashboard.js'

/** The composable in an effect scope of its own, as a host's component runs
 * it; outside a browser it fetches only when told to. */
function dashboard(fetchStatus: () => Promise<Status>) {
  const scope = effectScope()
  const dash = scope.run(() => useEventsDashboard(fetchStatus, { every: 0, locale: 'en' }))!
  return { dash, scope }
}

describe('useEventsDashboard', () => {
  it('fetches the status and says what it comes to', async () => {
    const { dash, scope } = dashboard(async () => status)
    expect(dash.banner.value).toBeNull()
    await dash.refresh()
    expect(dash.error.value).toBeNull()
    expect(dash.banner.value?.mood).toBe('down')
    expect(dash.banner.value?.title).toBe('Events are not leaving')
    expect(dash.consumers.value?.map((c) => c.name)).toEqual(['billing', 'shipping'])
    expect(dash.published.value?.byType).toEqual([{ type: 'order.shipped.v1', count: 3 }])
    scope.stop()
  })

  it('says why it has nothing to show, and keeps an older answer when a fetch fails', async () => {
    let fail = true
    const { dash, scope } = dashboard(async () => {
      if (fail) throw new Error('503')
      return status
    })
    await dash.refresh()
    expect(dash.error.value).toBe('The status could not be fetched: 503')
    fail = false
    await dash.refresh()
    expect(dash.error.value).toBeNull()
    fail = true
    await dash.refresh()
    expect(dash.status.value).not.toBeNull()
    expect(dash.stale.value).toBe('The status could not be fetched: 503')
    scope.stop()
  })

  it('refuses an answer of another contract', async () => {
    const { dash, scope } = dashboard(async () => ({ ...status, contract: 2 }))
    await dash.refresh()
    expect(dash.error.value).toMatch(/version 1, and the server answered version 2/)
    expect(dash.banner.value).toBeNull()
    scope.stop()
  })
})
