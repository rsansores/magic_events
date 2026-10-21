// `useEventsView` over a function that fetches the status: it fetches, polls
// and keeps what the last fetch said, for a host with no data layer of its
// own. A host that reads the relay's answers through one — vue-query, Pinia —
// calls `useEventsView` with them instead, and polls them its own way.
//
// It knows no URL: the host fetches `Events::status` from its own handler,
// behind its own authorization, and hands the dashboard the function that
// does.

import { computed, ref, shallowRef, toValue, type MaybeRefOrGetter } from 'vue'
import type { Status } from '../core/contract/types.js'
import { whileAlive } from './lifecycle.js'
import { useEventsView, type EventsViewOptions } from './useEventsView.js'

export interface EventsDashboardOptions extends Omit<EventsViewOptions, 'recent'> {
  /** How often to fetch the status again, in ms; 0 fetches it once.
   * Default 10 seconds. */
  every?: MaybeRefOrGetter<number | undefined>
}

export function useEventsDashboard(fetchStatus: () => Promise<Status>, options: EventsDashboardOptions = {}) {
  const answer = shallowRef<Status | null>(null)
  const failed = ref<string | null>(null)
  const loading = ref(false)
  const view = useEventsView(answer, options)
  const { t } = view

  let again = false
  /** Fetches the status now; asked while a fetch is under way, fetches once
   * more after it, so what the host just did shows. */
  async function refresh(): Promise<void> {
    if (loading.value) {
      again = true
      return
    }
    loading.value = true
    try {
      answer.value = await fetchStatus()
      failed.value = null
    } catch (e) {
      failed.value = e instanceof Error ? e.message : String(e)
    } finally {
      loading.value = false
    }
    if (again) {
      again = false
      await refresh()
    }
  }

  let timer: ReturnType<typeof setTimeout> | undefined
  let stopped = false
  const tick = async () => {
    await refresh()
    const every = toValue(options.every) ?? 10_000
    if (!stopped && every > 0) timer = setTimeout(tick, every)
  }
  whileAlive(
    () => {
      stopped = false
      void tick()
    },
    () => {
      stopped = true
      clearTimeout(timer)
    },
  )

  /** Why the status cannot be shown, when it cannot: a fetch that failed
   * with no answer before it, or the view's reason. */
  const error = computed(() =>
    !answer.value && failed.value != null ? t('fetch.failed', { error: failed.value }) : view.error.value,
  )
  /** Why the last fetch failed, while an older answer is still shown. */
  const stale = computed(() =>
    answer.value && failed.value != null ? t('fetch.failed', { error: failed.value }) : null,
  )

  return { ...view, error, stale, loading, refresh }
}

export type EventsDashboard = ReturnType<typeof useEventsDashboard>
