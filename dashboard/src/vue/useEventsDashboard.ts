// The dashboard without its looks: what to show, and how to say it. The
// ready-made `EventsDashboard` is one layout over it; a host with its own
// design system calls it in its own component and draws the same answers
// with the sentence functions of `magic_events_dashboard/core`, given
// `speech`.
//
// It knows no URL: the host fetches `Events::status` from its own handler,
// behind its own authorization, and hands the dashboard the function that
// does.

import { computed, ref, shallowRef, toValue, type MaybeRefOrGetter } from 'vue'
import type { Status } from '../core/contract/types.js'
import { bannerOf, mismatchOf, type Speech } from '../core/describe.js'
import { consumersOf, healthOf, publishedByDay, publishedByType } from '../core/derive.js'
import type { Messages } from '../core/i18n.js'
import { timeOf } from '../core/time.js'
import { useServerClock } from './clock.js'
import { useDashboardI18n } from './i18n.js'
import { whileAlive } from './lifecycle.js'

export interface EventsDashboardOptions {
  /** How often to fetch the status again, in ms; 0 fetches it once.
   * Default 10 seconds. */
  every?: MaybeRefOrGetter<number | undefined>
  /** Force a locale; the host's vue-i18n locale otherwise. */
  locale?: MaybeRefOrGetter<string | undefined>
  /** Messages to add or override, per locale. */
  messages?: MaybeRefOrGetter<Messages | undefined>
  /** How long an event may wait in the outbox before the events count as
   * lagging, in ms. Default one minute. */
  lagAfter?: MaybeRefOrGetter<number | undefined>
  /** How long before they count as stuck. Default 15 minutes. */
  stuckAfter?: MaybeRefOrGetter<number | undefined>
  /** How many messages a consumer may have pending before it counts as
   * behind. Default 1,000. */
  consumerLag?: MaybeRefOrGetter<number | undefined>
}

export function useEventsDashboard(fetchStatus: () => Promise<Status>, options: EventsDashboardOptions = {}) {
  const { t, locale } = useDashboardI18n(
    () => toValue(options.locale),
    () => toValue(options.messages),
  )
  const status = shallowRef<Status | null>(null)
  const failed = ref<string | null>(null)
  const loading = ref(false)

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
      status.value = await fetchStatus()
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
  const start = () => {
    stopped = false
    void tick()
  }
  const stop = () => {
    stopped = true
    clearTimeout(timer)
  }
  whileAlive(start, stop)

  const now = useServerClock(() => status.value)

  /** Why the status cannot be shown, when it cannot: a fetch that failed
   * with no answer before it, an answer of another contract, or one whose
   * time does not read. */
  const error = computed(() => {
    const answer = status.value
    if (!answer) return failed.value == null ? null : t('fetch.failed', { error: failed.value })
    return mismatchOf(answer, t) ?? (now.value == null ? t('contract.unreadable') : null)
  })
  /** Why the last fetch failed, while an older answer is still shown. */
  const stale = computed(() =>
    status.value && failed.value != null ? t('fetch.failed', { error: failed.value }) : null,
  )
  const speech = computed<Speech | null>(() =>
    now.value == null ? null : { t, locale: locale.value, now: now.value },
  )
  const usable = computed(() => (status.value && now.value != null && !error.value ? status.value : null))

  const health = computed(() =>
    usable.value && now.value != null
      ? healthOf(usable.value, {
          now: now.value,
          lagAfter: toValue(options.lagAfter),
          stuckAfter: toValue(options.stuckAfter),
          consumerLag: toValue(options.consumerLag),
        })
      : null,
  )
  const banner = computed(() => (health.value && speech.value ? bannerOf(health.value, speech.value) : null))
  const consumers = computed(() =>
    usable.value?.consumers ? consumersOf(usable.value, toValue(options.consumerLag)) : null,
  )
  const published = computed(() =>
    usable.value?.published
      ? { byDay: publishedByDay(usable.value), byType: publishedByType(usable.value) }
      : null,
  )
  const oldestPending = computed(() => timeOf(usable.value?.outbox.oldest_pending_at ?? null))
  const lastPublished = computed(() => timeOf(usable.value?.relay.last_published_at ?? null))

  return {
    status: usable,
    health,
    banner,
    consumers,
    published,
    oldestPending,
    lastPublished,
    error,
    stale,
    loading,
    refresh,
    speech,
    t,
    locale,
    now,
  }
}

export type EventsDashboard = ReturnType<typeof useEventsDashboard>
