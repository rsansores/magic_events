// The dashboard without its looks, over answers the host already holds: what
// to show, and how to say it. The host reads `Events::status` (and
// `Events::recent`) its own way — vue-query, Pinia, a socket — polls it as
// often as it likes and owns its failures; this works out the rest from
// whatever answer it holds now. `useEventsDashboard` is this over a fetch
// function, for a host with no data layer of its own.

import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import type { Recent, Status } from '../core/contract/types.js'
import { bannerOf, mismatchOf, type Speech } from '../core/describe.js'
import { consumersOf, healthOf, publishedByDay, publishedByType, recentOf } from '../core/derive.js'
import type { Messages } from '../core/i18n.js'
import { timeOf } from '../core/time.js'
import { useServerClock } from './clock.js'
import { useDashboardI18n } from './i18n.js'

export interface EventsViewOptions {
  /** `Events::recent`'s answer, when the host shows recent events. */
  recent?: MaybeRefOrGetter<Recent | null | undefined>
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

export function useEventsView(
  status: MaybeRefOrGetter<Status | null | undefined>,
  options: EventsViewOptions = {},
) {
  const { t, locale } = useDashboardI18n(
    () => toValue(options.locale),
    () => toValue(options.messages),
  )
  const answer = computed(() => toValue(status) ?? null)
  const now = useServerClock(() => answer.value)

  /** Why the answer cannot be shown, when it cannot: one of another
   * contract, or one whose time does not read. Nothing while there is no
   * answer: fetching it, and failing to, are the host's. */
  const error = computed(() => {
    const a = answer.value
    if (!a) return null
    return mismatchOf(a, t) ?? (now.value == null ? t('contract.unreadable') : null)
  })
  const speech = computed<Speech | null>(() =>
    now.value == null ? null : { t, locale: locale.value, now: now.value },
  )
  const usable = computed(() => (answer.value && now.value != null && !error.value ? answer.value : null))

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

  const recentAnswer = computed(() => toValue(options.recent) ?? null)
  /** Why the recent events cannot be shown: an answer of another contract. */
  const recentError = computed(() => (recentAnswer.value ? mismatchOf(recentAnswer.value, t) : null))
  /** The recent events, newest first; null without an answer that reads. */
  const recent = computed(() =>
    recentAnswer.value && !recentError.value ? recentOf(recentAnswer.value) : null,
  )

  return {
    status: usable,
    health,
    banner,
    consumers,
    published,
    oldestPending,
    lastPublished,
    recent,
    recentError,
    error,
    speech,
    t,
    locale,
    now,
  }
}

export type EventsView = ReturnType<typeof useEventsView>
