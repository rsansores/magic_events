// The dashboard's sentences: pure functions of what `derive` worked out, the
// messages, and the database's time. Each line is one whole message.

import { CONTRACT } from './contract/types.js'
import { readsContract, type Health, type Mood, type Reason } from './derive.js'
import { since, until } from './format.js'
import type { Translate } from './i18n.js'

/** How to say things: the messages, the locale, and the database's time now. */
export interface Speech {
  t: Translate
  locale: string
  now: number
}

const ago = (at: number, speech: Speech) => since(at, speech.now, speech.locale)
const ahead = (at: number, speech: Speech) => until(at, speech.now, speech.locale)

/** Why this dashboard cannot read an answer, when it cannot. */
export function mismatchOf(answer: { contract: number }, t: Translate): string | null {
  return readsContract(answer) ? null : t('contract.mismatch', { ours: CONTRACT, theirs: answer.contract })
}

export interface Banner {
  mood: Mood
  title: string
  lines: string[]
}

/** The banner: its title, and a sentence per reason. */
export function bannerOf(health: Health, speech: Speech): Banner {
  const lines =
    health.mood === 'ok'
      ? [speech.t('mood.ok.detail')]
      : health.reasons.flatMap((r) => describeReason(r, speech))
  return { mood: health.mood, title: speech.t(`mood.${health.mood}`), lines }
}

/** One reason, as one sentence or two. */
export function describeReason(r: Reason, speech: Speech): string[] {
  const { t } = speech
  switch (r.kind) {
    case 'not_running':
      return [t('reason.not_running')]
    case 'broker_failing': {
      const lines = [t('reason.broker_failing', { n: r.failures, error: r.error })]
      if (r.retryAt != null && r.retryAt > speech.now)
        lines.push(t('reason.broker_retry', { until: ahead(r.retryAt, speech) }))
      return lines
    }
    case 'broker_unreachable':
      return [t('reason.broker_unreachable', { error: r.error })]
    case 'parked':
      return [t('reason.parked', { n: r.n })]
    case 'backlog':
      return [t('reason.backlog', { n: r.n, since: ago(r.since, speech) })]
    case 'consumer_behind':
      return [t('reason.consumer_behind', { name: r.name, n: r.n })]
  }
}

/** "Last published 3 min. ago.", or that nothing was. */
export function describeLastPublished(at: number | null, speech: Speech): string {
  return at == null ? speech.t('relay.never') : speech.t('relay.last_published', { since: ago(at, speech) })
}

/** "2 min. ago", or `never` for nothing. */
export function describeSince(at: number | null, speech: Speech, never: string): string {
  return at == null ? never : ago(at, speech)
}
