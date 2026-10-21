// The dashboard's own strings, in English, and how one is said: every
// message is a whole sentence (or a whole label), and a count picks its form
// by the locale's plural rules. A host adds its own locales, or rewords any
// message, through `messages`.

import { count, normalizeLocale, pluralRules } from './format.js'

/** A message that depends on a count `n`: one form per plural category the
 * locale uses (`Intl.PluralRules`), `other` always. */
export type Plural = { [K in Intl.LDMLPluralRule]?: string } & { other: string }
export type Message = string | Plural
export type MessageTable = Record<string, Message>
export type Locale = 'en' | (string & {})
export type Messages = Partial<Record<Locale, MessageTable>>
/** A message by key: `params.n` picks a plural's form, and every number is
 * written the locale's way. */
export type Translate = (key: string, params?: Record<string, string | number>) => string

const en: MessageTable = {
  'mood.ok': 'Events are flowing',
  'mood.ok.detail': 'Every event that committed is published, and nothing waits.',
  'mood.lagging': 'Lagging',
  'mood.stuck': 'Needs attention',
  'mood.down': 'Events are not leaving',
  'reason.not_running': 'No process runs the relay: committed events wait in the outbox until one does.',
  'reason.broker_failing': {
    one: 'The broker failed the last pass: {error}',
    other: 'The broker failed the last {n} passes: {error}',
  },
  'reason.broker_retry': 'Next attempt: {until}',
  'reason.broker_unreachable': 'The stream could not be read: {error}',
  'reason.parked': {
    one: '{n} event was refused by the broker, and is set aside until it is sent again.',
    other: '{n} events were refused by the broker, and are set aside until they are sent again.',
  },
  'reason.backlog': {
    one: '{n} event waits to be published; it was emitted {since}.',
    other: '{n} events wait to be published; the oldest was emitted {since}.',
  },
  'reason.consumer_behind': {
    one: '{name} is {n} message behind.',
    other: '{name} is {n} messages behind.',
  },
  loading: 'Loading…',
  'fetch.failed': 'The status could not be fetched: {error}',
  'contract.mismatch':
    'This dashboard reads answers of version {ours}, and the server answered version {theirs}. Upgrade the dashboard and the crate together.',
  'contract.unreadable': 'The server’s answer does not read: its time is not a time.',
  'outbox.title': 'Outbox',
  'outbox.pending': 'Waiting',
  'outbox.parked': 'Set aside',
  'outbox.oldest': 'Oldest',
  'outbox.none': '—',
  'relay.title': 'Relay',
  'relay.running': 'Running',
  'relay.stopped': 'Not running',
  'relay.last_published': 'Last published {since}.',
  'relay.never': 'Nothing published yet.',
  'parked.title': 'Set aside',
  'parked.attempts': {
    one: 'Refused {n} time: {error}',
    other: 'Refused {n} times: {error}',
  },
  'parked.more': {
    one: 'And {n} more.',
    other: 'And {n} more.',
  },
  'stream.title': 'Stream',
  'stream.messages': {
    one: '{n} message held',
    other: '{n} messages held',
  },
  'stream.subjects.more': {
    one: 'And {n} more subject.',
    other: 'And {n} more subjects.',
  },
  'consumers.title': 'Consumers',
  'consumers.none': 'No system reads the stream yet.',
  'consumer.name': 'Consumer',
  'consumer.pending': 'Behind',
  'consumer.ack_pending': 'Unacknowledged',
  'consumer.redelivered': 'Redelivered',
  'consumer.last_active': 'Last delivered',
  'consumer.never': 'Never',
  'published.title': 'Published',
  'published.none': 'Nothing published in the last 30 days.',
  'published.day': 'Day',
  'published.type': 'Type',
  'published.count': 'Events',
  'recent.title': 'Recent events',
  'recent.none': 'No events under this view.',
  'recent.show': 'Show recent events',
}

export const builtinMessages: Messages = { en }

/** The table for a locale: its own, else its language's (`pt-BR` → `pt`). */
function tableFor(messages: Messages, locale: string): MessageTable | undefined {
  return messages[locale] ?? messages[locale.split('-')[0]!]
}

/** The built-in messages with the host's laid over them, key by key. */
export function mergeMessages(over: Messages | undefined): Messages {
  if (!over) return builtinMessages
  const merged: Messages = { ...builtinMessages }
  for (const locale of Object.keys(over)) merged[locale] = { ...builtinMessages[locale], ...over[locale] }
  return merged
}

/** How the dashboard says things in `locale` (normalized first), from
 * `messages` (the built-in ones by default); English stands in for a key
 * the locale lacks. */
export function translator(locale: string, messages: Messages = builtinMessages): Translate {
  const tag = normalizeLocale(locale)
  const table = tableFor(messages, tag)
  return (key, params) => {
    const message = table?.[key] ?? messages.en?.[key] ?? key
    const n = params?.n
    const form =
      typeof message === 'string'
        ? message
        : ((typeof n === 'number' ? message[pluralRules(tag).select(n)] : undefined) ?? message.other)
    if (!params) return form
    return form.replace(/\{(\w+)\}/g, (whole, name: string) => {
      const value = params[name]
      return value === undefined ? whole : typeof value === 'number' ? count(value, tag) : value
    })
  }
}
