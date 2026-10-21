// magic_events_dashboard/core: the relay's answers, typed, and what a
// dashboard works out from them and says — no framework.

export * from './contract/types.js'
export { readsContract, healthOf, consumersOf, publishedByDay, publishedByType, recentOf } from './derive.js'
export type { Mood, Reason, Health, HealthOptions, ConsumerRow, RecentRow } from './derive.js'
export { mismatchOf, bannerOf, describeReason, describeLastPublished, describeSince } from './describe.js'
export type { Speech, Banner } from './describe.js'
export { parseTimestamp, serverNow, timeOf } from './time.js'
export { since, until, count, normalizeLocale } from './format.js'
export { builtinMessages, translator } from './i18n.js'
export type { Messages, MessageTable, Message, Plural, Locale, Translate } from './i18n.js'
