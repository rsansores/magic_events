import { describe, expect, it } from 'vitest'
import { builtinMessages, mergeMessages, translator } from './i18n.js'

describe('translator', () => {
  it('picks a plural’s form by the locale’s rules', () => {
    const en = translator('en')
    expect(en('reason.parked', { n: 1 })).toMatch(/^1 event was refused/)
    expect(en('reason.parked', { n: 0 })).toMatch(/^0 events were refused/)
    expect(en('reason.parked', { n: 2000 })).toMatch(/^2,000 events were refused/)
  })

  it('falls back to the other form for a category a message does not have', () => {
    // French says 0 in the singular.
    const t = translator('fr', { fr: { k: { one: 'un {n}', other: 'des {n}' } } })
    expect(t('k', { n: 0 })).toBe('un 0')
    expect(t('k', { n: 2 })).toBe('des 2')
  })

  it('takes a regional locale’s language, and English for what it lacks', () => {
    const messages = mergeMessages({ pt: { 'outbox.title': 'Caixa de saída' } })
    expect(translator('pt_BR', messages)('outbox.title')).toBe('Caixa de saída')
    expect(translator('pt_BR', messages)('relay.title')).toBe('Relay')
    expect(translator('de')('outbox.title')).toBe('Outbox')
  })

  it('keeps English for what a host’s own locale does not say', () => {
    expect(translator('fr', { fr: { 'mood.ok': 'Tout va bien' } })('outbox.title')).toBe('Outbox')
    expect(translator('fr', { fr: { 'mood.ok': 'Tout va bien' } })('mood.ok')).toBe('Tout va bien')
  })

  it('lets a host reword a built-in message and keeps the rest', () => {
    const messages = mergeMessages({ en: { 'mood.ok': 'All good' } })
    expect(translator('en', messages)('mood.ok')).toBe('All good')
    expect(translator('en', messages)('mood.down')).toBe(builtinMessages.en!['mood.down'])
  })

  it('leaves a placeholder it was not given as it is, and never writes nothing in its place', () => {
    expect(translator('en')('reason.broker_retry')).toBe('Next attempt: {until}')
  })
})
