<script setup lang="ts">
// The ready-made dashboard: a banner that says in one line whether events
// are leaving, then the outbox and the relay, what was set aside, the
// stream, the consumers and what was published; and, when the host gives a
// way to fetch them, the most recent events.
//
// It knows no URL: the host passes the functions that fetch the relay's
// answers from its own handlers. Its look is in
// `magic_events_dashboard/styles.css`.

import { computed, ref, shallowRef, useId } from 'vue'
import type { Recent, Status } from '../core/contract/types.js'
import { describeLastPublished, describeSince, mismatchOf } from '../core/describe.js'
import { recentOf, type RecentRow } from '../core/derive.js'
import { count } from '../core/format.js'
import type { Messages } from '../core/i18n.js'
import { useEventsDashboard } from './useEventsDashboard.js'

const props = defineProps<{
  /** Fetches `Events::status` from the host's handler. */
  fetchStatus: () => Promise<Status>
  /** Fetches `Events::recent`, if the host serves it. */
  fetchRecent?: () => Promise<Recent>
  every?: number
  locale?: string
  messages?: Messages
  lagAfter?: number
  stuckAfter?: number
  consumerLag?: number
}>()

const { status, banner, consumers, published, oldestPending, lastPublished, error, stale, speech, t } =
  useEventsDashboard(() => props.fetchStatus(), {
    every: () => props.every,
    locale: () => props.locale,
    messages: () => props.messages,
    lagAfter: () => props.lagAfter,
    stuckAfter: () => props.stuckAfter,
    consumerLag: () => props.consumerLag,
  })

const n = (value: number) => count(value, speech.value?.locale ?? 'en')
const titleId = useId()

const recent = shallowRef<RecentRow[] | null>(null)
const recentError = ref<string | null>(null)
async function showRecent() {
  if (!props.fetchRecent) return
  try {
    const answer = await props.fetchRecent()
    const mismatch = mismatchOf(answer, t)
    recent.value = mismatch ? null : recentOf(answer)
    recentError.value = mismatch
  } catch (e) {
    recentError.value = t('fetch.failed', { error: e instanceof Error ? e.message : String(e) })
  }
}

const parkedMore = computed(() =>
  status.value ? status.value.outbox.parked - status.value.outbox.first_parked.length : 0,
)
/** The subjects a card lists, and how many more there are. */
const SUBJECTS_SHOWN = 5
const subjectsHidden = computed(() =>
  status.value?.stream
    ? status.value.stream.subjects_total - Math.min(SUBJECTS_SHOWN, status.value.stream.subjects.length)
    : 0,
)
</script>

<template>
  <section class="med" :aria-labelledby="titleId">
    <p v-if="error" class="med-banner" data-mood="down" role="alert">
      <span class="med-title" :id="titleId">{{ error }}</span>
    </p>
    <p v-else-if="!status || !banner || !speech" class="med-line" :id="titleId">{{ t('loading') }}</p>
    <template v-else>
      <div class="med-banner" :data-mood="banner.mood" role="status">
        <span class="med-dot" aria-hidden="true" />
        <div>
          <h2 class="med-title" :id="titleId">{{ banner.title }}</h2>
          <p v-for="line in banner.lines" :key="line" class="med-line">{{ line }}</p>
          <p v-if="stale" class="med-line">{{ stale }}</p>
        </div>
      </div>

      <div class="med-cards">
        <section class="med-card">
          <h3 class="med-heading">{{ t('outbox.title') }}</h3>
          <dl class="med-figures">
            <div>
              <dt>{{ t('outbox.pending') }}</dt>
              <dd>{{ n(status.outbox.pending) }}</dd>
            </div>
            <div>
              <dt>{{ t('outbox.parked') }}</dt>
              <dd>{{ n(status.outbox.parked) }}</dd>
            </div>
            <div>
              <dt>{{ t('outbox.oldest') }}</dt>
              <dd>{{ describeSince(oldestPending, speech, t('outbox.none')) }}</dd>
            </div>
          </dl>
        </section>
        <section class="med-card">
          <h3 class="med-heading">{{ t('relay.title') }}</h3>
          <p class="med-line" :data-running="status.relay.running">
            {{ status.relay.running ? t('relay.running') : t('relay.stopped') }}
          </p>
          <p class="med-line">{{ describeLastPublished(lastPublished, speech) }}</p>
        </section>
        <section v-if="status.stream" class="med-card">
          <h3 class="med-heading">{{ t('stream.title') }} · {{ status.stream.name }}</h3>
          <p class="med-line">{{ t('stream.messages', { n: status.stream.messages }) }}</p>
          <ul class="med-list">
            <li v-for="s in status.stream.subjects.slice(0, SUBJECTS_SHOWN)" :key="s.subject">
              <code>{{ s.subject }}</code> <span class="med-count">{{ n(s.messages) }}</span>
            </li>
          </ul>
          <p v-if="subjectsHidden > 0" class="med-line">
            {{ t('stream.subjects.more', { n: subjectsHidden }) }}
          </p>
        </section>
      </div>

      <section v-if="status.outbox.first_parked.length > 0" class="med-card">
        <h3 class="med-heading">{{ t('parked.title') }}</h3>
        <ul class="med-list">
          <li v-for="p in status.outbox.first_parked" :key="p.id">
            <code>{{ p.type }}</code> · <code>{{ p.subject }}</code>
            <p class="med-line">{{ t('parked.attempts', { n: p.attempts, error: p.last_error ?? '' }) }}</p>
          </li>
        </ul>
        <p v-if="parkedMore > 0" class="med-line">{{ t('parked.more', { n: parkedMore }) }}</p>
      </section>

      <section v-if="consumers" class="med-card">
        <h3 class="med-heading">{{ t('consumers.title') }}</h3>
        <p v-if="consumers.length === 0" class="med-line">{{ t('consumers.none') }}</p>
        <table v-else class="med-table">
          <thead>
            <tr>
              <th scope="col">{{ t('consumer.name') }}</th>
              <th scope="col">{{ t('consumer.pending') }}</th>
              <th scope="col">{{ t('consumer.ack_pending') }}</th>
              <th scope="col">{{ t('consumer.redelivered') }}</th>
              <th scope="col">{{ t('consumer.last_active') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="c in consumers" :key="c.name" :data-behind="c.behind">
              <th scope="row">
                {{ c.name }}
                <span v-for="f in c.filters" :key="f" class="med-filter"
                  ><code>{{ f }}</code></span
                >
              </th>
              <td>{{ n(c.pending) }}</td>
              <td>{{ n(c.ackPending) }}</td>
              <td>{{ n(c.redelivered) }}</td>
              <td>{{ describeSince(c.lastActive, speech, t('consumer.never')) }}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section v-if="published" class="med-card">
        <h3 class="med-heading">{{ t('published.title') }}</h3>
        <p v-if="published.byType.length === 0" class="med-line">{{ t('published.none') }}</p>
        <table v-else class="med-table">
          <thead>
            <tr>
              <th scope="col">{{ t('published.type') }}</th>
              <th scope="col">{{ t('published.count') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="p in published.byType" :key="p.type">
              <th scope="row">
                <code>{{ p.type }}</code>
              </th>
              <td>{{ n(p.count) }}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section v-if="fetchRecent" class="med-card">
        <h3 class="med-heading">{{ t('recent.title') }}</h3>
        <button type="button" class="med-button" @click="showRecent">{{ t('recent.show') }}</button>
        <p v-if="recentError" class="med-line" role="alert">{{ recentError }}</p>
        <p v-else-if="recent && recent.length === 0" class="med-line">{{ t('recent.none') }}</p>
        <ul v-else-if="recent" class="med-list">
          <li v-for="m in recent" :key="m.sequence">
            <code>{{ m.type }}</code> · <code>{{ m.subject }}</code> ·
            {{ describeSince(m.time, speech, '') }}
            <pre class="med-body">{{ m.body }}</pre>
          </li>
        </ul>
      </section>
    </template>
  </section>
</template>
