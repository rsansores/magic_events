<script setup lang="ts">
// The demo: the ready-made dashboard over made-up answers — the fixtures the
// relay's tests write — with a choice of mood; or, with `?live` in the URL,
// over a running shop's answers (`examples/shop`), through vite's proxy.

import { ref } from 'vue'
import EventsDashboard from '../src/vue/EventsDashboard.vue'
import type { Recent, Status } from '../src/core/contract/types.js'
import status from '../src/core/contract/fixtures/status.js'
import recent from '../src/core/contract/fixtures/recent.js'

const live = new URLSearchParams(window.location.search).has('live')

async function get<T>(path: string): Promise<T> {
  const answer = await fetch(path)
  if (!answer.ok) throw new Error(`${path}: ${answer.status} ${answer.statusText}`)
  return (await answer.json()) as T
}

const moods = {
  down: status,
  stuck: { ...status, relay: { ...status.relay, running: true } },
  ok: {
    ...status,
    relay: { ...status.relay, running: true },
    outbox: { pending: 0, oldest_pending_at: null, parked: 0, first_parked: [] },
  },
} satisfies Record<string, Status>

const mood = ref<keyof typeof moods>('down')
const fetchStatus = live ? () => get<Status>('/events/status') : async () => moods[mood.value]
const fetchRecent = live ? () => get<Recent>('/events/recent') : async () => recent
</script>

<template>
  <main style="max-width: 960px; margin: 24px auto; padding: 0 16px; font-family: system-ui, sans-serif">
    <p v-if="live">Live: the shop's <code>/events/status</code>, every 5 seconds.</p>
    <p v-else>
      <label v-for="(_, name) in moods" :key="name" style="margin-right: 12px">
        <input v-model="mood" type="radio" :value="name" /> {{ name }}
      </label>
      · <a href="?live">live</a>
    </p>
    <EventsDashboard
      :key="live ? 'live' : mood"
      :fetch-status="fetchStatus"
      :fetch-recent="fetchRecent"
      :every="live ? 5000 : 0"
    />
  </main>
</template>
