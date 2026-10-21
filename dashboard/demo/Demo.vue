<script setup lang="ts">
// The demo: the ready-made dashboard over made-up answers — the fixtures the
// relay's tests write — with the clock moved on so each mood shows.

import { ref } from 'vue'
import EventsDashboard from '../src/vue/EventsDashboard.vue'
import type { Status } from '../src/core/contract/types.js'
import status from '../src/core/contract/fixtures/status.js'
import recent from '../src/core/contract/fixtures/recent.js'

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
const fetchStatus = async () => moods[mood.value]
const fetchRecent = async () => recent
</script>

<template>
  <main style="max-width: 960px; margin: 24px auto; font-family: system-ui, sans-serif">
    <p>
      <label v-for="(_, name) in moods" :key="name" style="margin-right: 12px">
        <input v-model="mood" type="radio" :value="name" /> {{ name }}
      </label>
    </p>
    <EventsDashboard :key="mood" :fetch-status="fetchStatus" :fetch-recent="fetchRecent" :every="0" />
  </main>
</template>
