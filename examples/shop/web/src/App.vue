<script setup lang="ts">
// The shop's events page: the ready-made dashboard over the shop's own
// answers. A shop with a design system of its own would draw them with its
// own components, through `useEventsDashboard`.

import { EventsDashboard } from 'magic_events_dashboard'
import { fetchRecent, fetchStatus } from './api'
</script>

<template>
  <main class="page">
    <header>
      <h1>The shop's events</h1>
      <p>
        An order a second, each with its event in the same transaction; billing and shipping read them from
        the stream. Stop NATS (<code>docker compose stop nats</code>) to watch the outbox wait, and start it
        again to watch it drain.
      </p>
    </header>
    <EventsDashboard :fetch-status="fetchStatus" :fetch-recent="fetchRecent" :every="2000" />
  </main>
</template>

<style>
body {
  margin: 0;
  background: #f6f7fb;
  font-family: system-ui, sans-serif;
}
.page {
  max-width: 960px;
  margin: 0 auto;
  padding: 24px 16px 48px;
}
h1 {
  font-size: 1.4em;
  margin: 0 0 4px;
}
header p {
  margin: 0 0 16px;
  color: #5c6475;
}
</style>
