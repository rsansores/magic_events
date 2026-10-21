// magic_events_dashboard: the dashboard of the magic_events relay, for Vue 3.

// The ready-made dashboard; its look is in `magic_events_dashboard/styles.css`.
export { default as EventsDashboard } from './vue/EventsDashboard.vue'

// The same without its looks: a host with its own design system calls it in
// its own component and draws the answers its own way, with the sentence
// functions of the core.
export { useEventsDashboard } from './vue/useEventsDashboard.js'
export type {
  EventsDashboard as EventsDashboardState,
  EventsDashboardOptions,
} from './vue/useEventsDashboard.js'

// Everything framework-free, also as `magic_events_dashboard/core`.
export * from './core/index.js'
