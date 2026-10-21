// When a composable's timers run: while its component is mounted, or while
// its effect scope lives in a browser. On a server a page renders once, and
// no timer outlives the render; outside both, nothing starts.

import { getCurrentInstance, getCurrentScope, onBeforeUnmount, onMounted, onScopeDispose } from 'vue'

export function whileAlive(start: () => void, stop: () => void) {
  if (getCurrentInstance()) {
    onMounted(start)
    onBeforeUnmount(stop)
  } else if (getCurrentScope() && typeof window !== 'undefined') {
    start()
    onScopeDispose(stop)
  }
}
