// The database's time, moving on in the page: relative times ("3 min. ago")
// follow it without the browser's clock ever being compared with a time
// the database wrote.

import { computed, ref, watch } from 'vue'
import { serverNow } from '../core/time.js'
import { whileAlive } from './lifecycle.js'

/** The database's time now, from the `at` of the answer `answered` returns.
 * It ticks only in a browser: on the server a page renders once, and no
 * timer outlives the render. It needs a component or an effect scope to tie
 * its timer to: called outside both, it does not tick. */
export function useServerClock(answered: () => { at: string } | null | undefined) {
  const current = ref(Date.now())
  const receivedAt = ref(current.value)
  watch(answered, () => {
    receivedAt.value = current.value = Date.now()
  })

  let timer: ReturnType<typeof setInterval> | undefined
  const start = () => {
    timer ??= setInterval(() => (current.value = Date.now()), 15_000)
  }
  const stop = () => {
    clearInterval(timer)
    timer = undefined
  }
  whileAlive(start, stop)

  return computed(() => {
    const answer = answered()
    return answer ? serverNow(answer.at, receivedAt.value, current.value) : null
  })
}
