// What the page asks the shop: the relay's answers, as `Events::status` and
// `Events::recent` give them, so their types are the dashboard's.

import type { Recent, Status } from 'magic_events_dashboard'

async function get<T>(path: string): Promise<T> {
  const answer = await fetch(path)
  if (!answer.ok) throw new Error(`${answer.status} ${answer.statusText}: ${await answer.text()}`)
  return (await answer.json()) as T
}

export const fetchStatus = () => get<Status>('/events/status')
export const fetchRecent = () => get<Recent>('/events/recent')
