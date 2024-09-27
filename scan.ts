import { getNextRequest } from './service.ts'

const CLIENT_ID = Deno.env.get('CLIENT_ID')
const DISPATCHER_URL = Deno.env.get('DISPATCHER_URL')
const SCAN_INTERVAL = Number(Deno.env.get('SCAN_INTERVAL')) || 1000
const dispatcherUrl = new URL(
  DISPATCHER_URL || 'https://dispatch.devazuka.com',
)

const dispatcherInit = CLIENT_ID
  ? ({ headers: { 'x-client-id': CLIENT_ID } } as const)
  : undefined

const fulfilled = (value: unknown) => console.log('fulfilled', value)
const rejected = (value: unknown) => console.log('rejected', value)
const waitInterval = (s: (value: unknown) => void) => setTimeout(s, SCAN_INTERVAL)
while (true) {
  try {
    for await (const { href, execution } of getNextRequest(dispatcherUrl, dispatcherInit)) {
      console.log(href, 'started')
      execution?.then?.(fulfilled, rejected)
    }
  } catch (err) {
    console.log(err)
  }
  await new Promise(waitInterval)
}
