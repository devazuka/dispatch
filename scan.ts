import { parseArgs } from "jsr:@std/cli@^1.0.6/parse-args"

import { getNextRequest } from './service.ts'
const status = Deno.permissions.requestSync({ name: 'env' })
const env = {}
const { _, ...args } = parseArgs(Deno.args)

const unknownKeys = Object.keys(args).filter(k => !['id', 'interval', 'url'].includes(k))
const USAGE = `Possible arguments:
  --id: The client id, used by the dispatcher to know not to ask to many request to a single client
    env CLIENT_ID = request IP

  --interval: Polling interval for looking for new request to executes
    env SCAN_INTERVAL = 1000

  --url: Dispatcher service url to poll from
    env DISPATCHER_URL = 'https://dispatch.devazuka.com'
`
if (unknownKeys.length) {
  args.help || console.log('invalid argument:', unknownKeys)
  console.log(USAGE)
  Deno.exit(0)
}

if (status.state === "granted") {
  env.CLIENT_ID = Deno.env.get('CLIENT_ID')
  env.SCAN_INTERVAL = Deno.env.get('SCAN_INTERVAL')
  env.DISPATCHER_URL = Deno.env.get('DISPATCHER_URL')
}
Deno.permissions.revokeSync({ name: 'env' })

const CLIENT_ID = args.id || env.CLIENT_ID
const DISPATCHER_URL = args.url || env.DISPATCHER_URL
const SCAN_INTERVAL = args.interval || Number(env.SCAN_INTERVAL) || 1000
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
