import { crypto } from 'jsr:@std/crypto'
import { encodeBase58 } from 'jsr:@std/encoding/base58'
import { getNextRequest } from './service.ts'

const getKey = async (text: string) =>
  encodeBase58(
    await crypto.subtle.digest('SHA-384', new TextEncoder().encode(text)),
  )

const NOT_FOUND = new Response('{"message":"Not Found: Error 404"}', {
  status: 404,
  headers: { 'content-type': 'application/json' },
})

type RequestQueue = {
  name: string
  delay: number
  push: (req: PendingRequest) => PendingRequest
}
type BodyInit = Uint8Array
type ResolvePromise = (value: BodyInit) => void
type TimersInfo = Record<RequestQueue['name'], number>
type PendingRequestHandler = ResolvePromise | URL
type PendingRequest = {
  key: string
  href: string
  headers?: Record<string, string>
  createdAt: number
  startedAt?: number
  queue: RequestQueue['name']
  handlers: Set<PendingRequestHandler>
  attempts: number
}

const QUEUES: Record<string, RequestQueue> = {}
const TIMEOUT = 10 * 1000 // 10s default timeout on requests
const REQUESTS = new Map<string, PendingRequest>()

const encodeTimers = (timers: TimersInfo): string =>
  Object.entries(timers).join()

const decodeTimers = (encodedTimers?: string): TimersInfo => {
  const timers: TimersInfo = {}
  if (!encodedTimers) return timers
  let i = -1
  let queueName = ''
  let currentStr = ''
  const now = Date.now()
  while (++i <= encodedTimers.length) {
    const c = encodedTimers[i]
    if (!c || c === ',') {
      if (!queueName) {
        queueName = currentStr
        currentStr = ''
        continue
      }

      const unlockAt = Number(currentStr)
      currentStr = ''
      unlockAt && unlockAt > now && (timers[queueName] = unlockAt)
      queueName = ''
      continue
    }

    currentStr += c
  }

  return timers
}

const cleanupLocalStorageTimers = () => {
  for (const clientId of Object.keys(localStorage)) {
    const before = localStorage[clientId]
    const after = encodeTimers(decodeTimers(before))
    if (!after) {
      localStorage.removeItem(clientId)
      continue
    }
    if (before === after) continue
    localStorage[clientId] = after
  }
}

// Routinely cleanup localstorage just to avoid junk
setInterval(cleanupLocalStorageTimers, 60 * 60 * 1000)
cleanupLocalStorageTimers()

const getNextInQueue = (clientId: string) => {
  const timers = decodeTimers(localStorage[clientId])
  let oldest: undefined | PendingRequest
  let count = 0
  // TODO: add priority system a bit finer grained than FIFO
  const now = Date.now()
  for (const req of REQUESTS.values()) {
    // only true if we have a startedAt and it hasn't timed out yet
    // then it means the request is being handled and we are still waiting
    if (now - (req.startedAt as number) < TIMEOUT) continue
    if (timers[req.queue]) continue
    count++

    if ((oldest?.createdAt as number) < req.createdAt) continue
    oldest = req
  }
  if (!oldest) return { count: 0 }
  const queue = QUEUES[oldest.queue]
  timers[queue.name] = now + queue.delay
  localStorage[clientId] = encodeTimers(timers)
  return { next: oldest, count }
}

const registerQueue = async (
  name: string,
  delay = 60000,
  aliases: string[] = [],
) => {
  await Deno.mkdir(`./${name}`, { recursive: true })
  const push = (req: PendingRequest) => {
    req.createdAt = performance.now()
    req.queue = name
    req.handlers = new Set<PendingRequestHandler>()
    req.attempts = 0
    REQUESTS.set(req.key, req)
    return req
  }
  const queue: RequestQueue = { name, delay, push }
  for (const key of [name, ...aliases]) QUEUES[key] = queue
  return queue
}

for (const [name, { delay, aliases }] of Object.entries({
  'goodreads.com': { delay: 30000, aliases: ['www.goodreads.com'] },
  'beeceptor.com': { delay: 1000 },
  'annas-archive.org': {
    delay: 60000,
    aliases: ['annas-archive.se', 'annas-archive.li'],
  },
  'theaudiobookbay.se': {
    delay: 40000,
    aliases: ['185.247.224.117', 'audiobookbay.is', 'audiobookbay.lu'],
  },
})) {
  await registerQueue(name, delay, aliases)
}

const fail = (
  status: number,
  { stack, message }: { stack?: string; message: string },
) =>
  new Response(JSON.stringify({ stack, message, status }), {
    status,
    headers: { 'content-type': 'application/json' },
  })

const buildResponse = ({ handlers, key }: PendingRequest, reply?: URL) => {
  const headers = { 'x-request-key': key }
  if (reply) {
    handlers.add(reply)
    return new Response(null, { status: 202, headers })
  }

  const { resolve, promise } = Promise.withResolvers<BodyInit>()
  handlers.add(resolve)
  const stream = new ReadableStream({
    async pull(controller) {
      try {
        controller.enqueue(await promise)
      } catch (err) {
        const body = JSON.stringify({ message: err.message, stack: err.stack })
        controller.enqueue(new TextEncoder().encode(body))
      }
      controller.close()
      handlers.delete(resolve)
      handlers.size === 0 && REQUESTS.delete(key)
    },
    cancel(reason) {
      if (reason !== 'resource closed') {
        console.warn('unexpected reason', { reason })
      }
      handlers.delete(resolve)
      handlers.size === 0 && REQUESTS.delete(key)
    },
  })
  return new Response(stream, { headers })
}

type PostBody = {
  pathname: string
  search: string
  href: string
  expire?: number
  headers?: Record<string, string>
  reply?: URL
}

const streamFileAndClose = (file: Deno.FsFile) => {
  const reader = file.readable.getReader()
  return new ReadableStream({
    async pull(controller) {
      const { done, value } = await reader.read()
      if (done) {
        file.close()
        controller.close()
        return
      }
      controller.enqueue(value)
    },
    cancel() {
      file.close()
    },
  })
}

const { NotFound } = Deno.errors
const POST = async (
  queue: RequestQueue,
  { pathname, search, href, expire, headers, reply }: PostBody,
) => {
  const key = `${queue.name}/${await getKey(`${pathname}${search}`)}`
  const req = REQUESTS.get(key)
  if (req) return buildResponse(req, reply)
  try {
    const file = await Deno.open(key, { read: true })
    const { isFile, mtime, birthtime } = await file.stat()
    if (!isFile) throw Error(`${key} is not a file`)
    const headers = { 'x-from-cache': key }
    if (!expire) return new Response(streamFileAndClose(file), { headers })
    const cachedDate = mtime || birthtime
    if (!cachedDate) throw Error('unable to access updated time')
    if ((cachedDate.getTime() + expire) > Date.now()) {
      return new Response(streamFileAndClose(file), { headers })
    }
  } catch (err) {
    if (!(err instanceof NotFound)) return fail(500, err)
  }

  console.log('enqueue:', { key })
  return buildResponse(
    queue.push({ key, href, headers } as PendingRequest),
    reply,
  )
}

type Client = { id: string, activeAt: number, started: number, finished: number }
const CLIENTS: Record<string, Client> = {}
const getClient = ({ headers }: Request): Client | undefined => {
  const clientId =
    headers.get('x-client-id') ||
    headers.get('true-client-ip') ||
    headers.get('cf-connecting-ip') ||
    headers.get('x-forwared-for')
  if (!clientId) return
  const now = Date.now()
  let client = CLIENTS[clientId]
  if (client) {
    client.activeAt = now
  } else {
    client = { id: clientId, activeAt: now, started: 0, finished: 0 }
    CLIENTS[clientId] = client
  }
  return client
}

const handleRequestResponse = async (request: Request, key: string) => {
  // TODO: pass in the response headers & the status code
  // TODO: support redirect as file aliases
  const req = REQUESTS.get(key)
  if (!req) return NOT_FOUND
  const body = new Uint8Array(await request.arrayBuffer())
  const status = Number(request.headers.get('x-status'))

  const client = getClient(request)
  client && (client.finished++)
  console.log('receiving:', { key, status, client: client?.id })
  const writePending = status === 200 && Deno.writeFile(key, body)
  const pendingReplies = []
  for (const handler of req.handlers) {
    if (typeof handler === 'function') {
      handler(body)
      continue
    }
    const sendReply = async (attempts = 0) => {
      const res = await fetch(handler, {
        body,
        method: 'POST',
        headers: {
          'x-request-key': key,
          'x-request-href': req.href,
          'x-request-status': String(status),
        },
      })
      if (res.status !== 500) return // only retry on server error, otherwise, move on
      // wait a bit and retry until client is available
      attempts && (await new Promise(s => setTimeout(s, attempts * 750)))
      return sendReply(attempts + 1)
    }
    pendingReplies.push(sendReply(0))
  }
  REQUESTS.delete(key)
  await writePending
  return EMPTY
}

const handleRequestQueue = async (request: Request) => {
  try {
    const body = await request.json()
    const { search, href, pathname, hostname } = new URL(body.url)
    body.pathname = pathname
    body.search = search
    body.href = href
    const queue = QUEUES[hostname] || (await registerQueue(hostname))
    return POST(queue, body)
  } catch (err) {
    return fail(400, err)
  }
}

const dispatcheStartAt = Date.now()
type RequestStatus = { queue: string, href: string, createdAt: number, startedAt?: number }
const handleGetStatus = () => {
  const timers: Record<string, TimersInfo> = {}
  const requests: RequestStatus[] = []
  for (const [k, v] of Object.entries(localStorage)) {
    timers[k] = decodeTimers(v)
  }
  for (const { queue, href, createdAt, startedAt } of REQUESTS.values()) {
    requests.push({ queue, href, createdAt, startedAt })
  }
  const clients = Object.values(CLIENTS)
  const body = JSON.stringify({ clients, timers, requests, startAt: dispatcheStartAt })
  return new Response(body, { headers: { 'content-type': 'application/json' } })
}
const handleRequestNextInQueue = (request: Request) => {
  const client = getClient(request)
  if (!client) {
    const message = 'expect the x-client-id header to be set'
    return fail(400, { message })
  }

  // TODO: return delay from next request
  const { next, count } = getNextInQueue(client.id)
  if (!next) return EMPTY
  client.started++
  console.log('dispatching:', { key: next.key, client: client.id })

  // request was already started but timedout, so we retry it
  next.startedAt && next.attempts++
  next.startedAt = Date.now()
  return new Response(
    JSON.stringify({
      key: next.key,
      href: next.href,
      headers: next.headers,
      total: count,
    }),
    { headers: { 'content-type': 'application/json' } },
  )
}

// statuses: pending -> started -> (succeeded | failed)
const EMPTY = new Response(null, { status: 204 })
const httpHandler = (request: Request) => {
  const { method, url } = request
  const { pathname } = new URL(url)
  if (method === 'POST') {
    return pathname.length > 1
      ? handleRequestResponse(request, pathname.slice(1))
      : handleRequestQueue(request)
  }

  if (method === 'GET') {
    if (pathname === '/status' || pathname === '/status/') {
      return handleGetStatus()
    }
    return handleRequestNextInQueue(request)
  }

  return NOT_FOUND
}

const SCAN_INTERVAL = Number(Deno.env.get('SCAN_INTERVAL')) || 1000
const PORT = Deno.env.get('PORT') || 8000
const fulfilled = (value: unknown) => console.log('fulfilled', value)
const rejected = (value: unknown) => console.log('rejected', value)
const waitInterval = (s: (value: unknown) => void) => setTimeout(s, SCAN_INTERVAL)
const clientInit = { headers: { 'x-client-id': 'localhost' } } as const
const startDefaultFetcher = async () => {
  try {
    for await (const { href, execution } of getNextRequest(`http://localhost:${PORT}/`, clientInit)) {
      console.log(href, 'started')
      execution?.then?.(fulfilled, rejected)
    }
  } catch (err) {
    console.log(err)
  }
  await new Promise(waitInterval).finally(startDefaultFetcher)
}

export default { fetch: httpHandler }

startDefaultFetcher()

// USAGES:
const EXAMPLES = 0 // is 0 not to be executed
if (EXAMPLES) {
  /*
   ** Queue a request:
   **
   ** curl localhost:8786 -d '{"url":"https://goodreads.com/lol?w=6","expire":60000,"headers":{}}'
   */
  await fetch('https://localhost:8786', {
    body: JSON.stringify({
      url: 'https://goodreads.com/lol?w=6',
      expire: 60*1000, // 1 min
      headers: {},
      reply: 'https://valid.domain.tld/webhook', // optional, if missing will wait for the response
    }),
  })
  // If you give a `reply` url, it will only be called if the value is
  // not in the cache, in that case it will return a 202 ACCEPTED
  // so, check the status, if 202, expect a response
  // 2 headers are set: { 'x-request-href': href, 'x-request-key': key }

  /*
   ** Get the next available request to do
   **
   ** curl -H'x-client-id: aF7my75$' localhost:8786 | jq
   */
  const someUniqueId = 'aF7my75$'
  const res = await fetch(
    `localhost:8786`,
    { headers: { 'x-client-id': someUniqueId } }, // optional, will default to IP
  )
  const responseExample = (await res.json()) || {
    key: 'goodreads.com/3vLiLPeZetd3ANE9Pmq4dFqaAgpLyfDNd2aZRyn4dJjK7W4dpSqQm8V8ayQneDRMRH',
    href: 'https://goodreads.com/lol?w=6',
    total: 1, // how many requests available
  }

  /*
   ** Submit a response
   **
   ** curl localhost:8786/goodreads.com/3vLiLPeZetd3ANE9Pmq4dFqaAgpLyfDNd2aZRyn4dJjK7W4dpSqQm8V8ayQneDRMRH -d '{{BODY}}'
   */
  await fetch(`https://localhost:8786/${responseExample.key}`, {
    body: '{{BODY}}',
    method: 'POST',
    headers: { 'x-status': '200' },
  })

  // If the request is not found it will return 404
  // this can happen if the server restarted or it was handled by someone else (in case of timeouts)
}
