type DispatchedRequest = {
  key: string
  href: string
  headers?: Record<string, string>
  total: number
  delay: number
  status?: number
  message?: string
}

const CLIENT_ID = Deno.env.get('CLIENT_ID')
const DISPATCHER_URL = Deno.env.get('DISPATCHER_URL')
const SCAN_INTERVAL = Number(Deno.env.get('SCAN_INTERVAL')) || 0
const dispatcherUrl = new URL(
  DISPATCHER_URL || 'https://dispatch.devazuka.com',
)
const dispatcherInit = CLIENT_ID
  ? ({ headers: { 'x-client-id': CLIENT_ID } } as const)
  : undefined

const platforms = [
  'Macintosh; Intel Mac OS X 14_6_1',
  'Windows NT 10.0; Win64; x64',
  'X11; Linux x86_64',
]

const firefox = '129.0'
const webKit = '537.36'
const safari = '17.5'
const chrome = '128.0.0.0'
const vivaldi = '6.8.3381.55'
const appleWebKit = '605.1.15'
const gecko = '20100101'
const edge = chrome

const browsers = [
  `) AppleWebKit/${webKit} (KHTML, like Gecko) Chrome/${chrome} Safari/${webKit}`,
  `) AppleWebKit/${webKit} (KHTML, like Gecko) Chrome/${chrome} Safari/${webKit} Edg/${edge}`,
  `; rv:${firefox}) Gecko/${gecko} Firefox/${firefox}`,
  `) AppleWebKit/${appleWebKit} (KHTML, like Gecko) Version/${safari} Safari/${appleWebKit}`,
  `) AppleWebKit/${webKit} (KHTML, like Gecko) Chrome/${chrome} Safari/${webKit} Vivaldi/${vivaldi}`,
]

const pick = (arr: unknown[]) => arr[Math.random() % arr.length]

async function* getNextRequest() {
  while (true) {
    const dispatcherResponse = await fetch(dispatcherUrl, dispatcherInit)
    if (dispatcherResponse.status === 204) return
    const { key, href, headers, message } =
      (await dispatcherResponse.json()) as DispatchedRequest

    if (!dispatcherResponse.ok) throw Error(message)

    const signal = AbortSignal.timeout(10000)
    const execRequest = async (attempts = 0) => {
      if (attempts > 0) {
      	// Avoid loop spam, exponentially wait
        await new Promise(resolve => {
          const timeout = setTimeout(resolve, attempts * 750)
      		// Clear the timeout here, we don't want to hold the process
          signal.addEventListener('abort', () => clearTimeout(timeout))
        })
      }
      if (signal.aborted) return { key, error: `aborted: ${signal.reason}` }
      try {
        const ua = `Mozilla/5.0 (${pick(platforms)}${pick(browsers)}`
        const response = await fetch(href, {
          headers: { 'user-agent': ua, ...headers },
          redirect: 'follow',
          signal,
        })
        const { status } = response
        if (status === 429 || status === 403) return execRequest(attempts + 1)
        const body = await response.arrayBuffer()
      	await new Promise(s => setTimeout(s, 5000))
        return fetch(`${dispatcherUrl}${key}`, {
          method: 'POST',
          body,
          headers: { 'x-status': String(status) },
        }).then(() => ({ key, status }))
      } catch (err) {
      	console.log(err.stack)
        if (signal.aborted) return { key, error: `aborted: ${signal.reason}` }
        if (!(err instanceof Error)) return { key, error: String(err) }
        if (err.message === 'body failed') return execRequest(attempts + 1)
        return { key, error: err.message }
      }
    }
    const execution = execRequest(0)
    yield { execution, key, href, headers }
  }
}


if (SCAN_INTERVAL) {
  const fulfilled = (value: unknown) => console.log('fulfilled', value)
  const rejected = (value: unknown) => console.log('rejected', value)
  const waitInterval = (s: (value: unknown) => void) => setTimeout(s, SCAN_INTERVAL)
  while (true) {
    try {
      for await (const { href, execution } of getNextRequest()) {
        console.log(href, 'started')
        execution?.then?.(fulfilled, rejected)
      }
    } catch (err) {
      console.log(err)
    }
    await new Promise(waitInterval)
  }
} else {
  // In that simple case we only execute once
  const pending = await Array.fromAsync(getNextRequest())
  const results = await Promise.allSettled(pending.map(req => req.execution))
  console.log(results)
}
