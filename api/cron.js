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

const pick = (arr) => arr[Math.random() % arr.length]

export async function* getNextRequest() {
  while (true) {
    const dispatcherResponse = await fetch('https://dispatch.devazuka.com')
    if (dispatcherResponse.status === 204) return
    const { key, href, headers, message } =
      (await dispatcherResponse.json())

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
        return fetch(`https://dispatch.devazuka.com/${key}`, {
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

export default async function handler(req, res) {
  if (req.headers.get('Authorization') !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).end('Unauthorized')
  }
  try {
    let count = 0
    for await (const { href, execution } of getNextRequest(dispatcherUrl, dispatcherInit)) {
      console.log(href, 'started')
      execution?.then?.(fulfilled, rejected)
      count++
    }
    res.status(200).end(`${count} requests done`)
  } catch (err) {
    console.log(err)
  }
  res.status(500).end('done')
}
