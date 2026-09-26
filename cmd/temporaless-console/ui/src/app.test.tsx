import { act, type ReactNode } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { SourceError } from 'medallion-terminal-core/app'
import type { Template } from 'medallion-terminal-core/dashboard'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { App, Bootstrap, refuseSession, type Session, type UIConfig } from './app'

// React asks test environments to declare that updates are wrapped in act.
;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true

const TOKEN_CHECK = '/temporaless.v1.RunInspectionService/GetInspectionCapabilities'

// One polled panel whose answer echoes the store it was asked for and the
// bearer token the request carried, so the page shows which session and
// selection every dashboard call used.
const template: Template = {
  title: 'Executions',
  context: { values: { store: '' } },
  widgets: [{
    id: 'probe',
    component: 'json',
    title: 'Probe',
    source: { source_id: 'probe', params: { store: '${ctx.store}' }, refreshIntervalMs: 100 },
  }],
}
const bearer: UIConfig = { title: 'Executions', auth: 'bearer', template }
const loopback: UIConfig = { title: 'Executions', auth: 'loopback', template }

interface Call {
  path: string
  token?: string
  body?: { source_id?: string, params?: Record<string, string> }
}

// The dashboard's data calls are the terminal contract's TerminalService/Get.
const isPanelCall = (call: Call) => call.path.endsWith('.TerminalService/Get')

function answer(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}

const refused = () => answer(401, { code: 'unauthenticated', message: 'token refused' })
const echo = (call: Call) => answer(200, { json: { store: call.body?.params?.store ?? '', token: call.token ?? 'none' } })

// fakeConsole stands in for the console server behind the app's fetch: it
// records every call and answers with `respond`.
function fakeConsole(respond: (call: Call) => Response | Promise<Response>) {
  const calls: Call[] = []
  const fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url, 'http://console.test')
    const authorization = new Headers(init?.headers).get('Authorization')
    const call: Call = {
      path: url.pathname,
      token: authorization?.replace(/^Bearer /, ''),
      body: typeof init?.body === 'string' ? JSON.parse(init.body) : undefined,
    }
    calls.push(call)
    return respond(call)
  }
  return { fetch, calls }
}

let container: HTMLDivElement
let root: Root

beforeEach(() => {
  window.history.replaceState(null, '', '/')
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
})

afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
})

async function render(element: ReactNode) {
  await act(async () => root.render(element))
}

// until polls the page, letting React and pending responses settle between
// reads, and returns the first truthy read.
async function until<T>(read: () => T | null | undefined | false, what: string, timeoutMs = 3000): Promise<T> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const value = read()
    if (value) return value
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}; page text: ${container.textContent}`)
    await act(async () => {
      await new Promise(resolve => setTimeout(resolve, 20))
    })
  }
}

async function settle(ms: number) {
  await act(async () => {
    await new Promise(resolve => setTimeout(resolve, ms))
  })
}

const state = (name: string) => container.querySelector(`[data-state="${name}"]`)
const panelText = () => container.querySelector('pre')?.textContent ?? ''
const button = (label: string) => [...container.querySelectorAll('button')].find(element => element.textContent?.trim() === label)

async function signIn(token: string) {
  const input = await until(() => container.querySelector<HTMLInputElement>('input[type=password]'), 'the token field')
  await act(async () => {
    // React tracks the input's value; set it through the native setter so the
    // input event reaches onChange as it does when someone types.
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, token)
    input.dispatchEvent(new Event('input', { bubbles: true }))
  })
  await act(async () => {
    input.closest('form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }))
  })
}

async function signedInAs(token: string, store = '') {
  await until(() => panelText().includes(`"token": "${token}"`) && panelText().includes(`"store": "${store}"`), `the dashboard as ${token}`)
}

describe('refuseSession', () => {
  const error = new SourceError('token refused', { kind: 'unauthenticated', status: 401 })
  const earlier = new SourceError('expired', { kind: 'unauthenticated', status: 401 })
  const cases: { name: string, current: Session, refusedToken?: string, refuses: boolean }[] = [
    { name: 'the token this tab holds', current: { token: 'a' }, refusedToken: 'a', refuses: true },
    { name: 'a token this tab replaced', current: { token: 'b' }, refusedToken: 'a', refuses: false },
    { name: 'a token after signing out', current: {}, refusedToken: 'a', refuses: false },
    { name: 'a token while the gate shows an earlier refusal', current: { refused: earlier }, refusedToken: 'a', refuses: false },
    { name: 'a request that carried no token', current: { token: 'a' }, refusedToken: undefined, refuses: false },
  ]
  for (const test of cases) {
    it(`a 401 for ${test.name} ${test.refuses ? 'ends the session' : 'changes nothing'}`, () => {
      const next = refuseSession(test.current, test.refusedToken, error)
      if (test.refuses) expect(next).toEqual({ refused: error })
      else expect(next).toBe(test.current)
    })
  }
})

describe('bearer sign-in', () => {
  it('stays signed out until one capability check accepts the token', async () => {
    const server = fakeConsole(call => (call.path === TOKEN_CHECK ? answer(200, {}) : echo(call)))
    await render(<App config={bearer} fetch={server.fetch} />)

    expect(state('signed-out')).not.toBeNull()
    expect(container.textContent).toContain('Bearer token required')
    expect(server.calls).toHaveLength(0)

    await signIn('good')
    await signedInAs('good')
    expect(server.calls[0]).toMatchObject({ path: TOKEN_CHECK, token: 'good' })
    expect(server.calls.filter(call => call.path === TOKEN_CHECK)).toHaveLength(1)
    const panelCalls = server.calls.filter(isPanelCall)
    expect(panelCalls.length).toBeGreaterThan(0)
    expect(panelCalls.every(call => call.token === 'good')).toBe(true)
    expect(container.textContent).toContain('Signed in with a bearer token')
    expect(button('Sign out')).toBeDefined()
  })

  it('shows the session-expired state after one refused check, then accepts a new token', async () => {
    const server = fakeConsole(call => {
      if (call.path === TOKEN_CHECK) return call.token === 'good' ? answer(200, {}) : refused()
      return echo(call)
    })
    await render(<App config={bearer} fetch={server.fetch} />)

    await signIn('mistyped')
    await until(() => state('session-expired'), 'the session-expired state')
    expect(container.textContent).toContain('The console did not accept your token')
    expect(server.calls).toEqual([{ path: TOKEN_CHECK, token: 'mistyped', body: {} }])

    await signIn('good')
    await signedInAs('good')
    expect(state('session-expired')).toBeNull()
  })

  it('returns to the gate when the console refuses the token mid-session and reopens the same selection', async () => {
    window.history.replaceState(null, '', '/?ctx.store=engine')
    const revoked = new Set<string>()
    const server = fakeConsole(call => {
      if (revoked.has(call.token ?? '')) return refused()
      return call.path === TOKEN_CHECK ? answer(200, {}) : echo(call)
    })
    await render(<App config={bearer} fetch={server.fetch} />)

    await signIn('good')
    await signedInAs('good', 'engine')

    revoked.add('good')
    await until(() => state('session-expired'), 'the session-expired state')
    expect(button('Sign out')).toBeUndefined()
    expect(new URLSearchParams(window.location.search).get('ctx.store')).toBe('engine')
    const checks = server.calls.filter(call => call.path === TOKEN_CHECK).length

    await signIn('renewed')
    await signedInAs('renewed', 'engine')
    expect(server.calls.filter(call => call.path === TOKEN_CHECK)).toHaveLength(checks + 1)
    expect(server.calls.at(-1)?.token).toBe('renewed')
  })

  it('ignores a late 401 for a token that has since been replaced', async () => {
    // The first token's panel calls answer only when the test releases them,
    // after that token was signed out and replaced, as a slow response would.
    const held: (() => void)[] = []
    const server = fakeConsole(call => {
      if (call.path === TOKEN_CHECK) return answer(200, {})
      if (call.token === 'first') return new Promise(resolve => held.push(() => resolve(refused())))
      return echo(call)
    })
    await render(<App config={bearer} fetch={server.fetch} />)

    await signIn('first')
    await until(() => held.length > 0, 'a panel call carrying the first token')
    await act(async () => button('Sign out')!.click())
    await until(() => state('signed-out'), 'the signed-out state')

    await signIn('second')
    await signedInAs('second')

    await act(async () => held.splice(0).forEach(release => release()))
    await settle(300)
    expect(state('session-expired')).toBeNull()
    expect(button('Sign out')).toBeDefined()
    expect(panelText()).toContain('"token": "second"')
  })

  it('refuses the late 401 only because the token changed: the same token still ends the session', async () => {
    // The control for the test above: the same held 401, released while the
    // token that request carried is still signed in, does end the session.
    const held: (() => void)[] = []
    const server = fakeConsole(call => {
      if (call.path === TOKEN_CHECK) return answer(200, {})
      return new Promise(resolve => held.push(() => resolve(refused())))
    })
    await render(<App config={bearer} fetch={server.fetch} />)

    await signIn('first')
    await until(() => held.length > 0, 'a panel call carrying the first token')
    expect(button('Sign out')).toBeDefined()
    await act(async () => held.splice(0).forEach(release => release()))

    // The session ends: the sign-in gate replaces the dashboard. A panel's own
    // session-expired state is not enough, since terminal-core draws one in
    // any panel whose call gets a 401, whether or not the session ended.
    await until(() => container.querySelector('input[type=password]'), 'the sign-in gate')
    expect(container.textContent).toContain('The console did not accept your token')
    expect(container.textContent).toContain('Bearer token required')
    expect(button('Sign out')).toBeUndefined()
    expect(container.querySelector('.console-main')).toBeNull()
  })
})

describe('list cells', () => {
  it('give a clipped cell its whole value as a tooltip, and a cell that fits none', async () => {
    const failure = 'upstream_5xx: write:bronze failed after 6 attempts'
    const text = (key: string, label: string) => ({ key, label, type: 'RECORD_FIELD_TYPE_TEXT', readOnly: true })
    const list: Template = {
      title: 'Executions',
      widgets: [{ id: 'workflows', component: 'record_grid', title: 'Workflows', source: { source_id: 'temporaless.directory' } }],
    }
    const server = fakeConsole(() => answer(200, { records: {
      tableId: 'temporaless.directory',
      tableName: 'Workflows',
      primaryField: 'workflow_id',
      fields: [text('workflow_id', 'Workflow ID'), text('pending', 'Waiting on or failure')],
      records: [{ id: 'pull:polymarket', values: { workflow_id: 'pull:polymarket', pending: failure } }],
      capabilities: {},
    } }))
    await render(<App config={{ ...loopback, template: list }} fetch={server.fetch} />)

    const cell = (value: string) => [...container.querySelectorAll<HTMLElement>('td > button')].find(element => element.textContent === value)
    const clipped = await until(() => cell(failure), 'the failure cell')
    const fits = cell('pull:polymarket')!
    // jsdom lays nothing out, so each cell is given the widths Chromium
    // measured for it at 1280 px: its content, and the box that shows it.
    const lay = (element: HTMLElement, content: number, box: number) => Object.defineProperties(element, {
      scrollWidth: { value: content, configurable: true },
      clientWidth: { value: box, configurable: true },
    })
    lay(clipped, 322, 285)
    lay(fits, 130, 130)
    await act(async () => {
      for (const element of [clipped, fits]) element.dispatchEvent(new MouseEvent('pointerover', { bubbles: true }))
    })
    expect(clipped.title).toBe(failure)
    expect(fits.hasAttribute('title')).toBe(false)

    // Wider again (a resize), the cell loses the tooltip when focus reaches it.
    lay(clipped, 322, 445)
    await act(async () => clipped.focus())
    expect(clipped.hasAttribute('title')).toBe(false)
  })
})

describe('loopback access', () => {
  it('opens the dashboard with no gate and sends no token', async () => {
    const server = fakeConsole(echo)
    await render(<App config={loopback} fetch={server.fetch} />)

    await signedInAs('none')
    expect(container.querySelector('input[type=password]')).toBeNull()
    expect(container.textContent).toContain('Loopback access')
    expect(server.calls.every(call => call.token === undefined)).toBe(true)
  })
})

describe('bootstrap', () => {
  it('loads the configuration through the same transport, retrying after a failure', async () => {
    let configAnswers = 0
    const server = fakeConsole(call => {
      if (call.path === '/ui/config') {
        configAnswers += 1
        return configAnswers === 1 ? answer(503, { code: 'unavailable', message: 'starting' }) : answer(200, bearer)
      }
      return answer(404, { code: 'not_found', message: 'no such path' })
    })
    await render(<Bootstrap fetch={server.fetch} />)

    const retry = await until(() => button('Retry'), 'the Retry action')
    await act(async () => retry.click())
    await until(() => state('signed-out'), 'the sign-in gate')
    expect(server.calls.map(call => call.path)).toEqual(['/ui/config', '/ui/config'])
  })
})
