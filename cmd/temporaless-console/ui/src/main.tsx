import { StrictMode, useEffect, useLayoutEffect, useMemo, useRef, useState, type FormEvent } from 'react'
import { createRoot } from 'react-dom/client'
import { createProductFetch, ensureOk, toSourceError, type ProductFetch, type SourceError } from 'medallion-terminal-core/app'
import { Dashboard, type Template } from 'medallion-terminal-core/dashboard'
import {
  Badge,
  Button,
  ButtonGroup,
  DesignSystemProvider,
  FormField,
  Icon,
  Input,
  LoadingState,
  SessionExpiredState,
  SignedOutState,
  SourceErrorState,
} from 'medallion-terminal-core/toolkit'
import 'medallion-terminal-core/styles'
import './styles.css'

// The console host is deliberately thin: the server owns every source and the
// template; this page supplies chrome, the theme, the time zone times are
// shown in, the transport, and (in bearer mode) an in-memory token.

interface UIConfig {
  title: string
  auth: 'loopback' | 'bearer'
  template: Template
}

// Every call to the console goes through the terminal-core product
// transport: it adds x-request-id (the server logs it) and a traceparent,
// types failures as SourceError, and gives up on a backend that has not
// started answering within the timeout instead of freezing a panel's polls.
const REQUEST_TIMEOUT_MS = 30_000

function consoleFetch(onUnauthenticated?: (error: SourceError) => void): ProductFetch {
  return createProductFetch({ timeoutMs: REQUEST_TIMEOUT_MS, onUnauthenticated })
}

type Theme = 'dark' | 'light'
const THEME_KEY = 'temporaless-console.theme'

function initialTheme(): Theme {
  try {
    const saved = window.localStorage.getItem(THEME_KEY)
    if (saved === 'dark' || saved === 'light') return saved
  } catch {
    // Storage can be unavailable (private windows); fall back to the system.
  }
  return window.matchMedia?.('(prefers-color-scheme: light)').matches ? 'light' : 'dark'
}

function saveTheme(theme: Theme) {
  try {
    window.localStorage.setItem(THEME_KEY, theme)
  } catch {
    // A per-viewer convenience only.
  }
}

// The terminal-core tokens, type scale, and density are scoped to the
// .mtc-root that DesignSystemProvider renders around the console, and neither
// the package nor this host styles <html> or <body> with them: .mtc-root sets
// its own font size, so on <html> it would shrink the rem every token is
// measured in. The document only follows the theme's color scheme, and its
// canvas (seen when the page overscrolls) takes the app root's --mtc-bg.
function useDocumentCanvas(theme: Theme) {
  const appRoot = useRef<HTMLDivElement>(null)
  useLayoutEffect(() => {
    const html = document.documentElement
    html.style.colorScheme = theme
    if (appRoot.current) html.style.backgroundColor = getComputedStyle(appRoot.current).backgroundColor
  }, [theme])
  return appRoot
}

// Times are UTC unless the viewer switches to their browser's zone. The
// server formats every time in the zone the tz parameter names and labels
// each time column with it.
type TimeMode = 'utc' | 'local'
const TIME_KEY = 'temporaless-console.time'

function browserZone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'
  } catch {
    return 'UTC'
  }
}

function zoneFor(mode: TimeMode, localZone: string): string {
  return mode === 'local' ? localZone : 'UTC'
}

function savedTimeMode(): TimeMode {
  try {
    if (window.localStorage.getItem(TIME_KEY) === 'local') return 'local'
  } catch {
    // Storage can be unavailable; UTC is the default.
  }
  return 'utc'
}

function saveTimeMode(mode: TimeMode) {
  try {
    window.localStorage.setItem(TIME_KEY, mode)
  } catch {
    // A per-viewer convenience only.
  }
}

// The zone is the viewer's choice, not part of a shared link. The dashboard
// mirrors every context key into ctx.* query parameters, so the zone is not
// a context key here: the host binds the template's ${ctx.tz} source
// parameters to the chosen zone before each dashboard mount.
const ZONE_PARAM = '${ctx.tz}'

function withZone(template: Template, zone: string): Template {
  const { tz: _templateDefault, ...values } = template.context?.values ?? {}
  return {
    ...template,
    context: { values },
    widgets: template.widgets.map(widget => {
      const params = widget.source?.params
      if (!params) return widget
      const bound = Object.fromEntries(Object.entries(params).map(([key, value]) => [key, value.replaceAll(ZONE_PARAM, zone)]))
      return { ...widget, source: { ...widget.source, params: bound } }
    }),
  }
}

// A link can still carry ctx.tz (hand-written, or copied from a development
// build); drop it so the dashboard neither reads it nor copies it back.
function dropLinkedZone() {
  const url = new URL(window.location.href)
  if (!url.searchParams.has('ctx.tz')) return
  url.searchParams.delete('ctx.tz')
  window.history.replaceState(window.history.state, '', url)
}

// verifyToken asks the console which stores a token may inspect, a call that
// reads no records, so a wrong or expired token is refused once at the gate
// rather than by every panel of the dashboard.
const TOKEN_CHECK_PATH = '/temporaless.v1.RunInspectionService/GetInspectionCapabilities'

async function verifyToken(token: string): Promise<void> {
  await consoleFetch()(TOKEN_CHECK_PATH, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: '{}',
  }).then(ensureOk)
}

// SignInGate asks for a bearer token: first as the signed-out state, and
// again after the server refused a token (expired, revoked, or mistyped),
// whether at this gate or mid-session. The dashboard's selection lives in the
// page URL, so signing in again returns to the same store, workflow, and run.
function SignInGate({ refused, onSignedIn }: { refused?: SourceError, onSignedIn: (token: string) => void }) {
  const [token, setToken] = useState('')
  const [checking, setChecking] = useState(false)
  const [failure, setFailure] = useState(refused)
  const submit = (event: FormEvent) => {
    event.preventDefault()
    const value = token.trim()
    if (!value || checking) return
    setChecking(true)
    verifyToken(value).then(() => onSignedIn(value), (cause: unknown) => {
      setFailure(toSourceError(cause))
      setChecking(false)
    })
  }
  let state
  if (failure?.kind === 'unauthenticated') {
    state = (
      <SessionExpiredState
        error={failure}
        title="The console did not accept your token"
        description="It may have expired or been revoked. Paste a new bearer token to continue where you were."
      />
    )
  } else if (failure) {
    state = <SourceErrorState error={failure} />
  } else {
    state = (
      <SignedOutState
        title="Sign in to inspect executions"
        description="This console is read-only. Paste a bearer token issued for it; the token stays in this tab's memory and is gone on reload."
      />
    )
  }
  return (
    <main className="console-gate">
      <section className="console-gate-card" aria-label="Sign in">
        {state}
        <form onSubmit={submit} className="console-gate-form">
          <FormField label="Bearer token" required>
            <Input type="password" value={token} onChange={event => setToken(event.target.value)} autoComplete="off" spellCheck={false} placeholder="eyJhbGciOi…" autoFocus />
          </FormField>
          <Button type="submit" intent="primary" variant="solid" loading={checking} loadingLabel="Checking the token">
            {failure ? 'Continue' : 'Open console'}
          </Button>
        </form>
      </section>
    </main>
  )
}

// accessLabel names how this tab reaches the API; it never claims a sign-in
// that has not happened.
function accessLabel(auth: UIConfig['auth'], signedIn: boolean): string {
  if (auth === 'loopback') return 'Loopback access'
  return signedIn ? 'Signed in with a bearer token' : 'Bearer token required'
}

// Session is the bearer token this tab holds and, once the server refused
// it mid-session, the typed failure that says so.
interface Session {
  token?: string
  refused?: SourceError
}

function App({ config }: { config: UIConfig }) {
  const [theme, setTheme] = useState<Theme>(initialTheme)
  const [session, setSession] = useState<Session>({})
  const token = session.token
  const localZone = useMemo(browserZone, [])
  const [timeMode, setTimeMode] = useState<TimeMode>(() => {
    dropLinkedZone()
    return savedTimeMode()
  })
  const zone = zoneFor(timeMode, localZone)
  const template = useMemo(() => withZone(config.template, zone), [config.template, zone])
  const headers = useMemo(() => (token ? { Authorization: `Bearer ${token}` } : undefined), [token])
  // One transport per token. A 401 answers the token that request carried,
  // so a late refusal of a replaced token never signs the new one out.
  const transport = useMemo(() => consoleFetch(config.auth === 'bearer'
    ? error => setSession(current => (current.token === token ? { refused: error } : current))
    : undefined), [config.auth, token])
  const appRoot = useDocumentCanvas(theme)
  const toggleTheme = () => {
    const next = theme === 'dark' ? 'light' : 'dark'
    setTheme(next)
    saveTheme(next)
  }
  const chooseTime = (mode: TimeMode) => {
    if (mode === timeMode) return
    setTimeMode(mode)
    saveTimeMode(mode)
  }
  const needsToken = config.auth === 'bearer' && !token
  const signIn = (value: string) => setSession({ token: value })

  return (
    <DesignSystemProvider ref={appRoot} theme={theme} className="console-app">
      <header className="console-header">
        <div className="console-brand">
          <Icon name="workflow" className="console-mark" />
          <span className="console-product">Temporaless</span>
          <span className="console-divider" aria-hidden="true">/</span>
          <span className="console-surface">Console</span>
        </div>
        <div className="console-header-end">
          <Badge intent="success" dot>Read-only</Badge>
          <Badge intent="neutral">{accessLabel(config.auth, Boolean(token))}</Badge>
          <ButtonGroup label="Show times in" className="console-time">
            <Button
              size="small"
              variant={timeMode === 'utc' ? 'solid' : 'outline'}
              intent={timeMode === 'utc' ? 'primary' : 'neutral'}
              aria-pressed={timeMode === 'utc'}
              onClick={() => chooseTime('utc')}
            >
              UTC
            </Button>
            <Button
              size="small"
              variant={timeMode === 'local' ? 'solid' : 'outline'}
              intent={timeMode === 'local' ? 'primary' : 'neutral'}
              aria-pressed={timeMode === 'local'}
              title={`Show times in your browser's zone, ${localZone}`}
              onClick={() => chooseTime('local')}
            >
              Local · {localZone}
            </Button>
          </ButtonGroup>
          <Button size="small" onClick={toggleTheme} aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
            {theme === 'dark' ? 'Light theme' : 'Dark theme'}
          </Button>
          {config.auth === 'bearer' && token && <Button size="small" onClick={() => setSession({})}>Sign out</Button>}
        </div>
      </header>
      {needsToken ? <SignInGate refused={session.refused} onSignedIn={signIn} /> : (
        <main className="console-main">
          <Dashboard
            key={zone}
            template={template}
            backendUrl=""
            backendHeaders={headers}
            fetch={transport}
            chrome="minimal"
            templateTrust="trusted"
          />
        </main>
      )}
      <footer className="console-footer">
        <span>History is derived from durable records. Temporaless keeps point records, not a journal: overwritten intermediate states and released claims are not shown.</span>
      </footer>
    </DesignSystemProvider>
  )
}

function Bootstrap() {
  const [theme] = useState<Theme>(initialTheme)
  const appRoot = useDocumentCanvas(theme)
  const [config, setConfig] = useState<UIConfig>()
  const [error, setError] = useState<SourceError>()
  const [attempt, setAttempt] = useState(0)
  useEffect(() => {
    const controller = new AbortController()
    consoleFetch()('/ui/config', { cache: 'no-store', signal: controller.signal })
      .then(ensureOk)
      .then(async response => (await response.json()) as UIConfig)
      .then(setConfig)
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) setError(toSourceError(cause))
      })
    return () => controller.abort()
  }, [attempt])
  const retry = () => {
    setError(undefined)
    setAttempt(value => value + 1)
  }
  if (config) return <App config={config} />
  return (
    <DesignSystemProvider ref={appRoot} theme={theme} className="console-bootstrap">
      {error
        ? <SourceErrorState error={error} onRetry={retry} />
        : <LoadingState label="Opening the console" />}
    </DesignSystemProvider>
  )
}

const root = document.getElementById('root')
if (!root) throw new Error('Missing #root element')
createRoot(root).render(<StrictMode><Bootstrap /></StrictMode>)
