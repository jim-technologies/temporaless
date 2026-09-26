import { StrictMode, useEffect, useMemo, useState, type FormEvent } from 'react'
import { createRoot } from 'react-dom/client'
import { Dashboard, type Template } from 'medallion-terminal-core/dashboard'
import { Badge, Button, ButtonGroup, Callout, DesignSystemProvider, FormField, Input, LoadingState } from 'medallion-terminal-core/toolkit'
import 'medallion-terminal-core/styles'
import './styles.css'

// The console host is deliberately thin: the server owns every source and the
// template; this page supplies chrome, the theme, the time zone times are
// shown in, and (in bearer mode) an in-memory token.

interface UIConfig {
  title: string
  auth: 'loopback' | 'bearer'
  template: Template
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

// The terminal-core tokens are scoped to .mtc-root, so the document root
// carries that class and the theme too: the page background behind and
// around the app is the --mtc-bg token, not a hard-coded color.
function applyTheme(theme: Theme) {
  const root = document.documentElement
  root.classList.add('mtc-root')
  root.dataset.theme = theme
  root.style.colorScheme = theme
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

function TokenForm({ onSubmit }: { onSubmit: (token: string) => void }) {
  const [token, setToken] = useState('')
  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (token.trim()) onSubmit(token.trim())
  }
  return (
    <main className="console-gate">
      <section className="console-gate-card" aria-labelledby="gate-heading">
        <h1 id="gate-heading">Sign in to inspect executions</h1>
        <p className="console-muted">
          This console is read-only. Paste a bearer token issued for it; the token stays in this tab's memory and is gone on reload.
        </p>
        <form onSubmit={submit} className="console-gate-form">
          <FormField label="Bearer token" required>
            <Input type="password" value={token} onChange={event => setToken(event.target.value)} autoComplete="off" spellCheck={false} placeholder="eyJhbGciOi…" />
          </FormField>
          <Button type="submit" intent="primary" variant="solid">Open console</Button>
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

function App({ config }: { config: UIConfig }) {
  const [theme, setTheme] = useState<Theme>(initialTheme)
  const [token, setToken] = useState<string>()
  const localZone = useMemo(browserZone, [])
  const [timeMode, setTimeMode] = useState<TimeMode>(() => {
    dropLinkedZone()
    return savedTimeMode()
  })
  const zone = zoneFor(timeMode, localZone)
  const template = useMemo(() => withZone(config.template, zone), [config.template, zone])
  const headers = useMemo(() => (token ? { Authorization: `Bearer ${token}` } : undefined), [token])
  useEffect(() => applyTheme(theme), [theme])
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

  return (
    <DesignSystemProvider theme={theme} density="compact" className="console-app">
      <header className="console-header">
        <div className="console-brand">
          <span className="console-mark" aria-hidden="true" />
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
          {config.auth === 'bearer' && token && <Button size="small" onClick={() => setToken(undefined)}>Sign out</Button>}
        </div>
      </header>
      {needsToken ? <TokenForm onSubmit={setToken} /> : (
        <main className="console-main">
          <Dashboard
            key={zone}
            template={template}
            backendUrl=""
            backendHeaders={headers}
            theme={theme}
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
  const [config, setConfig] = useState<UIConfig>()
  const [error, setError] = useState('')
  useEffect(() => {
    const controller = new AbortController()
    fetch('/ui/config', { cache: 'no-store', signal: controller.signal })
      .then(async response => {
        if (!response.ok) throw new Error(`The console configuration did not load (HTTP ${response.status}).`)
        return (await response.json()) as UIConfig
      })
      .then(setConfig)
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : String(cause))
      })
    return () => controller.abort()
  }, [])
  if (config) return <App config={config} />
  return (
    <DesignSystemProvider theme={initialTheme()} className="console-bootstrap">
      {error
        ? <Callout intent="danger" title="The console could not open" actions={<Button onClick={() => window.location.reload()}>Try again</Button>}>{error}</Callout>
        : <LoadingState label="Opening the console" />}
    </DesignSystemProvider>
  )
}

applyTheme(initialTheme())
const root = document.getElementById('root')
if (!root) throw new Error('Missing #root element')
createRoot(root).render(<StrictMode><Bootstrap /></StrictMode>)
