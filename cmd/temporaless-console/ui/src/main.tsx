import { StrictMode, useEffect, useMemo, useState, type FormEvent } from 'react'
import { createRoot } from 'react-dom/client'
import { Dashboard, type Template } from 'medallion-terminal-core/dashboard'
import { Badge, Button, Callout, DesignSystemProvider, FormField, Input, LoadingState } from 'medallion-terminal-core/toolkit'
import 'medallion-terminal-core/styles'
import './styles.css'

// The console host is deliberately thin: the server owns every source and the
// template; this page supplies chrome, the theme, and (in bearer mode) an
// in-memory token.

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

function App({ config }: { config: UIConfig }) {
  const [theme, setTheme] = useState<Theme>(initialTheme)
  const [token, setToken] = useState<string>()
  const headers = useMemo(() => (token ? { Authorization: `Bearer ${token}` } : undefined), [token])
  useEffect(() => {
    document.documentElement.dataset.theme = theme
    document.documentElement.style.colorScheme = theme
  }, [theme])
  const toggleTheme = () => {
    const next = theme === 'dark' ? 'light' : 'dark'
    setTheme(next)
    saveTheme(next)
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
          <Badge intent="neutral">{config.auth === 'bearer' ? 'Signed in with a bearer token' : 'Loopback access'}</Badge>
          <Button size="small" onClick={toggleTheme} aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
            {theme === 'dark' ? 'Light theme' : 'Dark theme'}
          </Button>
          {config.auth === 'bearer' && token && <Button size="small" onClick={() => setToken(undefined)}>Sign out</Button>}
        </div>
      </header>
      {needsToken ? <TokenForm onSubmit={setToken} /> : (
        <main className="console-main">
          <Dashboard
            template={config.template}
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

const root = document.getElementById('root')
if (!root) throw new Error('Missing #root element')
createRoot(root).render(<StrictMode><Bootstrap /></StrictMode>)
