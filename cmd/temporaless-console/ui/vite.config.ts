import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

// The console serves the UI and the API from one origin. In development the
// Vite server proxies the UI config and every RPC path to a running console:
// CONSOLE_URL from the environment or a .env file, else the default listener.
// loadEnv keeps this file free of Node globals, so the UI type-checks on its
// own dependency tree.
export default defineConfig(({ mode }) => {
  const target = loadEnv(mode, '.', 'CONSOLE_').CONSOLE_URL || 'http://127.0.0.1:8080'
  return {
    base: '/',
    plugins: [react()],
    build: { outDir: '../assets/dist', emptyOutDir: true, chunkSizeWarningLimit: 2048 },
    server: { proxy: { '/ui/config': target, '^/[a-z0-9_.]+\\.[A-Za-z]+Service/': target } },
  }
})
