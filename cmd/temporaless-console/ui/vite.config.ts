import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The console serves the UI and the API from one origin. In development the
// Vite server proxies the UI config and every RPC path to a running console.
const target = process.env.CONSOLE_URL ?? 'http://127.0.0.1:8080'

export default defineConfig({
  base: '/',
  plugins: [react()],
  build: { outDir: '../assets/dist', emptyOutDir: true, chunkSizeWarningLimit: 2048 },
  server: { proxy: { '/ui/config': target, '^/[a-z0-9_.]+\\.[A-Za-z]+Service/': target } },
})
