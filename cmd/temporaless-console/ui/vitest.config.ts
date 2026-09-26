import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

// The UI host's tests render the app in jsdom against a fake console passed
// in as its fetch; nothing is patched and nothing touches the network.
export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.tsx'],
  },
})
