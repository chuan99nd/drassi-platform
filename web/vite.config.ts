import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // Proxy /api/* to the Go backend (cmd/drassi-server, internal/api) so the
    // SPA can call same-origin `/api/...` in dev with no CORS setup needed.
    // See tasks/M0/T-M0-07-runners-api-and-fe-skeleton.md.
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
})
