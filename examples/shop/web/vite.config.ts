import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// `pnpm build` writes dist/, which the shop serves at http://127.0.0.1:3000.
// `pnpm dev` serves the page on :5173 with hot reload, and passes what it asks
// of the shop through.
export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: { '/events': 'http://127.0.0.1:3000' },
  },
})
