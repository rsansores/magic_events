/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'node:path'

// `vite` serves the demo (index.html, made-up data; `?live` reads a running
// shop, at SHOP_URL or localhost:3000, through the proxy); `vite build`
// builds the library a host embeds.
export default defineConfig(({ command }) => ({
  plugins: [vue()],
  server: {
    proxy: { '/events': process.env.SHOP_URL ?? 'http://localhost:3000' },
  },
  build:
    command === 'build'
      ? {
          lib: {
            entry: {
              dashboard: resolve(import.meta.dirname, 'src/index.ts'),
              core: resolve(import.meta.dirname, 'src/core/index.ts'),
            },
            formats: ['es'],
          },
          rollupOptions: { external: ['vue'] },
        }
      : undefined,
  test: {
    include: ['src/**/*.test.ts'],
    environment: 'node',
  },
}))
