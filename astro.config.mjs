import { defineConfig } from 'astro/config';
import node from '@astrojs/node';
import tailwindcss from '@tailwindcss/vite';
import sentry from '@sentry/astro';

export default defineConfig({
  output: 'server',
  adapter: node({ mode: 'standalone' }),
  site: 'https://plainhazard.com',
  build: {
    inlineStylesheets: 'auto',
  },
  vite: {
    plugins: [tailwindcss()],
    build: { target: 'es2022' },
  },
  integrations: [
    sentry({
      dsn: 'https://7f85f5b92eaf070ef3cb7ae5d98c35c7@o4510827630231552.ingest.de.sentry.io/4511031099064400',
      enabled: { client: false, server: true },
      sourceMapsUploadOptions: {
        enabled: false,
      },
    }),
  ],
});
