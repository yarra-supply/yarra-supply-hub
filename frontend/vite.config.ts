import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { fileURLToPath } from 'node:url';

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    host: 'localhost',
    port: 5173,
    https: false,
    // 如需本地 HTTPS，在此读取证书：
    // https: {
    //   cert: readFileSync(new URL('./certs/app.local.test.pem', import.meta.url)),
    //   key: readFileSync(new URL('./certs/app.local.test-key.pem', import.meta.url)),
    // },
    proxy: {
      '/api': { target: 'http://localhost:8000', changeOrigin: true },
    },
  },
});
