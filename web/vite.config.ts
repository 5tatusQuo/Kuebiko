import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

export default defineConfig({
  plugins: [solid()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/ws": { target: "ws://127.0.0.1:7878", ws: true },
      "/healthz": "http://127.0.0.1:7878"
    }
  },
  build: { target: "es2022", sourcemap: true }
});

