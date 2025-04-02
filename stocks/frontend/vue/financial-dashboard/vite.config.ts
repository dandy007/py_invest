import { fileURLToPath, URL } from 'node:url' // Potřebujete importovat
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    vue(),
  ],
  resolve: {
    alias: {
      // TOTO JE DŮLEŽITÉ: @ mapuje na src adresář
      '@': fileURLToPath(new URL('./src', import.meta.url)),
      'stream': 'readable-stream',
      'plotly.js': 'plotly.js/dist/plotly.js',
      'buffer': 'buffer/'
    }
  },
  optimizeDeps: {
    esbuildOptions: {
      // Node.js global to browser globalThis
      define: {
        global: 'globalThis' // <- DŮLEŽITÉ pro esbuild při pre-bundlingu
      },
      // Enable esbuild polyfill plugins if needed, often helps with Buffer/process
      // Potřebujete nainstalovat tyto pluginy:
      // npm install --save-dev @esbuild-plugins/node-globals-polyfill @esbuild-plugins/node-modules-polyfill
      // import { NodeGlobalsPolyfillPlugin } from '@esbuild-plugins/node-globals-polyfill'
      // import { NodeModulesPolyfillPlugin } from '@esbuild-plugins/node-modules-polyfill'
      // plugins: [
      //   NodeGlobalsPolyfillPlugin({
      //       process: true, // Pokud by byl problém i s 'process'
      //       buffer: true, // Explicitně povolit Buffer polyfill
      //   }),
      //   NodeModulesPolyfillPlugin() // Obecnější polyfill pro Node moduly
      // ]
      // Alternativně, často stačí jen define pro Buffer v hlavní konfiguraci níže
    }
  },
   // Někdy je nutné definovat Buffer i globálně pro běh v prohlížeči
   define: {
     // 'process.env': {}, // Pokud by se objevil error s process.env
     // 'global': {},      // Někdy potřeba místo 'globalThis' v optimizeDeps
     // Explicitně definujte globální Buffer pomocí importu z polyfillu
     'Buffer': ['buffer', 'Buffer']
   }
})