// ── External Dependencies & Registrations
import { copyFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { defineConfig } from 'vite';
import dts from 'vite-plugin-dts';
import path from 'node:path';
import type { Plugin } from 'vite';
import Sonda from 'sonda/vite';
import { fileURLToPath, URL } from 'node:url';

// ── DPUse Framework
import { recordShippedPackages } from '@dpuse/dpuse-development/vite';

// ── Data
import config from './config.json' with { type: 'json' };

// ── Plugins ──────────────────────────────────────────────────────────────────────────────────────────────────────────

// Ships DuckDB's '.wasm' file and worker script beside the connector's script, where the connector finds them. The
// '.wasm' file is too large to inline, and a worker needs a script file of its own.
function shipDuckDBFiles(): Plugin {
    const duckdbDistributionPath = path.dirname(createRequire(import.meta.url).resolve('@duckdb/duckdb-wasm/dist/duckdb-base.wasm'));
    return {
        name: 'ship-duckdb-files',
        writeBundle(options) {
            for (const fileName of ['duckdb-base.wasm', 'duckdb-browser-base.worker.js']) copyFileSync(path.join(duckdbDistributionPath, fileName), path.join(options.dir ?? 'dist', fileName));
        }
    };
}

// ── Vite Configuration ───────────────────────────────────────────────────────────────────────────────────────────────

const viteConfig = defineConfig({
    build: {
        lib: {
            entry: fileURLToPath(new URL('src/index.ts', import.meta.url)),
            fileName: (format) => `${config.id}.${format}.js`,
            formats: ['es']
        },
        rolldownOptions: {
            plugins: [Sonda({ filename: 'index', format: 'json', brotli: false, gzip: true, open: false, outputDir: './bundle-analysis-reports/sonda' })]
        },
        sourcemap: 'hidden',
        target: 'esnext'
    },
    // Tests and config files sit in the tsconfig so they get type-checked, but their declarations must not reach the
    // published package. 'entryRoot' keeps the types under 'dist/types/src', where package.json points.
    // 'recordShippedPackages' writes the record of what the build ships, which 'npm run document' lists licences from.
    plugins: [shipDuckDBFiles(), dts({ entryRoot: '.', exclude: ['tests/**', '*.config.*'], outDirs: 'dist/types' }), recordShippedPackages()],
    resolve: {
        alias: {
            '~': fileURLToPath(new URL('./', import.meta.url)),
            '@': fileURLToPath(new URL('src', import.meta.url))
        }
    }
});

export default viteConfig;
