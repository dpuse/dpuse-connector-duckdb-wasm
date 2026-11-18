// ── External Dependencies & Registrations
import { createRequire } from 'node:module';
import { defineConfig } from 'vite';
import dts from 'vite-plugin-dts';
import path from 'node:path';
import type { Plugin } from 'vite';
import Sonda from 'sonda/vite';
import { copyFileSync, readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath, URL } from 'node:url';

// ── DPUse Framework
import { recordShippedPackages } from '@dpuse/dpuse-development/vite';

// ── Data
import config from './config.json' with { type: 'json' };

// ── Constants ────────────────────────────────────────────────────────────────────────────────────────────────────────

const WORKER_FILE_NAME = 'duckdb-browser-base.worker.js';

// ── Plugins ──────────────────────────────────────────────────────────────────────────────────────────────────────────

// Ships DuckDB's '.wasm' file and worker script beside the connector's script, where the connector finds them. The
// '.wasm' file is too large to inline, and a worker needs a script file of its own. The worker script names a source map
// that DuckDB does not publish, so that comment is dropped, or Safari's Web Inspector logs a 404 for it. The shipped
// worker script opens with a notice saying so, as it is no longer the file DuckDB published.
function shipDuckDBFiles(): Plugin {
    const duckdbDistributionPath = path.dirname(createRequire(import.meta.url).resolve('@duckdb/duckdb-wasm/dist/duckdb-base.wasm'));
    const changeNotice =
        "/*! The worker script of @duckdb/duckdb-wasm (MIT), changed by DPUse: its closing 'sourceMappingURL' comment is removed, as DuckDB publishes no source map for it. Nothing else is changed. */";
    return {
        name: 'ship-duckdb-files',
        writeBundle(options) {
            const outputPath = options.dir ?? 'dist';
            copyFileSync(path.join(duckdbDistributionPath, 'duckdb-base.wasm'), path.join(outputPath, 'duckdb-base.wasm'));
            const workerScript = readFileSync(path.join(duckdbDistributionPath, WORKER_FILE_NAME), 'utf-8'); // eslint-disable-line security/detect-non-literal-fs-filename -- DuckDB's own package folder.
            writeFileSync(path.join(outputPath, WORKER_FILE_NAME), `${changeNotice}\n${workerScript.replace(/\n\/\/# sourceMappingURL=\S+\s*$/u, '\n')}`); // eslint-disable-line security/detect-non-literal-fs-filename -- The build's own output folder.
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
