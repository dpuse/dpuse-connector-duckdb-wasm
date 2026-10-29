# DPUse Connector DuckDB Wasm

<!-- OPENING_START -->

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![DPUse version](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fapi.dpuse.app%2Fconfigs%2Fdpuse-connector-duckdb-wasm&query=%24.data.version&prefix=v&label=DPUse&color=blue)](https://github.com/dpuse/dpuse-connector-duckdb-wasm/releases/latest)
[![CI](https://github.com/dpuse/dpuse-connector-duckdb-wasm/actions/workflows/ci.yml/badge.svg)](https://github.com/dpuse/dpuse-connector-duckdb-wasm/actions/workflows/ci.yml)

Stores data views for investigation in a DuckDB database saved in the browser (OPFS).

[Report a Vulnerability](https://github.com/dpuse/dpuse-connector-duckdb-wasm/security/advisories/new) · [Open an Issue](https://github.com/dpuse/dpuse-connector-duckdb-wasm/issues)

## About DPUse

[DPUse](https://www.dpuse.app) (Data Positioning & Use) is an in-browser application that positions your data for use through three core activities: sourcing, contextualising, and publishing.

**Sourcing** uses a library of [Connectors](https://www.dpuse.app/connectors) to establish [Connections](https://www.dpuse.app) to applications, databases, file stores, and curated datasets; these connections are subsequently used to configure structured [Data Views](https://www.dpuse.app) from the underlying sources.

**Contextualising** extracts chronological events from those [Data Views](https://www.dpuse.app) and maps them into comprehensive [Context Models](https://www.dpuse.app). This gives the DPUse Engine the structural framework needed to generate deterministic transactions, facts, or observations.

**Publishing** uses a library of [Presenters](https://www.dpuse.app) to render standard [Presentations](https://www.dpuse.app) immediately using the contextualised data; additionally, [Cookbooks](https://www.dpuse.app) of [Recipes](https://www.dpuse.app) let you build Data Apps using your preferred tools.

In addition, DPUse provides [Tools](https://www.dpuse.app) used by the application, and you can use them to construct connectors and presenters.

## Introduction

Stores data views for investigation in a DuckDB database saved in the browser's private file system (OPFS). Data stays on this device. Only one tab can use it at a time. Faster than SQLite for grouping large data, but a larger download (about 8 MB) and more memory (about 200–400 MB).

<!-- OPENING_END -->

<!-- SUPPORTED_ACTIONS_START -->

## Supported Actions

Connectors conform to a unified interface contract by implementing a specific subset of standard actions. These standardised actions allow the DPUse application to interact with any underlying data source in the same way, enabling Connectors to be built independently and loaded dynamically at runtime.

This connector is a Bidirectional connector that supports both read and write actions. Connectors can also function as a Source (read-only) or Destination (write-only), depending on the actions they support. The table below lists all connector actions and highlights those supported by this connector.

| Action               | Supported |
| :------------------- | :-------: |
| Abort Operation      |     ✓     |
| Audit Object Content |           |
| Close Store          |     ✓     |
| Create Object        |           |
| Describe Connection  |           |
| Drop Object          |     ✓     |
| Find Object          |           |
| Get Info             |           |
| Get Readable Stream  |           |
| Get Record           |           |
| List Nodes           |           |
| Load Object Records  |     ✓     |
| Open Store           |     ✓     |
| Preview Object       |           |
| Query Records        |     ✓     |
| Remove Records       |           |
| Retrieve Chunks      |           |
| Retrieve Records     |           |
| Upsert Records       |           |

<!-- SUPPORTED_ACTIONS_END -->

<!-- USAGE_START -->

## Usage

This connector is automatically uploaded to the DPUse Engine cloud once released and becomes instantly available to all new browser app instances, with existing instances notified of the update.

You may view or clone this repository for your own purposes, such as building a new, similar connector, though there is currently no process to accept third-party connectors into DPUse at this stage.

```bash
git clone https://github.com/dpuse/dpuse-connector-duckdb-wasm.git
cd dpuse-connector-duckdb-wasm
npm install
```

_Requires [Node.js](https://nodejs.org/) 24 or later, [npm](https://www.npmjs.com/) 12 or later, and [TypeScript](https://www.typescriptlang.org/) 6.0.3 or later._

This repository is managed using the common set of actions provided by [@dpuse/dpuse-development](https://github.com/dpuse/dpuse-development). See the `scripts` block in [package.json](https://github.com/dpuse/dpuse-connector-duckdb-wasm/blob/main/package.json) for details.

<!-- USAGE_END -->

<!-- DEPENDENCY_LICENSES_START -->

## Dependency Licenses

License data is updated each time `npm run document` is run, using [license-checker](https://github.com/RSeidelsohn/license-checker-rseidelsohn). The following table lists every package whose code, styles or assets are included in this project's build, as recorded by the build itself. Modules loaded at run time are not included; each documents its own. These dependencies have been checked and confirmed to use 0BSD, Apache-2.0, or MIT, all of which allow commercial use. All are used unmodified, so any licence conditions that apply only to modified versions are not triggered. Developers cloning this repository should independently verify development dependencies.

| Dependency                                                   |     Version     | License(s) | Document                                                                      |
| :----------------------------------------------------------- | :-------------: | :--------- | :---------------------------------------------------------------------------- |
| [@dpuse/dpuse-shared](https://github.com/dpuse/dpuse-shared) |     1.0.160     | MIT        | [LICENSE](licenses/downloads/@dpuse/dpuse-shared@1.0.160-LICENSE.txt)         |
| [@duckdb/duckdb-wasm](https://github.com/duckdb/duckdb-wasm) | 1.33.1-dev110.0 | MIT        | [LICENSE](licenses/downloads/@duckdb/duckdb-wasm@1.33.1-dev110.0-LICENSE.txt) |
| [apache-arrow](https://github.com/apache/arrow)              |     17.0.0      | Apache-2.0 | [LICENSE](licenses/downloads/apache-arrow@17.0.0-LICENSE.txt)                 |
| [flatbuffers](https://github.com/google/flatbuffers)         |    24.12.23     | Apache-2.0 | [LICENSE](licenses/downloads/flatbuffers@24.12.23-LICENSE.txt)                |
| [tslib](https://github.com/Microsoft/tslib)                  |      2.8.1      | 0BSD       | [LICENSE](licenses/downloads/tslib@2.8.1-LICENSE.txt)                         |
| [valibot](https://github.com/open-circle/valibot)            |      1.5.0      | MIT        | [LICENSE](licenses/downloads/valibot@1.5.0-LICENSE.txt)                       |

### Dependency Tree

The dependency tree below shows how each package in the table above is reached — direct and transitive — along with its installed version, release date, and update status. A package that does not ship itself, such as one whose parts are bundled separately, is left out and what ships beneath it is shown in its place. Packages flagged ❗ have a newer version available; ⚠️ indicates a package that hasn't been updated in the last 6 months or longer. Neither flag necessarily indicates a problem: we let new releases stabilise before upgrading, and some packages are mature and stable (have limited or no dependencies), so they require no active development.

- **[@dpuse/dpuse-shared](https://github.com/dpuse/dpuse-shared)** 1.0.160 — this month: 2026-10-28
    - **[valibot](https://github.com/open-circle/valibot)** 1.5.0 — 1 mth ago: 2026-09-09
- **[@duckdb/duckdb-wasm](https://github.com/duckdb/duckdb-wasm)** 1.33.1-dev110.0 — this month: 2026-10-22 → latest: 1.33.1-dev57.0 — 4 mths ago: 2026-06-22 ❗
    - **[apache-arrow](https://github.com/apache/arrow)** 17.0.0 — 27 mths ago: 2024-07-16 ⚠️ → latest: 21.2.0 — 3 mths ago: 2026-07-21 ❗
- **[apache-arrow](https://github.com/apache/arrow)** 17.0.0 — 27 mths ago: 2024-07-16 ⚠️ → latest: 21.2.0 — 3 mths ago: 2026-07-21 ❗
    - **[tslib](https://github.com/Microsoft/tslib)** 2.8.1 — 23 mths ago: 2024-10-31 ⚠️
    - **[flatbuffers](https://github.com/google/flatbuffers)** 24.12.23 — 22 mths ago: 2024-12-23 ⚠️ → latest: 25.9.23 — 13 mths ago: 2025-09-24 ⚠️ ❗
    - **[tslib](https://github.com/Microsoft/tslib)** 2.8.1 — 23 mths ago: 2024-10-31 ⚠️

<!-- DEPENDENCY_LICENSES_END -->

<!-- BUNDLE_START -->

## Bundle Analysis

This report is updated with each release, from the bundle the release builds, using [Sonda](https://sonda.dev/), which analyses final source maps to reveal the actual effects of tree-shaking and minification rather than relying on pre-build estimates.

_Note: Sonda's Vite reports currently exclude CSS files, since Vite does not generate source maps for CSS._

| Chunk/Module/File                                                             | Composition                                                                                     |
| :---------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------- |
| **dist/dpuse-connector-duckdb-wasm.es.js**                                    | 257.9&nbsp;kB&nbsp;·&nbsp;gzip&nbsp;54.9&nbsp;kB&nbsp;·&nbsp;100.0%&nbsp;of&nbsp;the&nbsp;build |
| &nbsp;&nbsp;&nbsp;&nbsp;apache-arrow → ipc/reader.mjs + 84 more               | `██████████████░░░░░░`&nbsp;71.5%&nbsp;·&nbsp;184.3&nbsp;kB                                     |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ ipc/reader.mjs              | `▒░░░░░░░░░░░░░░░░░░░`&nbsp;4.8%&nbsp;·&nbsp;12.4&nbsp;kB                                       |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ type.mjs                    | `▒░░░░░░░░░░░░░░░░░░░`&nbsp;4.4%&nbsp;·&nbsp;11.2&nbsp;kB                                       |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ data.mjs                    | `▒░░░░░░░░░░░░░░░░░░░`&nbsp;3.3%&nbsp;·&nbsp;8.6&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ ipc/metadata/message.mjs    | `▒░░░░░░░░░░░░░░░░░░░`&nbsp;3.3%&nbsp;·&nbsp;8.4&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ visitor.mjs                 | `▒░░░░░░░░░░░░░░░░░░░`&nbsp;2.9%&nbsp;·&nbsp;7.4&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ util/int.mjs                | `░░░░░░░░░░░░░░░░░░░░`&nbsp;2.5%&nbsp;·&nbsp;6.4&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ visitor/vectorloader.mjs    | `░░░░░░░░░░░░░░░░░░░░`&nbsp;2.5%&nbsp;·&nbsp;6.4&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ visitor/set.mjs             | `░░░░░░░░░░░░░░░░░░░░`&nbsp;2.3%&nbsp;·&nbsp;5.8&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ ipc/writer.mjs              | `░░░░░░░░░░░░░░░░░░░░`&nbsp;2.2%&nbsp;·&nbsp;5.8&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ vector.mjs                  | `░░░░░░░░░░░░░░░░░░░░`&nbsp;2.2%&nbsp;·&nbsp;5.6&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ visitor/get.mjs             | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.9%&nbsp;·&nbsp;4.8&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ ipc/message.mjs             | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.8%&nbsp;·&nbsp;4.7&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ io/adapters.mjs             | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.6%&nbsp;·&nbsp;4.2&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ recordbatch.mjs             | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.5%&nbsp;·&nbsp;4.0&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ table.mjs                   | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.5%&nbsp;·&nbsp;4.0&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ visitor/typecomparator.mjs  | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.5%&nbsp;·&nbsp;3.9&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ util/bn.mjs                 | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.3%&nbsp;·&nbsp;3.2&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ visitor/vectorassembler.mjs | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.2%&nbsp;·&nbsp;3.2&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ io/stream.mjs               | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.2%&nbsp;·&nbsp;3.2&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ util/buffer.mjs             | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.2%&nbsp;·&nbsp;3.1&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ ipc/metadata/json.mjs       | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.1%&nbsp;·&nbsp;2.9&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/schema.mjs               | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.0%&nbsp;·&nbsp;2.6&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ schema.mjs                  | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.0%&nbsp;·&nbsp;2.5&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ io/file.mjs                 | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.0%&nbsp;·&nbsp;2.5&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/field.mjs                | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.9%&nbsp;·&nbsp;2.4&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ io/interfaces.mjs           | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.9%&nbsp;·&nbsp;2.4&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ visitor/indexof.mjs         | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.9%&nbsp;·&nbsp;2.3&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/footer.mjs               | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.9%&nbsp;·&nbsp;2.3&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ visitor/typeassembler.mjs   | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.9%&nbsp;·&nbsp;2.3&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ visitor/iterator.mjs        | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.9%&nbsp;·&nbsp;2.3&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ ipc/metadata/file.mjs       | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.9%&nbsp;·&nbsp;2.3&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ util/vector.mjs             | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.9%&nbsp;·&nbsp;2.2&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ row/struct.mjs              | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.8%&nbsp;·&nbsp;2.1&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/message.mjs              | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.8%&nbsp;·&nbsp;2.0&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ row/map.mjs                 | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.8%&nbsp;·&nbsp;2.0&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ enum.mjs                    | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.8%&nbsp;·&nbsp;2.0&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ util/chunk.mjs              | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.7%&nbsp;·&nbsp;1.9&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ util/bit.mjs                | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.7%&nbsp;·&nbsp;1.7&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/record-batch.mjs         | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.6%&nbsp;·&nbsp;1.6&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/union.mjs                | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.6%&nbsp;·&nbsp;1.5&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/dictionary-encoding.mjs  | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.5%&nbsp;·&nbsp;1.2&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/decimal.mjs              | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.4%&nbsp;·&nbsp;1.1&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/dictionary-batch.mjs     | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.4%&nbsp;·&nbsp;1.0&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/body-compression.mjs     | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.4%&nbsp;·&nbsp;1008&nbsp;B                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ util/recordbatch.mjs        | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.4%&nbsp;·&nbsp;1004&nbsp;B                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/timestamp.mjs            | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.4%&nbsp;·&nbsp;952&nbsp;B                                         |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/key-value.mjs            | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.4%&nbsp;·&nbsp;925&nbsp;B                                         |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/time.mjs                 | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.3%&nbsp;·&nbsp;922&nbsp;B                                         |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/int.mjs                  | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.3%&nbsp;·&nbsp;907&nbsp;B                                         |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ util/compat.mjs             | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.3%&nbsp;·&nbsp;893&nbsp;B                                         |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/fixed-size-binary.mjs    | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.3%&nbsp;·&nbsp;801&nbsp;B                                         |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ fb/floating-point.mjs       | `░░░░░░░░░░░░░░░░░░░░`&nbsp;0.3%&nbsp;·&nbsp;797&nbsp;B                                         |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ 33 smaller files            | `▒░░░░░░░░░░░░░░░░░░░`&nbsp;5.0%&nbsp;·&nbsp;12.9&nbsp;kB                                       |
| &nbsp;&nbsp;&nbsp;&nbsp;@duckdb/duckdb-wasm → dist/duckdb-browser.mjs         | `██░░░░░░░░░░░░░░░░░░`&nbsp;9.5%&nbsp;·&nbsp;24.5&nbsp;kB                                       |
| &nbsp;&nbsp;&nbsp;&nbsp;src → duckdbSql.ts + 1 more                           | `█░░░░░░░░░░░░░░░░░░░`&nbsp;4.0%&nbsp;·&nbsp;10.2&nbsp;kB                                       |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ 2 smaller files             | `▒░░░░░░░░░░░░░░░░░░░`&nbsp;4.0%&nbsp;·&nbsp;10.2&nbsp;kB                                       |
| &nbsp;&nbsp;&nbsp;&nbsp;flatbuffers → mjs/builder.js + 3 more                 | `█░░░░░░░░░░░░░░░░░░░`&nbsp;3.8%&nbsp;·&nbsp;9.8&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ 4 smaller files             | `▒░░░░░░░░░░░░░░░░░░░`&nbsp;3.8%&nbsp;·&nbsp;9.8&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;@dpuse/dpuse-shared → dist/dpuse-shared.es.js         | `█░░░░░░░░░░░░░░░░░░░`&nbsp;3.3%&nbsp;·&nbsp;8.6&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;tslib → tslib.es6.mjs                                 | `░░░░░░░░░░░░░░░░░░░░`&nbsp;1.0%&nbsp;·&nbsp;2.5&nbsp;kB                                        |
| &nbsp;&nbsp;&nbsp;&nbsp;(bundler output, whitespace & JSON)                   | `█░░░░░░░░░░░░░░░░░░░`&nbsp;7.0%&nbsp;·&nbsp;18.1&nbsp;kB                                       |

Bars show each row's share of its output file. ↳ rows are part of the row above.

- n more = the row also holds n more files from the same package or folder, each no larger than the one named. A package can appear under several output files, each holding different files, never the same file twice.

(bundler output, whitespace & JSON) = bytes Sonda can't trace to a source file: whitespace (indentation and line breaks), code the bundler generates (region comments, the combined import/export lines, its small runtime helper and wrappers), and imported JSON such as `config.json`, which the bundler doesn't map. The JSON and the generated code are real bytes that ship; the whitespace mostly disappears once compressed.

<!-- BUNDLE_END -->

<!-- QUALITY_SECURITY_START -->

## Quality & Security

This section is updated each time `npm run document` is run. Settings come from the repository's workflow files and GitHub. Test coverage and the Fallow score are measured at the same time.

### Testing

| Check                | Status | What it does                                                                                                                                                                                                                                                                                                          |
| :------------------- | :----- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Unit tests           | ✅ On  | [Vitest](https://vitest.dev) runs the unit tests. Part of the [CI workflow](https://github.com/dpuse/dpuse-connector-duckdb-wasm/actions/workflows/ci.yml) on every push and pull request to `main`.                                                                                                                  |
| Property-based tests | ❌ Off | [fast-check](https://fast-check.dev) runs many random inputs per test to find edge cases, alongside the unit tests.                                                                                                                                                                                                   |
| Test coverage        | ✅ On  | ![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fdpuse%2Fdpuse-connector-duckdb-wasm%2Fmain%2Fcode-health-reports%2Fvitest%2Fbadge.json) [Vitest's V8 coverage](https://vitest.dev/guide/coverage) measures the share of source lines the unit tests run. The target is 80%. |

### Code Quality

| Check         | Status | What it does                                                                                                                                                                                                                                                                                                           |
| :------------ | :----- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Code analysis | ✅ On  | [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=dpuse_dpuse-connector-duckdb-wasm&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=dpuse_dpuse-connector-duckdb-wasm) [SonarCloud](https://sonarcloud.io) checks every push for bugs, code smells and vulnerabilities. |
| Linting       | ✅ On  | [ESLint](https://eslint.org) checks the code for errors and style problems. Part of the [CI workflow](https://github.com/dpuse/dpuse-connector-duckdb-wasm/actions/workflows/ci.yml) on every push and pull request to `main`.                                                                                         |

### Security Analysis

| Check           | Status | What it does                                                                                                                                                                                                                                                                                                                                                                                     |
| :-------------- | :----- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Push protection | ❌ Off | [GitHub push protection](https://docs.github.com/en/code-security/secret-scanning/push-protection-for-repositories-and-organizations) blocks pushes that contain credentials.                                                                                                                                                                                                                    |
| Static analysis | ✅ On  | [![CodeQL](https://github.com/dpuse/dpuse-connector-duckdb-wasm/actions/workflows/codeql.yml/badge.svg)](https://github.com/dpuse/dpuse-connector-duckdb-wasm/security/code-scanning) [CodeQL](https://codeql.github.com) scans GitHub Actions and JavaScript/TypeScript for security vulnerabilities, using the extended security queries, on every push and pull request to `main` and weekly. |
| Secret scanning | ❌ Off | [GitHub secret scanning](https://docs.github.com/en/code-security/secret-scanning) detects credentials, such as API keys and tokens, committed to the repository.                                                                                                                                                                                                                                |

### Dependencies

| Check               | Status | What it does                                                                                                                                                                                                                                                                                                                       |
| :------------------ | :----- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Vulnerability audit | ✅ On  | [npm audit](https://docs.npmjs.com/cli/commands/npm-audit) fails when a shipped dependency has any known vulnerability, or a development dependency has a high or critical one. Part of the [CI workflow](https://github.com/dpuse/dpuse-connector-duckdb-wasm/actions/workflows/ci.yml) on every push and pull request to `main`. |
| Supply chain risk   | ✅ On  | [Socket](https://socket.dev) flags malicious packages, typosquatting and suspicious behaviour that may not yet have a CVE.                                                                                                                                                                                                         |
| Security alerts     | ✅ On  | [Dependabot](https://docs.github.com/en/code-security/dependabot) alerts when a dependency has a known vulnerability, using the GitHub Advisory Database.                                                                                                                                                                          |
| Security updates    | ❌ Off | [Dependabot](https://docs.github.com/en/code-security/dependabot) opens pull requests that update vulnerable dependencies. These are handled manually.                                                                                                                                                                             |
| Version updates     | ❌ Off | [Dependabot](https://docs.github.com/en/code-security/dependabot) opens pull requests for new dependency versions. These are handled manually.                                                                                                                                                                                     |

### OpenSSF 🚧

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/dpuse/dpuse-connector-duckdb-wasm/badge)](https://scorecard.dev/viewer/?uri=github.com/dpuse/dpuse-connector-duckdb-wasm)

This project is working towards the [OpenSSF Best Practices](https://www.bestpractices.dev) Passing badge, a self-certification covering security policy, vulnerability reporting, build processes, code quality, and more. Currently the [OpenSSF Scorecard](https://scorecard.dev) provides an independent automated assessment of the project's security practices and is an ongoing area of improvement.

### Reporting Vulnerabilities

Please do not open public GitHub issues for security vulnerabilities. See [SECURITY.md](./SECURITY.md) for how to report one privately, the full disclosure policy, and expected response times.

<!-- QUALITY_SECURITY_END -->

<!-- CONTRIBUTING_LICENSE_START -->

## Contributing

This repository is maintained solely by its owner and does not, at present, accept external contributions into the canonical repo. Its source is published openly under the MIT License — every DPUse project is fully open source except DPUse Engine, which remains closed and proprietary.

For security vulnerabilities, see [Reporting Vulnerabilities](#reporting-vulnerabilities). For bugs, inconsistencies, or other feedback, [open a GitHub issue](https://github.com/dpuse/dpuse-connector-duckdb-wasm/issues) — feedback is read, but responses and fixes are at the maintainer's discretion.

## License

This project is licensed under the MIT License, permitting free use, modification, and distribution.

[MIT](./LICENSE) © 2026 Jonathan Terrell

<!-- CONTRIBUTING_LICENSE_END -->
