// ── External Dependencies & Registrations
import type { Table } from 'apache-arrow';
import { AsyncDuckDB, type AsyncDuckDBConnection, DuckDBAccessMode, VoidLogger } from '@duckdb/duckdb-wasm';

// ── DPUse Framework
import { normalizeToError } from '@dpuse/dpuse-shared';
import type {
    ConnectorConfig,
    ConnectorInterface,
    ConnectorUtilities,
    DropObjectOptions,
    LoadObjectRecordsOptions,
    LoadObjectRecordsResult,
    OpenStoreResult,
    QueryRecordsOptions,
    QueryRecordsResult,
    StoreRecord,
    ToolConfig
} from '@dpuse/dpuse-shared';

// ── Local Framework
import {
    constructArrowTable,
    constructCreateTableSql,
    constructStagedInsertSql,
    constructStoredColumns,
    constructStoreQuery,
    fromStoredValue,
    OBJECTS_TABLE,
    quoteIdentifier,
    STAGING_TABLE,
    type StoredColumn
} from '@/duckdbSql';

// ── Data
import config from '~/config.json';

// ── Types ────────────────────────────────────────────────────────────────────────────────────────────────────────────

interface OpenStore {
    connection: AsyncDuckDBConnection;
    database: AsyncDuckDB;
}

// ── Constants ────────────────────────────────────────────────────────────────────────────────────────────────────────

const DATABASE_PATH = 'opfs://dpuse-connector-duckdb-wasm.duckdb';
const LOCK_NAME = 'dpuse-connector-duckdb-wasm'; // Held by the one tab that has the store open.
const TRUSTED_TYPES_POLICY_NAME = 'dpuse-connector-duckdb-wasm'; // Must be listed in the app's CSP 'trusted-types' directive.
const WASM_FILE_NAME = 'duckdb-base.wasm';
const WORKER_FILE_NAME = 'duckdb-browser-base.worker.js';

// Approves only the blob URL that starts DuckDB's worker, where the page enforces Trusted Types.
const workerScriptURLPolicy = createWorkerScriptURLPolicy();

// ── Connector ────────────────────────────────────────────────────────────────────────────────────────────────────────

// Keeps data views in one DuckDB database saved in the browser's private file system (OPFS). Only one tab can open the
// database, so opening takes a Web Lock first and closing releases it. DuckDB runs in a worker of its own, which this
// connector starts from the files served beside its script.
export class Connector implements ConnectorInterface {
    abortController: AbortController | undefined;
    readonly config: ConnectorConfig;
    connectorUtilities: ConnectorUtilities;
    openStoreState: OpenStore | undefined;
    releaseLock: (() => void) | undefined;
    readonly toolConfigs;

    constructor(connectorUtilities: ConnectorUtilities, toolConfigs: ToolConfig[]) {
        this.abortController = undefined;
        this.config = config as ConnectorConfig;
        this.connectorUtilities = connectorUtilities;
        this.openStoreState = undefined;
        this.releaseLock = undefined;
        this.toolConfigs = toolConfigs;
    }

    // ── Operations ───────────────────────────────────────────────────────────────────────────────────────────────────

    abortOperation(): void {
        if (!this.abortController) return;
        this.abortController.abort();
        this.abortController = undefined;
    }

    // Ending DuckDB's worker is what closes its files, so another tab can open the store.
    async closeStore(): Promise<void> {
        const openStore = this.openStoreState;
        this.openStoreState = undefined;
        try {
            if (openStore) {
                await openStore.connection.close();
                await openStore.database.terminate();
                await removeScratchFolders();
            }
        } catch (error) {
            throw normalizeToError(error);
        } finally {
            this.releaseHeldLock();
        }
    }

    async dropObject(options: DropObjectOptions): Promise<void> {
        const { connection } = this.establishOpenStore();
        try {
            await connection.query('BEGIN TRANSACTION');
            await connection.query(`DROP TABLE IF EXISTS ${quoteIdentifier(options.path)}`);
            await runStatement(connection, `DELETE FROM ${OBJECTS_TABLE} WHERE path = ?`, [options.path]);
            await connection.query('COMMIT');
        } catch (error) {
            await connection.query('ROLLBACK');
            throw normalizeToError(error);
        }
    }

    // Replaces an object in one transaction, so a failed or stopped load leaves the earlier one in place.
    async loadObjectRecords(options: LoadObjectRecordsOptions, chunks: AsyncIterable<StoreRecord[]>): Promise<LoadObjectRecordsResult> {
        const { connection } = this.establishOpenStore();
        const storedColumns = constructStoredColumns(options.columnConfigs);
        this.abortController = new AbortController();
        const signal = this.abortController.signal;
        let recordCount = 0;
        try {
            await connection.query('BEGIN TRANSACTION');
            await connection.query(`DROP TABLE IF EXISTS ${quoteIdentifier(options.path)}`);
            await connection.query(constructCreateTableSql(options.path, storedColumns));
            await runStatement(connection, `INSERT OR REPLACE INTO ${OBJECTS_TABLE} (path, columns) VALUES (?, ?)`, [options.path, JSON.stringify(storedColumns)]);
            const stagedInsertSql = constructStagedInsertSql(options.path, storedColumns);
            for await (const records of chunks) {
                signal.throwIfAborted();
                await connection.insertArrowTable(constructArrowTable(records, storedColumns), { create: true, name: STAGING_TABLE });
                await connection.query(stagedInsertSql);
                await connection.query(`DROP TABLE ${STAGING_TABLE}`);
                recordCount += records.length;
            }
            await connection.query('COMMIT');
            await connection.query('CHECKPOINT'); // Writes the load into the database file now, not at some later point.
            return { recordCount };
        } catch (error) {
            await connection.query('ROLLBACK');
            throw normalizeToError(error);
        } finally {
            this.abortController = undefined;
        }
    }

    async openStore(): Promise<OpenStoreResult> {
        if (this.openStoreState) return { storeStateId: 'ready' };
        if (!(await isPrivateFileSystemAvailable())) return { storeStateId: 'unavailable' };
        if (!(await this.acquireLock())) return { storeStateId: 'heldElsewhere' };
        try {
            await removeScratchFolders(); // Clears any left by a tab that closed while DuckDB was running.
            this.openStoreState = await startDuckDB();
            await this.openStoreState.connection.query(`CREATE TABLE IF NOT EXISTS ${OBJECTS_TABLE} (path VARCHAR PRIMARY KEY, columns VARCHAR NOT NULL)`);
            return { storeStateId: 'ready' };
        } catch (error) {
            await this.closeStore();
            throw normalizeToError(error);
        }
    }

    async queryRecords(options: QueryRecordsOptions): Promise<QueryRecordsResult> {
        const { connection } = this.establishOpenStore();
        try {
            const storedColumnsTable = await runStatement(connection, `SELECT columns FROM ${OBJECTS_TABLE} WHERE path = ?`, [options.path]);
            const storedColumnsJSON = storedColumnsTable.getChildAt(0)?.get(0) as string | null | undefined;
            if (storedColumnsJSON == null) throw new Error(`'${options.path}' has not been loaded.`);
            const storeQuery = constructStoreQuery(JSON.parse(storedColumnsJSON) as StoredColumn[], options);
            const pageTable = await runStatement(connection, storeQuery.pageSql, storeQuery.values);
            const countTable = await runStatement(connection, storeQuery.countSql, storeQuery.values);
            return { records: readRecords(pageTable, storeQuery.resultColumns), totalCount: Number(countTable.getChildAt(0)?.get(0) ?? 0) };
        } catch (error) {
            throw normalizeToError(error);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────

    // Asks for the lock without waiting, so a tab that cannot have it is told at once. A request that fails, e.g. where
    // Web Locks are blocked, ends first and so rejects. The lock is held until 'releaseHeldLock' is called.
    private async acquireLock(): Promise<boolean> {
        const lockAcquisition = Promise.withResolvers<boolean>();
        const lockRequest = navigator.locks.request(LOCK_NAME, { ifAvailable: true }, (lock) => {
            if (lock == null) {
                lockAcquisition.resolve(false);
                return;
            }
            lockAcquisition.resolve(true);
            return new Promise<void>((release) => {
                this.releaseLock = release;
            });
        });
        const endedRequest = (async (): Promise<boolean> => {
            await lockRequest;
            return false;
        })();
        return await Promise.race([lockAcquisition.promise, endedRequest]);
    }

    private establishOpenStore(): OpenStore {
        if (!this.openStoreState) throw new Error('The store is not open.');
        return this.openStoreState;
    }

    private releaseHeldLock(): void {
        this.releaseLock?.();
        this.releaseLock = undefined;
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────────

function createWorkerScriptURLPolicy(): Pick<TrustedTypePolicy, 'createScriptURL'> | undefined {
    try {
        return globalThis.trustedTypes?.createPolicy(TRUSTED_TYPES_POLICY_NAME, {
            createScriptURL: (url: string): string => {
                if (url.startsWith('blob:')) return url;
                throw new Error(`Blocked TrustedScriptURL: ${url}`);
            }
        });
    } catch {
        return undefined;
    }
}

// Some private windows give no private file system, and DuckDB fails to open without one.
async function isPrivateFileSystemAvailable(): Promise<boolean> {
    try {
        await navigator.storage.getDirectory();
        return true;
    } catch {
        return false;
    }
}

// DuckDB leaves a scratch folder of about 66 files in the private file system each time it starts, and does not remove
// it when it ends. Only the tab holding the lock runs DuckDB, so no scratch folder is in use before it starts. Just
// after DuckDB ends, its files can still be held for a moment, so a folder that cannot be removed yet is left for the
// next start to remove.
async function removeScratchFolders(): Promise<void> {
    try {
        const rootFolder = await navigator.storage.getDirectory();
        const duckdbFolder = await rootFolder.getDirectoryHandle('duckdb');
        await duckdbFolder.removeEntry('scratch', { recursive: true });
    } catch {
        // No folder to remove, or not removable yet.
    }
}

function readRecords(table: Table, resultColumns: (StoredColumn | undefined)[]): StoreRecord[] {
    return Array.from({ length: table.numRows }, (_unused, rowIndex) =>
        resultColumns.map((storedColumn, columnIndex) => fromStoredValue(table.getChildAt(columnIndex)?.get(rowIndex), storedColumn))
    );
}

async function runStatement(connection: AsyncDuckDBConnection, sql: string, values: unknown[]): Promise<Table> {
    const statement = await connection.prepare(sql);
    try {
        return await statement.query(...values);
    } finally {
        await statement.close();
    }
}

// A browser will not start a worker from another site's script, and the connector is served from the engine's site. So
// the worker starts from a local blob that loads DuckDB's worker script from beside this one. DuckDB's script only runs
// as a classic worker, and where the page enforces Trusted Types the blob must approve that one URL itself, because a
// policy cannot be shared with another worker.
async function startDuckDB(): Promise<OpenStore> {
    const workerScriptURL = JSON.stringify(new URL(WORKER_FILE_NAME, import.meta.url).href);
    const policyName = JSON.stringify(TRUSTED_TYPES_POLICY_NAME);
    const bootstrapScript = `const url = ${workerScriptURL};
const policy = self.trustedTypes?.createPolicy(${policyName}, { createScriptURL: (candidate) => { if (candidate !== url) throw new Error('Blocked TrustedScriptURL: ' + candidate); return candidate; } });
importScripts(policy ? policy.createScriptURL(url) : url);`;
    const blobURL = URL.createObjectURL(new Blob([bootstrapScript], { type: 'text/javascript' }));
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- This is the browser's 'Worker', not Node's experimental one.
    const worker = new Worker((workerScriptURLPolicy?.createScriptURL(blobURL) ?? blobURL) as string);
    const database = new AsyncDuckDB(new VoidLogger(), worker);
    // DuckDB waits for ever when its worker fails to start, so a worker error ends the start instead.
    const workerFailure = Promise.withResolvers<never>();
    worker.addEventListener('error', (event) => {
        workerFailure.reject(new Error(`DuckDB's worker failed to start: ${event.message || 'no reason given'}.`));
    });
    try {
        await Promise.race([database.instantiate(new URL(WASM_FILE_NAME, import.meta.url).href), workerFailure.promise]);
        await database.open({ accessMode: DuckDBAccessMode.READ_WRITE, path: DATABASE_PATH });
        return { connection: await database.connect(), database };
    } catch (error) {
        await database.terminate(); // Ends the worker, so a failed start leaves no files open.
        throw error;
    } finally {
        URL.revokeObjectURL(blobURL);
    }
}
