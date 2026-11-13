// ── External Dependencies & Registrations
// TODO: Review both pinned versions in 'package.json' regularly. '.ncurc.json' rejects them, so the usual dependency
// update never raises them. '@duckdb/duckdb-wasm' is pinned forward to 1.33.1-dev110.0, npm's 'next', because 'latest'
// (1.33.1-dev57.0) was found not to write to the browser's private file system. Move to 'latest' once it includes that.
// 'apache-arrow' is pinned to 17.0.0 because DuckDB requires ^17.0.0, so it can only move when DuckDB moves.
// Each review looks for security fixes in both packages and for a supported production release of DuckDB, then moves the
// pins and runs 'tests/duckdbSql.roundTrip.test.ts'.
import type { Table } from 'apache-arrow';
import { AsyncDuckDB, type AsyncDuckDBConnection, DuckDBAccessMode, VoidLogger } from '@duckdb/duckdb-wasm';

// ── DPUse Framework
import { acquireWebLock, normalizeToError, resolveRequestSignal, resolveStorePath } from '@dpuse/dpuse-shared';
import type {
    ConnectionNodeConfig,
    ConnectorInterface,
    ConnectorUtilities,
    DropObjectOptions,
    ListNodesOptions,
    ListNodesResult,
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
    constructTableReference,
    fromStoredValue,
    quoteIdentifier,
    STAGING_TABLE,
    type StoredColumn,
    TABLE_COUNT_SQL,
    TABLE_NAMES_SQL
} from '@/duckdbSql';

// ── Types ────────────────────────────────────────────────────────────────────────────────────────────────────────────

interface OpenStore {
    attachedDatabaseNames: Set<string>; // The databases named in paths, attached since the store opened.
    connection: AsyncDuckDBConnection;
    database: AsyncDuckDB;
}

// ── Constants ────────────────────────────────────────────────────────────────────────────────────────────────────────

const DATABASE_FILE_PATTERN = /^([\w-]+)\.duckdb$/u; // A database's file in the private file system. Its write-ahead log, '….duckdb.wal', does not match.
const DEFAULT_DATABASE_NAME = 'default'; // Used by a path that names no database, so 'orders' and '/default/orders' are the same table.
const DATABASE_PATH = `opfs://${DEFAULT_DATABASE_NAME}.duckdb`; // DuckDB names the database it opens after its file, so the default database is 'default'.
const LOCK_NAME = 'dpuse-connector-duckdb-wasm'; // Held by the one tab that has the store open.
const TRUSTED_TYPES_POLICY_NAME = 'dpuse-connector-duckdb-wasm'; // Must be listed in the app's CSP 'trusted-types' directive.
const WASM_FILE_NAME = 'duckdb-base.wasm';
const WORKER_FILE_NAME = 'duckdb-browser-base.worker.js';

// Approves only the blob URL that starts DuckDB's worker, where the page enforces Trusted Types.
const workerScriptURLPolicy = createWorkerScriptURLPolicy();

// ── Connector ────────────────────────────────────────────────────────────────────────────────────────────────────────

// Keeps data views in DuckDB databases saved in the browser's private file system (OPFS). A path '/database/table' names
// its database, and each database is a file of its own, attached on first use. Only one tab can open the store, so
// opening takes a Web Lock first and closing releases it, for every database at once. DuckDB runs in a worker of its own, which this
// connector starts from the files served beside its script.
export class Connector implements ConnectorInterface {
    private readonly connectorUtilities: ConnectorUtilities;
    private readonly toolConfigs;
    openStoreState: OpenStore | undefined;
    releaseLock: (() => void) | undefined;

    constructor(connectorUtilities: ConnectorUtilities, toolConfigs: ToolConfig[]) {
        this.connectorUtilities = connectorUtilities;
        this.openStoreState = undefined;
        this.releaseLock = undefined;
        this.toolConfigs = toolConfigs;
    }

    // ── Operations ───────────────────────────────────────────────────────────────────────────────────────────────────

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
        const openStore = this.establishOpenStore();
        const { connection } = openStore;
        try {
            await attachDatabase(openStore, options.path);
            await connection.query(`DROP TABLE IF EXISTS ${constructTableReference(options.path)}`);
        } catch (error) {
            throw normalizeToError(error);
        }
    }

    // Each database is a folder at the top, holding its tables. A database is found from its file in the private file
    // system, so one not attached since the store opened is listed too.
    async listNodes(options: ListNodesOptions): Promise<ListNodesResult> {
        try {
            await this.openStoreForListing();
            const openStore = this.establishOpenStore();
            const connectionNodeConfigs = options.folderPath === '' ? await listTopNodes(openStore) : await listDatabaseNodes(openStore, options.folderPath);
            return { cursor: undefined, connectionNodeConfigs, isMore: false, totalCount: connectionNodeConfigs.length };
        } catch (error) {
            throw normalizeToError(error);
        }
    }

    // Replaces an object in one transaction, so a failed or stopped load leaves the earlier one in place. The columns'
    // types are not kept, because every query brings them.
    async loadObjectRecords(options: LoadObjectRecordsOptions, chunks: AsyncIterable<StoreRecord[]>): Promise<LoadObjectRecordsResult> {
        const openStore = this.establishOpenStore();
        const { connection } = openStore;
        const { databaseName } = await attachDatabase(openStore, options.path);
        const storedColumns = constructStoredColumns(options.columnConfigs);
        const signal = resolveRequestSignal(options);
        let recordCount = 0;
        try {
            await connection.query('BEGIN TRANSACTION');
            await connection.query(`DROP TABLE IF EXISTS ${constructTableReference(options.path)}`);
            await connection.query(constructCreateTableSql(options.path, storedColumns));
            const stagedInsertSql = constructStagedInsertSql(options.path, storedColumns);
            for await (const records of chunks) {
                signal.throwIfAborted();
                await connection.insertArrowTable(constructArrowTable(records, storedColumns), { create: true, name: STAGING_TABLE });
                await connection.query(stagedInsertSql);
                await connection.query(`DROP TABLE ${STAGING_TABLE}`);
                recordCount += records.length;
            }
            await connection.query('COMMIT');
            await connection.query(databaseName == null ? 'CHECKPOINT' : `CHECKPOINT ${quoteIdentifier(databaseName)}`); // Writes the load into the database file now, not at some later point.
            return { recordCount };
        } catch (error) {
            await connection.query('ROLLBACK');
            throw normalizeToError(error);
        }
    }

    async openStore(): Promise<OpenStoreResult> {
        if (this.openStoreState) return { storeStateId: 'ready' };
        if (!(await isPrivateFileSystemAvailable())) return { storeStateId: 'unavailable' };
        const releaseLock = await acquireWebLock(LOCK_NAME); // Held until the store closes.
        if (releaseLock == null) return { storeStateId: 'heldElsewhere' };
        this.releaseLock = releaseLock;
        try {
            await removeScratchFolders(); // Clears any left by a tab that closed while DuckDB was running.
            this.openStoreState = await startDuckDB();
            return { storeStateId: 'ready' };
        } catch (error) {
            await this.closeStore();
            throw normalizeToError(error);
        }
    }

    async queryRecords(options: QueryRecordsOptions): Promise<QueryRecordsResult> {
        const openStore = this.establishOpenStore();
        const { connection } = openStore;
        try {
            const { databaseName, tableName } = await attachDatabase(openStore, options.path);
            const tableCountTable = await runStatement(connection, TABLE_COUNT_SQL, [tableName, databaseName ?? null]);
            if (Number(tableCountTable.getChildAt(0)?.get(0) ?? 0) === 0) throw new Error(`'${options.path}' has not been loaded.`);
            const storeQuery = constructStoreQuery(constructStoredColumns(options.columnConfigs), options); // The query brings the columns the object was loaded with.
            const pageTable = await runStatement(connection, storeQuery.pageSql, storeQuery.values);
            const countTable = await runStatement(connection, storeQuery.countSql, storeQuery.values);
            return { records: readRecords(pageTable, storeQuery.resultColumns), totalCount: Number(countTable.getChildAt(0)?.get(0) ?? 0) };
        } catch (error) {
            throw normalizeToError(error);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────

    private establishOpenStore(): OpenStore {
        if (!this.openStoreState) throw new Error('The store is not open.');
        return this.openStoreState;
    }

    // Items can be listed before any data view has opened the store, so listing opens it first. The store then stays open
    // until it is closed, as it does for a data view.
    private async openStoreForListing(): Promise<void> {
        const { storeStateId } = await this.openStore();
        if (storeStateId === 'heldElsewhere') throw new Error('The store is open in another tab.');
        if (storeStateId === 'unavailable') throw new Error('This browser gives no storage to keep the store in, as in some private windows.');
    }

    private releaseHeldLock(): void {
        this.releaseLock?.();
        this.releaseLock = undefined;
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────────

async function attachDatabase(openStore: OpenStore, path: string): Promise<{ databaseName: string | undefined; tableName: string }> {
    const { databaseName, tableName } = resolveStorePath(path);
    if (databaseName != null) await attachNamedDatabase(openStore, databaseName);
    return { databaseName, tableName };
}

// Attaches a named database on first use. DuckDB only reads files in the private file system that it has been told of,
// so the file is registered before it is attached. The default database is open from the start.
async function attachNamedDatabase(openStore: OpenStore, databaseName: string): Promise<void> {
    if (databaseName === DEFAULT_DATABASE_NAME || openStore.attachedDatabaseNames.has(databaseName)) return;
    const databasePath = `opfs://${databaseName}.duckdb`; // 'resolveStorePath' and the folder path check allow only letters, digits, '_' and '-' in a name.
    await openStore.database.registerOPFSFileName(databasePath);
    await openStore.connection.query(`ATTACH '${databasePath}' AS ${quoteIdentifier(databaseName)}`);
    openStore.attachedDatabaseNames.add(databaseName);
}

// A database's folder is named and identified by the database's name, which a path to one of its tables starts with.
function constructFolderNodeConfig(databaseName: string, childCount: number): ConnectionNodeConfig {
    return {
        childCount,
        description: '',
        extension: undefined,
        folderPath: '',
        icon: null,
        iconDark: null,
        id: databaseName,
        label: databaseName,
        lastModifiedAt: undefined,
        mimeType: undefined,
        name: databaseName,
        size: undefined,
        typeId: 'folder'
    };
}

// A table is identified by its path, as the other actions take it.
function constructObjectNodeConfig(folderPath: string, tableName: string, path: string): ConnectionNodeConfig {
    return {
        childCount: undefined,
        description: '',
        extension: undefined,
        folderPath,
        icon: null,
        iconDark: null,
        id: path,
        label: tableName,
        lastModifiedAt: undefined,
        mimeType: undefined,
        name: tableName,
        size: undefined,
        typeId: 'object'
    };
}

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

// A folder path names one database, as '/database'. Attaching a database with no file would create one, so a database
// with no file is refused.
async function listDatabaseNodes(openStore: OpenStore, folderPath: string): Promise<ConnectionNodeConfig[]> {
    const databaseName = /^\/([\w-]+)$/u.exec(folderPath)?.[1];
    if (databaseName == null) throw new Error(`Encountered invalid folder path '${folderPath}'.`);
    const databaseNames = await listDatabaseNames();
    if (!databaseNames.includes(databaseName)) throw new Error(`Database '${databaseName}' does not exist.`);
    await attachNamedDatabase(openStore, databaseName);
    const tableNames = await listTableNames(openStore, databaseName);
    return tableNames.map((tableName) => constructObjectNodeConfig(folderPath, tableName, `${folderPath}/${tableName}`));
}

async function listDatabaseNames(): Promise<string[]> {
    const rootFolder = await navigator.storage.getDirectory();
    const databaseNames: string[] = [];
    for await (const [fileName, handle] of rootFolder.entries()) {
        const databaseName = handle.kind === 'file' ? DATABASE_FILE_PATTERN.exec(fileName)?.[1] : undefined;
        if (databaseName != null) databaseNames.push(databaseName);
    }
    return databaseNames.toSorted((left, right) => left.localeCompare(right));
}

async function listTableNames(openStore: OpenStore, databaseName: string): Promise<string[]> {
    const table = await runStatement(openStore.connection, TABLE_NAMES_SQL, [databaseName]);
    return Array.from({ length: table.numRows }, (_unused, rowIndex) => String(table.getChildAt(0)?.get(rowIndex)));
}

// Databases are attached one at a time, because they share the store's one connection.
async function listTopNodes(openStore: OpenStore): Promise<ConnectionNodeConfig[]> {
    const databaseNames = await listDatabaseNames();
    const folderNodeConfigs: ConnectionNodeConfig[] = [];
    for (const databaseName of databaseNames) {
        await attachNamedDatabase(openStore, databaseName);
        const tableNames = await listTableNames(openStore, databaseName);
        folderNodeConfigs.push(constructFolderNodeConfig(databaseName, tableNames.length));
    }
    return folderNodeConfigs;
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
        return { attachedDatabaseNames: new Set(), connection: await database.connect(), database };
    } catch (error) {
        await database.terminate(); // Ends the worker, so a failed start leaves no files open.
        throw error;
    } finally {
        URL.revokeObjectURL(blobURL);
    }
}
