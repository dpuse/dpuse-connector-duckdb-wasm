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
import { acquireWebLock, convertStoreRecordToParsedRecord, normalizeToError, resolveRequestSignal } from '@dpuse/dpuse-shared';
import type {
    AuditObjectContentOptions,
    AuditObjectContentResult,
    ColumnAuditConfig,
    ConnectionNodeConfig,
    ConnectorInterface,
    ConnectorUtilities,
    ContentAuditConfig,
    DeclaredColumnConfig,
    DropObjectOptions,
    FindObjectOptions,
    FindObjectResult,
    ListNodesOptions,
    ListNodesResult,
    LoadObjectRecordsOptions,
    LoadObjectRecordsResult,
    ObjectColumnConfig,
    OpenStoreResult,
    PreviewObjectOptions,
    PreviewObjectResult,
    QueryRecordsOptions,
    QueryRecordsResult,
    ReadingConfig,
    RetrieveRecordsChunk,
    RetrieveRecordsOptions,
    RetrieveRecordsSummary,
    StoreRecord,
    ToolConfig
} from '@dpuse/dpuse-shared';

// ── Local Framework
import {
    constructArrowTable,
    constructCreateTableSql,
    constructRecordChunkSql,
    constructStagedInsertSql,
    constructStoredColumns,
    constructStoreQuery,
    constructTableReference,
    DATABASE_FILES_SQL,
    DEFAULT_DATABASE_NAME,
    fromStoredValue,
    identifyColumnType,
    quoteIdentifier,
    resolveStoreTable,
    STAGING_TABLE,
    type StoredColumn,
    TABLE_COLUMNS_SQL,
    TABLE_COUNT_SQL,
    TABLE_NAMES_SQL
} from '@/duckdbSql';

// ── Types ────────────────────────────────────────────────────────────────────────────────────────────────────────────

interface OpenStore {
    attachedDatabases: Map<string, Promise<void>>; // The databases named in paths, attached or being attached since the store opened.
    connection: AsyncDuckDBConnection;
    database: AsyncDuckDB;
}

// A table a source action reads, with its columns as they were created.
interface SourceTable {
    columnConfigs: ObjectColumnConfig[];
    path: string;
}

// ── Constants ────────────────────────────────────────────────────────────────────────────────────────────────────────

const DATABASE_FILE_PATTERN = /^opfs:\/\/([\w-]+)\.duckdb$/u; // A database's file, as DuckDB lists it.
const DATABASE_PATH = `opfs://${DEFAULT_DATABASE_NAME}.duckdb`;
const DEFAULT_CHUNK_SIZE = 10_000; // Records read at a time by an audit or a retrieve.
const LOCK_NAME = 'dpuse-connector-duckdb-wasm'; // Held by the one tab that has the store open.
const PREVIEW_RECORD_COUNT = 50;
const START_ATTEMPT_COUNT = 10; // Starts tried while an ended worker still holds the database's files.
const START_RETRY_DELAY_MS = 200;
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
    pendingOpenStore: Promise<OpenStoreResult> | undefined;
    releaseLock: (() => void) | undefined;

    constructor(connectorUtilities: ConnectorUtilities, toolConfigs: ToolConfig[]) {
        this.connectorUtilities = connectorUtilities;
        this.openStoreState = undefined;
        this.pendingOpenStore = undefined;
        this.releaseLock = undefined;
        this.toolConfigs = toolConfigs;
    }

    // ── Operations ───────────────────────────────────────────────────────────────────────────────────────────────────

    // A table's columns have the types they were created with, so the audit counts values against them rather than
    // working types out. A table has no bytes, so 'byteLimit' does not apply, and the audit reads every record unless
    // 'stopSignal' fires.
    async auditObjectContent(options: AuditObjectContentOptions, chunk: (recordCount: number, byteCount: number) => void): Promise<AuditObjectContentResult> {
        const signal = resolveRequestSignal(options);
        try {
            const asAt = Date.now();
            const startedAt = performance.now();
            const openStore = await this.openStoreForReading();
            const sourceTable = await establishSourceTable(openStore, options.path);
            const columnAuditor = this.connectorUtilities.constructColumnAuditor(false, constructDeclaredColumnConfigs(sourceTable.columnConfigs));
            let recordCount = 0;
            let isComplete = true;
            const recordChunks = readRecordChunks(openStore, sourceTable, options.chunkSize ?? DEFAULT_CHUNK_SIZE);
            for await (const records of recordChunks) {
                signal.throwIfAborted();
                if (options.stopSignal?.aborted === true) {
                    isComplete = false;
                    break;
                }
                columnAuditor.auditRecords(records.map((record) => convertStoreRecordToParsedRecord(record)));
                recordCount += records.length;
                chunk(records.length, 0);
            }
            const { columnConfigs } = columnAuditor.completeAudit();
            return { contentAuditConfig: constructContentAuditConfig(asAt, startedAt, columnConfigs, recordCount, isComplete), hasHeaders: false }; // A table's columns are named by the database, not by a header record.
        } catch (error) {
            throw normalizeToError(error);
        }
    }

    // Ending DuckDB's worker is what closes its files, so another tab can open the store. The worker is ended even when
    // its connection fails to close, such as during a load. Otherwise it would keep its files open after the lock is
    // released, and the next open would fail.
    async closeStore(): Promise<void> {
        const openStore = this.openStoreState;
        this.openStoreState = undefined;
        try {
            if (openStore) {
                try {
                    await openStore.connection.close();
                } finally {
                    await openStore.database.terminate();
                }
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

    // An object's id is its path, so finding it only checks that it is there.
    async findObject(options: FindObjectOptions): Promise<FindObjectResult> {
        try {
            const openStore = await this.openStoreForReading();
            const sourceTable = await findSourceTable(openStore, options.id);
            return { path: sourceTable == null ? undefined : options.id };
        } catch (error) {
            throw normalizeToError(error);
        }
    }

    // Each database is a folder at the top, holding its tables. A database is found from its file, so one not attached
    // since the store opened is listed too.
    async listNodes(options: ListNodesOptions): Promise<ListNodesResult> {
        try {
            const openStore = await this.openStoreForReading();
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

    // Requests that arrive while DuckDB is starting share the one start. A second start would find the lock taken by the
    // first and report the store as open in another tab.
    async openStore(): Promise<OpenStoreResult> {
        if (this.openStoreState) return { storeStateId: 'ready' };
        this.pendingOpenStore ??= this.startStore();
        try {
            return await this.pendingOpenStore;
        } finally {
            this.pendingOpenStore = undefined; // A failed start is forgotten, so the next request tries again.
        }
    }

    // A table's columns and their types come from the table itself, so nothing is detected and the user has nothing to
    // choose. The sample is shown as text, as a file's is.
    async previewObject(options: PreviewObjectOptions): Promise<PreviewObjectResult> {
        try {
            const asAt = Date.now();
            const startedAt = performance.now();
            const openStore = await this.openStoreForReading();
            const sourceTable = await establishSourceTable(openStore, options.path);
            const { records } = await readRecordChunk(openStore, sourceTable, PREVIEW_RECORD_COUNT, undefined);
            const countTable = await runStatement(openStore.connection, `SELECT count(*) FROM ${constructTableReference(options.path)}`, []);
            const totalCount = Number(countTable.getChildAt(0)?.get(0) ?? 0);
            const parsedRecords = records.map((record) => convertStoreRecordToParsedRecord(record));
            const { columnConfigs } = this.connectorUtilities.inferDataTypes(parsedRecords, false, constructDeclaredColumnConfigs(sourceTable.columnConfigs));
            return {
                contentAuditConfig: constructContentAuditConfig(asAt, startedAt, columnConfigs, records.length, records.length === totalCount),
                readingConfig: constructReadingConfig(),
                sampleConfig: { hasHeaders: false, issues: [], parsedRecords, size: undefined, text: undefined }
            };
        } catch (error) {
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

    // Hands over each chunk typed, as the store holds it. Nothing more is read until the next chunk is asked for.
    async *retrieveRecords(options: RetrieveRecordsOptions): AsyncGenerator<RetrieveRecordsChunk, RetrieveRecordsSummary, undefined> {
        const signal = resolveRequestSignal(options);
        try {
            const openStore = await this.openStoreForReading();
            const sourceTable = await establishSourceTable(openStore, options.path);
            let recordCount = 0;
            const recordChunks = readRecordChunks(openStore, sourceTable, options.chunkSize ?? DEFAULT_CHUNK_SIZE);
            for await (const records of recordChunks) {
                signal.throwIfAborted();
                yield { records, typeId: 'storeRecordArray' };
                recordCount += records.length;
            }
            return { recordCount };
        } catch (error) {
            throw normalizeToError(error);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────

    private establishOpenStore(): OpenStore {
        if (!this.openStoreState) throw new Error('The store is not open.');
        return this.openStoreState;
    }

    // A connection's items can be listed and read before any data view has opened the store, so reading opens it first.
    // The store then stays open until it is closed, as it does for a data view.
    private async openStoreForReading(): Promise<OpenStore> {
        const { storeStateId } = await this.openStore();
        if (storeStateId === 'heldElsewhere') throw new Error('The store is open in another tab.');
        if (storeStateId === 'unavailable') throw new Error('This browser gives no storage to keep the store in, as in some private windows.');
        return this.establishOpenStore();
    }

    // Waits for the lock, so the store is only ever started by the tab that holds it.
    private async startStore(): Promise<OpenStoreResult> {
        if (!(await isPrivateFileSystemAvailable())) return { storeStateId: 'unavailable' };
        const releaseLock = await acquireWebLock(LOCK_NAME); // Held until the store closes.
        if (releaseLock == null) return { storeStateId: 'heldElsewhere' };
        this.releaseLock = releaseLock;
        try {
            await removeScratchFolders(); // Clears any left by a tab that closed while DuckDB was running.
            this.openStoreState = await startDuckDBWhenFilesAreFree();
            return { storeStateId: 'ready' };
        } catch (error) {
            await this.closeStore();
            throw normalizeToError(error);
        }
    }

    private releaseHeldLock(): void {
        this.releaseLock?.();
        this.releaseLock = undefined;
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────────

// The database name is undefined for the default database, which is open from the start.
async function attachDatabase(openStore: OpenStore, path: string): Promise<{ databaseName: string | undefined; tableName: string }> {
    const { databaseName, tableName } = resolveStoreTable(path);
    if (databaseName != null) await attachNamedDatabase(openStore, databaseName);
    return { databaseName, tableName };
}

// DuckDB only reads files in the private file system that it has been told of. So the database's file is registered
// before it is attached, with the write-ahead log files kept beside it, as DuckDB does for the database it opens. A
// failed attach is forgotten, so the next request tries again.
async function attachDatabaseFile(openStore: OpenStore, databaseName: string): Promise<void> {
    const databasePath = `opfs://${databaseName}.duckdb`; // 'resolveStorePath' and the folder path check allow only letters, digits, '_' and '-' in a name.
    try {
        for (const filePath of [databasePath, `${databasePath}.wal`, `${databasePath}.wal.checkpoint`, `${databasePath}.wal.recovery`]) {
            await openStore.database.registerOPFSFileName(filePath);
        }
        await openStore.connection.query(`ATTACH '${databasePath}' AS ${quoteIdentifier(databaseName)}`);
    } catch (error) {
        openStore.attachedDatabases.delete(databaseName);
        throw error;
    }
}

// Attaches a named database on first use. Requests that arrive together share one attach, because a second would find
// the database already attached and fail. The default database is open from the start.
async function attachNamedDatabase(openStore: OpenStore, databaseName: string): Promise<void> {
    if (databaseName === DEFAULT_DATABASE_NAME) return;
    let pendingAttach = openStore.attachedDatabases.get(databaseName);
    if (pendingAttach == null) {
        pendingAttach = attachDatabaseFile(openStore, databaseName);
        openStore.attachedDatabases.set(databaseName, pendingAttach);
    }
    await pendingAttach;
}

// A table's columns have no audited counts, only their types, so the preview and audit take the types rather than work
// them out. A column with no known type is left for them to work out.
function constructDeclaredColumnConfigs(columnConfigs: ObjectColumnConfig[]): DeclaredColumnConfig[] {
    return columnConfigs.map((columnConfig) => ({
        dataSubtypeId: columnConfig.dataSubtypeId,
        dataTypeId: columnConfig.dataTypeId === 'unknown' ? undefined : columnConfig.dataTypeId,
        label: undefined,
        name: columnConfig.label.en ?? ''
    }));
}

// A table has no bytes, lines or comments, so only its records are counted.
function constructContentAuditConfig(asAt: number, startedAt: number, columnConfigs: ColumnAuditConfig[], recordCount: number, isComplete: boolean): ContentAuditConfig {
    return {
        asAt,
        byteCount: 0,
        columns: columnConfigs,
        commentLineCount: undefined,
        duration: performance.now() - startedAt,
        emptyLineCount: undefined,
        invalidFieldLengthCount: undefined,
        isComplete,
        lineCount: undefined,
        recordCount,
        sourceWaitDuration: undefined
    };
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

// A table is not a file, so it has no data format, encoding or delimiter, and its columns are named by the database.
function constructReadingConfig(): ReadingConfig {
    return {
        dataFormatId: 'unknown',
        encodingConfidenceLevel: undefined,
        encodingId: undefined,
        fileType: undefined,
        hasHeaders: false,
        isDeclared: true,
        parserId: undefined,
        recordDelimiterId: undefined,
        valueDelimiterId: undefined
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

// Throws when there is no such table.
async function establishSourceTable(openStore: OpenStore, path: string): Promise<SourceTable> {
    const sourceTable = await findSourceTable(openStore, path);
    if (sourceTable == null) throw new Error(`'${path}' does not exist.`);
    return sourceTable;
}

// A source action reads a table that is already there, so a database with no file is never attached, because attaching
// it would create it. Undefined when there is no such table.
async function findSourceTable(openStore: OpenStore, path: string): Promise<SourceTable | undefined> {
    const { databaseName, tableName } = resolveStoreTable(path);
    if (databaseName != null) {
        const databaseNames = await listDatabaseNames(openStore);
        if (!databaseNames.includes(databaseName)) return undefined;
        await attachNamedDatabase(openStore, databaseName);
    }
    const columnTable = await runStatement(openStore.connection, TABLE_COLUMNS_SQL, [tableName, databaseName ?? null]);
    const columnConfigs = Array.from({ length: columnTable.numRows }, (_unused, rowIndex) => ({
        ...identifyColumnType(String(columnTable.getChildAt(1)?.get(rowIndex))),
        label: { en: String(columnTable.getChildAt(0)?.get(rowIndex)) }
    }));
    return columnConfigs.length === 0 ? undefined : { columnConfigs, path };
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
    const databaseNames = await listDatabaseNames(openStore);
    if (!databaseNames.includes(databaseName)) throw new Error(`Database '${databaseName}' does not exist.`);
    await attachNamedDatabase(openStore, databaseName);
    const tableNames = await listTableNames(openStore, databaseName);
    return tableNames.map((tableName) => constructObjectNodeConfig(folderPath, tableName, `${folderPath}/${tableName}`));
}

async function listDatabaseNames(openStore: OpenStore): Promise<string[]> {
    const fileTable = await runStatement(openStore.connection, DATABASE_FILES_SQL, []);
    return Array.from({ length: fileTable.numRows }, (_unused, rowIndex) => DATABASE_FILE_PATTERN.exec(String(fileTable.getChildAt(0)?.get(rowIndex)))?.[1])
        .filter((databaseName): databaseName is string => databaseName != null)
        .toSorted((left, right) => left.localeCompare(right));
}

async function listTableNames(openStore: OpenStore, databaseName: string): Promise<string[]> {
    const table = await runStatement(openStore.connection, TABLE_NAMES_SQL, [databaseName === DEFAULT_DATABASE_NAME ? null : databaseName]); // Null names the default database, whatever DuckDB calls it.
    return Array.from({ length: table.numRows }, (_unused, rowIndex) => String(table.getChildAt(0)?.get(rowIndex)));
}

// Databases are attached one at a time, because they share the store's one connection.
async function listTopNodes(openStore: OpenStore): Promise<ConnectionNodeConfig[]> {
    const databaseNames = await listDatabaseNames(openStore);
    const folderNodeConfigs: ConnectionNodeConfig[] = [];
    for (const databaseName of databaseNames) {
        await attachNamedDatabase(openStore, databaseName);
        const tableNames = await listTableNames(openStore, databaseName);
        folderNodeConfigs.push(constructFolderNodeConfig(databaseName, tableNames.length));
    }
    return folderNodeConfigs;
}

// Each record comes after its rowid, which marks where the next chunk starts.
async function readRecordChunk(
    openStore: OpenStore,
    sourceTable: SourceTable,
    chunkSize: number,
    afterRowId: number | undefined
): Promise<{ lastRowId: number | undefined; records: StoreRecord[] }> {
    const storedColumns = constructStoredColumns(sourceTable.columnConfigs);
    const table = await runStatement(openStore.connection, constructRecordChunkSql(sourceTable.path, chunkSize, afterRowId != null), afterRowId == null ? [] : [afterRowId]);
    const records = Array.from({ length: table.numRows }, (_unused, rowIndex) =>
        storedColumns.map((storedColumn, columnIndex) => fromStoredValue(table.getChildAt(columnIndex + 1)?.get(rowIndex), storedColumn))
    );
    const lastRowId: unknown = table.numRows > 0 ? table.getChildAt(0)?.get(table.numRows - 1) : undefined;
    return { lastRowId: lastRowId == null ? undefined : Number(lastRowId), records };
}

// Every record of a table, a chunk at a time, ending with the first chunk that is not full.
async function* readRecordChunks(openStore: OpenStore, sourceTable: SourceTable, chunkSize: number): AsyncGenerator<StoreRecord[]> {
    let afterRowId: number | undefined;
    for (;;) {
        const { lastRowId, records } = await readRecordChunk(openStore, sourceTable, chunkSize, afterRowId);
        if (records.length > 0) yield records;
        if (lastRowId == null || records.length < chunkSize) return;
        afterRowId = lastRowId;
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

// A worker that has just ended can still hold the database's files for a moment, so DuckDB cannot open them and the
// browser reports 'NoModificationAllowedError'. That happens when the store opens again just after it closed, so the
// start is tried again after a short wait.
async function startDuckDBWhenFilesAreFree(): Promise<OpenStore> {
    for (let attemptNumber = 1; ; attemptNumber++) {
        try {
            return await startDuckDB();
        } catch (error) {
            const isFileHeld = error instanceof Error && (error.name === 'NoModificationAllowedError' || error.message.includes('Access Handles cannot be created'));
            if (!isFileHeld || attemptNumber >= START_ATTEMPT_COUNT) throw error;
            await new Promise((resolve) => {
                setTimeout(resolve, START_RETRY_DELAY_MS);
            });
        }
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
        return { attachedDatabases: new Map(), connection: await database.connect(), database };
    } catch (error) {
        await database.terminate(); // Ends the worker, so a failed start leaves no files open.
        throw error;
    } finally {
        URL.revokeObjectURL(blobURL);
    }
}
