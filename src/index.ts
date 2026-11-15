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
import { acquireWebLock, constructStoreError, convertStoreRecordToParsedRecord, findStoreErrorId } from '@dpuse/dpuse-shared';
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
    OpenStoreOptions,
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
import { ConnectorError, normalizeToError, resolveRequestSignal } from '@dpuse/dpuse-shared';

// ── Local Framework
import {
    constructArrowTable,
    constructCreateTableSql,
    constructLoadingTablePath,
    constructRecordChunkSql,
    constructStagedInsertSql,
    constructStoredColumns,
    constructStoreQuery,
    constructTableReference,
    DATABASE_FILES_SQL,
    DEFAULT_DATABASE_NAME,
    fromStoredValue,
    identifyColumnType,
    LOADING_TABLE_NAMES_SQL,
    quoteIdentifier,
    resolveBatchRecordCount,
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
const HELD_ELSEWHERE_MESSAGE = 'The store is open in another tab or window.';
const LOCK_NAME = 'dpuse-connector-duckdb-wasm'; // Held by the one tab that has the store open.
// DuckDB holds data in memory up to this limit and spills the rest to its temporary directory. Its default is far above
// what iOS allows a page, so a large load grew until iOS reloaded the page.
const MEMORY_LIMIT = '256MB';
const PREVIEW_RECORD_COUNT = 50;
const START_ATTEMPT_COUNT = 10; // Starts tried while an ended worker still holds the database's files.
const START_RETRY_DELAY_MS = 200;
// DuckDB spills what does not fit its memory limit to JavaScript memory by default, so a large load still grew until iOS
// reloaded the page. Spilling to the private file system keeps it out of memory.
const TEMPORARY_DIRECTORY = 'opfs://tmp';
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
    pendingCloseStore: Promise<void> | undefined;
    pendingOpenStore: Promise<OpenStoreResult> | undefined;
    releaseLock: (() => Promise<void>) | undefined;
    storeWasTakenOver: boolean; // Another tab took the store, so this tab says so until it opens the store again.

    constructor(connectorUtilities: ConnectorUtilities, toolConfigs: ToolConfig[]) {
        this.connectorUtilities = connectorUtilities;
        this.openStoreState = undefined;
        this.pendingCloseStore = undefined;
        this.pendingOpenStore = undefined;
        this.releaseLock = undefined;
        this.storeWasTakenOver = false;
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
            throw this.normalizeActionError(error);
        }
    }

    // A close takes a moment, because DuckDB's worker has to end. An open that arrives meanwhile, such as when a screen
    // closes the store and the next one opens it, waits for the close, so it does not find the lock still held.
    async closeStore(): Promise<void> {
        const pendingCloseStore = this.endStore();
        this.pendingCloseStore = pendingCloseStore;
        try {
            await pendingCloseStore;
        } finally {
            if (this.pendingCloseStore === pendingCloseStore) this.pendingCloseStore = undefined;
        }
    }

    async dropObject(options: DropObjectOptions): Promise<void> {
        const openStore = this.establishOpenStore();
        const { connection } = openStore;
        try {
            await attachDatabase(openStore, options.path);
            await connection.query(`DROP TABLE IF EXISTS ${constructTableReference(options.path)}`);
        } catch (error) {
            throw this.normalizeActionError(error);
        }
    }

    // An object's id is its path, so finding it only checks that it is there.
    async findObject(options: FindObjectOptions): Promise<FindObjectResult> {
        try {
            const openStore = await this.openStoreForReading();
            if ((await findSourceTable(openStore, options.id)) != null) return { path: options.id };
            throw new ConnectorError(`Object '${options.id}' not found.`, 'dpuse-connector-duckdb-wasm.Connector.findObject');
        } catch (error) {
            throw this.normalizeActionError(error);
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
            throw this.normalizeActionError(error);
        }
    }

    // Builds the object in a table of its own, committed a batch at a time. DuckDB spills the part of a transaction that
    // does not fit its memory limit, and in the browser it cannot reopen a spill file in a later query, so a load held in
    // one transaction stalled or failed. Once every record is in, one short transaction puts the new table in place of
    // the earlier one, so a failed or stopped load leaves the earlier one in place. The columns' types are not kept,
    // because every query brings them.
    async loadObjectRecords(options: LoadObjectRecordsOptions, chunks: AsyncIterable<StoreRecord[]>): Promise<LoadObjectRecordsResult> {
        const openStore = this.establishOpenStore();
        const { connection } = openStore;
        const { databaseName, tableName } = await attachDatabase(openStore, options.path);
        const storedColumns = constructStoredColumns(options.columnConfigs);
        const signal = resolveRequestSignal(options);
        const loadingTablePath = constructLoadingTablePath(options.path);
        const loadingTableReference = constructTableReference(loadingTablePath);
        const batchRecordCount = resolveBatchRecordCount(storedColumns.length);
        const startedAt = performance.now();
        let commitCount = 0;
        let recordCount = 0;
        try {
            await connection.query(`DROP TABLE IF EXISTS ${loadingTableReference}`); // Left by an earlier load of the object that never finished.
            await connection.query(constructCreateTableSql(loadingTablePath, storedColumns));
            const stagedInsertSql = constructStagedInsertSql(loadingTablePath, storedColumns);
            let uncommittedRecordCount = 0;
            await connection.query('BEGIN TRANSACTION');
            for await (const records of chunks) {
                signal.throwIfAborted();
                await connection.insertArrowTable(constructArrowTable(records, storedColumns), { create: true, name: STAGING_TABLE });
                await connection.query(stagedInsertSql);
                await connection.query(`DROP TABLE ${STAGING_TABLE}`);
                recordCount += records.length;
                uncommittedRecordCount += records.length;
                if (uncommittedRecordCount < batchRecordCount) continue;
                await connection.query('COMMIT');
                commitCount++;
                logLoadStep(`commit ${String(commitCount)} at ${String(recordCount)} records, ${describeElapsed(startedAt)}, ${await describeDuckDBMemory(openStore)}`);
                await connection.query('BEGIN TRANSACTION');
                uncommittedRecordCount = 0;
            }
            await connection.query(`DROP TABLE IF EXISTS ${constructTableReference(options.path)}`);
            await connection.query(`ALTER TABLE ${loadingTableReference} RENAME TO ${quoteIdentifier(tableName)}`);
            await connection.query('COMMIT');
            await connection.query(databaseName == null ? 'CHECKPOINT' : `CHECKPOINT ${quoteIdentifier(databaseName)}`); // Writes the load into the database file now, not at some later point.
            logLoadStep(`done: ${String(recordCount)} records in ${String(commitCount + 1)} commits of up to ${String(batchRecordCount)}, ${describeElapsed(startedAt)}, ${await describeDuckDBMemory(openStore)}`);
            return { recordCount };
        } catch (error) {
            if (!this.storeWasTakenOver) await discardLoadingTable(connection, loadingTableReference); // A store another tab took is already closed, which undid the batch in progress.
            throw this.normalizeActionError(error);
        }
    }

    // Requests that arrive while DuckDB is starting share the one start. A second start would find the lock taken by the
    // first and report the store as open in another tab.
    async openStore(options: OpenStoreOptions = {}): Promise<OpenStoreResult> {
        if (this.openStoreState) return { storeStateId: 'ready' };
        try {
            await this.pendingCloseStore;
        } catch {
            // A close that failed has still released the lock.
        }
        this.pendingOpenStore ??= this.startStore(options.takeOver === true);
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
            throw this.normalizeActionError(error);
        }
    }

    async queryRecords(options: QueryRecordsOptions): Promise<QueryRecordsResult> {
        const openStore = this.establishOpenStore();
        const { connection } = openStore;
        try {
            const { databaseName, tableName } = await attachDatabase(openStore, options.path);
            const tableCountTable = await runStatement(connection, TABLE_COUNT_SQL, [tableName, databaseName ?? null]);
            if (Number(tableCountTable.getChildAt(0)?.get(0) ?? 0) === 0) {
                throw constructStoreError('notLoaded', `'${options.path}' has not been loaded.`, 'dpuse-connector-duckdb-wasm.Connector.queryRecords');
            }
            const storeQuery = constructStoreQuery(constructStoredColumns(options.columnConfigs), options); // The query brings the columns the object was loaded with.
            const pageTable = await runStatement(connection, storeQuery.pageSql, storeQuery.values);
            const countTable = await runStatement(connection, storeQuery.countSql, storeQuery.values);
            return { records: readRecords(pageTable, storeQuery.resultColumns), totalCount: Number(countTable.getChildAt(0)?.get(0) ?? 0) };
        } catch (error) {
            throw this.normalizeActionError(error);
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
            throw this.normalizeActionError(error);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────

    // Ending DuckDB's worker is what closes its files, so another tab can open the store. The worker is ended even when
    // its connection fails to close, such as during a load. Otherwise it would keep its files open after the lock is
    // released, and the next open would fail.
    private async endStore(): Promise<void> {
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
            await this.releaseHeldLock();
        }
    }

    private establishOpenStore(): OpenStore {
        if (this.openStoreState) return this.openStoreState;
        if (this.storeWasTakenOver) throw constructStoreError('heldElsewhere', HELD_ELSEWHERE_MESSAGE, 'dpuse-connector-duckdb-wasm.Connector.establishOpenStore');
        throw new Error('The store is not open.');
    }

    // A connection's items can be listed and read before any data view has opened the store, so reading opens it first.
    // The store then stays open until it is closed, as it does for a data view.
    private async openStoreForReading(): Promise<OpenStore> {
        const { storeStateId } = await this.openStore();
        if (storeStateId === 'heldElsewhere') throw constructStoreError('heldElsewhere', HELD_ELSEWHERE_MESSAGE, 'dpuse-connector-duckdb-wasm.Connector.openStoreForReading');
        if (storeStateId === 'unavailable') throw new Error('This browser gives no storage to keep the store in, as in some private windows.');
        return this.establishOpenStore();
    }

    // Waits for the lock, so the store is only ever started by the tab that holds it.
    private async startStore(shouldTakeOver: boolean): Promise<OpenStoreResult> {
        if (!(await isPrivateFileSystemAvailable())) return { storeStateId: 'unavailable' };
        const onLost = (): void => {
            this.storeWasTakenOver = true;
            void this.closeStoreAfterTakeover();
        };
        const releaseLock = await acquireWebLock(LOCK_NAME, { onLost, takeOver: shouldTakeOver }); // Held until the store closes.
        if (releaseLock == null) return { storeStateId: 'heldElsewhere' };
        this.releaseLock = releaseLock;
        try {
            await removeScratchFolders(); // Clears any left by a tab that closed while DuckDB was running.
            this.openStoreState = await startDuckDBWhenFilesAreFree();
            this.storeWasTakenOver = false;
            return { storeStateId: 'ready' };
        } catch (error) {
            await this.closeStore();
            throw normalizeToError(error);
        }
    }

    // When another tab takes the lock over, this tab ends DuckDB, which finishes any write and frees the files for that
    // tab. Its start waits for them. Nothing waits on this close, so a failure is not thrown, and the next open reports
    // any problem with the files.
    private async closeStoreAfterTakeover(): Promise<void> {
        try {
            await this.closeStore();
        } catch {
            // Already closed as far as it can be.
        }
    }

    // A request still running when another tab took the store fails on the ended DuckDB. It is reported as the store being
    // open elsewhere, as every later request is.
    private normalizeActionError(error: unknown): Error {
        return this.storeWasTakenOver && findStoreErrorId(error) == null
            ? constructStoreError('heldElsewhere', HELD_ELSEWHERE_MESSAGE, 'dpuse-connector-duckdb-wasm.Connector.normalizeActionError', { cause: error })
            : normalizeToError(error);
    }

    // Resolves once the lock is free for the next open.
    private async releaseHeldLock(): Promise<void> {
        const releaseLock = this.releaseLock;
        this.releaseLock = undefined;
        await releaseLock?.();
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
// failed attach is forgotten, so the next request tries again. No load runs in a database before it is attached, so any
// table a load was building there is one that never finished.
async function attachDatabaseFile(openStore: OpenStore, databaseName: string): Promise<void> {
    const databasePath = `opfs://${databaseName}.duckdb`; // 'resolveStorePath' and the folder path check allow only letters, digits, '_' and '-' in a name.
    try {
        for (const filePath of [databasePath, `${databasePath}.wal`, `${databasePath}.wal.checkpoint`, `${databasePath}.wal.recovery`]) {
            await openStore.database.registerOPFSFileName(filePath);
        }
        await openStore.connection.query(`ATTACH '${databasePath}' AS ${quoteIdentifier(databaseName)}`);
        await dropUnfinishedLoadingTables(openStore.connection, databaseName);
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

// A load that never finished, such as when iOS reloaded the page, leaves the table it was building. The default database
// is named by undefined.
async function dropUnfinishedLoadingTables(connection: AsyncDuckDBConnection, databaseName: string | undefined): Promise<void> {
    const table = await runStatement(connection, LOADING_TABLE_NAMES_SQL, [databaseName ?? null]);
    for (let rowIndex = 0; rowIndex < table.numRows; rowIndex++) {
        const tableName = String(table.getChildAt(0)?.get(rowIndex));
        const tablePath = databaseName == null ? tableName : ['', databaseName, tableName].join('/');
        await connection.query(`DROP TABLE IF EXISTS ${constructTableReference(tablePath)}`);
    }
}

// A failed load can leave a batch in progress or none, and 'ROLLBACK' with none in progress fails, so that failure is
// ignored. A table that cannot be dropped is left for the next start or attach to drop, so the load's own failure is the
// one reported.
async function discardLoadingTable(connection: AsyncDuckDBConnection, loadingTableReference: string): Promise<void> {
    try {
        await connection.query('ROLLBACK');
    } catch {
        // No batch was in progress.
    }
    try {
        await connection.query(`DROP TABLE IF EXISTS ${loadingTableReference}`);
    } catch {
        // Dropped when its database is next opened.
    }
}

// Temporary load timing test. Logs each commit of a load to the console with the time taken so far and DuckDB's memory,
// to show where a load spends its time. Remove this, 'describeElapsed', 'describeDuckDBMemory' and their uses in
// 'loadObjectRecords' once tested.
function logLoadStep(step: string): void {
    console.info(`[duckdb load] ${step}`);
}

function describeElapsed(startedAt: number): string {
    return `${(Math.round((performance.now() - startedAt) / 100) / 10).toFixed(1)} s`;
}

// DuckDB's own count of the memory it holds, its limit, and what it has spilled to temporary storage.
async function describeDuckDBMemory(openStore: OpenStore): Promise<string> {
    const table = await runStatement(
        openStore.connection,
        "SELECT sum(memory_usage_bytes)::DOUBLE, sum(temporary_storage_bytes)::DOUBLE, current_setting('memory_limit') FROM duckdb_memory()",
        []
    );
    const toMegabytes = (value: unknown): string => `${String(Math.round(Number(value ?? 0) / 1_048_576))} MB`;
    return `DuckDB memory ${toMegabytes(table.getChildAt(0)?.get(0))} of ${String(table.getChildAt(2)?.get(0))}, temporary ${toMegabytes(table.getChildAt(1)?.get(0))}`;
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
// it when it ends. A load that ends without its commit, such as when iOS reloads the page, also leaves the data it spilled
// in the temporary directory, which DuckDB keeps at '/duckdb/fs/tmp' and never clears. Only the tab holding the lock runs
// DuckDB, so neither folder is in use before it starts. Just after DuckDB ends, its files can still be held for a moment,
// so a folder that cannot be removed yet is left for the next start to remove. Each folder is removed on its own, so one
// that is missing or held does not keep the other.
async function removeScratchFolders(): Promise<void> {
    try {
        const rootFolder = await navigator.storage.getDirectory();
        const duckdbFolder = await rootFolder.getDirectoryHandle('duckdb');
        await removeFolder(duckdbFolder, 'scratch');
        await removeFolder(await duckdbFolder.getDirectoryHandle('fs'), 'tmp'); // Where 'TEMPORARY_DIRECTORY' is kept.
    } catch {
        // No folder to remove.
    }
}

async function removeFolder(parentFolder: FileSystemDirectoryHandle, name: string): Promise<void> {
    try {
        await parentFolder.removeEntry(name, { recursive: true });
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
        await database.open({ accessMode: DuckDBAccessMode.READ_WRITE, path: DATABASE_PATH, temporaryDirectory: TEMPORARY_DIRECTORY });
        const connection = await database.connect();
        await connection.query(`SET memory_limit = '${MEMORY_LIMIT}'`);
        await dropUnfinishedLoadingTables(connection, undefined); // No load has started yet, so any table a load was building is one that never finished.
        return { attachedDatabases: new Map(), connection, database };
    } catch (error) {
        await database.terminate(); // Ends the worker, so a failed start leaves no files open.
        throw error;
    } finally {
        URL.revokeObjectURL(blobURL);
    }
}
