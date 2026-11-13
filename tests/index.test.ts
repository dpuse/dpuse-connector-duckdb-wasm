// ── External Dependencies & Registrations
import { afterEach, describe, expect, it, vi } from 'vitest';

// ── DPUse Framework
import type { ConnectorUtilities, StoreRecord } from '@dpuse/dpuse-shared';

// ── Local Framework
import { Connector } from '@/index';
import { DATABASE_FILES_SQL, TABLE_COLUMNS_SQL, TABLE_NAMES_SQL } from '@/duckdbSql';

// ── Types ────────────────────────────────────────────────────────────────────────────────────────────────────────────

// A table as the fake DuckDB holds it: its columns with their DuckDB types, and its rows as DuckDB returns them.
interface FakeTable {
    columns: [string, string][];
    rows: unknown[][];
}

type FakeDatabases = Record<string, Record<string, FakeTable>>;

type SqlValue = number | string | null; // The connector passes names as text, null for the default database, and a rowid as a number.

// ── Mocks ────────────────────────────────────────────────────────────────────────────────────────────────────────────

// DuckDB's browser build starts a worker, which tests cannot run, so a stand-in whose open can be made to fail takes its
// place.
const { openDuckDB } = vi.hoisted(() => ({ openDuckDB: vi.fn<() => Promise<void>>() }));
vi.mock('@duckdb/duckdb-wasm', () => ({
    AsyncDuckDB: class {
        connect = (): Promise<unknown> => Promise.resolve({ close: () => Promise.resolve() });
        instantiate = (): Promise<void> => Promise.resolve();
        open = openDuckDB;
        terminate = (): Promise<void> => Promise.resolve();
    },
    DuckDBAccessMode: { READ_WRITE: 2 },
    VoidLogger: vi.fn()
}));

// ── Constants ────────────────────────────────────────────────────────────────────────────────────────────────────────

const DAY = 86_400_000;
const PREVIEW_OPTIONS = { chunkSize: undefined, dataFormatId: undefined, encodingId: undefined, extension: undefined, mimeType: undefined, parserId: undefined, valueDelimiterId: undefined };
const READ_OPTIONS = { chunkSize: 2, dataFormatId: 'unknown' as const, encodingId: undefined, parserId: undefined, valueDelimiterId: undefined };

// ── Tests ────────────────────────────────────────────────────────────────────────────────────────────────────────────

describe('Connector', () => {
    afterEach(() => {
        vi.unstubAllGlobals();
        openDuckDB.mockReset();
    });

    it('refuses store actions until the store is open', async () => {
        const connector = new Connector({} as never, []);
        const queryOptions = { aggregates: [], columnConfigs: [], filters: [], groupColumnIndexes: [], limit: 1, offset: 0, path: 'sales', sorts: [] };

        await expect(connector.queryRecords(queryOptions)).rejects.toThrow('The store is not open.');
        await expect(connector.dropObject({ path: 'sales' })).rejects.toThrow('The store is not open.');
    });

    it('closes without error when the store was never opened', async () => {
        await expect(new Connector({} as never, []).closeStore()).resolves.toBeUndefined();
    });

    it("ends DuckDB's worker and releases the lock even when its connection fails to close", async () => {
        const connector = new Connector({} as never, []);
        const terminate = vi.fn(() => Promise.resolve());
        const releaseLock = vi.fn();
        connector.openStoreState = { attachedDatabases: new Map(), connection: { close: () => Promise.reject(new Error('A query is running.')) }, database: { terminate } } as never;
        connector.releaseLock = releaseLock;

        await expect(connector.closeStore()).rejects.toThrow('A query is running.');
        expect(terminate).toHaveBeenCalledOnce();
        expect(releaseLock).toHaveBeenCalledOnce();
    });

    it("starts DuckDB again while an ended worker still holds the database's file", async () => {
        stubBrowser();
        openDuckDB.mockRejectedValueOnce(new DOMException('Access Handles cannot be created if there is another open Access Handle.', 'NoModificationAllowedError'));
        openDuckDB.mockResolvedValue();

        await expect(new Connector({} as never, []).openStore()).resolves.toEqual({ storeStateId: 'ready' });
        expect(openDuckDB).toHaveBeenCalledTimes(2);
    });

    it('fails at once when DuckDB fails to start for any other reason', async () => {
        stubBrowser();
        openDuckDB.mockRejectedValue(new Error('Out of memory.'));

        await expect(new Connector({} as never, []).openStore()).rejects.toThrow('Out of memory.');
        expect(openDuckDB).toHaveBeenCalledOnce();
    });

    it('waits for a close still in progress before opening, so the lock is free', async () => {
        const connector = new Connector({} as never, []);
        const termination = Promise.withResolvers<undefined>();
        connector.openStoreState = { attachedDatabases: new Map(), connection: { close: () => Promise.resolve() }, database: { terminate: () => termination.promise } } as never;
        const startStore = vi.fn(() => Promise.resolve({ storeStateId: 'ready' as const }));
        Object.assign(connector, { startStore });

        const closing = connector.closeStore();
        const opening = connector.openStore();
        await Promise.resolve();
        expect(startStore).not.toHaveBeenCalled();
        termination.resolve(undefined);

        await expect(opening).resolves.toEqual({ storeStateId: 'ready' });
        await closing;
        expect(startStore).toHaveBeenCalledOnce();
    });

    it('starts DuckDB once for requests that arrive together', async () => {
        const connector = new Connector({} as never, []);
        const startStore = vi.fn(() => Promise.resolve({ storeStateId: 'ready' as const }));
        Object.assign(connector, { startStore });

        await Promise.all([connector.openStore(), connector.openStore()]);

        expect(startStore).toHaveBeenCalledOnce();
    });

    describe('listNodes', () => {
        it('lists each database, the default one included, as a folder at the top', async () => {
            const { connector } = constructFakeConnector({ default: { orders: numberTable([]) }, empty: {}, sales: { invoices: numberTable([]), returns: numberTable([]) } });

            const result = await connector.listNodes({ folderPath: '' });

            expect(result).toEqual({ cursor: undefined, connectionNodeConfigs: expect.any(Array) as unknown, isMore: false, totalCount: 3 });
            expect(result.connectionNodeConfigs.map(({ childCount, folderPath, id, name, typeId }) => ({ childCount, folderPath, id, name, typeId }))).toEqual([
                { childCount: 1, folderPath: '', id: 'default', name: 'default', typeId: 'folder' },
                { childCount: 0, folderPath: '', id: 'empty', name: 'empty', typeId: 'folder' },
                { childCount: 2, folderPath: '', id: 'sales', name: 'sales', typeId: 'folder' }
            ]);
        });

        it("lists the default database's tables without attaching it, because it is open from the start", async () => {
            const { connector, registerOPFSFileName } = constructFakeConnector({ default: { orders: numberTable([]) } });

            const result = await connector.listNodes({ folderPath: '/default' });

            expect(result.connectionNodeConfigs).toEqual([expect.objectContaining({ folderPath: '/default', id: '/default/orders', name: 'orders', typeId: 'object' })]);
            expect(registerOPFSFileName).not.toHaveBeenCalled();
        });

        it("lists a database's tables, each identified by its path, attaching the database and its log files once", async () => {
            const { connector, registerOPFSFileName } = constructFakeConnector({ default: {}, sales: { invoices: numberTable([]), returns: numberTable([]) } });

            const [result] = await Promise.all([connector.listNodes({ folderPath: '/sales' }), connector.listNodes({ folderPath: '/sales' })]);

            expect(result.connectionNodeConfigs).toEqual([
                expect.objectContaining({ extension: undefined, folderPath: '/sales', id: '/sales/invoices', label: 'invoices', name: 'invoices', typeId: 'object' }),
                expect.objectContaining({ extension: undefined, folderPath: '/sales', id: '/sales/returns', label: 'returns', name: 'returns', typeId: 'object' })
            ]);
            expect(result.totalCount).toBe(2);
            expect(registerOPFSFileName.mock.calls).toEqual([
                ['opfs://sales.duckdb'],
                ['opfs://sales.duckdb.wal'],
                ['opfs://sales.duckdb.wal.checkpoint'],
                ['opfs://sales.duckdb.wal.recovery']
            ]);
        });

        it('lists only the empty default database in a new store', async () => {
            const { connector } = constructFakeConnector({ default: {} });

            const result = await connector.listNodes({ folderPath: '' });

            expect(result.connectionNodeConfigs).toEqual([expect.objectContaining({ childCount: 0, id: 'default', typeId: 'folder' })]);
            expect(result.totalCount).toBe(1);
        });

        it('refuses a database with no file, without creating one', async () => {
            const { connector, registerOPFSFileName } = constructFakeConnector({ default: {} });

            await expect(connector.listNodes({ folderPath: '/missing' })).rejects.toThrow("Database 'missing' does not exist.");
            expect(registerOPFSFileName).not.toHaveBeenCalled();
        });

        it('refuses a folder path that does not name one database', async () => {
            const { connector } = constructFakeConnector({ default: {} });

            await expect(connector.listNodes({ folderPath: '/sales/returns' })).rejects.toThrow("Encountered invalid folder path '/sales/returns'.");
        });

        it('opens a closed store before listing it', async () => {
            const { connector } = constructFakeConnector({ default: { orders: numberTable([]) } });
            const { openStoreState } = connector;
            connector.openStoreState = undefined;
            const openStore = vi.spyOn(connector, 'openStore').mockImplementation(() => {
                connector.openStoreState = openStoreState;
                return Promise.resolve({ storeStateId: 'ready' });
            });

            const result = await connector.listNodes({ folderPath: '/default' });

            expect(openStore).toHaveBeenCalledOnce();
            expect(result.connectionNodeConfigs).toEqual([expect.objectContaining({ id: '/default/orders' })]);
        });

        it.each([
            { message: 'The store is open in another tab.', storeStateId: 'heldElsewhere' as const },
            { message: 'This browser gives no storage to keep the store in, as in some private windows.', storeStateId: 'unavailable' as const }
        ])('refuses to list when the store cannot be opened because it is $storeStateId', async ({ message, storeStateId }) => {
            const connector = new Connector({} as never, []);
            vi.spyOn(connector, 'openStore').mockResolvedValue({ storeStateId });

            await expect(connector.listNodes({ folderPath: '' })).rejects.toThrow(message);
        });
    });

    describe('findObject', () => {
        it('gives back the path of a table that is there, in the default database or another', async () => {
            const { connector } = constructFakeConnector({ default: { orders: numberTable([]) }, sales: { returns: numberTable([]) } });

            await expect(connector.findObject({ id: '/default/orders' })).resolves.toEqual({ path: '/default/orders' });
            await expect(connector.findObject({ id: '/sales/returns' })).resolves.toEqual({ path: '/sales/returns' });
        });

        it('gives no path for a table or database that is not there, without creating the database', async () => {
            const { connector, registerOPFSFileName } = constructFakeConnector({ default: {} });

            await expect(connector.findObject({ id: '/default/missing' })).resolves.toEqual({ path: undefined });
            await expect(connector.findObject({ id: '/missing/orders' })).resolves.toEqual({ path: undefined });
            expect(registerOPFSFileName).not.toHaveBeenCalled();
        });
    });

    describe('previewObject', () => {
        it('hands the sample and the types the table was created with to type inference, and reads it as a table', async () => {
            const { connector, inferDataTypes } = constructFakeConnector({ default: { events: eventTable() } });

            const result = await connector.previewObject({ ...PREVIEW_OPTIONS, path: '/default/events' });

            expect(inferDataTypes).toHaveBeenCalledWith(
                [
                    [
                        { value: '1', valueWasQuoted: false },
                        { value: '2024-03-04T00:00:00.000Z', valueWasQuoted: false },
                        { value: 'true', valueWasQuoted: false },
                        { value: 'Ann', valueWasQuoted: false }
                    ],
                    [
                        { value: '2', valueWasQuoted: false },
                        { value: null, valueWasQuoted: false },
                        { value: 'false', valueWasQuoted: false },
                        { value: 'Bo', valueWasQuoted: false }
                    ]
                ],
                false,
                [
                    { dataSubtypeId: 'integer', dataTypeId: 'numeric', label: undefined, name: 'c0' },
                    { dataSubtypeId: 'date', dataTypeId: 'temporal', label: undefined, name: 'c1' },
                    { dataSubtypeId: undefined, dataTypeId: 'boolean', label: undefined, name: 'c2' },
                    { dataSubtypeId: undefined, dataTypeId: 'string', label: undefined, name: 'c3' }
                ]
            );
            expect(result.contentAuditConfig).toEqual(expect.objectContaining({ byteCount: 0, columns: ['inferred'], isComplete: true, recordCount: 2 }));
            expect(result.readingConfig).toEqual(expect.objectContaining({ dataFormatId: 'unknown', hasHeaders: false, isDeclared: true }));
            expect(result.sampleConfig).toEqual(expect.objectContaining({ hasHeaders: false, text: undefined }));
        });

        it('fails with a clear message for a table that is not there', async () => {
            const { connector } = constructFakeConnector({ default: {} });

            await expect(connector.previewObject({ ...PREVIEW_OPTIONS, path: '/default/missing' })).rejects.toThrow("'/default/missing' does not exist.");
        });
    });

    describe('auditObjectContent', () => {
        it('audits every record a chunk at a time against the types the table was created with', async () => {
            const { auditRecords, connector, constructColumnAuditor } = constructFakeConnector({ default: { amounts: numberTable([[1], [2], [3]]) } });
            const chunk = vi.fn();

            const result = await connector.auditObjectContent({ ...READ_OPTIONS, byteLimit: undefined, hasHeaders: false, path: 'amounts', stopSignal: undefined, supportsTransferableStreams: false }, chunk);

            expect(constructColumnAuditor).toHaveBeenCalledWith(false, [{ dataSubtypeId: 'integer', dataTypeId: 'numeric', label: undefined, name: 'c0' }]);
            expect(auditRecords.mock.calls).toEqual([[[[{ value: '1', valueWasQuoted: false }], [{ value: '2', valueWasQuoted: false }]]], [[[{ value: '3', valueWasQuoted: false }]]]]);
            expect(chunk.mock.calls).toEqual([
                [2, 0],
                [1, 0]
            ]);
            expect(result).toEqual({ contentAuditConfig: expect.objectContaining({ columns: ['audited'], isComplete: true, recordCount: 3 }) as unknown, hasHeaders: false });
        });

        it('stops when asked, keeping what it has audited', async () => {
            const { connector } = constructFakeConnector({ default: { amounts: numberTable([[1], [2], [3]]) } });
            const stopController = new AbortController();
            stopController.abort();

            const result = await connector.auditObjectContent(
                { ...READ_OPTIONS, byteLimit: undefined, hasHeaders: false, path: 'amounts', stopSignal: stopController.signal, supportsTransferableStreams: false },
                vi.fn()
            );

            expect(result.contentAuditConfig).toEqual(expect.objectContaining({ isComplete: false, recordCount: 0 }));
        });
    });

    describe('retrieveRecords', () => {
        it('hands over every record typed, a chunk at a time, then the count', async () => {
            const { connector } = constructFakeConnector({ default: {}, sales: { amounts: numberTable([[1], [2], [3]]) } });

            const { chunks, summary } = await collectRetrieval(connector.retrieveRecords({ ...READ_OPTIONS, path: '/sales/amounts' }));

            expect(chunks).toEqual([[[1], [2]], [[3]]]);
            expect(summary).toEqual({ recordCount: 3 });
        });

        it('stops when its request is aborted', async () => {
            const { connector } = constructFakeConnector({ default: { amounts: numberTable([[1], [2], [3]]) } });
            const abortController = new AbortController();
            abortController.abort(new Error('Aborted.'));

            await expect(collectRetrieval(connector.retrieveRecords({ ...READ_OPTIONS, path: 'amounts', signal: abortController.signal }))).rejects.toThrow('Aborted.');
        });
    });
});

// ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────────

// The browser features DuckDB's start needs: a private file system, Web Locks and workers.
function stubBrowser(): void {
    vi.stubGlobal('navigator', {
        locks: { request: (_name: string, _options: unknown, grant: (lock: unknown) => Promise<void> | undefined) => grant({}) },
        storage: { getDirectory: () => Promise.resolve({ getDirectoryHandle: () => Promise.reject(new Error('Not found.')) }) }
    });
    vi.stubGlobal(
        'Worker',
        class {
            addEventListener(): void {
                // A stand-in worker never fails to start.
            }
        }
    );
}

// DuckDB runs in a worker of its own, so a fake connection that answers the connector's SQL from the tables given
// stands in for it. Each database's file is listed as DuckDB lists it.
function constructFakeConnector(databases: FakeDatabases): {
    auditRecords: ReturnType<typeof vi.fn>;
    connector: Connector;
    constructColumnAuditor: ReturnType<typeof vi.fn>;
    inferDataTypes: ReturnType<typeof vi.fn>;
    registerOPFSFileName: ReturnType<typeof vi.fn>;
} {
    const registerOPFSFileName = vi.fn(() => Promise.resolve());
    const connection = {
        prepare: (sql: string) => Promise.resolve({ close: () => Promise.resolve(), query: (...values: SqlValue[]) => Promise.resolve(answerSql(databases, sql, values)) }),
        query: () => Promise.resolve()
    };
    const auditRecords = vi.fn();
    const constructColumnAuditor = vi.fn(() => ({ auditRecords, completeAudit: () => ({ columnConfigs: ['audited'], hasHeaders: false }) }));
    const inferDataTypes = vi.fn(() => ({ columnConfigs: ['inferred'], hasHeaders: false }));
    const connector = new Connector({ constructColumnAuditor, inferDataTypes } as unknown as ConnectorUtilities, []);
    connector.openStoreState = { attachedDatabases: new Map(), connection, database: { registerOPFSFileName } } as never;
    return { auditRecords, connector, constructColumnAuditor, inferDataTypes, registerOPFSFileName };
}

// Answers in the shape of an Arrow table, which is all the connector reads.
function answerSql(databases: FakeDatabases, sql: string, values: SqlValue[]): { getChildAt: (columnIndex: number) => { get: (rowIndex: number) => unknown }; numRows: number } {
    const rows = selectRows(databases, sql, values);
    return { getChildAt: (columnIndex) => ({ get: (rowIndex) => rows[rowIndex]?.[columnIndex] }), numRows: rows.length };
}

function selectRows(databases: FakeDatabases, sql: string, values: SqlValue[]): unknown[][] {
    if (sql === DATABASE_FILES_SQL) return Object.keys(databases).map((databaseName) => [`opfs://${databaseName}.duckdb`]);
    if (sql === TABLE_NAMES_SQL) return Object.keys(databases[values[0] ?? 'default'] ?? {}).map((tableName) => [tableName]);
    if (sql === TABLE_COLUMNS_SQL) return databases[values[1] ?? 'default']?.[values[0] ?? '']?.columns ?? [];
    const tableReference = sql.split(' FROM ', 2)[1]?.split(' ', 1)[0] ?? '';
    const names = tableReference.split('.').map((quotedName) => quotedName.slice(1, -1));
    const [databaseName = '', tableName = ''] = names.length === 2 ? names : ['default', names[0]];
    const rows = databases[databaseName]?.[tableName]?.rows ?? [];
    if (sql.startsWith('SELECT count(*)')) return [[BigInt(rows.length)]];
    const afterRowId = Number(values[0] ?? 0);
    const limit = Number(/LIMIT (\d+)/u.exec(sql)?.[1]);
    return rows.map((row, rowIndex) => [BigInt(rowIndex + 1), ...row]).filter(([rowId]) => Number(rowId) > afterRowId).slice(0, limit);
}

// Whole numbers come back from DuckDB as bigints.
function numberTable(values: number[][]): FakeTable {
    return { columns: [['c0', 'BIGINT']], rows: values.map((row) => row.map(BigInt)) };
}

// Dates come back from DuckDB as milliseconds since 1970.
function eventTable(): FakeTable {
    return {
        columns: [
            ['c0', 'BIGINT'],
            ['c1', 'DATE'],
            ['c2', 'BOOLEAN'],
            ['c3', 'VARCHAR']
        ],
        rows: [
            [1n, 19_786 * DAY, true, 'Ann'],
            [2n, null, false, 'Bo']
        ]
    };
}

async function collectRetrieval(retrieval: AsyncGenerator<{ records: unknown }, { recordCount: number }>): Promise<{ chunks: StoreRecord[][]; summary: { recordCount: number } }> {
    const chunks: StoreRecord[][] = [];
    for (;;) {
        const next = await retrieval.next();
        if (next.done === true) return { chunks, summary: next.value };
        chunks.push(next.value.records as StoreRecord[]);
    }
}
