// ── External Dependencies & Registrations
import { createRequire } from 'node:module';
import path from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { type Table, tableToIPC } from 'apache-arrow';

// ── DPUse Framework
import type { ObjectColumnConfig, StoreRecord } from '@dpuse/dpuse-shared';

// ── Local Framework
import { Connector } from '@/index';

// ── Types ────────────────────────────────────────────────────────────────────────────────────────────────────────────

// The part of the package's Node build these tests use. It ships with no types of its own.
interface Connection {
    insertArrowFromIPCStream: (buffer: Uint8Array, options: { create: boolean; name: string }) => void;
    prepare: (sql: string) => { close: () => void; query: (...values: unknown[]) => Table };
    query: (sql: string) => Table;
}

interface NodeBlockingBuild {
    createDuckDB: (
        bundles: Record<string, { mainModule: string; mainWorker: string }>,
        logger: unknown,
        runtime: unknown
    ) => Promise<{ connect: () => Connection; instantiate: () => Promise<unknown>; open: (options: { accessMode: number; path: string }) => void }>;
    DuckDBAccessMode: { READ_WRITE: number };
    NODE_RUNTIME: unknown;
    VoidLogger: new () => unknown;
}

// ── Mocks ────────────────────────────────────────────────────────────────────────────────────────────────────────────

// The connector's own start of DuckDB needs a browser worker. These tests hand it a connection to the Node build instead.
vi.mock('@duckdb/duckdb-wasm', () => ({ AsyncDuckDB: vi.fn(), DuckDBAccessMode: { READ_WRITE: 2 }, VoidLogger: vi.fn() }));

// ── Constants ────────────────────────────────────────────────────────────────────────────────────────────────────────

const COLUMN_CONFIGS = [{ dataSubtypeId: undefined, dataTypeId: 'numeric' }] as ObjectColumnConfig[];
const WIDE_COLUMN_COUNT = 1000; // Wide enough that a load of a few thousand records takes more than one batch of 5 million values.

// ── State ────────────────────────────────────────────────────────────────────────────────────────────────────────────

const connection = await constructConnection();

// ── Tests ────────────────────────────────────────────────────────────────────────────────────────────────────────────

// These run the load's SQL in the real DuckDB, because the swap of the new table for the earlier one is DuckDB's to get
// right.
describe('loadObjectRecords', () => {
    it('replaces the earlier load and leaves no table of its own behind', async () => {
        const connector = constructConnector();
        await load(connector, 'sales', [[[1], [2]]]);

        await expect(load(connector, 'sales', [[[3]], [[4], [5]]])).resolves.toEqual({ recordCount: 3 });
        expect(readNumbers('SELECT c0 FROM sales ORDER BY c0')).toEqual([3, 4, 5]);
        expect(listInternalTableNames()).toEqual([]);
    });

    it('commits a wide load a batch at a time and swaps it in at the end', async () => {
        const connector = constructConnector();
        const queries = recordQueries();
        const wideColumnConfigs = Array.from({ length: WIDE_COLUMN_COUNT }, () => COLUMN_CONFIGS[0]) as ObjectColumnConfig[];
        const wideChunk = Array.from({ length: 2000 }, (_unused, recordIndex) => Array.from({ length: WIDE_COLUMN_COUNT }, () => recordIndex));

        const result = await connector.loadObjectRecords({ columnConfigs: wideColumnConfigs, path: 'wide' }, toChunks([wideChunk, wideChunk, wideChunk]));

        expect(result).toEqual({ recordCount: 6000 });
        expect(queries.filter((sql) => sql === 'COMMIT')).toHaveLength(2); // One batch of 5,000 records, then the rest with the swap.
        expect(readNumbers('SELECT count(*) FROM wide')).toEqual([6000]);
    });

    it('keeps the earlier load and drops its own table when the load fails', async () => {
        const connector = constructConnector();
        await load(connector, 'orders', [[[1]]]);

        await expect(load(connector, 'orders', [[[2]]], new Error('The source failed.'))).rejects.toThrow('The source failed.');
        expect(readNumbers('SELECT c0 FROM orders')).toEqual([1]);
        expect(listInternalTableNames()).toEqual([]);
    });

    it('keeps the earlier load when the load is stopped after a batch was committed', async () => {
        const connector = constructConnector();
        await load(connector, 'stock', [[[1]]]);
        const controller = new AbortController();
        const wideColumnConfigs = Array.from({ length: WIDE_COLUMN_COUNT }, () => COLUMN_CONFIGS[0]) as ObjectColumnConfig[];
        const wideChunk = Array.from({ length: 5000 }, () => Array.from({ length: WIDE_COLUMN_COUNT }, () => 2));
        async function* stoppedChunks(): AsyncGenerator<StoreRecord[]> {
            await Promise.resolve();
            yield wideChunk; // Fills a batch, which is committed.
            controller.abort(new Error('Stopped.'));
            yield wideChunk;
        }

        await expect(connector.loadObjectRecords({ columnConfigs: wideColumnConfigs, path: 'stock', signal: controller.signal }, stoppedChunks())).rejects.toThrow('Stopped.');
        expect(readNumbers('SELECT count(*) FROM stock')).toEqual([1]);
        expect(listInternalTableNames()).toEqual([]);
    });

    it('replaces a table left by an earlier load of the object that never finished', async () => {
        const connector = constructConnector();
        connection.query('CREATE TABLE "_dpuse_loading_items" (c0 VARCHAR)'); // As a load leaves it when the page reloads.

        await expect(load(connector, 'items', [[[7]]])).resolves.toEqual({ recordCount: 1 });
        expect(readNumbers('SELECT c0 FROM items')).toEqual([7]);
        expect(listInternalTableNames()).toEqual([]);
    });
});

// ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────────

async function constructConnection(): Promise<Connection> {
    const require = createRequire(import.meta.url);
    const { createDuckDB, DuckDBAccessMode: accessModes, NODE_RUNTIME, VoidLogger } = require('@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs') as NodeBlockingBuild;
    const packageFolder = path.dirname(require.resolve('@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs')); // The '.wasm' files are named differently from one DuckDB build to another, and each build picks the bundle it knows.
    const database = await createDuckDB(
        { base: { mainModule: path.join(packageFolder, 'duckdb-base.wasm'), mainWorker: '' }, mvp: { mainModule: path.join(packageFolder, 'duckdb-mvp.wasm'), mainWorker: '' } },
        new VoidLogger(),
        NODE_RUNTIME
    );
    await database.instantiate();
    database.open({ accessMode: accessModes.READ_WRITE, path: ':memory:' });
    return database.connect();
}

// A connector whose open store is the Node build's connection, answering as the browser build's asynchronous one does.
function constructConnector(): Connector {
    const connector = new Connector({} as never, []);
    const asyncConnection = {
        insertArrowTable: (table: Table, options: { create: boolean; name: string }): Promise<void> => {
            connection.insertArrowFromIPCStream(tableToIPC(table, 'stream'), options);
            return Promise.resolve();
        },
        prepare: (sql: string) => {
            const statement = connection.prepare(sql);
            const close = (): Promise<void> => {
                statement.close();
                return Promise.resolve();
            };
            return Promise.resolve({ close, query: (...values: unknown[]) => Promise.resolve(statement.query(...values)) });
        },
        query: (sql: string) => Promise.resolve(connection.query(sql))
    };
    connector.openStoreState = { attachedDatabases: new Map(), connection: asyncConnection, database: {} } as never;
    return connector;
}

// Loads one-column records, chunk by chunk, failing after the last chunk when an error is given.
async function load(connector: Connector, tablePath: string, chunks: StoreRecord[][], error?: Error): Promise<unknown> {
    return connector.loadObjectRecords({ columnConfigs: COLUMN_CONFIGS, path: tablePath }, toChunks(chunks, error));
}

async function* toChunks(chunks: StoreRecord[][], error?: Error): AsyncGenerator<StoreRecord[]> {
    for (const chunk of chunks) {
        await Promise.resolve();
        yield chunk;
    }
    if (error != null) throw error;
}

// Records every statement the connection runs from here on.
function recordQueries(): string[] {
    const queries: string[] = [];
    const query = connection.query.bind(connection);
    connection.query = (sql: string): Table => {
        queries.push(sql);
        return query(sql);
    };
    return queries;
}

function readNumbers(sql: string): number[] {
    const table = connection.query(sql);
    return Array.from({ length: table.numRows }, (_unused, rowIndex) => Number(table.getChildAt(0)?.get(rowIndex)));
}

// The store's own tables, which a load must not leave behind.
function listInternalTableNames(): string[] {
    const table = connection.query("SELECT table_name FROM duckdb_tables() WHERE starts_with(table_name, '_dpuse_') ORDER BY table_name");
    return Array.from({ length: table.numRows }, (_unused, rowIndex) => String(table.getChildAt(0)?.get(rowIndex)));
}
