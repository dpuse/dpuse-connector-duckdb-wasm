// ── External Dependencies & Registrations
import { createRequire } from 'node:module';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { type Table, tableToIPC } from 'apache-arrow';

// ── DPUse Framework
import type { ObjectColumnConfig, QueryRecordsOptions, StoreRecord } from '@dpuse/dpuse-shared';

// ── Local Framework
import { constructArrowTable, constructCreateTableSql, constructStagedInsertSql, constructStoredColumns, constructStoreQuery } from '@/duckdbSql';
import { constructTableReference, fromStoredValue, STAGING_TABLE, TABLE_COUNT_SQL } from '@/duckdbSql';

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

// ── Constants ────────────────────────────────────────────────────────────────────────────────────────────────────────

const DAY = 86_400_000;
const STORED_COLUMNS = constructStoredColumns([
    { dataSubtypeId: 'date', dataTypeId: 'temporal' },
    { dataSubtypeId: 'time', dataTypeId: 'temporal' },
    { dataSubtypeId: 'dateTime', dataTypeId: 'temporal' }
] as ObjectColumnConfig[]);

// ── State ────────────────────────────────────────────────────────────────────────────────────────────────────────────

const connection = await constructConnection();

// ── Tests ────────────────────────────────────────────────────────────────────────────────────────────────────────────

// These run the SQL in the real DuckDB, because the units it returns are what the conversions must match.
describe('duckdbSql temporal round trip', () => {
    it('creates true date and time columns', () => {
        load([constructTemporalRecord(19_786, 36_672_123)]);

        const typeTable = connection.query('SELECT typeof(c0), typeof(c1), typeof(c2) FROM dates');
        expect([0, 1, 2].map((columnIndex) => String(typeTable.getChildAt(columnIndex)?.get(0)))).toEqual(['DATE', 'TIME', 'TIMESTAMP']);
    });

    it('returns every date, time and date-time exactly as loaded, across the whole calendar and day', () => {
        const firstDay = Date.UTC(1000, 0, 1) / DAY;
        const lastDay = Date.UTC(9999, 11, 31) / DAY;
        const records: StoreRecord[] = [];
        for (let dayNumber = firstDay; dayNumber <= lastDay; dayNumber += 97) records.push(constructTemporalRecord(dayNumber, (dayNumber - firstDay) % DAY));
        records.push(constructTemporalRecord(0, 0), constructTemporalRecord(-1, DAY - 1), constructTemporalRecord(lastDay, DAY - 1));

        load(records);

        expect(query({ sorts: [{ columnIndex: 2, directionId: 'ascending' }] })).toEqual(
            records.toSorted((left, right) => (left[2] as Date).getTime() - (right[2] as Date).getTime())
        );
    });

    it('filters, sorts, groups and finds the earliest and latest date by the value loaded', () => {
        load([constructTemporalRecord(19_786, 3000), constructTemporalRecord(19_787, 2000), constructTemporalRecord(19_785, 1000), constructTemporalRecord(19_786, 4000)]);
        const targetDate = new Date(19_786 * DAY);

        expect(query({ filters: [{ columnIndex: 0, operatorId: 'equals', value: targetDate }] })).toHaveLength(2);
        expect(query({ filters: [{ columnIndex: 0, operatorId: 'greaterThan', value: targetDate }] })).toHaveLength(1);
        expect(query({ filters: [{ columnIndex: 1, operatorId: 'lessThan', value: new Date(2000) }] })).toHaveLength(1);
        expect(query({ filters: [{ columnIndex: 2, operatorId: 'greaterThanOrEquals', value: new Date(19_787 * DAY) }] })).toHaveLength(1);
        expect(query({ sorts: [{ columnIndex: 0, directionId: 'ascending' }] }).map((record) => record[0])).toEqual([
            new Date(19_785 * DAY),
            targetDate,
            targetDate,
            new Date(19_787 * DAY)
        ]);
        expect(
            query({
                aggregates: [{ columnIndex: 1, functionId: 'minimum' }],
                groupColumnIndexes: [0],
                sorts: [{ columnIndex: 0, directionId: 'ascending' }]
            })
        ).toEqual([
            [new Date(19_785 * DAY), new Date(1000)],
            [targetDate, new Date(3000)],
            [new Date(19_787 * DAY), new Date(2000)]
        ]);
    });

    it('still reads a date and a time stored as timestamps, as they were loaded before', () => {
        connection.query('DROP TABLE IF EXISTS dates');
        connection.query("CREATE TABLE dates AS SELECT TIMESTAMP '2024-03-04' AS c0, TIMESTAMP '1970-01-01 10:11:12.123' AS c1, TIMESTAMP '2024-03-04 10:11:12' AS c2");

        expect(query({})).toEqual([[new Date('2024-03-04T00:00:00Z'), new Date('1970-01-01T10:11:12.123Z'), new Date('2024-03-04T10:11:12Z')]]);
    });
});

describe('duckdbSql large number round trip', () => {
    const numberColumns = constructStoredColumns([
        { dataSubtypeId: 'bigint', dataTypeId: 'numeric' },
        { dataSubtypeId: 'integer', dataTypeId: 'numeric' },
        { dataSubtypeId: 'decimal', dataTypeId: 'numeric' }
    ] as ObjectColumnConfig[]);
    const records: StoreRecord[] = [
        [9_223_372_036_854_775_807n, 1, 11.494704278406504],
        [-9_223_372_036_854_775_808n, 2, 0.1],
        [9_007_199_254_740_993n, 3, -2.5]
    ];
    const queryNumbers = (options: Partial<QueryRecordsOptions>): StoreRecord[] => query({ path: 'numbers', ...options }, numberColumns);

    it('returns whole numbers beyond ±2^53 exactly as bigints, other whole numbers as numbers, and decimals at full precision', () => {
        load(records, numberColumns, 'numbers');

        const typeTable = connection.query('SELECT typeof(c0), typeof(c1), typeof(c2) FROM numbers LIMIT 1');
        expect([0, 1, 2].map((columnIndex) => String(typeTable.getChildAt(columnIndex)?.get(0)))).toEqual(['BIGINT', 'BIGINT', 'DOUBLE']);
        expect(queryNumbers({ sorts: [{ columnIndex: 0, directionId: 'ascending' }] })).toEqual([records[1], records[2], records[0]]);
    });

    it('filters on a whole number beyond ±2^53, and keeps it exact in a minimum or maximum', () => {
        load(records, numberColumns, 'numbers');

        expect(queryNumbers({ filters: [{ columnIndex: 0, operatorId: 'equals', value: 9_007_199_254_740_993n }] })).toEqual([records[2]]);

        load(
            [
                [9_007_199_254_740_993n, 2, 1.5],
                [9_223_372_036_854_775_807n, 3, 1.5]
            ],
            numberColumns,
            'numbers'
        );
        expect(
            queryNumbers({
                aggregates: [
                    { columnIndex: 0, functionId: 'maximum' },
                    { columnIndex: 1, functionId: 'sum' }
                ],
                groupColumnIndexes: [2]
            })
        ).toEqual([[1.5, 9_223_372_036_854_775_807n, 5]]);
    });
});

describe('duckdbSql attached database round trip', () => {
    it('loads and queries a table in another database, leaving a table of the same name in the default one alone', () => {
        const numberColumns = constructStoredColumns([{ dataSubtypeId: 'integer', dataTypeId: 'numeric' }] as ObjectColumnConfig[]);
        connection.query('ATTACH IF NOT EXISTS \':memory:\' AS "archive"'); // The connector attaches a file of its own. An in-memory database stands in for it.

        load([[1], [2]], numberColumns, 'orders');
        load([[3]], numberColumns, '/archive/orders');

        expect(query({ path: '/archive/orders' }, numberColumns)).toEqual([[3]]);
        expect(query({ path: 'orders' }, numberColumns)).toEqual([[1], [2]]);
    });

    it('finds a loaded table in the default database or in another, and not one that was never loaded', () => {
        const numberColumns = constructStoredColumns([{ dataSubtypeId: 'integer', dataTypeId: 'numeric' }] as ObjectColumnConfig[]);
        connection.query('ATTACH IF NOT EXISTS \':memory:\' AS "archive"');
        load([[1]], numberColumns, 'counted');
        load([[1]], numberColumns, '/archive/counted');

        expect(countTables('counted', null)).toBe(1);
        expect(countTables('counted', 'archive')).toBe(1);
        expect(countTables('missing', null)).toBe(0);
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

function constructTemporalRecord(dayNumber: number, timeOfDay: number): StoreRecord {
    return [new Date(dayNumber * DAY), new Date(timeOfDay), new Date(dayNumber * DAY + timeOfDay)];
}

function load(records: StoreRecord[], storedColumns = STORED_COLUMNS, path = 'dates'): void {
    connection.query(`DROP TABLE IF EXISTS ${constructTableReference(path)}`);
    connection.query(constructCreateTableSql(path, storedColumns));
    connection.insertArrowFromIPCStream(tableToIPC(constructArrowTable(records, storedColumns), 'stream'), { create: true, name: STAGING_TABLE });
    connection.query(constructStagedInsertSql(path, storedColumns));
    connection.query(`DROP TABLE ${STAGING_TABLE}`);
}

function countTables(tableName: string, databaseName: string | null): number {
    const statement = connection.prepare(TABLE_COUNT_SQL);
    const table = statement.query(tableName, databaseName);
    statement.close();
    return Number(table.getChildAt(0)?.get(0));
}

function query(options: Partial<QueryRecordsOptions>, storedColumns = STORED_COLUMNS): StoreRecord[] {
    const { pageSql, resultColumns, values } = constructStoreQuery(storedColumns, {
        aggregates: [],
        columnConfigs: [],
        filters: [],
        groupColumnIndexes: [],
        limit: 100_000,
        offset: 0,
        path: 'dates',
        sorts: [],
        ...options
    });
    const statement = connection.prepare(pageSql);
    const table = statement.query(...values);
    statement.close();
    return Array.from({ length: table.numRows }, (_value, rowIndex) =>
        resultColumns.map((storedColumn, columnIndex) => fromStoredValue(table.getChildAt(columnIndex)?.get(rowIndex), storedColumn))
    );
}
