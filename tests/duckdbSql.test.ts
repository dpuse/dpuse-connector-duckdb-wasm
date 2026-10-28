// ── External Dependencies & Registrations
import { describe, expect, it } from 'vitest';

// ── DPUse Framework
import type { QueryRecordsOptions } from '@dpuse/dpuse-shared';

// ── Local Framework
import { constructArrowTable, constructCreateTableSql, constructStagedInsertSql, constructStoreQuery, fromStoredValue, type StoredColumn } from '@/duckdbSql';

// ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────────

const STORED_COLUMNS: StoredColumn[] = [
    { dataSubtypeId: undefined, dataTypeId: 'string' },
    { dataSubtypeId: 'integer', dataTypeId: 'numeric' },
    { dataSubtypeId: undefined, dataTypeId: 'boolean' },
    { dataSubtypeId: 'date', dataTypeId: 'temporal' },
    { dataSubtypeId: undefined, dataTypeId: 'unknown' }
];

function constructQuery(options: Partial<QueryRecordsOptions>): QueryRecordsOptions {
    return { aggregates: [], filters: [], groupColumnIndexes: [], limit: 100, offset: 0, path: 'sales', sorts: [], ...options };
}

// ── Tests ────────────────────────────────────────────────────────────────────────────────────────────────────────────

describe('constructCreateTableSql', () => {
    it('creates a column of the audited type for each column, named by position', () => {
        expect(constructCreateTableSql('my "view"', STORED_COLUMNS)).toBe('CREATE TABLE "my ""view""" (c0 VARCHAR, c1 DOUBLE, c2 BOOLEAN, c3 TIMESTAMP, c4 VARCHAR)');
    });
});

describe('constructArrowTable', () => {
    it('builds typed columns, with nulls where values are missing', () => {
        const date = new Date('2026-10-28T00:00:00Z');
        const table = constructArrowTable(
            [
                ['Ünïcode', 3, true, date, 'ignored'],
                [null, null, false, null, null],
                ['', 2n, null, date, null]
            ],
            STORED_COLUMNS
        );
        const columnValues = (columnIndex: number): unknown[] => [0, 1, 2].map((rowIndex): unknown => table.getChildAt(columnIndex)?.get(rowIndex));

        expect(table.numRows).toBe(3);
        expect(columnValues(0)).toEqual(['Ünïcode', null, '']);
        expect(columnValues(1)).toEqual([3, null, 2]);
        expect(columnValues(2)).toEqual([true, false, null]);
        expect(columnValues(3)).toEqual([date.getTime(), null, date.getTime()]);
        expect(columnValues(4)).toEqual([null, null, null]);
    });
});

describe('constructStagedInsertSql', () => {
    it('turns staged milliseconds back into timestamps', () => {
        expect(constructStagedInsertSql('sales', STORED_COLUMNS)).toBe('INSERT INTO "sales" SELECT c0, c1, c2, epoch_ms(CAST(c3 AS BIGINT)), c4 FROM _dpuse_staging');
    });
});

describe('constructStoreQuery', () => {
    it('selects every column, with bound filter values and empty values sorted last', () => {
        const storeQuery = constructStoreQuery(
            STORED_COLUMNS,
            constructQuery({
                filters: [
                    { columnIndex: 0, operatorId: 'contains', value: '50%_off' },
                    { columnIndex: 3, operatorId: 'greaterThan', value: new Date(1000) },
                    { columnIndex: 2, operatorId: 'isEmpty', value: null }
                ],
                limit: 10,
                offset: 20,
                sorts: [{ columnIndex: 1, directionId: 'descending' }]
            })
        );

        expect(storeQuery.pageSql).toBe(
            String.raw`SELECT c0, c1, c2, c3, c4 FROM "sales" WHERE c0 ILIKE ? ESCAPE '\' AND c3 > epoch_ms(CAST(? AS BIGINT)) AND c2 IS NULL ORDER BY 2 DESC NULLS LAST LIMIT 10 OFFSET 20`
        );
        expect(storeQuery.values).toEqual([String.raw`%50\%\_off%`, 1000]);
        expect(storeQuery.countSql).toBe(String.raw`SELECT count(*) FROM (SELECT c0, c1, c2, c3, c4 FROM "sales" WHERE c0 ILIKE ? ESCAPE '\' AND c3 > epoch_ms(CAST(? AS BIGINT)) AND c2 IS NULL)`);
    });

    it('groups, with aggregates after the group values', () => {
        const storeQuery = constructStoreQuery(
            STORED_COLUMNS,
            constructQuery({
                aggregates: [
                    { columnIndex: undefined, functionId: 'count' },
                    { columnIndex: 1, functionId: 'average' },
                    { columnIndex: 3, functionId: 'maximum' }
                ],
                groupColumnIndexes: [0]
            })
        );

        expect(storeQuery.pageSql).toBe('SELECT c0, count(*), avg(c1), max(c3) FROM "sales" GROUP BY c0 LIMIT 100 OFFSET 0');
        expect(storeQuery.resultColumns).toEqual([STORED_COLUMNS[0], undefined, undefined, STORED_COLUMNS[3]]);
    });

    it('refuses a column that does not exist and a count that is not whole', () => {
        expect(() => constructStoreQuery(STORED_COLUMNS, constructQuery({ groupColumnIndexes: [5] }))).toThrow('Column 5 does not exist.');
        expect(() => constructStoreQuery(STORED_COLUMNS, constructQuery({ limit: 1.5 }))).toThrow('1.5 is not a valid count.');
    });
});

describe('fromStoredValue', () => {
    it('returns dates for timestamps and numbers for counts', () => {
        expect(fromStoredValue(1000, STORED_COLUMNS[3])).toEqual(new Date(1000));
        expect(fromStoredValue(5n, undefined)).toBe(5);
        expect(fromStoredValue(null, STORED_COLUMNS[0])).toBeNull();
        expect(fromStoredValue(true, STORED_COLUMNS[2])).toBe(true);
    });
});
