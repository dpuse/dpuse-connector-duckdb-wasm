// ── External Dependencies & Registrations
import { Bool, Float64, makeData, makeVector, Table, Utf8, type Vector } from 'apache-arrow';

// ── DPUse Framework
import type {
    DataSubtypeId,
    DataTypeId,
    ObjectColumnConfig,
    QueryAggregate,
    QueryFilter,
    QueryRecordsOptions,
    QuerySort,
    StoreRecord,
    StoreValue
} from '@dpuse/dpuse-shared';

// ── Types ────────────────────────────────────────────────────────────────────────────────────────────────────────────

// What the store keeps about each loaded object, to turn stored values back into their audited types.
export interface StoredColumn {
    dataSubtypeId: DataSubtypeId | undefined;
    dataTypeId: DataTypeId;
}

export interface StoreQuery {
    countSql: string;
    pageSql: string;
    resultColumns: (StoredColumn | undefined)[]; // Undefined for counts, sums and averages, which are plain numbers.
    values: QueryValue[];
}

interface PresentValue {
    index: number;
    value: Exclude<StoreValue, null>;
}

type QueryValue = boolean | number | string | null;

// ── Constants ────────────────────────────────────────────────────────────────────────────────────────────────────────

// Holds each object's audited column types. Named so it cannot clash with a data view's table, whose name is its path.
export const OBJECTS_TABLE = '_dpuse_objects';

// Each chunk of a load goes into this table first, then into the object's table with each value cast to its type.
export const STAGING_TABLE = '_dpuse_staging';

const LIKE_ESCAPE_SQL = String.raw`ESCAPE '\'`; // Lets a filter match '%' and '_' as written, not as wildcards.

// ── Actions ──────────────────────────────────────────────────────────────────────────────────────────────────────────

export function constructStoredColumns(columnConfigs: ObjectColumnConfig[]): StoredColumn[] {
    return columnConfigs.map(({ dataSubtypeId, dataTypeId }) => ({ dataSubtypeId, dataTypeId }));
}

export function constructCreateTableSql(path: string, storedColumns: StoredColumn[]): string {
    const columnDefinitions = storedColumns.map((storedColumn, columnIndex) => `${columnName(columnIndex)} ${resolveColumnType(storedColumn)}`);
    return `CREATE TABLE ${quoteIdentifier(path)} (${columnDefinitions.join(', ')})`;
}

// Records go into DuckDB as Arrow columns. Each column is built straight from typed arrays, because Arrow's own builders
// compile code at run time, which the app's Content Security Policy forbids. Dates go in as milliseconds and become
// timestamps in 'constructStagedInsertSql'.
export function constructArrowTable(records: StoreRecord[], storedColumns: StoredColumn[]): Table {
    const vectors = Object.fromEntries(
        storedColumns.map((storedColumn, columnIndex) => [columnName(columnIndex), constructArrowVector(records.map((record) => record[columnIndex] ?? null), storedColumn)])
    );
    return new Table(vectors);
}

export function constructStagedInsertSql(path: string, storedColumns: StoredColumn[]): string {
    const selections = storedColumns.map((storedColumn, columnIndex) =>
        storedColumn.dataTypeId === 'temporal' ? `epoch_ms(CAST(${columnName(columnIndex)} AS BIGINT))` : columnName(columnIndex)
    );
    return `INSERT INTO ${quoteIdentifier(path)} SELECT ${selections.join(', ')} FROM ${STAGING_TABLE}`;
}

export function constructStoreQuery(storedColumns: StoredColumn[], options: QueryRecordsOptions): StoreQuery {
    const isGrouped = options.groupColumnIndexes.length > 0;
    const resultColumns = isGrouped ? constructGroupedResultColumns(storedColumns, options) : storedColumns;
    const selections = isGrouped
        ? [...options.groupColumnIndexes.map((columnIndex) => columnName(columnIndex)), ...options.aggregates.map((aggregate) => constructAggregateSql(aggregate))]
        : storedColumns.map((_storedColumn, columnIndex) => columnName(columnIndex));

    const whereParts = options.filters.map((filter) => constructFilterSql(filter, storedColumns));
    const whereSql = whereParts.length > 0 ? ` WHERE ${whereParts.map((part) => part.sql).join(' AND ')}` : '';
    const groupSql = isGrouped ? ` GROUP BY ${options.groupColumnIndexes.map((columnIndex) => columnName(columnIndex)).join(', ')}` : '';
    const baseSql = `SELECT ${selections.join(', ')} FROM ${quoteIdentifier(options.path)}${whereSql}${groupSql}`;
    const pageSql = `${baseSql}${constructOrderSql(options.sorts, selections.length)} LIMIT ${String(checkCount(options.limit))} OFFSET ${String(checkCount(options.offset))}`;

    return { countSql: `SELECT count(*) FROM (${baseSql})`, pageSql, resultColumns, values: whereParts.flatMap((part) => part.values) };
}

// Timestamps come back from Arrow as milliseconds since 1970, and counts as bigints.
// eslint-disable-next-line sonarjs/function-return-type -- A store value is one of several types, by its column.
export function fromStoredValue(value: unknown, storedColumn: StoredColumn | undefined): StoreValue {
    if (value == null) return null;
    if (storedColumn?.dataTypeId === 'temporal') return new Date(Number(value));
    if (typeof value === 'bigint') return Number(value);
    return typeof value === 'boolean' || typeof value === 'number' || typeof value === 'string' ? value : null;
}

export function quoteIdentifier(name: string): string {
    return `"${name.replaceAll('"', '""')}"`;
}

// ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────────

// A set bit in an Arrow validity bitmap marks a value that is present.
function constructArrowVector(values: StoreValue[], storedColumn: StoredColumn): Vector {
    const length = values.length;
    const nullBitmap = new Uint8Array(Math.ceil(length / 8));
    const presentValues: PresentValue[] = [];
    for (const [index, value] of values.entries()) {
        if (value == null || storedColumn.dataTypeId === 'unknown') continue;
        setBit(nullBitmap, index);
        presentValues.push({ index, value });
    }
    const nullCount = length - presentValues.length;

    switch (storedColumn.dataTypeId) {
        case 'boolean':
            return makeVector(makeData({ data: constructBooleanData(presentValues, length), length, nullBitmap, nullCount, type: new Bool() }));
        case 'numeric':
        case 'temporal':
            return makeVector(makeData({ data: constructNumberData(presentValues, length), length, nullBitmap, nullCount, type: new Float64() }));
        case 'string':
        case 'unknown': {
            const { data, valueOffsets } = constructTextData(presentValues, length);
            return makeVector(makeData({ data, length, nullBitmap, nullCount, type: new Utf8(), valueOffsets }));
        }
    }
}

function constructBooleanData(presentValues: PresentValue[], length: number): Uint8Array {
    const data = new Uint8Array(Math.ceil(length / 8));
    for (const { index, value } of presentValues) if (value === true) setBit(data, index);
    return data;
}

function constructNumberData(presentValues: PresentValue[], length: number): Float64Array {
    const data = new Float64Array(length);
    for (const { index, value } of presentValues) data[index] = value instanceof Date ? value.getTime() : Number(value);
    return data;
}

// Arrow text is one run of UTF-8 bytes, with offsets marking where each value starts.
function constructTextData(presentValues: PresentValue[], length: number): { data: Uint8Array; valueOffsets: Int32Array } {
    const encoder = new TextEncoder();
    const encodedValues = new Map(presentValues.map(({ index, value }) => [index, encoder.encode(String(value))]));
    const valueOffsets = new Int32Array(length + 1);
    for (let index = 0; index < length; index++) valueOffsets[index + 1] = (valueOffsets[index] ?? 0) + (encodedValues.get(index)?.length ?? 0);
    const data = new Uint8Array(valueOffsets[length] ?? 0);
    for (const [index, encodedValue] of encodedValues) data.set(encodedValue, valueOffsets[index]);
    return { data, valueOffsets };
}

// Limits and offsets are written into the SQL, because DuckDB will not take a bound value for them.
function checkCount(count: number): number {
    if (!Number.isSafeInteger(count) || count < 0) throw new Error(`${String(count)} is not a valid count.`);
    return count;
}

// Column positions come from the caller, so each is checked before it becomes part of the SQL.
function checkIndex(columnIndex: number, columnCount: number): number {
    if (!Number.isSafeInteger(columnIndex) || columnIndex < 0 || columnIndex >= columnCount) throw new Error(`Column ${String(columnIndex)} does not exist.`);
    return columnIndex;
}

// Columns are named by position, because an audited column has a label but no id.
function columnName(columnIndex: number): string {
    return `c${String(columnIndex)}`;
}

function constructAggregateSql(aggregate: QueryAggregate): string {
    if (aggregate.functionId === 'count') return 'count(*)';
    if (aggregate.columnIndex == null) throw new Error(`Aggregate '${aggregate.functionId}' needs a column.`);
    const name = columnName(aggregate.columnIndex);
    switch (aggregate.functionId) {
        case 'average':
            return `avg(${name})`;
        case 'countDistinct':
            return `count(DISTINCT ${name})`;
        case 'maximum':
            return `max(${name})`;
        case 'minimum':
            return `min(${name})`;
        case 'sum':
            return `sum(${name})`;
    }
}

// Text matches ignore case, as SQLite's do. A date is passed as milliseconds and turned back into a timestamp.
function constructFilterSql(filter: QueryFilter, storedColumns: StoredColumn[]): { sql: string; values: QueryValue[] } {
    const name = columnName(checkIndex(filter.columnIndex, storedColumns.length));
    const valueSql = filter.value instanceof Date ? 'epoch_ms(CAST(? AS BIGINT))' : '?';
    const value = toQueryValue(filter.value);
    const text = escapeLikeText(String(filter.value ?? ''));
    switch (filter.operatorId) {
        case 'contains':
            return { sql: `${name} ILIKE ? ${LIKE_ESCAPE_SQL}`, values: [`%${text}%`] };
        case 'endsWith':
            return { sql: `${name} ILIKE ? ${LIKE_ESCAPE_SQL}`, values: [`%${text}`] };
        case 'equals':
            return { sql: `${name} IS NOT DISTINCT FROM ${valueSql}`, values: [value] };
        case 'greaterThan':
            return { sql: `${name} > ${valueSql}`, values: [value] };
        case 'greaterThanOrEquals':
            return { sql: `${name} >= ${valueSql}`, values: [value] };
        case 'isEmpty':
            return { sql: `${name} IS NULL`, values: [] };
        case 'isNotEmpty':
            return { sql: `${name} IS NOT NULL`, values: [] };
        case 'lessThan':
            return { sql: `${name} < ${valueSql}`, values: [value] };
        case 'lessThanOrEquals':
            return { sql: `${name} <= ${valueSql}`, values: [value] };
        case 'notEquals':
            return { sql: `${name} IS DISTINCT FROM ${valueSql}`, values: [value] };
        case 'startsWith':
            return { sql: `${name} ILIKE ? ${LIKE_ESCAPE_SQL}`, values: [`${text}%`] };
    }
}

// A grouped result holds the group's values, then one value per aggregate. A minimum or maximum keeps its column's type,
// while counts, sums and averages are plain numbers.
function constructGroupedResultColumns(storedColumns: StoredColumn[], options: QueryRecordsOptions): (StoredColumn | undefined)[] {
    const groupColumns = options.groupColumnIndexes.map((columnIndex) => storedColumns[checkIndex(columnIndex, storedColumns.length)]);
    const aggregateColumns = options.aggregates.map((aggregate) => {
        if (aggregate.columnIndex != null) checkIndex(aggregate.columnIndex, storedColumns.length);
        const doesKeepColumnType = aggregate.functionId === 'maximum' || aggregate.functionId === 'minimum';
        return doesKeepColumnType && aggregate.columnIndex != null ? storedColumns[aggregate.columnIndex] : undefined;
    });
    return [...groupColumns, ...aggregateColumns];
}

// Sorts name a position in the result, which SQL accepts as a 1-based column number. Empty values sort last, as in the
// SQLite store.
function constructOrderSql(sorts: QuerySort[], resultColumnCount: number): string {
    if (sorts.length === 0) return '';
    const sortSql = sorts.map((sort) => `${String(checkIndex(sort.columnIndex, resultColumnCount) + 1)} ${sort.directionId === 'descending' ? 'DESC' : 'ASC'} NULLS LAST`);
    return ` ORDER BY ${sortSql.join(', ')}`;
}

function escapeLikeText(text: string): string {
    return text.replaceAll('\\', '\\\\').replaceAll('%', String.raw`\%`).replaceAll('_', String.raw`\_`);
}

function setBit(bitmap: Uint8Array, index: number): void {
    const byteIndex = Math.floor(index / 8);
    bitmap[byteIndex] = (bitmap[byteIndex] ?? 0) | (1 << index % 8);
}

// Every number is stored as a double, so a value too large for one loses precision.
function resolveColumnType(storedColumn: StoredColumn): string {
    switch (storedColumn.dataTypeId) {
        case 'boolean':
            return 'BOOLEAN';
        case 'numeric':
            return 'DOUBLE';
        case 'temporal':
            return 'TIMESTAMP';
        case 'string':
        case 'unknown':
            return 'VARCHAR'; // Every value of a column with no audited type is null.
    }
}

// eslint-disable-next-line sonarjs/function-return-type -- A bound value is one of several types, by its column.
function toQueryValue(value: StoreValue): QueryValue {
    if (value instanceof Date) return value.getTime();
    return typeof value === 'bigint' ? Number(value) : value;
}
