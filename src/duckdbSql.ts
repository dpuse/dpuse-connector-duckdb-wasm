// ── External Dependencies & Registrations
import { Bool, DateDay, Float64, Int64, makeData, makeVector, Table, TimeMillisecond, TimestampMillisecond, Utf8, type Vector } from 'apache-arrow';

// ── DPUse Framework
import { resolveStorePath } from '@dpuse/dpuse-shared';
import type { DataSubtypeId, DataTypeId, ObjectColumnConfig, QueryAggregate, QueryFilter, QueryRecordsOptions, QuerySort, StoreRecord } from '@dpuse/dpuse-shared';
import type { StoreChunk, StoreColumnChunk, StorePackedNumberColumn, StoreValue } from '@dpuse/dpuse-shared';

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

type QueryValue = boolean | number | string | null;

// ── Constants ────────────────────────────────────────────────────────────────────────────────────────────────────────

// The database DuckDB opens with the store, used by a path that names no database. A path that names it, such as
// '/default/orders', reaches the same table as 'orders'.
export const DEFAULT_DATABASE_NAME = 'default';

// Lists the store's database files. DuckDB keeps them in a folder of its own in the private file system, and holds a new
// one in a scratch area until it writes it out, so only DuckDB can list them all.
export const DATABASE_FILES_SQL = "SELECT file FROM glob('opfs://*.duckdb') ORDER BY file";

// Names a table's columns, with the type each was created with, in the database given or, when it is null, in the
// store's default database.
export const TABLE_COLUMNS_SQL = 'SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = ? AND database_name = coalesce(?, current_database()) ORDER BY column_index';

// Counts the tables with the name given, in the database given or, when it is null, in the store's default database.
export const TABLE_COUNT_SQL = 'SELECT count(*) FROM duckdb_tables() WHERE table_name = ? AND database_name = coalesce(?, current_database())';

// The store's own tables start with this, so they are never listed as data views.
const INTERNAL_TABLE_PREFIX = '_dpuse_';

// A load builds its table under this prefix, beside the object's, and renames it to the object's name once every record
// is in.
const LOADING_TABLE_PREFIX = `${INTERNAL_TABLE_PREFIX}loading_`;

// Each chunk of a load goes into this table first, then into the table being loaded with each value cast to its type.
export const STAGING_TABLE = `${INTERNAL_TABLE_PREFIX}staging`;

// Names the store's tables in the database given or, when it is null, in the store's default database. The store's own
// tables are left out, because they hold a load in progress, not a data view.
export const TABLE_NAMES_SQL = `SELECT table_name FROM duckdb_tables() WHERE database_name = coalesce(?, current_database()) AND NOT temporary AND NOT starts_with(table_name, '${INTERNAL_TABLE_PREFIX}') ORDER BY table_name`;

// Names the tables of loads that never finished, such as when the page reloaded, in the database given or, when it is
// null, in the store's default database.
export const LOADING_TABLE_NAMES_SQL = `SELECT table_name FROM duckdb_tables() WHERE database_name = coalesce(?, current_database()) AND starts_with(table_name, '${LOADING_TABLE_PREFIX}')`;

// The most values a load leaves uncommitted, about 160 MB of numbers, within the store's 256 MB memory limit.
const BATCH_VALUE_LIMIT = 20_000_000;

// The records in one of DuckDB's row groups. A load that commits whole row groups has DuckDB write them straight into
// the database file. A smaller commit goes through the write-ahead log first and is written again from there.
const ROW_GROUP_RECORD_COUNT = 122_880;

const LIKE_ESCAPE_SQL = String.raw`ESCAPE '\'`; // Lets a filter match '%' and '_' as written, not as wildcards.

const MICROSECONDS_PER_MILLISECOND = 1000;
const MILLISECONDS_PER_DAY = 86_400_000;
const MAX_ASCII_CODE = 0x7f;
const MAX_BYTES_PER_CHARACTER = 3; // The most bytes one UTF-16 unit takes in UTF-8.
const INITIAL_BYTES_PER_TEXT = 16; // The room a column of text starts with for each value, before it grows.

// ── Actions ──────────────────────────────────────────────────────────────────────────────────────────────────────────

export function constructStoredColumns(columnConfigs: ObjectColumnConfig[]): StoredColumn[] {
    return columnConfigs.map(({ dataSubtypeId, dataTypeId }) => ({ dataSubtypeId, dataTypeId }));
}

export function constructCreateTableSql(path: string, storedColumns: StoredColumn[]): string {
    const columnDefinitions = storedColumns.map((storedColumn, columnIndex) => `${columnName(columnIndex)} ${resolveColumnType(storedColumn)}`);
    return `CREATE TABLE ${constructTableReference(path)} (${columnDefinitions.join(', ')})`;
}

// Records go into DuckDB as Arrow columns, each in the Arrow type of its column in the table, so a chunk is appended to
// the table as it is, with nothing to convert on the way. Each column is built straight from typed arrays, because
// Arrow's own builders compile code at run time, which the app's Content Security Policy forbids.
export function constructArrowTable(chunk: StoreChunk, storedColumns: StoredColumn[]): Table {
    const arrowVectors = Array.isArray(chunk) ? constructArrowVectors(chunk, storedColumns) : constructArrowVectorsFromColumns(chunk, storedColumns);
    const vectors: Record<string, Vector> = {};
    for (const [columnIndex, vector] of arrowVectors.entries()) vectors[columnName(columnIndex)] = vector;
    return new Table(vectors);
}

export function constructStoreQuery(storedColumns: StoredColumn[], options: QueryRecordsOptions): StoreQuery {
    const isGrouped = options.groupColumnIndexes.length > 0;
    const resultColumns = isGrouped ? constructGroupedResultColumns(storedColumns, options) : storedColumns;
    const selections = isGrouped
        ? [...options.groupColumnIndexes.map((columnIndex) => columnName(columnIndex)), ...options.aggregates.map((aggregate) => constructAggregateSql(aggregate, storedColumns))]
        : storedColumns.map((_storedColumn, columnIndex) => columnName(columnIndex));

    const whereParts = options.filters.map((filter) => constructFilterSql(filter, storedColumns));
    const whereSql = whereParts.length > 0 ? ` WHERE ${whereParts.map((part) => part.sql).join(' AND ')}` : '';
    const groupSql = isGrouped ? ` GROUP BY ${options.groupColumnIndexes.map((columnIndex) => columnName(columnIndex)).join(', ')}` : '';
    const baseSql = `SELECT ${selections.join(', ')} FROM ${constructTableReference(options.path)}${whereSql}${groupSql}`;
    const pageSql = `${baseSql}${constructOrderSql(options.sorts, selections.length)} LIMIT ${String(checkCount(options.limit))} OFFSET ${String(checkCount(options.offset))}`;

    return { countSql: `SELECT count(*) FROM (${baseSql})`, pageSql, resultColumns, values: whereParts.flatMap((part) => part.values) };
}

// Dates and timestamps come back from Arrow as milliseconds since 1970, times as bigint microseconds since midnight, and
// whole numbers and counts as bigints. A column of whole numbers beyond ±2^53 keeps its bigints, so no digit is lost.
// Other whole numbers become plain numbers, unless one is too large to be exact.
// eslint-disable-next-line sonarjs/function-return-type -- A store value is one of several types, by its column.
export function fromStoredValue(value: unknown, storedColumn: StoredColumn | undefined): StoreValue {
    if (value == null) return null;
    if (storedColumn?.dataTypeId === 'temporal') return new Date(typeof value === 'bigint' ? Number(value) / MICROSECONDS_PER_MILLISECOND : Number(value));
    if (typeof value === 'bigint') return storedColumn?.dataSubtypeId === 'bigint' || !isSafeBigInt(value) ? value : Number(value);
    return typeof value === 'boolean' || typeof value === 'number' || typeof value === 'string' ? value : null;
}

// Reads one chunk of a table's records in rowid order, with each record's rowid first. A chunk starts after the last
// rowid of the one before, so a chunk far into the table is found as quickly as the first.
export function constructRecordChunkSql(path: string, chunkSize: number, hasAfterRowId: boolean): string {
    const whereSql = hasAfterRowId ? ' WHERE rowid > CAST(? AS BIGINT)' : '';
    return `SELECT rowid, * FROM ${constructTableReference(path)}${whereSql} ORDER BY rowid LIMIT ${String(checkCount(chunkSize))}`;
}

// The opposite of 'resolveColumnType'. Whole numbers of every size are kept as 'BIGINT', so a whole number column reads
// as integers, with any beyond ±2^53 kept exact by 'fromStoredValue'. A column of any other type, from a table the store
// did not create, has no known type, so its type is worked out from its values.
export function identifyColumnType(dataType: string): Pick<ObjectColumnConfig, 'dataSubtypeId' | 'dataTypeId'> {
    switch (dataType) {
        case 'BIGINT':
            return { dataSubtypeId: 'integer', dataTypeId: 'numeric' };
        case 'BOOLEAN':
            return { dataSubtypeId: undefined, dataTypeId: 'boolean' };
        case 'DATE':
            return { dataSubtypeId: 'date', dataTypeId: 'temporal' };
        case 'DOUBLE':
            return { dataSubtypeId: 'decimal', dataTypeId: 'numeric' };
        case 'TIME':
            return { dataSubtypeId: 'time', dataTypeId: 'temporal' };
        case 'TIMESTAMP':
            return { dataSubtypeId: 'dateTime', dataTypeId: 'temporal' };
        case 'VARCHAR':
            return { dataSubtypeId: undefined, dataTypeId: 'string' };
        default:
            return { dataSubtypeId: undefined, dataTypeId: 'unknown' };
    }
}

export function quoteIdentifier(name: string): string {
    return `"${name.replaceAll('"', '""')}"`;
}

// A table in the store's default database is named on its own. One in another database is named with its database.
export function constructTableReference(path: string): string {
    const { databaseName, tableName } = resolveStoreTable(path);
    return databaseName == null ? quoteIdentifier(tableName) : `${quoteIdentifier(databaseName)}.${quoteIdentifier(tableName)}`;
}

// The records a load commits at a time: as many whole row groups as fit 'BATCH_VALUE_LIMIT', or, for a table too wide
// for even one, as many records as fit it.
export function resolveBatchRecordCount(columnCount: number): number {
    const valueCountPerRecord = Math.max(1, columnCount);
    const rowGroupCount = Math.floor(BATCH_VALUE_LIMIT / (ROW_GROUP_RECORD_COUNT * valueCountPerRecord));
    return rowGroupCount > 0 ? rowGroupCount * ROW_GROUP_RECORD_COUNT : Math.max(1, Math.floor(BATCH_VALUE_LIMIT / valueCountPerRecord));
}

// The path of the table a load builds before it takes the object's place, in the object's database.
export function constructLoadingTablePath(path: string): string {
    const { databaseName, tableName } = resolveStoreTable(path);
    const loadingTableName = `${LOADING_TABLE_PREFIX}${tableName}`;
    return databaseName == null ? loadingTableName : `/${databaseName}/${loadingTableName}`;
}

// The database a path names, undefined for the default database, which DuckDB opens with the store rather than attaches.
export function resolveStoreTable(path: string): { databaseName: string | undefined; tableName: string } {
    const { databaseName, tableName } = resolveStorePath(path);
    return { databaseName: databaseName === DEFAULT_DATABASE_NAME ? undefined : databaseName, tableName };
}

// ── Helpers ──────────────────────────────────────────────────────────────────────────────────────────────────────────

// Builds one column of a chunk: takes each value that is present as the records are read, then gives the column as an
// Arrow vector. A set bit in a validity bitmap marks a value that is present.
interface ColumnBuilder {
    finish: (nullCount: number) => Vector;
    isWritten: (index: number, value: Exclude<StoreValue, null>) => boolean; // Writes the value and says whether it did.
    nullBitmap: Uint8Array;
    presentCount: number;
}

// Each record is read once and its values written into every column, because reading down one column at a time
// visits each record once for every column, which is far slower over records held one by one.
function constructArrowVectors(records: StoreRecord[], storedColumns: StoredColumn[]): Vector[] {
    const { length } = records;
    const builders = storedColumns.map((storedColumn) => constructColumnBuilder(storedColumn, length));
    let index = 0;
    for (const record of records) {
        let columnIndex = 0;
        for (const builder of builders) {
            const value = record[columnIndex++];
            if (value == null || !builder.isWritten(index, value)) continue;
            setBit(builder.nullBitmap, index);
            builder.presentCount++;
        }
        index++;
    }
    return builders.map((builder) => builder.finish(length - builder.presentCount));
}

// A chunk that came as columns needs no rearranging: each column's values are read down its own list. A column of
// decimal numbers that came as a packed block is already as Arrow keeps it, so the block is the column, with no value
// read or copied.
function constructArrowVectorsFromColumns({ columns, recordCount }: StoreColumnChunk, storedColumns: StoredColumn[]): Vector[] {
    return storedColumns.map((storedColumn, columnIndex) => {
        const column = columns[columnIndex] ?? [];
        if (!Array.isArray(column) && storedColumn.dataTypeId === 'numeric' && !isWholeNumberColumn(storedColumn)) {
            return makeVector(makeData({ data: column.values, length: recordCount, nullBitmap: column.validity, nullCount: column.nullCount, type: new Float64() }));
        }
        const values = Array.isArray(column) ? column : Array.from({ length: recordCount }, (_, index) => readPackedNumber(column, index)); // A packed block for a column the store does not keep as decimal numbers, which the engine does not send.
        const builder = constructColumnBuilder(storedColumn, recordCount);
        for (let index = 0; index < recordCount; index++) {
            const value = values[index];
            if (value == null || !builder.isWritten(index, value)) continue;
            setBit(builder.nullBitmap, index);
            builder.presentCount++;
        }
        return builder.finish(recordCount - builder.presentCount);
    });
}

// A packed number as a value, for a column the store does not keep as decimal numbers.
function readPackedNumber({ validity, values }: StorePackedNumberColumn, index: number): number | null {
    const isPresent = ((validity[Math.floor(index / 8)] ?? 0) & (1 << (index % 8))) !== 0;
    return isPresent ? (values[index] ?? null) : null;
}

// A column with no audited type has no values, so it takes none and is all null.
function constructColumnBuilder(storedColumn: StoredColumn, length: number): ColumnBuilder {
    const nullBitmap = new Uint8Array(Math.ceil(length / 8));
    switch (storedColumn.dataTypeId) {
        case 'boolean':
            return constructBooleanBuilder(nullBitmap, length);
        case 'numeric':
            return isWholeNumberColumn(storedColumn) ? constructWholeNumberBuilder(nullBitmap, length) : constructNumberBuilder(nullBitmap, length);
        case 'temporal':
            return constructTemporalBuilder(nullBitmap, length, storedColumn);
        case 'string':
            return constructTextBuilder(nullBitmap, length);
        case 'unknown':
            return {
                finish: () => makeVector(makeData({ data: new Uint8Array(0), length, nullBitmap, nullCount: length, type: new Utf8(), valueOffsets: new Int32Array(length + 1) })),
                isWritten: () => false,
                nullBitmap,
                presentCount: 0
            };
    }
}

function constructBooleanBuilder(nullBitmap: Uint8Array, length: number): ColumnBuilder {
    const data = new Uint8Array(Math.ceil(length / 8));
    return {
        finish: (nullCount) => makeVector(makeData({ data, length, nullBitmap, nullCount, type: new Bool() })),
        isWritten: (index, value) => {
            if (value === true) setBit(data, index);
            return true;
        },
        nullBitmap,
        presentCount: 0
    };
}

// The engine only gives a column of whole numbers whole values within 64 bits, so each fits exactly.
function constructWholeNumberBuilder(nullBitmap: Uint8Array, length: number): ColumnBuilder {
    const data = new BigInt64Array(length);
    return {
        finish: (nullCount) => makeVector(makeData({ data, length, nullBitmap, nullCount, type: new Int64() })),
        isWritten: (index, value) => {
            data[index] = typeof value === 'bigint' ? value : BigInt(Number(value));
            return true;
        },
        nullBitmap,
        presentCount: 0
    };
}

function constructNumberBuilder(nullBitmap: Uint8Array, length: number): ColumnBuilder {
    const data = new Float64Array(length);
    return {
        finish: (nullCount) => makeVector(makeData({ data, length, nullBitmap, nullCount, type: new Float64() })),
        isWritten: (index, value) => {
            data[index] = value instanceof Date ? value.getTime() : Number(value);
            return true;
        },
        nullBitmap,
        presentCount: 0
    };
}

// A date goes in as its day number, a time as the milliseconds of its day, and a date and time as milliseconds, each
// in the Arrow type DuckDB takes straight into a DATE, TIME or TIMESTAMP column. A value that is not a moment in time
// is left null.
function constructTemporalBuilder(nullBitmap: Uint8Array, length: number, { dataSubtypeId }: StoredColumn): ColumnBuilder {
    if (dataSubtypeId === 'date' || dataSubtypeId === 'time') {
        const data = new Int32Array(length);
        const isDate = dataSubtypeId === 'date';
        return {
            finish: (nullCount) =>
                isDate ? makeVector(makeData({ data, length, nullBitmap, nullCount, type: new DateDay() })) : makeVector(makeData({ data, length, nullBitmap, nullCount, type: new TimeMillisecond() })),
            isWritten: (index, value) => {
                const milliseconds = Math.round(value instanceof Date ? value.getTime() : Number(value));
                if (!Number.isFinite(milliseconds)) return false;
                data[index] = isDate ? Math.floor(milliseconds / MILLISECONDS_PER_DAY) : ((milliseconds % MILLISECONDS_PER_DAY) + MILLISECONDS_PER_DAY) % MILLISECONDS_PER_DAY;
                return true;
            },
            nullBitmap,
            presentCount: 0
        };
    }
    const data = new BigInt64Array(length);
    return {
        finish: (nullCount) => makeVector(makeData({ data, length, nullBitmap, nullCount, type: new TimestampMillisecond() })),
        isWritten: (index, value) => {
            const milliseconds = Math.round(value instanceof Date ? value.getTime() : Number(value));
            if (!Number.isFinite(milliseconds)) return false;
            data[index] = BigInt(milliseconds);
            return true;
        },
        nullBitmap,
        presentCount: 0
    };
}

// Arrow text is one run of UTF-8 bytes, with offsets marking where each value starts. The run grows as text arrives,
// and is written into directly. Text of plain ASCII, as most is, is copied a character at a time, which is quicker
// than encoding each value on its own.
function constructTextBuilder(nullBitmap: Uint8Array, length: number): ColumnBuilder {
    const encoder = new TextEncoder();
    const valueOffsets = new Int32Array(length + 1);
    let data = new Uint8Array(length * INITIAL_BYTES_PER_TEXT);
    let byteCount = 0;
    return {
        finish: (nullCount) => {
            for (let index = 0; index < length; index++) valueOffsets[index + 1] = Math.max(valueOffsets[index + 1] ?? 0, valueOffsets[index] ?? 0); // A null takes no bytes, so it ends where the value before it ended.
            return makeVector(makeData({ data: data.subarray(0, byteCount), length, nullBitmap, nullCount, type: new Utf8(), valueOffsets }));
        },
        isWritten: (index, value) => {
            const text = typeof value === 'string' ? value : String(value);
            const neededByteCount = byteCount + text.length * MAX_BYTES_PER_CHARACTER;
            if (neededByteCount > data.length) {
                const largerData = new Uint8Array(neededByteCount * 2);
                largerData.set(data.subarray(0, byteCount));
                data = largerData;
            }
            byteCount = writeText(data, byteCount, text, encoder);
            valueOffsets[index + 1] = byteCount;
            return true;
        },
        nullBitmap,
        presentCount: 0
    };
}

// Writes text at 'byteCount' and gives the count after it.
function writeText(data: Uint8Array, byteCount: number, text: string, encoder: TextEncoder): number {
    let nextByteCount = byteCount;
    for (let index = 0; index < text.length; index++) {
        const code = text.codePointAt(index) ?? 0;
        if (code > MAX_ASCII_CODE) return byteCount + encoder.encodeInto(text, data.subarray(byteCount)).written;
        data[nextByteCount++] = code;
    }
    return nextByteCount;
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

// DuckDB adds up 64-bit whole numbers as 128-bit ones, which Arrow returns in a form the store cannot read. The sum is
// turned back into a 64-bit whole number, and one too large for that is null rather than an error.
function constructAggregateSql(aggregate: QueryAggregate, storedColumns: StoredColumn[]): string {
    if (aggregate.functionId === 'count') return 'count(*)';
    if (aggregate.columnIndex == null) throw new Error(`Aggregate '${aggregate.functionId}' needs a column.`);
    const name = columnName(aggregate.columnIndex);
    const storedColumn = storedColumns[aggregate.columnIndex];
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
            return storedColumn != null && isWholeNumberColumn(storedColumn) ? `TRY_CAST(sum(${name}) AS BIGINT)` : `sum(${name})`;
    }
}

// Milliseconds since 1970 as the column's own date type.
function constructTemporalSql(millisecondsSql: string, storedColumn: StoredColumn | undefined): string {
    const timestampSql = `epoch_ms(CAST(${millisecondsSql} AS BIGINT))`;
    if (storedColumn?.dataSubtypeId === 'date') return `CAST(${timestampSql} AS DATE)`;
    return storedColumn?.dataSubtypeId === 'time' ? `CAST(${timestampSql} AS TIME)` : timestampSql;
}

// Text matches ignore case, as SQLite's do. A date is passed as milliseconds and turned back into its column's type.
// DuckDB passes values as JSON, which has no bigints, so a bigint is passed as text and turned back into a whole number.
function constructFilterSql(filter: QueryFilter, storedColumns: StoredColumn[]): { sql: string; values: QueryValue[] } {
    const name = columnName(checkIndex(filter.columnIndex, storedColumns.length));
    const valueSql = constructFilterValueSql(filter.value, storedColumns[filter.columnIndex]);
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
    return text
        .replaceAll('\\', '\\\\')
        .replaceAll('%', String.raw`\%`)
        .replaceAll('_', String.raw`\_`);
}

function setBit(bitmap: Uint8Array, index: number): void {
    const byteIndex = Math.floor(index / 8);
    bitmap[byteIndex] = (bitmap[byteIndex] ?? 0) | (1 << (index % 8));
}

// Whole numbers are stored as 64-bit integers, so every one the engine reads as a number is exact. 'BIGINT' rather than
// the smallest type that fits, because a partial audit can miss larger values, and DuckDB compresses small values in a
// 'BIGINT' column anyway. Other numbers are doubles, which hold any value written with up to 17 significant digits.
function resolveColumnType(storedColumn: StoredColumn): string {
    switch (storedColumn.dataTypeId) {
        case 'boolean':
            return 'BOOLEAN';
        case 'numeric':
            return isWholeNumberColumn(storedColumn) ? 'BIGINT' : 'DOUBLE';
        case 'temporal':
            return resolveTemporalColumnType(storedColumn);
        case 'string':
        case 'unknown':
            return 'VARCHAR'; // Every value of a column with no audited type is null.
    }
}

function resolveTemporalColumnType(storedColumn: StoredColumn): string {
    if (storedColumn.dataSubtypeId === 'date') return 'DATE';
    return storedColumn.dataSubtypeId === 'time' ? 'TIME' : 'TIMESTAMP';
}

function constructFilterValueSql(value: StoreValue, storedColumn: StoredColumn | undefined): string {
    if (value instanceof Date) return constructTemporalSql('?', storedColumn);
    return typeof value === 'bigint' ? 'CAST(? AS BIGINT)' : '?';
}

function isSafeBigInt(value: bigint): boolean {
    return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER);
}

function isWholeNumberColumn(storedColumn: StoredColumn): boolean {
    return storedColumn.dataSubtypeId === 'integer' || storedColumn.dataSubtypeId === 'bigint';
}

// eslint-disable-next-line sonarjs/function-return-type -- A bound value is one of several types, by its column.
function toQueryValue(value: StoreValue): QueryValue {
    if (value instanceof Date) return value.getTime();
    return typeof value === 'bigint' ? String(value) : value;
}
