import { Connector } from '@/index';
import { afterEach, describe, expect, it, vi } from 'vitest';

describe('Connector', () => {
    afterEach(() => {
        vi.unstubAllGlobals();
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

    describe('listNodes', () => {
        it('lists each database, the default one included, as a folder at the top', async () => {
            const { connector } = constructListingConnector({ default: ['orders'], empty: [], sales: ['invoices', 'returns'] });

            const result = await connector.listNodes({ folderPath: '' });

            expect(result).toEqual({ cursor: undefined, connectionNodeConfigs: expect.any(Array) as unknown, isMore: false, totalCount: 3 });
            expect(result.connectionNodeConfigs.map(({ childCount, folderPath, id, name, typeId }) => ({ childCount, folderPath, id, name, typeId }))).toEqual([
                { childCount: 1, folderPath: '', id: 'default', name: 'default', typeId: 'folder' },
                { childCount: 0, folderPath: '', id: 'empty', name: 'empty', typeId: 'folder' },
                { childCount: 2, folderPath: '', id: 'sales', name: 'sales', typeId: 'folder' }
            ]);
        });

        it("lists the default database's tables without attaching it, because it is open from the start", async () => {
            const { connector, registerOPFSFileName } = constructListingConnector({ default: ['orders'] });

            const result = await connector.listNodes({ folderPath: '/default' });

            expect(result.connectionNodeConfigs).toEqual([expect.objectContaining({ folderPath: '/default', id: '/default/orders', name: 'orders', typeId: 'object' })]);
            expect(registerOPFSFileName).not.toHaveBeenCalled();
        });

        it("lists a database's tables, each identified by its path, attaching the database once", async () => {
            const { connector, registerOPFSFileName } = constructListingConnector({ default: [], sales: ['invoices', 'returns'] });

            const result = await connector.listNodes({ folderPath: '/sales' });
            await connector.listNodes({ folderPath: '/sales' });

            expect(result.connectionNodeConfigs).toEqual([
                expect.objectContaining({ extension: undefined, folderPath: '/sales', id: '/sales/invoices', label: 'invoices', name: 'invoices', typeId: 'object' }),
                expect.objectContaining({ extension: undefined, folderPath: '/sales', id: '/sales/returns', label: 'returns', name: 'returns', typeId: 'object' })
            ]);
            expect(result.totalCount).toBe(2);
            expect(registerOPFSFileName).toHaveBeenCalledExactlyOnceWith('opfs://sales.duckdb');
        });

        it('lists only the empty default database in a new store', async () => {
            const { connector } = constructListingConnector({ default: [] });

            const result = await connector.listNodes({ folderPath: '' });

            expect(result.connectionNodeConfigs).toEqual([expect.objectContaining({ childCount: 0, id: 'default', typeId: 'folder' })]);
            expect(result.totalCount).toBe(1);
        });

        it('refuses a database with no file, without creating one', async () => {
            const { connector, registerOPFSFileName } = constructListingConnector({ default: [] });

            await expect(connector.listNodes({ folderPath: '/missing' })).rejects.toThrow("Database 'missing' does not exist.");
            expect(registerOPFSFileName).not.toHaveBeenCalled();
        });

        it('refuses a folder path that does not name one database', async () => {
            const { connector } = constructListingConnector({ default: [] });

            await expect(connector.listNodes({ folderPath: '/sales/returns' })).rejects.toThrow("Encountered invalid folder path '/sales/returns'.");
        });

        it('opens a closed store before listing it', async () => {
            const { connector } = constructListingConnector({ default: ['orders'] });
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
});

// DuckDB keeps its databases in the browser's private file system, so a folder of named files and a connection that
// answers with the tables given stand in for them. A write-ahead log
// and DuckDB's own folder sit beside the databases, as they do in the browser.
function constructListingConnector(tableNamesByDatabaseName: Record<string, string[]>): { connector: Connector; registerOPFSFileName: ReturnType<typeof vi.fn> } {
    const fileNames = Object.keys(tableNamesByDatabaseName).map((databaseName) => `${databaseName}.duckdb`);
    const entries: [string, { kind: string }][] = [
        ...fileNames.map((fileName): [string, { kind: string }] => [fileName, { kind: 'file' }]),
        ['sales.duckdb.wal', { kind: 'file' }],
        ['duckdb', { kind: 'directory' }]
    ];
    vi.stubGlobal('navigator', {
        storage: {
            getDirectory: () => Promise.resolve({ entries: () => yieldEntries(entries) })
        }
    });
    const registerOPFSFileName = vi.fn(() => Promise.resolve());
    const connection = {
        prepare: () =>
            Promise.resolve({
                close: () => Promise.resolve(),
                query: (databaseName: string | null) => {
                    const tableNames = tableNamesByDatabaseName[databaseName ?? 'default'] ?? [];
                    return Promise.resolve({ getChildAt: () => ({ get: (rowIndex: number) => tableNames[rowIndex] }), numRows: tableNames.length });
                }
            }),
        query: () => Promise.resolve()
    };
    const connector = new Connector({} as never, []);
    connector.openStoreState = { attachedDatabaseNames: new Set(), connection, database: { registerOPFSFileName } } as never;
    return { connector, registerOPFSFileName };
}

async function* yieldEntries(entries: [string, { kind: string }][]): AsyncGenerator<[string, { kind: string }]> {
    for (const entry of entries) {
        await Promise.resolve();
        yield entry;
    }
}
