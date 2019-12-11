export type OutputWriter = (bs: Uint8Array) => void;

export const enum FrameType {
    REQ = 1,
    MSG = 2,
    DAT = 3,
    FLO = 4,
    CLS = 5,
}

export const enum RequestType {
    QUERY = 1,
    INSERT = 2,
}

export interface Frame<T> {
    // https://github.com/microsoft/TypeScript/issues/33892
    // static encode(opts: T): Uint8Array;
    frameType: FrameType,
    channelId: number;
    revive(): T;
}

export interface Row {
    columns: string[];
    columnTypes: string[];
    values: any[];
}

export interface QueryResult extends AsyncIterable<Row> {
    gather(): Promise<Row[]>;
    closed: boolean;
}

export interface KsqlDBConnection {
    // executeDDL(command: string): Promise<void>;
    // describe(target: string): Promise<object>;
    // listStreams(): Promise<object[]>;
    // listTopics(): Promise<object[]>;

    streamQuery(query: string, pull?: boolean): Promise<QueryResult>;
    executeQuery(query: string): Promise<Row[]>;
    //insertInto(target: string, row: object): Promise<void>;
}

export interface KsqlDBClient {
    connectWebsocket(endpoint: string): Promise<KsqlDBConnection>;
}
