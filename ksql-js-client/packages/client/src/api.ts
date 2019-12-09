export interface FrameWriter {
    handle(bs: Uint8Array): void;
}

export const enum RequestType {
    QUERY = 1,
    INSERT = 2,
}

export interface FrameHandler {
    handleMessageFrame(channelId: number, buffer: Uint8Array): void;
    handleDataFrame(channelId: number, buffer: Uint8Array): void;
    handleFlowFrame(channelId: number, bytes: number): void;
    handleCloseFrame(channelId: number): void;
    handleRequestFrame(channelId: number, requestType: RequestType, buffer: Uint8Array): void;
}

export interface Row {
    columns: string[];
    columnTypes: string[];
    values: any[];
}

export interface KsqlDBSession {
    streamQuery(query: string, pull: boolean): AsyncIterable<Row>;
    executeQuery(query: string): Promise<Row[]>;
    insertInto(target: string, row: object): Promise<void>;
}

export interface KsqlDBConnection {
    session(): KsqlDBSession;
    executeDDL(command: string): Promise<void>;
    describe(target: string): Promise<object>;
    listStreams(): Promise<object[]>;
    listTopics(): Promise<object[]>;
}
