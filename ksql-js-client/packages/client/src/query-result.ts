import {
    Frame,
    Row,
    QueryResult,
    OutputWriter,
} from './api';

import {
    MessageFrame,
    DataFrame,
    FlowFrame,
    CloseFrame,
} from './protocol';

import {
    sync,
    stream,
    Stream,
    Subscription,
    CloseMode
} from '@thi.ng/rstream';

import { filter } from '@thi.ng/transducers';

interface QueryResultHeader {
    columns: string[];
    columnTypes: string[];
}

const parseMessageFrame = (
    messageFrame: MessageFrame
): { queryId: number, header: QueryResultHeader } => {
    const { message } = messageFrame.revive();

    return {
        // TODO: assert structure
        queryId: message['query-id'],
        header: {
            columns: message.cols,
            columnTypes: message['col-types']
        }
    };
}

const createRow = (header: QueryResultHeader, data: DataFrame) => {
    const { payload } = data.revive();
    const values = JSON.parse(payload.toString());
    return { ...header, values };
}

class QueryResultImpl implements QueryResult {
    queryId: number
    promiseStream: Stream<[Function, Function]> = stream<[Function, Function]>()
    fulfilledStream: Subscription<any, any>
    closed: boolean = false

    constructor(
        private channelId: number,
        private inputChannel: Subscription<Frame<any>, Frame<any>>,
        private writeOutput: OutputWriter,
        messageFrame: MessageFrame,
    ) {
        const { queryId, header } = parseMessageFrame(messageFrame);
        this.queryId = queryId;

        const dataStream = inputChannel.transform(
            filter((x) => x instanceof DataFrame)
        );

        this.fulfilledStream = sync({
            src: { promise: this.promiseStream, data: dataStream },
            reset: true,
            all: true,
            backPressure: 1000,
        }).subscribe({
            next: ({ promise, data }: { promise: [Function, Function], data: DataFrame }) => {
                const [resolve, reject] = promise;
                try {
                    const row = createRow(header, data)
                    writeOutput(FlowFrame.encode({ channelId, val: data.bytes.length }))
                    resolve({ value: row, done: false });
                } catch (err) {
                    reject(err);
                }
            }
        });

        inputChannel.subscribe({
            next: (_) => setTimeout(() => this.close()), // defer parent stream close until next tick
        }, filter((x) => x instanceof CloseFrame), {
            closeIn: CloseMode.FIRST
        });
    }

    [Symbol.asyncIterator](): AsyncIterator<Row> {
        return {
            next: () => {
                if (this.closed) {
                    return Promise.resolve({ value: undefined, done: true });
                }

                return new Promise((resolve, reject) => {
                    this.promiseStream.next([resolve, reject]);
                });
            },

            return: () => {
                this.close();
                return Promise.resolve({ value: undefined, done: true });
            }
        }
    }

    close() {
        if (this.closed) {
            return;
        }

        this.closed = true;

        // flush pending
        this.promiseStream.subscribe({
            next: ([resolve]) => resolve({ value: undefined, done: true })
        });

        this.inputChannel.done();
        this.promiseStream.done();
    }
}

export const createPendingQueryResult = (
    channelId: number,
    inputChannel: Subscription<Frame<any>, Frame<any>>,
    writeOutput: OutputWriter
): Promise<QueryResult> => new Promise((resolve, reject) => {
    // await the RequestFrame response    
    // TODO: reject after timeout
    inputChannel.subscribe({
        next: (messageFrame: MessageFrame) => {
            resolve(new QueryResultImpl(channelId, inputChannel, writeOutput, messageFrame))
        }
    }, filter((x) => x instanceof MessageFrame), {
        closeIn: CloseMode.FIRST
    });
});
