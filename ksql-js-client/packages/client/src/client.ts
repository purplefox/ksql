import { Stream, PubSub, pubsub, Subscription } from '@thi.ng/rstream';
import { map, filter } from '@thi.ng/transducers';
import {
    KsqlDBClient,
    KsqlDBConnection,
    Frame,
    RequestType,
    OutputWriter,
} from './api';
import { RequestFrame, decodeBuffer, DataFrame, FlowFrame } from './protocol';
import { createPendingQueryResult } from './query-result';
import { createWebsocketConnection } from './websocket';


class InsertHandler {
    writeWindowSize = 1024 * 1024
    pendingInserts: [Buffer, Function][] = []

    constructor(
        private channelId: number,
        private target: string,
        private inputChannel: Subscription<Frame<any>, Frame<any>>,
        private writeOutput: OutputWriter,
    ) {
        inputChannel.subscribe({
            next: (frame: FlowFrame) => {
                this.writeWindowSize += frame.val;
                this.checkSendOutgoing();
            }
        }, filter((x) => x instanceof FlowFrame));
    }

    sendInsertData(data: object): Promise<void> {
        return new Promise((resolve, reject) => {
            const buffer = Buffer.from(JSON.stringify(data));
            if (this.writeWindowSize >= buffer.length) {
                this.deliver(buffer, resolve);
            } else {
                this.pendingInserts.push([buffer, resolve]);
            }
        });
    }

    checkSendOutgoing() {
        while (this.pendingInserts.length) {
            const [buffer, resolve] = this.pendingInserts.shift();
            if (this.writeWindowSize >= buffer.length) {
                this.deliver(buffer, resolve);
            } else {
                break;
            }
        }
    }

    deliver(buffer: Buffer, resolve: Function) {
        this.writeWindowSize -= buffer.length;
        this.writeOutput(DataFrame.encode({ channelId: this.channelId, payload: buffer }))
        resolve();
    }
}

class Connection implements KsqlDBConnection {
    inputChannels: PubSub<Uint8Array, Frame<any>>
    nextChannelId: number = 0
    insertHandlerByTarget: Map<string, InsertHandler> = new Map

    constructor(
        private inputStream: Stream<Uint8Array>,
        private writeOutput: OutputWriter,
    ) {
        this.inputChannels = pubsub({
            xform: map(decodeBuffer),
            topic: (frame) => frame.channelId,
        });

        this.inputStream.subscribe(this.inputChannels);
    }

    streamQuery(query: string, pull?: boolean, mapRow?: boolean) {
        const channelId = this.nextChannelId++;
        const inputChannel: Subscription<Frame<any>, Frame<any>> = this.inputChannels.subscribeTopic(channelId);

        const pendingQueryResult = createPendingQueryResult(channelId, inputChannel, this.writeOutput, mapRow);

        const message = {
            type: 'query',
            'channel-id': channelId,
            query,
            pull
        };

        this.writeOutput(RequestFrame.encode({
            channelId,
            requestType: RequestType.QUERY,
            message
        }));

        return pendingQueryResult;
    }

    async executeQuery(query: string, mapRow?: boolean) {
        const queryResult = await this.streamQuery(query, true, mapRow);
        return queryResult.gather();
    }

    insertInto(target: string, row: object) {
        let handler = this.insertHandlerByTarget.get('target');

        if (!handler) {
            const channelId = this.nextChannelId++;
            const inputChannel: Subscription<Frame<any>, Frame<any>> = this.inputChannels.subscribeTopic(channelId);

            handler = new InsertHandler(channelId, target, inputChannel, this.writeOutput);
            this.insertHandlerByTarget.set(target, handler);

            const message = {
                type: 'insert',
                'channel-id': channelId,
                target,
            };

            this.writeOutput(RequestFrame.encode({
                channelId,
                requestType: RequestType.INSERT,
                message
            }));
        }

        return handler.sendInsertData(row);
    }
}

class DefaultKsqlDBClient implements KsqlDBClient {

    async connectWebsocket(endpoint: string): Promise<KsqlDBConnection> {
        const [inputStream, writeOutput] = await createWebsocketConnection(endpoint);
        return new Connection(inputStream, writeOutput);
    }
}

export const createKsqlDBConnection = (
    endpoint: string
): Promise<KsqlDBConnection> =>
    (new DefaultKsqlDBClient).connectWebsocket(endpoint);
