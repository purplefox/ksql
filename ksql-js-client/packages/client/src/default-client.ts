import {
    KsqlDBClient,
    KsqlDBConnection,
    Frame,
    RequestType,
    OutputWriter,
} from './api';

import {
    RequestFrame,
    decodeBuffer,
} from './protocol';

import { createPendingQueryResult } from './query-result';

import * as WebSocket from 'ws';

import { stream, Stream, PubSub, pubsub, Subscription } from '@thi.ng/rstream';

import { map } from '@thi.ng/transducers';

export const wsStreamSource = (ws: WebSocket) => {
    return stream<Uint8Array>((src) => {
        ws.onmessage = (evt) => {
            src.next(new Uint8Array(evt.data as ArrayBuffer));
        };
        ws.onclose = () => src.done();
    });
};

export class Connection implements KsqlDBConnection {
    inputChannels: PubSub<Uint8Array, Frame<any>>
    nextChannelId: number = 0

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

    streamQuery(query: string, pull?: boolean) {
        const channelId = this.nextChannelId++;
        const inputChannel: Subscription<Frame<any>, Frame<any>> = this.inputChannels.subscribeTopic(channelId);

        const pendingQueryResult = createPendingQueryResult(channelId, inputChannel, this.writeOutput);

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

}

export class DefaultKsqlDBClient implements KsqlDBClient {

    connectWebsocket(endpoint: string): Promise<KsqlDBConnection> {
        return new Promise((resolve, reject) => {
            const ws = new WebSocket(endpoint);
            ws.binaryType = 'arraybuffer';

            ws.on('open', () => {
                const conn = new Connection(wsStreamSource(ws), (bs) => {
                    ws.send(bs);
                    // todo: retry?
                });
                resolve(conn);
            });
        });
    }
}
