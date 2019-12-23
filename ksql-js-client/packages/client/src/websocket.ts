import { stream, Stream } from '@thi.ng/rstream'
import { OutputWriter } from './api';

interface WebSocketLike extends Partial<WebSocket> {
    new(endpoint: string): WebSocket
}

let WebSocketImpl: WebSocketLike;
if (typeof global.process !== 'undefined') {
    WebSocketImpl = require('ws');
} else {
    WebSocketImpl = WebSocket;
}

const wsStreamSource = (ws: WebSocket) => {
    return stream<Uint8Array>((src) => {
        ws.onmessage = (evt) => {
            src.next(new Uint8Array(evt.data as ArrayBuffer));
        };
        ws.onclose = () => src.done();
    });
};

export const createWebsocketConnection = (
    endpoint: string
): Promise<[Stream<Uint8Array>, OutputWriter]> => {

    return new Promise((resolve, _) => {
        const ws = new WebSocketImpl(endpoint);
        ws.binaryType = 'arraybuffer';

        ws.onopen = () => {
            const inputStream = wsStreamSource(ws);
            const writeOutput: OutputWriter = (bs) => { ws.send(bs); };
            resolve([inputStream, writeOutput]);
        };
    });
}
