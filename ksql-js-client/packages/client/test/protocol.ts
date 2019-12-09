import * as assert from 'assert';

import {
    MAGIC_NUMBER,
    VERSION,
    FrameType,
    encodeMessageFrame,
    encodeDataFrame,
    encodeFlowFrame,
    encodeCloseFrame,
    encodeRequestFrame,
    decodeBuffer,
} from '../src/protocol';

import { FrameHandler, RequestType } from '../src/api';

const assertHeaders = (bs: Uint8Array, channelId: number, frameType: FrameType) => {
    const frameBuf = Buffer.from(bs);
    assert.equal(MAGIC_NUMBER, frameBuf.readInt32BE(0))
    assert.equal(VERSION, frameBuf.readInt32BE(4));
    assert.equal(channelId, frameBuf.readInt32BE(8));
    assert.equal(frameType, frameBuf.readInt32BE(12));
}

const elideHeaders = (bs: Uint8Array) => {
    return bs.slice(16);
}

describe('encoding', () => {

    describe('messageFrame', () => {
        const channelId = 42;
        const payload = [5, 'Book', 9.99];
        const frame = encodeMessageFrame(channelId, payload);

        it('has headers', () => {
            assertHeaders(frame, channelId, FrameType.MSG);
        });

        it('payload matches sample from java api prototype impl', () => {
            // [5, 'Book', 9.99]
            const sampled = [91, 53, 44, 34, 66, 111, 111, 107, 34, 44, 57, 46, 57, 57, 93];
            assert.deepStrictEqual(sampled, [...elideHeaders(frame).values()]);
        });

        it('payload bytes are utf8-encoded json', () => {
            const ex = Buffer.from(JSON.stringify(payload));
            assert.deepEqual(
                [...ex.values()],
                [...elideHeaders(frame).values()]
            )
        });
    });

    describe('dataFrame', () => {
        const channelId = 1;
        const payload = Buffer.from('ksqlDB');
        const frame = encodeDataFrame(channelId, payload);

        it('has headers', () => {
            assertHeaders(frame, channelId, FrameType.DAT);
        });

        it('payload is tail bytes', () => {
            assert.deepEqual(
                [...payload.values()],
                [...elideHeaders(frame).values()]
            );
        });
    });

    describe('flowFrame', () => {
        const channelId = 1;
        const frame = encodeFlowFrame(channelId, 0xeeeeee);

        it('has headers', () => {
            assertHeaders(frame, channelId, FrameType.FLO);
        });

        it('has int32 payload', () => {
            const buf = Buffer.from(frame);
            assert.equal(0xeeeeee, buf.readInt32BE(16));
        });
    });

    describe('closeFrame', () => {
        const channelId = 1;
        const frame = encodeCloseFrame(channelId);

        it('has headers', () => {
            assertHeaders(frame, channelId, FrameType.CLS);
        });
    });

    describe('requestFrame', () => {
        const channelId = 0;
        const payload = { diaphonous: { damask: [1] } };
        const frame = encodeRequestFrame(channelId, RequestType.QUERY, payload);

        it('has headers', () => {
            assertHeaders(frame, channelId, FrameType.REQ);
        });

        it('encodes (short) request type', () => {
            const buf = Buffer.from(elideHeaders(frame));
            assert.equal(RequestType.QUERY, buf.readInt16BE(0));
        })

        it('payload bytes are utf8-encoded json', () => {
            const ex = Buffer.from(JSON.stringify(payload));
            assert.deepEqual(
                [...ex.values()],
                [...elideHeaders(frame).values()].slice(2) // skip request type
            )
        });
    });
});


describe('decoding', () => {

    const noopHandler: FrameHandler = {
        handleMessageFrame: (channelId: number, buffer: Uint8Array) => { },
        handleDataFrame: (channelId: number, buffer: Uint8Array) => { },
        handleFlowFrame: (channelId: number, bytes: number) => { },
        handleCloseFrame: (channelId: number) => { },
        handleRequestFrame: (channelId: number, requestType: RequestType, buffer: Uint8Array) => { },
    };

    it('handles Message frames', (handleMessageFrame) => {
        const handler = {
            ...noopHandler,
            handleMessageFrame,
        };

        decodeBuffer(handler, encodeMessageFrame(0, {}));
    });

    it('handles Data frames', (handleDataFrame) => {
        const handler = {
            ...noopHandler,
            handleDataFrame,
        };

        decodeBuffer(handler, encodeDataFrame(0, Buffer.alloc(1)));
    });

    it('handles Flow frames', (handleFlowFrame) => {
        const handler = {
            ...noopHandler,
            handleFlowFrame,
        };

        decodeBuffer(handler, encodeFlowFrame(0, 99));
    });

    it('handles Close frames', (handleCloseFrame) => {
        const handler = {
            ...noopHandler,
            handleCloseFrame,
        };

        decodeBuffer(handler, encodeCloseFrame(0));
    });

    it('handles Request frames', (handleRequestFrame) => {
        const handler = {
            ...noopHandler,
            handleRequestFrame,
        };

        decodeBuffer(handler, encodeRequestFrame(0, RequestType.INSERT, {}));
    });
})
