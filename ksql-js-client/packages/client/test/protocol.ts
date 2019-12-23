import * as assert from 'assert';

import {
    MAGIC_NUMBER,
    VERSION,
    MessageFrame,
    DataFrame,
    FlowFrame,
    CloseFrame,
    RequestFrame,
    decodeBuffer,
} from '../src/protocol';

import { RequestType, FrameType } from '../src/api';

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
        const message = [5, 'Book', 9.99];
        const frame = MessageFrame.encode({ channelId, message });

        it('has headers', () => {
            assertHeaders(frame, channelId, FrameType.MSG);
        });

        it('payload matches sample from java api prototype impl', () => {
            // [5, 'Book', 9.99]
            const sampled = [91, 53, 44, 34, 66, 111, 111, 107, 34, 44, 57, 46, 57, 57, 93];
            assert.deepStrictEqual(sampled, [...elideHeaders(frame).values()]);
        });

        it('payload bytes are utf8-encoded json', () => {
            const ex = Buffer.from(JSON.stringify(message));
            assert.deepEqual(
                [...ex.values()],
                [...elideHeaders(frame).values()]
            )
        });
    });

    describe('dataFrame', () => {
        const channelId = 1;
        const payload = Buffer.from('ksqlDB');
        const frame = DataFrame.encode({ channelId, payload });

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
        const frame = FlowFrame.encode({ channelId, val: 0xeeeeee });

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
        const frame = CloseFrame.encode({ channelId });

        it('has headers', () => {
            assertHeaders(frame, channelId, FrameType.CLS);
        });
    });

    describe('requestFrame', () => {
        const channelId = 0;
        const message = { diaphonous: { damask: [1] } };
        const frame = RequestFrame.encode({ channelId, requestType: RequestType.QUERY, message });

        it('has headers', () => {
            assertHeaders(frame, channelId, FrameType.REQ);
        });

        it('encodes (short) request type', () => {
            const buf = Buffer.from(elideHeaders(frame));
            assert.equal(RequestType.QUERY, buf.readInt16BE(0));
        })

        it('payload bytes are utf8-encoded json', () => {
            const ex = Buffer.from(JSON.stringify(message));
            assert.deepEqual(
                [...ex.values()],
                [...elideHeaders(frame).values()].slice(2) // skip request type
            )
        });
    });
});


describe('decoding', () => {

    it('handles Message frames', () => {
        const message = { foo: 1 }
        const ret = decodeBuffer(MessageFrame.encode({ channelId: 0, message }));
        assert.equal(ret.channelId, 0);
        assert.deepStrictEqual(ret.revive(), { message });
    });

    it('handles Data frames', () => {
        const payload = Buffer.alloc(1);
        const ret = decodeBuffer(DataFrame.encode({ channelId: 0, payload }));
        assert.equal(ret.channelId, 0);
        assert.deepStrictEqual(ret.revive(), { payload });
    });

    it('handles Flow frames', () => {
        const val = 99;
        const ret = decodeBuffer(FlowFrame.encode({ channelId: 0, val }));
        assert.equal(ret.channelId, 0);
        assert.deepStrictEqual(ret.revive(), { val });
    });

    it('handles Close frames', () => {
        const ret = decodeBuffer(CloseFrame.encode({ channelId: 0 }));
        assert.equal(ret.channelId, 0);
    });

    it('handles Request frames', () => {
        const requestType = RequestType.QUERY;
        const message = { foo: false };
        const ret = decodeBuffer(RequestFrame.encode({ channelId: 0, requestType, message }));
        assert.deepStrictEqual(ret.revive(), { requestType, message });
    });
});
