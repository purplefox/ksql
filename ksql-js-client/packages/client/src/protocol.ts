import { BitInputStream, BitOutputStream } from '@thi.ng/bitstream';
import { FrameHandler, RequestType } from './api';

export const MAGIC_NUMBER = 1802727788;
export const VERSION = 1;

export const enum FrameType {
    REQ = 1,
    MSG = 2,
    DAT = 3,
    FLO = 4,
    CLS = 5,
}

const createFrame = (channelId: number, frameType: FrameType) => {
    const frame = new BitOutputStream();
    // headers
    frame.writeWords([MAGIC_NUMBER, VERSION, channelId, frameType], 32);
    return frame;
};

export const encodeMessageFrame = (channelId: number, message: object) => {
    // TODO: polyfill Buffer.from for browsers
    const payload = Buffer.from(JSON.stringify(message));
    const frame = createFrame(channelId, FrameType.MSG);
    frame.writeWords(payload, 8);
    return frame.bytes();
};

export const encodeDataFrame = (channelId: number, payload: Buffer) => {
    const frame = createFrame(channelId, FrameType.DAT);
    frame.writeWords(payload, 8);
    return frame.bytes();
}

export const encodeFlowFrame = (channelId: number, bytes: number) => {
    const frame = createFrame(channelId, FrameType.FLO);
    frame.write(bytes, 32);
    return frame.bytes();
};

export const encodeCloseFrame = (channelId: number) => {
    const frame = createFrame(channelId, FrameType.CLS);
    return frame.bytes();
};

export const encodeRequestFrame = (
    channelId: number,
    requestType: RequestType,
    message: object
) => {
    const payload = Buffer.from(JSON.stringify(message));
    const frame = createFrame(channelId, FrameType.REQ);
    frame.write(requestType, 16);
    frame.writeWords(payload, 8);
    return frame.bytes();
};

export const decodeBuffer = (handler: FrameHandler, buffer: Uint8Array) => {
    if (buffer.length < 16) {
        throw new Error('Not enough data');
    }

    const rdr = new BitInputStream(buffer);

    const [
        magic,
        version,
        channelId,
        frameType,
    ] = rdr.readWords(4, 32);

    if (magic !== MAGIC_NUMBER) {
        throw new Error('Bytes lack ksqlDB preamble');
    }

    if (version !== VERSION) {
        throw new Error('Unsupported version');
    }

    switch (frameType) {
        case FrameType.MSG:
            handler.handleMessageFrame(channelId, buffer.subarray(16));
            break;

        case FrameType.DAT:
            handler.handleDataFrame(channelId, buffer.subarray(16));
            break;

        case FrameType.FLO:
            const val = rdr.read(32);
            handler.handleFlowFrame(channelId, val);
            break;

        case FrameType.CLS:
            handler.handleCloseFrame(channelId);
            break;

        case FrameType.REQ:
            const requestType: RequestType = rdr.read(16);
            handler.handleRequestFrame(channelId, requestType, buffer.subarray(18));
            break;

        default:
            throw new Error(`Unhandled frame type: ${frameType}`);
    }
};
