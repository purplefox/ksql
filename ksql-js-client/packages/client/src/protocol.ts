import { BitInputStream, BitOutputStream } from '@thi.ng/bitstream';
import { Frame, FrameType, RequestType } from './api';

export const MAGIC_NUMBER = 1802727788;
export const VERSION = 1;

const baseFrame = (channelId: number, frameType: FrameType) => {
    const frame = new BitOutputStream();
    // headers
    frame.writeWords([MAGIC_NUMBER, VERSION, channelId, frameType], 32);
    return frame;
};

type BaseProps = { channelId: number };
type MessageProps = { message: object };

export class MessageFrame implements Frame<MessageProps> {

    static encode(props: BaseProps & MessageProps) {
        const payload = Buffer.from(JSON.stringify(props.message));
        const frame = baseFrame(props.channelId, FrameType.MSG);
        frame.writeWords(payload, 8);
        return frame.bytes();
    }

    frameType = FrameType.MSG
    static frameType = FrameType.MSG

    constructor(public channelId: number, public bytes: Uint8Array) { }

    revive() {
        return {
            message: JSON.parse(Buffer.from(this.bytes).toString()),
        };
    }
}

type DataProps = { payload: Uint8Array };

export class DataFrame implements Frame<DataProps> {

    static encode(props: BaseProps & DataProps) {
        const frame = baseFrame(props.channelId, FrameType.DAT);
        frame.writeWords(props.payload, 8);
        return frame.bytes();
    }

    frameType = FrameType.DAT
    static frameType = FrameType.DAT

    constructor(public channelId: number, public bytes: Uint8Array) { }

    revive() {
        return {
            payload: Buffer.from(this.bytes),
        };
    }
}

type FlowProps = { val: number };

export class FlowFrame implements Frame<FlowProps> {

    static encode(props: BaseProps & FlowProps) {
        const frame = baseFrame(props.channelId, FrameType.FLO);
        frame.write(props.val, 32);
        return frame.bytes();
    }

    frameType = FrameType.FLO
    static frameType = FrameType.FLO

    constructor(public channelId: number, public val: number) { }

    revive() {
        return { val: this.val, };
    }
}

type CloseProps = {};

export class CloseFrame implements Frame<CloseProps> {

    static encode(props: BaseProps & CloseProps) {
        const frame = baseFrame(props.channelId, FrameType.CLS);
        return frame.bytes();
    }

    frameType = FrameType.CLS
    static frameType = FrameType.CLS

    constructor(public channelId: number) { }

    revive() {
        return {};
    }
}

type RequestProps = { requestType: RequestType, message: object };

export class RequestFrame implements Frame<RequestProps> {

    static encode(props: BaseProps & RequestProps) {
        const payload = Buffer.from(JSON.stringify(props.message));
        const frame = baseFrame(props.channelId, FrameType.REQ);
        frame.write(props.requestType, 16);
        frame.writeWords(payload, 8);
        return frame.bytes();
    }

    constructor(public channelId: number, public requestType: RequestType, public bytes: Uint8Array) { }

    frameType = FrameType.REQ
    static frameType = FrameType.REQ

    revive() {
        return {
            requestType: this.requestType,
            message: JSON.parse(Buffer.from(this.bytes).toString()),
        };
    }
}

export const decodeBuffer = (buffer: Uint8Array): Frame<any> => {
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
            return new MessageFrame(channelId, buffer.subarray(16));

        case FrameType.DAT:
            return new DataFrame(channelId, buffer.subarray(16));

        case FrameType.FLO:
            const val = rdr.read(32);
            return new FlowFrame(channelId, val);

        case FrameType.CLS:
            return new CloseFrame(channelId);

        case FrameType.REQ:
            const requestType: RequestType = rdr.read(16);
            return new RequestFrame(channelId, requestType, buffer.subarray(18));

        default:
            throw new Error(`Unhandled frame ${frameType}`);
    }
};
