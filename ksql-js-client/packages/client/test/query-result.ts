import * as assert from 'assert';

import { stream } from '@thi.ng/rstream';

import { createPendingQueryResult } from '../src/query-result';

import {
    MessageFrame,
    DataFrame,
    FlowFrame,
    CloseFrame,
    decodeBuffer,
} from '../src/protocol';

import {
    Frame,
    QueryResult,
} from '../src/api';


describe('QueryResult', () => {

    const messageFrame = (channelId = 0) => new MessageFrame(channelId, Buffer.from(JSON.stringify({
        'query-id': 0,
        'cols': ['k', 'v'],
        'col-types': ['STRING', 'STRING']
    })));

    const dataFrame = (channelId, payload) => new DataFrame(channelId, Buffer.from(JSON.stringify(payload)));

    it('resolves after reading MessageFrame from input', (done) => {
        const src = stream<Frame<any>>();
        createPendingQueryResult(0, src, (_) => { })
            .then((_) => {
                done();
            }, done);

        src.next(messageFrame());
    });

    it('delivers rows', (done) => {
        const rowValues = [['foo', 'bar'], ['baz', 'quux']];
        const src = stream<Frame<any>>();

        createPendingQueryResult(0, src, (bs) => { })
            .then(async (query) => {
                try {
                    let i = 0;
                    for await (const row of query) {
                        assert.deepStrictEqual(row.values, rowValues[i]);
                        if (++i == rowValues.length) {
                            done();
                        }
                    }
                } catch (err) {
                    done(err);
                }


            }, done);

        src.next(messageFrame());
        src.next(dataFrame(0, rowValues[0]));
        src.next(dataFrame(0, rowValues[1]));
    });

    it('writes flow frames', (done) => {
        const dataFrames = [['foo', 'bar'], ['baz', 'quux']].map((payload) => dataFrame(0, payload));
        const src = stream<Frame<any>>();
        let written = new Uint8Array();

        createPendingQueryResult(0, src, (bs: Uint8Array) => { written = bs; })
            .then(async (query) => {
                try {
                    let i = 0;
                    for await (const row of query) {
                        assert.equal((<FlowFrame>decodeBuffer(written)).val, dataFrames[i].bytes.length);
                        if (++i == dataFrames.length) {
                            done();
                        }
                    }
                } catch (err) {
                    done(err);
                }


            }, done);

        src.next(messageFrame());
        src.next(dataFrames[0]);
        src.next(dataFrames[1]);
    });


    it('cleans up on close frame', (done) => {
        const dataFrames = [['foo', 'bar'], ['baz', 'quux']].map((payload) => dataFrame(0, payload));
        const src = stream<Frame<any>>();

        createPendingQueryResult(0, src, (_) => { })
            .then(async (query) => {
                try {
                    let i = 0;
                    for await (const row of query) {
                        i++;
                    }
                    assert.equal(i, dataFrames.length)
                    done();
                } catch (err) {
                    done(err);
                }


            }, done);

        src.next(messageFrame());
        src.next(dataFrames[0]);
        src.next(dataFrames[1]);
        src.next(new CloseFrame(0));
    });


    it('cleans up on async iterator break out', (done) => {
        const dataFrames = [['foo', 'bar'], ['baz', 'quux']].map((payload) => dataFrame(0, payload));
        const src = stream<Frame<any>>();
        let written = new Uint8Array();

        createPendingQueryResult(0, src, (_) => { })
            .then(async (query) => {
                try {
                    let i = 0;
                    for await (const row of query) {
                        if (++i == 1) {
                            break;
                        }
                    }
                    assert(query.closed);
                    done();
                } catch (err) {
                    done(err);
                }


            }, done);

        src.next(messageFrame());
        src.next(dataFrames[0]);
        src.next(dataFrames[1]);
    });


});
