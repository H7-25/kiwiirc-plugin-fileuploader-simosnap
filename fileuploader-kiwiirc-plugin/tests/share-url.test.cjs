const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

async function fixture(sequence, options = {}) {
    let requests = 0;
    const messages = [];
    const events = [];
    const context = vm.createContext({
        AbortController: options.withoutAbort ? undefined : AbortController,
        atob,
        setTimeout: (fn) => setImmediate(fn),
        clearTimeout: clearImmediate,
        fetch: async (_url, request) => {
            assert.equal(request.method, 'HEAD');
            const next = sequence[Math.min(requests++, sequence.length - 1)];
            if (next === 'network') throw new Error('offline');
            if (next === 'never') return new Promise(() => {});
            if (next === 'timeout') return new Promise((_resolve, reject) => {
                request.signal.addEventListener('abort', () => reject(new Error('timeout')), { once: true });
            });
            return { status: next, headers: { get: () => options.metadata || null } };
        },
    });
    const entry = new vm.SourceTextModule(fs.readFileSync(path.join(__dirname, '../src/handlers/uppy/share-completed-upload-url.js'), 'utf8'), { context });
    await entry.link((specifier) => {
        if (specifier === '@/config.js') return new vm.SourceTextModule('export function getSetting() { return "Uploaded file: %URL%"; }', { context });
        const relative = specifier.replace('@/', '../src/') + '.js';
        return new vm.SourceTextModule(fs.readFileSync(path.join(__dirname, relative), 'utf8'), { context });
    });
    await entry.evaluate();
    const handler = entry.namespace.shareCompletedUploadUrl({ emit: (...args) => events.push(args) });
    const file = {
        size: 4,
        type: 'audio/webm',
        meta: { name: 'voice.webm' },
        kiwiFileUploaderTargetBuffer: { say: (...args) => {
            messages.push(args);
            if (options.failSay) throw new Error('buffer failed');
        } },
    };
    return {
        run: () => handler(file, { uploadURL: 'https://example.test/files/id' }),
        messages,
        events,
        requests: () => requests,
    };
}

test('shares the same URL, audio tag and expiration after successful HEAD', async () => {
    const f = await fixture([200], { metadata: 'expires ' + Buffer.from('12345').toString('base64') });
    await f.run();
    assert.equal(f.requests(), 1);
    assert.equal(f.messages.length, 1);
    assert.equal(f.messages[0][0], 'Uploaded file: https://example.test/files/id/voice.webm');
    const tags = JSON.parse(f.messages[0][1].tags['+kiwiirc.com/fileuploader']);
    assert.equal(tags.type, 'audio/webm');
    assert.equal(tags.expires, 12345);
    assert.equal(f.events.length, 1);
});

test('retries a 423 and a server error, then shares exactly once', async () => {
    const f = await fixture([423, 503, 200]);
    await f.run();
    assert.equal(f.requests(), 3);
    assert.equal(f.messages.length, 1);
    assert.equal(f.events.length, 1);
});

for (const failure of [423, 503, 'network', 'timeout']) {
    test(`bounded retries and URL fallback for ${failure}`, async () => {
        const f = await fixture([failure]);
        await f.run();
        assert.equal(f.requests(), 4);
        assert.equal(f.messages.length, 1);
        assert.equal(f.events.length, 1);
        assert.equal(JSON.parse(f.messages[0][1].tags['+kiwiirc.com/fileuploader']).expires, undefined);
    });
}

for (const status of [401, 403, 404, 410]) {
    test(`preserves permanent refusal ${status}`, async () => {
        const f = await fixture([status]);
        await f.run();
        assert.equal(f.requests(), 1);
        assert.equal(f.messages.length, 0);
        assert.equal(f.events.length, 0);
    });
}

test('preserves legacy HEAD 412 behavior', async () => {
    const f = await fixture([412]);
    await f.run();
    assert.equal(f.messages.length, 1);
});

test('does not retry or duplicate events when the chat buffer itself fails', async () => {
    const f = await fixture([200], { failSay: true });
    await assert.rejects(f.run(), /buffer failed/);
    assert.equal(f.requests(), 1);
    assert.equal(f.events.length, 1);
    assert.equal(f.messages.length, 1);
});

test('timeout remains bounded without AbortController or an abort-aware fetch', async () => {
    const f = await fixture(['never'], { withoutAbort: true });
    await f.run();
    assert.equal(f.requests(), 4);
    assert.equal(f.messages.length, 1);
    assert.equal(f.events.length, 1);
});
