import { friendlyUrl } from '@/utils/friendly-url';
import { decodeMetadata } from '@/utils/decode-metadata';

import * as config from '@/config.js';

const headRetryDelays = [200, 500, 1000];
const headTimeout = 2000;

function isTemporaryStatus(status) {
    return [408, 423, 425, 429].includes(status) || status >= 500;
}

// A successful upload URL remains useful when only the optional metadata
// request is temporarily unavailable. Never turn one transient HEAD into a
// silently lost chat message; permanent rejection retains the previous behavior.
export function shareCompletedUploadUrl(kiwiApi) {
    return async function handleUploadSuccess(file, response) {
        const url = friendlyUrl(file, response);
        for (let attempt = 0; attempt <= headRetryDelays.length; attempt++) {
            const controller = typeof AbortController === 'function' ? new AbortController() : null;
            let timeout;
            let headResp;
            try {
                const deadline = new Promise((_resolve, reject) => {
                    timeout = setTimeout(() => {
                        controller?.abort();
                        reject(new Error('Upload metadata request timed out'));
                    }, headTimeout);
                });
                // eslint-disable-next-line no-await-in-loop
                headResp = await Promise.race([
                    fetch(url, {
                        method: 'HEAD',
                        ...(controller ? { signal: controller.signal } : {}),
                    }),
                    deadline,
                ]);
            } catch {
                // Network errors and timeouts use the same bounded retry path.
            } finally {
                clearTimeout(timeout);
            }
            if (headResp) {
                if (headResp.status === 200 || headResp.status === 412) {
                    sendUploadEvent(kiwiApi, url, file, headResp);
                    return;
                }
                if (!isTemporaryStatus(headResp.status)) {
                    return;
                }
            }
            if (attempt < headRetryDelays.length) {
                // eslint-disable-next-line no-await-in-loop
                await new Promise((resolve) => {
                    setTimeout(resolve, headRetryDelays[attempt]);
                });
            }
        }
        sendUploadEvent(kiwiApi, url, file);
    };
}

function sendUploadEvent(kiwiApi, url, file, headResp) {
    let rawMetadata;
    if (headResp) {
        rawMetadata = headResp.headers.get('upload-metadata');
    }

    let metadata = {};
    if (rawMetadata) {
        metadata = decodeMetadata(rawMetadata);
    }

    // emit a global kiwi event
    const event = { url, file, metadata };
    kiwiApi.emit('fileuploader.uploaded', event);

    const buffer = file.kiwiFileUploaderTargetBuffer;
    const tagData = {
        size: file.size,
        type: file.type,
    };
    if (metadata.expires) {
        tagData.expires = parseInt(metadata.expires, 10);
    }

    const msgTemplate = config.getSetting('uploadMessage');
    const newUrl = url.replace('simosnap.com/', 'simosnap.com/files/');
    const message = msgTemplate.replace('%URL%', newUrl);

    buffer.say(message, {
        tags: { '+kiwiirc.com/fileuploader': JSON.stringify(tagData) },
    });
}
