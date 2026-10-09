import test from 'node:test';
import assert from 'node:assert';
import { mock } from 'node:test';
import { createServer } from 'node:http';
import * as realEtl from '@tak-ps/etl';

// A minimal stand-in CloudTAK API: just enough of the TaskLayer shape for
// Task.init()/env()/submit() to succeed, with 'Include Shaking Contours'
// left at its default (false is only applied when the key is omitted
// from `environment`, so we omit it here deliberately).
const fakeLayer = {
    id: 1,
    created: new Date().toISOString(),
    updated: new Date().toISOString(),
    connection: null,
    username: null,
    uuid: 'test-uuid',
    name: 'etl-geonet-quakes',
    description: 'test layer',
    enabled: true,
    logging: false,
    task: 'etl-geonet-quakes',
    memory: 128,
    timeout: 60,
    priority: 'off',
    alarm_period: 60,
    alarm_evals: 1,
    alarm_points: 1,
    incoming: {
        layer: 1,
        created: new Date().toISOString(),
        updated: new Date().toISOString(),
        enabled_styles: false,
        styles: {},
        data: null,
        cron: null,
        ephemeral: {},
        webhooks: false,
        environment: {
            'MMI': '-1',
            'Max Age Minutes': '10080',
            'Include Shaking Contours': false,
            'Minimum Contour MMI': '3'
        },
        config: {}
    }
};

const server = createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/api/layer/1') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify(fakeLayer));
        return;
    }

    if (req.method === 'POST' && req.url?.startsWith('/api/layer/1/cot')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok' }));
        return;
    }

    res.writeHead(404);
    res.end();
});

await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
const address = server.address();
const port = typeof address === 'object' && address ? address.port : 0;

// Runs in its own process under `node --test` so the mock below does not
// collide with the other contours-fetch-*.test.ts scenario files.
process.env.ETL_API = `http://127.0.0.1:${port}`;
process.env.ETL_LAYER = '1';
process.env.ETL_TOKEN = 'etl.test-token';

let contourCalls = 0;

const { default: realDefault, ...realNamed } = realEtl;
mock.module('@tak-ps/etl', {
    defaultExport: realDefault,
    namedExports: {
        ...realNamed,
        fetch: async (url: string) => {
            if (typeof url === 'string' && url.includes('shakinglayers.geonet.org.nz')) {
                contourCalls++;
            }
            if (typeof url === 'string' && url.includes('api.geonet.org.nz/quake')) {
                // A single pushable quake - if contours were (incorrectly)
                // fetched despite the toggle being off, contourCalls would
                // be > 0 below.
                return {
                    ok: true,
                    json: async () => ({
                        features: [{
                            type: 'Feature',
                            properties: {
                                publicID: '2026p742111',
                                time: new Date().toISOString(),
                                depth: 10,
                                magnitude: 5,
                                mmi: 5,
                                locality: 'Test Locality',
                                quality: 'best'
                            },
                            geometry: { type: 'Point', coordinates: [174.0, -41.0] }
                        }]
                    })
                };
            }
            return { ok: true, json: async () => ({ features: [] }) };
        }
    }
});

const { default: Task } = await import('../task.js');

test('Include Shaking Contours defaults to false, so control() makes zero contour fetches', async () => {
    const task = await Task.init();
    await task.control();

    assert.equal(contourCalls, 0, 'no contour fetches should occur when the toggle is off');

    server.close();
});
