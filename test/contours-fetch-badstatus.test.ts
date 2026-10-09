import test from 'node:test';
import assert from 'node:assert';
import { mock } from 'node:test';
import * as realEtl from '@tak-ps/etl';

// Each contours-fetch-*.test.ts file runs in its own process under
// `node --test`, so mocking '@tak-ps/etl' here does not collide with the
// other scenario files. The mock must be installed before task.ts (which
// imports `fetch` from '@tak-ps/etl') is ever loaded in this process.
process.env.ETL_API = process.env.ETL_API || 'http://localhost:5001';
process.env.ETL_LAYER = process.env.ETL_LAYER || '1';
process.env.ETL_TOKEN = process.env.ETL_TOKEN || 'etl.test-token';

const { default: realDefault, ...realNamed } = realEtl;
mock.module('@tak-ps/etl', {
    defaultExport: realDefault,
    namedExports: {
        ...realNamed,
        fetch: async () => ({ ok: false, status: 400, statusText: 'Bad Request' })
    }
});

const { fetchQuakeContours } = await import('../task.js');

test('fetchQuakeContours returns [] and does not throw on a non-OK response (HTTP 400)', async () => {
    const features = await fetchQuakeContours('doesnotexist');
    assert.deepEqual(features, []);
});
