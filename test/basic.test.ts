import test from 'node:test';
import assert from 'node:assert';
import { SchemaType, DataFlowType, InvocationType, StaticCapabilities, PERMISSIONS } from '@tak-ps/etl';

// task.ts calls Task.init() at module scope which requires an ETL environment,
// so these must be set before the dynamic import below
process.env.ETL_API = process.env.ETL_API || 'http://localhost:5001';
process.env.ETL_LAYER = process.env.ETL_LAYER || '1';
process.env.ETL_TOKEN = process.env.ETL_TOKEN || 'etl.test-token';

const { default: Task } = await import('../task.js');

test('Task static config', () => {
    assert.equal(Task.name, 'etl-geonet-quakes');
    assert.deepEqual(Task.flow, [DataFlowType.Incoming]);
    assert.deepEqual(Task.invocation, [InvocationType.Schedule]);
});

test('Incoming Input schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Incoming);

    assert.equal(schema.type, 'object');
    for (const key of [
        'MMI',
        'Max Age Minutes',
        'Include Shaking Contours',
        'Minimum Contour MMI'
    ]) {
        assert.ok(schema.properties[key], `Env schema missing property: ${key}`);
    }

    assert.equal(schema.properties['MMI'].type, 'string');
    assert.equal(schema.properties['MMI'].default, '5');
    assert.equal(schema.properties['Max Age Minutes'].type, 'string');
    assert.equal(schema.properties['Max Age Minutes'].default, '10080');
    assert.equal(schema.properties['Include Shaking Contours'].type, 'boolean');
    assert.equal(schema.properties['Include Shaking Contours'].default, false);
    assert.equal(schema.properties['Minimum Contour MMI'].type, 'string');
    assert.equal(schema.properties['Minimum Contour MMI'].default, '3');
});

test('Incoming Output schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Output, DataFlowType.Incoming);

    assert.equal(schema.type, 'object');
    for (const key of [
        'publicID',
        'timeUTC',
        'timeLocal',
        'depth',
        'magnitude',
        'mmi',
        'locality',
        'quality',
        'intensity'
    ]) {
        assert.ok(schema.properties[key], `Output schema missing property: ${key}`);
    }

    assert.equal(schema.properties.magnitude.type, 'number');
    assert.equal(schema.properties.publicID.type, 'string');
});

test('Outgoing flow is not provided', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Outgoing);

    assert.deepEqual(schema.properties, {});
});

test('capabilities.json is a valid manifest matching the task', async () => {
    const doc = await StaticCapabilities.read(new URL('../capabilities.json', import.meta.url).pathname);

    assert.equal(doc.name, 'GeoNet Quakes');
    assert.ok(doc.permissions.length > 0);

    for (const permission of doc.permissions) {
        // Resources are expressed as <permission>:<level>, where <level> may be a wildcard
        const [name, level] = permission.resource.split(':');
        assert.ok(PERMISSIONS[name], `Unknown permission: ${permission.resource}`);
        assert.ok(level === '*' || PERMISSIONS[name].includes(level), `Unknown permission level: ${permission.resource}`);
    }

    assert.equal(doc.invocations.incoming?.schedule?.default.schedule, 'rate(2 minutes)');
});
