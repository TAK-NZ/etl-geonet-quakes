import test from 'node:test';
import assert from 'node:assert';
import type { Feature } from 'geojson';

// task.ts calls Task.init() at module scope which requires an ETL environment,
// so these must be set before the dynamic import below
process.env.ETL_API = process.env.ETL_API || 'http://localhost:5001';
process.env.ETL_LAYER = process.env.ETL_LAYER || '1';
process.env.ETL_TOKEN = process.env.ETL_TOKEN || 'etl.test-token';

const { buildContourFeatures } = await import('../task.js');

// fetchQuakeContours (network/error-handling behaviour) is covered by the
// mock.module-based tests in test/contours-fetch-*.test.ts - each lives in
// its own file so every scenario gets a fresh, unmocked module registry
// (node:test's experimental module mocking does not support re-mocking the
// same specifier more than once within a single process/file).

// Shape mirrors the real GNS Science Shaking Layers response: one
// MultiLineString feature per MMI level, half-levels at weight 4, whole
// levels at weight 2.
const SAMPLE_RAW_FEATURES: Feature[] = [
    {
        type: 'Feature',
        properties: { value: 1, units: 'mmi', color: '#ffffff', weight: 2 },
        geometry: { type: 'MultiLineString', coordinates: [[[166.1, -34.9], [166.2, -35.0]]] }
    },
    {
        type: 'Feature',
        properties: { value: 1.5, units: 'mmi', color: '#dfe6ff', weight: 4 },
        geometry: { type: 'MultiLineString', coordinates: [[[166.3, -35.1], [166.4, -35.2]]] }
    },
    {
        type: 'Feature',
        properties: { value: 2, units: 'mmi', color: '#bfccff', weight: 2 },
        geometry: { type: 'MultiLineString', coordinates: [[[166.5, -35.3], [166.6, -35.4]]] }
    },
    {
        type: 'Feature',
        properties: { value: 4.5, units: 'mmi', color: '#7cffc7', weight: 4 },
        geometry: { type: 'MultiLineString', coordinates: [[[166.7, -35.5], [166.8, -35.6]]] }
    }
] as unknown as Feature[];

const STALE_ISO = '2026-10-08T12:00:00.000Z';

test('buildContourFeatures drops features below minMMI', () => {
    const features = buildContourFeatures('2026p742111', SAMPLE_RAW_FEATURES, 3, STALE_ISO) as Array<{ id: string }>;

    assert.equal(features.length, 1);
    assert.equal(features[0].id, 'earthquake-2026p742111-mmi-4.5-1');
});

test('buildContourFeatures maps color/weight to stroke/stroke-width and drops color/weight', () => {
    const features = buildContourFeatures('2026p742111', SAMPLE_RAW_FEATURES, 1, STALE_ISO) as Array<{
        properties: Record<string, unknown>;
    }>;

    for (const feature of features) {
        assert.ok('stroke' in feature.properties, 'missing stroke');
        assert.ok('stroke-width' in feature.properties, 'missing stroke-width');
        assert.equal(feature.properties['stroke-opacity'], 1);
        assert.ok(!('color' in feature.properties), 'color should not leak into properties');
        assert.ok(!('weight' in feature.properties), 'weight should not leak into properties');
    }

    assert.equal(features[0].properties.stroke, '#ffffff');
    assert.equal(features[0].properties['stroke-width'], 2);
    assert.equal(features[1].properties.stroke, '#dfe6ff');
    assert.equal(features[1].properties['stroke-width'], 4);
});

test('buildContourFeatures id format mirrors the quake id convention', () => {
    const features = buildContourFeatures('2026p742111', SAMPLE_RAW_FEATURES, 1, STALE_ISO) as Array<{ id: string }>;

    assert.deepEqual(features.map(f => f.id), [
        'earthquake-2026p742111-mmi-1-1',
        'earthquake-2026p742111-mmi-1.5-1',
        'earthquake-2026p742111-mmi-2-1',
        'earthquake-2026p742111-mmi-4.5-1'
    ]);
});

test('buildContourFeatures emits only LineString geometry (CloudTAK rejects MultiLineString)', () => {
    const features = buildContourFeatures('2026p742111', SAMPLE_RAW_FEATURES, 1, STALE_ISO) as Array<{
        geometry: { type: string; coordinates: unknown };
    }>;

    for (const feature of features) {
        assert.equal(feature.geometry.type, 'LineString');
    }
    assert.deepEqual(
        features[0].geometry.coordinates,
        (SAMPLE_RAW_FEATURES[0].geometry as { coordinates: unknown[] }).coordinates[0]
    );
});

test('buildContourFeatures splits a multi-part MultiLineString into one feature per part with unique ids', () => {
    const raw = [{
        type: 'Feature',
        properties: { value: 4, units: 'mmi', color: '#80ffff', weight: 2 },
        geometry: {
            type: 'MultiLineString',
            coordinates: [
                [[166.1, -34.9], [166.2, -35.0]],
                [[179.5, -45.8], [179.4, -45.9], [179.3, -46.0]],
                [[170.0, -40.0]] // degenerate single-point part, must be dropped
            ]
        }
    }] as unknown as Feature[];

    const features = buildContourFeatures('2026p742111', raw, 1, STALE_ISO) as Array<{
        id: string;
        properties: Record<string, unknown>;
        geometry: { type: string; coordinates: unknown[] };
    }>;

    assert.deepEqual(features.map(f => f.id), [
        'earthquake-2026p742111-mmi-4-1',
        'earthquake-2026p742111-mmi-4-2'
    ]);
    assert.equal(new Set(features.map(f => f.id)).size, features.length);
    assert.equal(features[1].geometry.coordinates.length, 3);
    for (const feature of features) {
        assert.equal(feature.properties.stroke, '#80ffff');
    }
});

test('buildContourFeatures sets stale and callsign', () => {
    const features = buildContourFeatures('2026p742111', SAMPLE_RAW_FEATURES, 1, STALE_ISO) as Array<{
        properties: { stale: string; callsign: string };
    }>;

    for (const feature of features) {
        assert.equal(feature.properties.stale, STALE_ISO);
    }
    assert.equal(features[0].properties.callsign, 'MMI 1');
    assert.equal(features[1].properties.callsign, 'MMI 1.5');
});
