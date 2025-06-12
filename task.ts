import { Static, Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType, InputFeatureCollection } from '@tak-ps/etl';

const Env = Type.Object({
    'Min Magnitude': Type.String({
        description: 'Minimum earthquake magnitude to include',
        default: '2.5'
    }),
    'Bounding Box': Type.String({
        description: 'Bounding box as minLat,maxLat,minLon,maxLon',
        default: '-90,90,-180,180'
    }),
    'Max Age Minutes': Type.String({
        description: 'Maximum age of displayed earthquakes in minutes. Maximum possible value is 7 days (10080 minutes).',
        default: '60'
    }),
    'CoT Lifetime Seconds': Type.String({
        description: 'Lifetime of CoT markers in seconds',
        default: '600'
    })
});

// Define a type for USGS GeoJSON features
const USGSFeature = Type.Object({
    id: Type.String(),
    properties: Type.Object({
        mag: Type.Number(),
        place: Type.String(),
        time: Type.Number(),
        url: Type.String()
    }),
    geometry: Type.Object({
        type: Type.Literal('Point'),
        coordinates: Type.Array(Type.Number(), { minItems: 2, maxItems: 3 })
    })
});

export default class Task extends ETL {
    static name = 'etl-earthquake';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Env;
            } else {
                return USGSFeature;
            }
        } else {
            return Type.Object({});
        }
    }

    async control() {
        const env = await this.env(Env);
        // Parse bounding box string
        const [minLat, maxLat, minLon, maxLon] = env['Bounding Box'].split(',').map(Number);
        const minMagnitude = Number(env['Min Magnitude']);
        const maxAgeMinutes = Number(env['Max Age Minutes']);
        const cotLifetimeSeconds = Number(env['CoT Lifetime Seconds']);
        const url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson';
        const res = await fetch(url);
        const body = await res.json() as { features: Static<typeof USGSFeature>[] };
        const now = Date.now();
        const features: Static<typeof InputFeatureCollection>["features"] = [];
        for (const feature of body.features) {
            const props = feature.properties;
            const coords = feature.geometry.coordinates;
            const mag = props.mag;
            const time = props.time;
            const lat = coords[1];
            const lon = coords[0];
            const depth = coords[2] || 0;
            // Handle antimeridian-crossing bounding boxes
            const lonInBox = minLon <= maxLon
                ? lon >= minLon && lon <= maxLon
                : lon >= minLon || lon <= maxLon;
            if (
                mag < minMagnitude ||
                lat < minLat || lat > maxLat ||
                !lonInBox ||
                (now - time) > maxAgeMinutes * 60 * 1000
            ) continue;
            features.push({
                id: `earthquake-${feature.id}`,
                type: 'Feature',
                properties: {
                    callsign: 'M ' + mag.toFixed(1),
                    // type: 'a-u-G', // 'a-f-X-i-g-e' isn't recognized by some CoT viewers,
                    type: 'a-f-X-i-g-e',
                    icon: 'f7f71666-8b28-4b57-9fbb-e38e61d33b79/Google/earthquake.png',
                    time: new Date(time).toISOString(),
                    start: new Date(time).toISOString(),
                    stale: new Date(time + cotLifetimeSeconds * 1000).toISOString(),
                    remarks: [
                        `Magnitude: ${mag}`,
                        `Place: ${props.place}`,
                        `Time: ${new Date(time).toISOString()}`,
                        `Depth: ${depth} km`,
                        `More info: ${props.url}`
                    ].join('\n')
                },
                geometry: {
                    type: "Point",
                    coordinates: [lon, lat]
                }
            });
        }
        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features
        };
        console.log(`ok - fetched ${features.length} earthquakes`);
        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

