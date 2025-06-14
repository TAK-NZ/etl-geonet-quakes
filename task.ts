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
    })
});

// Define a type for USGS GeoJSON features
const USGSFeature = Type.Object({
    id: Type.String(),
    properties: Type.Object({
        title: Type.String(),        
        mag: Type.Number(),
        place: Type.String(),
        time: Type.Number(),
        type: Type.String(),
        alert: Type.String(),
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
            const type = props.type;
            const alert = props.alert || 'None';
            const lat = coords[1];
            const lon = coords[0];
            const depth = coords[2] || 0;
            // Handle antimeridian-crossing bounding boxes
            const lonInBox = minLon <= maxLon
                ? lon >= minLon && lon <= maxLon
                : lon >= minLon || lon <= maxLon;
            const alertLevel = (props.alert || 'none').toLowerCase();
            const estimatedFatalities = {
                'none': '0',
                'green': '0',
                'yellow': '1 - 99',
                'orange': '100 - 999',
                'red': '1,000+'
            }[alertLevel] || '0';
            const estimatedLosses = {
                'none': '< $1 million',
                'green': '< $1 million',
                'yellow': '$1 million - $100 million',
                'orange': '$100 million - $1 billion',
                'red': '$1 billion+'
            }[alertLevel] || '< $1 million';
            const markerColor = {
                'none': '#ffffff',
                'green': '#00ff00',
                'yellow': '#ffff00',
                'orange': '#ff7f00',
                'red': '#ff0000'
            }[alertLevel] || '#ffffff';
            if (
                mag < minMagnitude ||
                lat < minLat || lat > maxLat ||
                !lonInBox ||
                (now - time) > maxAgeMinutes * 60 * 1000 ||
                type !== 'earthquake'
            ) continue;
            features.push({
                id: `earthquake-${feature.id}`,
                type: 'Feature',
                properties: {
                    callsign: props.title,
                    type: 'a-o-X-i-g-e', // Other, Incident, Geophysical, Event
                    icon: 'f7f71666-8b28-4b57-9fbb-e38e61d33b79/Google/earthquake.png',
                    time: new Date(time).toISOString(),
                    start: new Date(time).toISOString(),
                    'marker-color': markerColor,
                    remarks: [
                        `Magnitude: ${mag}`,
                        `Place: ${props.place}`,
                        `Time: ${new Date(time).toISOString()}`,
                        `Depth: ${depth} km`,
                        `Alert Level: ${props.alert || 'None'}`,
                        `Estimated Fatalities: ${estimatedFatalities}`,
                        `Estimated Losses: ${estimatedLosses}`
                    ].join('\n'),
                    links: [{
                        uid: feature.id,
                        relation: 'r-u',
                        mime: 'text/html',
                        url: props.url,
                        remarks: 'USGS Event Page'
                    }]
                },
                geometry: {
                    type: "Point",
                    coordinates: [lon, lat, -depth]
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

