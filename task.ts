import { Static, Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType, InputFeatureCollection } from '@tak-ps/etl';

// Constants for alert level mappings
type AlertLevel = 'none' | 'green' | 'yellow' | 'orange' | 'red';

const ALERT_COLORS: Record<AlertLevel, string> = {
    'none': '#ffffff',
    'green': '#00ff00',
    'yellow': '#ffff00',
    'orange': '#ff7f00',
    'red': '#ff0000'
};

const ESTIMATED_FATALITIES: Record<AlertLevel, string> = {
    'none': '0',
    'green': '0',
    'yellow': '1 - 99',
    'orange': '100 - 999',
    'red': '1,000+'
};

const ESTIMATED_LOSSES: Record<AlertLevel, string> = {
    'none': '< $1 million USD',
    'green': '< $1 million USD',
    'yellow': '$1 million USD - $100 million USD',
    'orange': '$100 million USD - $1 billion USD',
    'red': '$1 billion+ USD'
};

const MAX_AGE_LIMIT = 10080; // 7 days in minutes

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
        try {
            const env = await this.env(Env);
            
            // Parse and validate input parameters
            const [minLat, maxLat, minLon, maxLon] = env['Bounding Box'].split(',').map(Number);
            if ([minLat, maxLat, minLon, maxLon].some(isNaN)) {
                throw new Error('Invalid bounding box format. Expected: minLat,maxLat,minLon,maxLon');
            }
            
            const minMagnitude = Number(env['Min Magnitude']);
            if (isNaN(minMagnitude)) {
                throw new Error('Invalid minimum magnitude value');
            }
            
            let maxAgeMinutes = Number(env['Max Age Minutes']);
            if (isNaN(maxAgeMinutes)) {
                throw new Error('Invalid max age minutes value');
            }
            // Enforce maximum age limit
            maxAgeMinutes = Math.min(maxAgeMinutes, MAX_AGE_LIMIT);
            
            console.log(`ok - Fetching earthquakes with magnitude >= ${minMagnitude} within bounding box [${minLat},${maxLat},${minLon},${maxLon}] from the last ${maxAgeMinutes} minutes`);
            
            const url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson';
            const res = await fetch(url);
            
            if (!res.ok) {
                throw new Error(`Failed to fetch data: ${res.status} ${res.statusText}`);
            }
            
            const body = await res.json() as { features: Static<typeof USGSFeature>[] };
            const now = Date.now();
            const features: Static<typeof InputFeatureCollection>["features"] = [];
        for (const feature of body.features) {
            const props = feature.properties;
            const coords = feature.geometry.coordinates;
            const mag = props.mag;
            const time = props.time;
            const type = props.type;
            const lat = coords[1];
            const lon = coords[0];
            const depth = coords[2] || 0;
            // Handle antimeridian-crossing bounding boxes (e.g., Pacific Ocean crossing 180° longitude)
            const lonInBox = minLon <= maxLon
                ? lon >= minLon && lon <= maxLon
                : lon >= minLon || lon <= maxLon;
                
            const alertLevel = (props.alert || 'none').toLowerCase() as AlertLevel;
            const estimatedFatalities = ESTIMATED_FATALITIES[alertLevel as AlertLevel] || '0';
            const estimatedLosses = ESTIMATED_LOSSES[alertLevel as AlertLevel] || '< $1 million USD';
            const markerColor = ALERT_COLORS[alertLevel as AlertLevel] || '#ffffff';
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
        } catch (error) {
            console.error(`Error in ETL process: ${error instanceof Error ? error.message : String(error)}`);
            throw error;
        }
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

