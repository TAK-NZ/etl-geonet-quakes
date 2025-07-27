import { Static, Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType, InputFeatureCollection } from '@tak-ps/etl';

// MMI color mapping from GeoNet CSS
const MMI_COLORS: Record<number, string> = {
    '-1': '#FFF7F3',
    0: 'transparent',
    1: '#FFF7F3',
    2: '#FEEDDE',
    3: '#FDD0A2',
    4: '#FDAE6B',
    5: '#FD8D3C',
    6: '#F16913',
    7: '#F03B20',
    8: '#BD0026',
    9: '#A30021'
};

// MMI intensity descriptions
const MMI_INTENSITY: Record<number, string> = {
    '-1': 'Unnoticeable',
    1: 'Unnoticeable',
    2: 'Weak',
    3: 'Weak',
    4: 'Light',
    5: 'Moderate',
    6: 'Strong',
    7: 'Very Strong',
    8: 'Severe',
    9: 'Violent'
};

const Env = Type.Object({
    'MMI': Type.String({
        description: 'Minimum Modified Mercalli Intensity (-1 to 8)',
        default: '5'
    }),
    'Max Age Minutes': Type.String({
        description: 'Maximum age of displayed earthquakes in minutes',
        default: '10080'
    })
});

// Define a type for GeoNet GeoJSON features
const GeoNetFeature = Type.Object({
    type: Type.Literal('Feature'),
    properties: Type.Object({
        publicID: Type.String(),
        time: Type.String(),
        depth: Type.Number(),
        magnitude: Type.Number(),
        mmi: Type.Number(),
        locality: Type.String(),
        quality: Type.String()
    }),
    geometry: Type.Object({
        type: Type.Literal('Point'),
        coordinates: Type.Array(Type.Number(), { minItems: 2, maxItems: 3 })
    })
});

export default class Task extends ETL {
    static name = 'etl-geonet-quakes';
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
                return GeoNetFeature;
            }
        } else {
            return Type.Object({});
        }
    }

    async control() {
        try {
            const env = await this.env(Env);
            
            const mmi = Number(env['MMI']);
            if (isNaN(mmi) || mmi < -1 || mmi > 8) {
                throw new Error('Invalid MMI value. Must be between -1 and 8');
            }
            
            const maxAgeMinutes = Number(env['Max Age Minutes']);
            if (isNaN(maxAgeMinutes)) {
                throw new Error('Invalid max age minutes value');
            }
            
            console.log(`ok - Fetching earthquakes with MMI >= ${mmi} from the last ${maxAgeMinutes} minutes`);
            
            const url = `https://api.geonet.org.nz/quake?MMI=${mmi}`;
            const res = await fetch(url);
            
            if (!res.ok) {
                throw new Error(`Failed to fetch data: ${res.status} ${res.statusText}`);
            }
            
            const body = await res.json() as { features: Static<typeof GeoNetFeature>[] };
            const now = Date.now();
            const features: Static<typeof InputFeatureCollection>["features"] = [];
            
            for (const feature of body.features) {
                const props = feature.properties;
                const coords = feature.geometry.coordinates;
                const eventTime = new Date(props.time).getTime();
                const ageMinutes = (now - eventTime) / (1000 * 60);
                
                if (ageMinutes > maxAgeMinutes) continue;
                
                const lon = coords[0];
                const lat = coords[1];
                const depth = props.depth;
                
                features.push({
                    id: `earthquake-${props.publicID}`,
                    type: 'Feature',
                    properties: {
                        callsign: `M${props.magnitude.toFixed(1)} ${props.locality}`,
                        type: 'a-o-X-i-g-e', // Other, Incident, Geophysical, Event
                        icon: 'ad78aafb-83a6-4c07-b2b9-a897a8b6a38f:Shapes/earthquake.png',
                        time: props.time,
                        start: props.time,
                        'marker-color': MMI_COLORS[props.mmi] || '#000000',
                        remarks: [
                            `Magnitude: ${props.magnitude.toFixed(2)}`,
                            `MMI: ${props.mmi}`,
                            `Intensity: ${MMI_INTENSITY[props.mmi] || 'Unknown'}`,
                            `Location: ${props.locality}`,
                            `Time: ${props.time}`,
                            `Depth: ${depth.toFixed(1)} km`,
                            `Information Quality: ${props.quality}`
                        ].join('\n')
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

