import { Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType } from '@tak-ps/etl';

// MMI icon mapping
const MMI_ICONS: Record<number, string> = {
    1: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.25A.EarthquakeWeak.png',
    2: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.25A.EarthquakeWeak.png',
    3: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.25A.EarthquakeWeak.png',
    4: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.25A.EarthquakeWeak.png',
    5: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.26A.EarthquakeLight.png',
    6: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.27A.EarthquakeModerate.png',
    7: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.28A.EarthquakeStrong.png',
    8: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.29A.EarthquakeSevere.png',
    9: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.29A.EarthquakeSevere.png',
    10: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.29A.EarthquakeSevere.png',
    11: 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.29A.EarthquakeSevere.png'
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

// Flat shape of the structured data exposed as `metadata` on each submitted
// feature. This is what CloudTAK displays as the Layer Schema and what
// downstream consumers (display-proxy templates, filters, etc.) query via
// `metadata.<field>`.
const GeoNetQuakeMetadata = Type.Object({
    publicID: Type.String(),
    timeUTC: Type.String(),
    timeLocal: Type.String(),
    depth: Type.Number(),
    magnitude: Type.Number(),
    mmi: Type.Number(),
    locality: Type.String(),
    quality: Type.String(),
    intensity: Type.String()
});

// Shape of a single GeoJSON Feature as returned by the GeoNet Quake API.
// Used only to type-check/parse the incoming API response, not exposed
// directly as the Layer Schema.
interface GeoNetFeature {
    type: 'Feature';
    properties: {
        publicID: string;
        time: string;
        depth: number;
        magnitude: number;
        mmi: number;
        locality: string;
        quality: string;
    };
    geometry: {
        type: 'Point';
        coordinates: number[];
    };
}

const NZ_DATE_FORMAT = new Intl.DateTimeFormat('en-NZ', {
    timeZone: 'Pacific/Auckland',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
});
const NZ_TIME_FORMAT = new Intl.DateTimeFormat('en-NZ', {
    timeZone: 'Pacific/Auckland',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
});
const NZ_TZ_NAME_FORMAT = new Intl.DateTimeFormat('en-NZ', {
    timeZone: 'Pacific/Auckland',
    timeZoneName: 'short'
});

/**
 * Get the NZ timezone abbreviation (NZST or NZDT) for a given point in time
 */
function getNZTimeZoneName(eventTime: Date): string {
    const part = NZ_TZ_NAME_FORMAT.formatToParts(eventTime)
        .find(p => p.type === 'timeZoneName');
    return part ? part.value : 'NZT';
}

/**
 * Format a "time ago" string, using the largest whole unit that applies:
 * minutes if under an hour, hours if under a day, otherwise days.
 */
function formatTimeAgo(eventTime: Date, now: number): string {
    const diffMs = now - eventTime.getTime();
    const diffMinutes = Math.floor(diffMs / (1000 * 60));

    if (diffMinutes < 60) {
        return `${diffMinutes} minute${diffMinutes === 1 ? '' : 's'} ago`;
    }

    const diffHours = Math.floor(diffMinutes / 60);
    if (diffHours < 24) {
        return `${diffHours} hour${diffHours === 1 ? '' : 's'} ago`;
    }

    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays} day${diffDays === 1 ? '' : 's'} ago`;
}

/**
 * Format a UTC time string as NZ local time, e.g.
 * "02/08/2026, 20:36 NZST (10 hours ago)"
 */
function formatNZLocalTime(timeUTC: string, now: number): string {
    const eventTime = new Date(timeUTC);
    const datePart = NZ_DATE_FORMAT.format(eventTime);
    const timePart = NZ_TIME_FORMAT.format(eventTime);
    const tzName = getNZTimeZoneName(eventTime);
    return `${datePart}, ${timePart} ${tzName} (${formatTimeAgo(eventTime, now)})`;
}

/**
 * Format an ISO 8601 UTC time string with an explicit " UTC" suffix instead
 * of the "Z" designator, e.g. "2026-08-02T08:35:27.040 UTC"
 */
function formatUTCTime(timeUTC: string): string {
    return `${timeUTC.replace(/Z$/, '')} UTC`;
}

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
                return GeoNetQuakeMetadata;
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
            
            const body = await res.json() as { features: GeoNetFeature[] };
            const now = Date.now();
            const features: object[] = [];
            
            for (const feature of body.features) {
                const props = feature.properties;
                const coords = feature.geometry.coordinates;
                const eventTime = new Date(props.time).getTime();
                const ageMinutes = (now - eventTime) / (1000 * 60);
                
                if (ageMinutes > maxAgeMinutes) continue;

                // GeoNet occasionally flags an auto-detected event as
                // "deleted" after further review (reclassified as a quarry
                // blast, duplicate detection, etc). Exclude it from this
                // submission entirely rather than passing it through as an
                // active earthquake — omitting a previously-submitted id
                // from the features/uids array is how CloudTAK expires a
                // feature, so a quake that gets deleted after we already
                // published it will still be cleaned up correctly, just on
                // the next run rather than this one.
                if (props.quality === 'deleted') continue;
                
                const lon = coords[0];
                const lat = coords[1];
                const depth = props.depth;
                
                const timeLocal = formatNZLocalTime(props.time, now);

                features.push({
                    id: `earthquake-${props.publicID}`,
                    type: 'Feature',
                    properties: {
                        callsign: `M${props.magnitude.toFixed(1)} ${props.locality}`,
                        type: 'a-o-X-i-g-e', // Other, Incident, Geophysical, Event
                        icon: MMI_ICONS[props.mmi] || 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:NaturalHazards/NH.24.Earthquake.png',
                        time: props.time,
                        start: props.time,
                        stale: new Date(Date.now() + 5 * 60 * 1000).toISOString(),
                        metadata: {
                            magnitude: props.magnitude,
                            mmi: props.mmi,
                            intensity: MMI_INTENSITY[props.mmi] || 'Unknown',
                            locality: props.locality,
                            depth: props.depth,
                            quality: props.quality,
                            publicID: props.publicID,
                            timeUTC: props.time,
                            timeLocal
                        },
                        remarks: [
                            `Magnitude: ${props.magnitude.toFixed(2)}`,
                            `MMI: ${props.mmi}`,
                            `Intensity: ${MMI_INTENSITY[props.mmi] || 'Unknown'}`,
                            `Location: ${props.locality}`,
                            `Time (UTC): ${formatUTCTime(props.time)}`,
                            `Time (NZ): ${timeLocal}`,
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
            
            const fc: { type: string; features: object[] } = {
                type: 'FeatureCollection',
                features
            };
            console.log(`ok - fetched ${features.length} earthquakes`);
            await this.submit(fc as unknown as Parameters<typeof this.submit>[0]);
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

