import { Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType } from '@tak-ps/etl';
import type { Feature, LineString, MultiLineString, Position } from 'geojson';

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
    }),
    'Include Shaking Contours': Type.Boolean({
        description: 'Also push GNS shaking-layer MMI contour lines for each earthquake',
        default: false
    }),
    'Minimum Contour MMI': Type.String({
        description: 'Only emit shaking contour lines with MMI value at or above this (1-12, half-steps allowed). Only used when Include Shaking Contours is enabled',
        default: '3'
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

// Properties on a single contour feature as returned by the GNS Science
// Shaking Layers service. `color`/`weight` are the service's own styling
// hints, not simplestyle - they must be mapped to `stroke`/`stroke-width`
// before being submitted, otherwise CloudTAK ignores them and renders the
// contour with its default (broken) styling.
interface ContourSourceProps {
    value: number;
    units: string;
    color: string;
    weight: number;
}

/**
 * Fetch the GNS shaking-layer MMI contour lines for a quake.
 *
 * Returns the raw GeoJSON features unchanged (MultiLineString, one per MMI
 * level). Any non-OK response, network error, timeout or invalid JSON is
 * handled gracefully - this never throws, so a missing/unavailable shakemap
 * for one event can never block submission of the quake points.
 */
export async function fetchQuakeContours(publicID: string): Promise<Feature[]> {
    const url = `https://shakinglayers.geonet.org.nz/ws/download/${publicID}/latest/shakemap/intensity_mmi_contour_lines.json`;

    try {
        const res = await fetch(url, { timeout: 10000 });

        if (!res.ok) {
            console.warn(`warn - no contours for ${publicID}: ${res.status} ${res.statusText}`);
            return [];
        }

        const body = await res.json() as { features: Feature[] };
        return body.features || [];
    } catch (error) {
        console.warn(`warn - no contours for ${publicID}: ${error instanceof Error ? error.message : String(error)}`);
        return [];
    }
}

/**
 * Map raw GNS shaking-layer contour features to CloudTAK-ready features.
 *
 * Drops any feature below `minMMI`. Source `color`/`weight` are mapped to
 * the simplestyle `stroke`/`stroke-width`/`stroke-opacity` properties that
 * TAK/CloudTAK actually use for line styling - `color`/`weight` are NOT
 * copied through, as leaving them in place is what causes CloudTAK to fall
 * back to its default (incorrect) contour colors.
 */
export function buildContourFeatures(
    publicID: string,
    raw: Feature[],
    minMMI: number,
    staleISO: string
): object[] {
    const features: object[] = [];

    for (const feature of raw) {
        const props = feature.properties as unknown as ContourSourceProps;
        if (!props || props.value < minMMI) continue;

        // CloudTAK's CoT endpoint only accepts Point/LineString/Polygon
        // geometries (MultiLineString fails schema validation for the whole
        // POST), so each part of a MultiLineString becomes its own LineString
        // feature with a stable per-part id.
        const geometry = feature.geometry as LineString | MultiLineString;
        const parts: Position[][] = geometry.type === 'MultiLineString'
            ? geometry.coordinates
            : [geometry.coordinates];

        parts.forEach((coordinates, i) => {
            if (coordinates.length < 2) return;

            features.push({
                id: `earthquake-${publicID}-mmi-${props.value}-${i + 1}`,
                type: 'Feature',
                properties: {
                    callsign: `MMI ${props.value}`,
                    stroke: props.color,
                    'stroke-width': props.weight,
                    'stroke-opacity': 1,
                    stale: staleISO,
                    remarks: [
                        `MMI: ${props.value}`,
                        'Source: GNS Science Shaking Layers',
                        `Event: ${publicID}`
                    ].join('\n')
                },
                geometry: { type: 'LineString', coordinates }
            });
        });
    }

    return features;
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

            const includeContours = env['Include Shaking Contours'] === true;
            const minContourMMI = Number(env['Minimum Contour MMI']);
            if (includeContours && (isNaN(minContourMMI) || minContourMMI < 1 || minContourMMI > 12)) {
                throw new Error('Invalid Minimum Contour MMI value. Must be between 1 and 12');
            }

            console.log(`ok - Fetching earthquakes with MMI >= ${mmi} from the last ${maxAgeMinutes} minutes`);
            if (includeContours) {
                console.log(`ok - Including shaking contour lines with MMI >= ${minContourMMI}`);
            }
            
            const url = `https://api.geonet.org.nz/quake?MMI=${mmi}`;
            const res = await fetch(url);
            
            if (!res.ok) {
                throw new Error(`Failed to fetch data: ${res.status} ${res.statusText}`);
            }
            
            const body = await res.json() as { features: GeoNetFeature[] };
            const now = Date.now();
            const features: object[] = [];
            const pushedIDs: string[] = [];
            
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
                            `Time (UTC): ${props.time}`,
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

                pushedIDs.push(props.publicID);
            }

            if (includeContours && pushedIDs.length) {
                // Mirrors the quake points' stale value exactly, computed once
                // so every contour feature for this run shares the same
                // expiry regardless of how long the fetches below take.
                const contourStale = new Date(Date.now() + 5 * 60 * 1000).toISOString();

                // Fetch contours with a small concurrency cap rather than all
                // at once, to avoid hammering the shaking-layers service.
                const CONCURRENCY = 5;
                for (let i = 0; i < pushedIDs.length; i += CONCURRENCY) {
                    const batch = pushedIDs.slice(i, i + CONCURRENCY);
                    const batchResults = await Promise.all(
                        batch.map(id => fetchQuakeContours(id))
                    );
                    for (let j = 0; j < batch.length; j++) {
                        features.push(...buildContourFeatures(batch[j], batchResults[j], minContourMMI, contourStale));
                    }
                }
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

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}

