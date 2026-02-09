import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { lastValueFrom } from 'rxjs';
import { DataSource } from 'typeorm';
import { GeoType, DEFAULT_OVERPASS_URL, WGS84_SRID } from '@libs/common';
import { ParseStreetsOsmModule } from './parse-streets-osm.module';

const logger = new Logger('ParseStreetsFast');

// Configuration
const CONCURRENCY = 1; // Sequential to avoid rate limits
const DELAY_BETWEEN_REQUESTS = 3000; // 3 seconds between requests
const BATCH_SIZE = 100; // Streets per batch insert
const MAX_RETRIES = 3;
const RETRY_DELAY_BASE = 5000; // 5 sec base, doubles each retry

// Alternative Overpass servers
const OVERPASS_SERVERS = [
  'https://overpass-api.de/api/interpreter',
  'https://overpass.kumi.systems/api/interpreter',
  'https://maps.mail.ru/osm/tools/overpass/api/interpreter',
];
let currentServerIndex = 0;

type OverpassElement = {
  type: 'node' | 'way';
  id: number;
  lat?: number;
  lon?: number;
  nodes?: number[];
  tags?: Record<string, string>;
};

type OverpassWay = {
  id: number;
  nodes: number[];
  tags: Record<string, string>;
};

type SettlementInfo = {
  id: number;
  name: string;
  type: string;
};

type StreetData = {
  geoId: number;
  osmId: string;
  nameUk: string;
  nameRu: string;
  nameEn: string;
  namesUk: string;
  namesRu: string;
  namesEn: string;
  alias: string;
  geometryWkt: string;
};

const translitMap: Record<string, string> = {
  а: 'a', б: 'b', в: 'v', г: 'h', ґ: 'g', д: 'd', е: 'e', є: 'ye',
  ж: 'zh', з: 'z', и: 'y', і: 'i', ї: 'yi', й: 'y', к: 'k', л: 'l',
  м: 'm', н: 'n', о: 'o', п: 'p', р: 'r', с: 's', т: 't', у: 'u',
  ф: 'f', х: 'kh', ц: 'ts', ч: 'ch', ш: 'sh', щ: 'shch', ь: '',
  ю: 'yu', я: 'ya', "'": '', ʼ: '',
};

function generateAlias(name: string): string {
  return name
    .toLowerCase()
    .split('')
    .map((char) => translitMap[char] || char)
    .join('')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
    .substring(0, 100);
}

function addTagValues(value: string | undefined, set: Set<string>): void {
  if (!value) return;
  value.split(';').forEach((v) => {
    const trimmed = v.trim();
    if (trimmed) set.add(trimmed);
  });
}

function extractNamesFromWays(
  ways: OverpassWay[],
  streetName: string,
): { name: { uk: string; ru: string; en: string }; names: { uk: string; ru: string; en: string } } {
  const nameVariants = { uk: new Set<string>(), ru: new Set<string>(), en: new Set<string>() };

  ways.forEach((way) => {
    addTagValues(way.tags['name:uk'], nameVariants.uk);
    addTagValues(way.tags.name, nameVariants.uk);
    addTagValues(way.tags['name:ru'], nameVariants.ru);
    addTagValues(way.tags['name:en'], nameVariants.en);
    addTagValues(way.tags['alt_name:uk'], nameVariants.uk);
    addTagValues(way.tags['alt_name:ru'], nameVariants.ru);
    addTagValues(way.tags['alt_name:en'], nameVariants.en);
    addTagValues(way.tags.alt_name, nameVariants.uk);
    addTagValues(way.tags.old_name, nameVariants.uk);
    addTagValues(way.tags['old_name:uk'], nameVariants.uk);
    addTagValues(way.tags['old_name:ru'], nameVariants.ru);
    addTagValues(way.tags['old_name:en'], nameVariants.en);
  });

  return {
    name: {
      uk: Array.from(nameVariants.uk)[0] || streetName,
      ru: Array.from(nameVariants.ru)[0] || '',
      en: Array.from(nameVariants.en)[0] || '',
    },
    names: {
      uk: Array.from(nameVariants.uk).join('; '),
      ru: Array.from(nameVariants.ru).join('; '),
      en: Array.from(nameVariants.en).join('; '),
    },
  };
}

function buildGeometryWkt(lineSegments: string[][]): string {
  if (lineSegments.length === 1) {
    return `LINESTRING(${lineSegments[0].join(', ')})`;
  }
  const lineStrings = lineSegments.map((segment) => `(${segment.join(', ')})`).join(', ');
  return `MULTILINESTRING(${lineStrings})`;
}

async function getPolygonCoords(dataSource: DataSource, geoId: number): Promise<string> {
  const result = await dataSource.query(
    `SELECT ST_AsText(ST_ExteriorRing((ST_Dump(polygon)).geom)) as coords FROM geo WHERE id = $1`,
    [geoId],
  );

  if (!result?.[0]?.coords) {
    throw new Error(`Could not get polygon for geo ${geoId}`);
  }

  const coordsMatch = result[0].coords.match(/LINESTRING\((.*)\)/);
  if (!coordsMatch) {
    throw new Error(`Invalid polygon format for geo ${geoId}`);
  }

  return coordsMatch[1]
    .split(',')
    .map((pair: string) => {
      const [lon, lat] = pair.trim().split(' ');
      return `${lat} ${lon}`;
    })
    .join(' ');
}

function getNextOverpassServer(): string {
  const server = OVERPASS_SERVERS[currentServerIndex];
  currentServerIndex = (currentServerIndex + 1) % OVERPASS_SERVERS.length;
  return server;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function queryOverpassApi(httpService: HttpService, polyCoords: string): Promise<OverpassElement[]> {
  const query = `[out:json][timeout:300];(way["highway"]["name"](poly:"${polyCoords}"););(._;>;);out body;`;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    const server = getNextOverpassServer();

    try {
      const response = await lastValueFrom(
        httpService.post(server, `data=${encodeURIComponent(query)}`, {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          timeout: 310000,
        }),
      );

      return response.data.elements || [];
    } catch (error: unknown) {
      const err = error as { response?: { status?: number }; message?: string };
      const status = err.response?.status;

      // Rate limited or server error - wait and retry with different server
      if (status === 429 || status === 504 || status === 503) {
        const delay = RETRY_DELAY_BASE * Math.pow(2, attempt);
        logger.warn(`Overpass ${status} from ${server}, retry ${attempt + 1}/${MAX_RETRIES} in ${delay}ms`);
        await sleep(delay);
        continue;
      }

      // Other error - throw
      throw error;
    }
  }

  throw new Error(`Overpass API failed after ${MAX_RETRIES} retries`);
}

function parseOverpassResponse(elements: OverpassElement[]): {
  nodeCoordinates: Map<number, { lat: number; lon: number }>;
  ways: OverpassWay[];
} {
  const nodeCoordinates = new Map<number, { lat: number; lon: number }>();
  const ways: OverpassWay[] = [];

  elements.forEach((el) => {
    if (el.type === 'node' && el.lat !== undefined && el.lon !== undefined) {
      nodeCoordinates.set(el.id, { lat: el.lat, lon: el.lon });
    } else if (el.type === 'way' && el.nodes && el.tags) {
      ways.push({ id: el.id, nodes: el.nodes, tags: el.tags });
    }
  });

  return { nodeCoordinates, ways };
}

function groupWaysByStreetName(ways: OverpassWay[]): Map<string, OverpassWay[]> {
  const streetGroups = new Map<string, OverpassWay[]>();

  ways.forEach((way) => {
    const primaryName = way.tags.name || way.tags['name:uk'];
    if (!primaryName) return;

    if (!streetGroups.has(primaryName)) {
      streetGroups.set(primaryName, []);
    }
    streetGroups.get(primaryName)!.push(way);
  });

  return streetGroups;
}

function processStreetGroup(
  geoId: number,
  streetName: string,
  ways: OverpassWay[],
  nodeCoordinates: Map<number, { lat: number; lon: number }>,
): StreetData | null {
  const osmIds: number[] = [];
  const lineSegments: string[][] = [];

  ways.forEach((way) => {
    osmIds.push(way.id);
    const coordinates = way.nodes
      .map((nodeId) => nodeCoordinates.get(nodeId))
      .filter((coord): coord is { lat: number; lon: number } => coord !== undefined)
      .map((coord) => `${coord.lon} ${coord.lat}`);

    if (coordinates.length > 1) {
      lineSegments.push(coordinates);
    }
  });

  if (!lineSegments.length) return null;

  const { name, names } = extractNamesFromWays(ways, streetName);
  const geometryWkt = buildGeometryWkt(lineSegments);
  const alias = generateAlias(name.uk);

  return {
    geoId,
    osmId: osmIds[0].toString(),
    nameUk: name.uk,
    nameRu: name.ru,
    nameEn: name.en,
    namesUk: names.uk,
    namesRu: names.ru,
    namesEn: names.en,
    alias,
    geometryWkt,
  };
}

async function bulkUpsertStreets(dataSource: DataSource, streets: StreetData[]): Promise<number> {
  if (streets.length === 0) return 0;

  // Use ON CONFLICT for upsert
  const values: unknown[] = [];
  const placeholders: string[] = [];

  streets.forEach((s, i) => {
    const offset = i * 9;
    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}::jsonb, $${offset + 4}::jsonb, $${offset + 5}, ST_Multi(ST_GeomFromText($${offset + 6}, ${WGS84_SRID})), $${offset + 7}, $${offset + 8}, $${offset + 9})`);
    values.push(
      s.geoId,
      s.osmId,
      JSON.stringify({ uk: s.nameUk, ru: s.nameRu, en: s.nameEn }),
      JSON.stringify({ uk: s.namesUk, ru: s.namesRu, en: s.namesEn }),
      s.alias,
      s.geometryWkt,
      s.nameUk, // for conflict check
      s.nameRu,
      s.nameEn,
    );
  });

  // First, try to insert new streets
  const insertQuery = `
    INSERT INTO streets (geo_id, osm_id, name, names, alias, line)
    SELECT v.geo_id, v.osm_id, v.name, v.names, v.alias, v.line
    FROM (VALUES ${placeholders.map((p, i) =>
      `($${i * 9 + 1}::int, $${i * 9 + 2}::text, $${i * 9 + 3}::jsonb, $${i * 9 + 4}::jsonb, $${i * 9 + 5}::text, ST_Multi(ST_GeomFromText($${i * 9 + 6}, ${WGS84_SRID})), $${i * 9 + 7}::text, $${i * 9 + 8}::text, $${i * 9 + 9}::text)`
    ).join(', ')}) AS v(geo_id, osm_id, name, names, alias, line, name_uk, name_ru, name_en)
    WHERE NOT EXISTS (
      SELECT 1 FROM streets s
      WHERE s.geo_id = v.geo_id AND s.name->>'uk' = v.name_uk
    )
    ON CONFLICT DO NOTHING
  `;

  // Simpler approach: upsert one by one but in a transaction
  let inserted = 0;

  await dataSource.transaction(async (manager) => {
    for (const s of streets) {
      try {
        // Check if exists
        const existing = await manager.query(
          `SELECT id FROM streets WHERE geo_id = $1 AND name->>'uk' = $2`,
          [s.geoId, s.nameUk],
        );

        if (existing.length > 0) {
          // Update existing
          await manager.query(
            `UPDATE streets SET
              osm_id = $1,
              name = $2::jsonb,
              names = $3::jsonb,
              alias = $4,
              line = ST_Multi(ST_GeomFromText($5, ${WGS84_SRID}))
            WHERE id = $6`,
            [
              s.osmId,
              JSON.stringify({ uk: s.nameUk, ru: s.nameRu, en: s.nameEn }),
              JSON.stringify({ uk: s.namesUk, ru: s.namesRu, en: s.namesEn }),
              s.alias,
              s.geometryWkt,
              existing[0].id,
            ],
          );
        } else {
          // Insert new
          await manager.query(
            `INSERT INTO streets (geo_id, osm_id, name, names, alias, line)
            VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, ST_Multi(ST_GeomFromText($6, ${WGS84_SRID})))`,
            [
              s.geoId,
              s.osmId,
              JSON.stringify({ uk: s.nameUk, ru: s.nameRu, en: s.nameEn }),
              JSON.stringify({ uk: s.namesUk, ru: s.namesRu, en: s.namesEn }),
              s.alias,
              s.geometryWkt,
            ],
          );
          inserted++;
        }
      } catch (err) {
        // Skip on error
      }
    }
  });

  return inserted;
}

async function parseSettlement(
  dataSource: DataSource,
  httpService: HttpService,
  settlement: SettlementInfo,
): Promise<{ streets: number; errors: number }> {
  try {
    const polyCoords = await getPolygonCoords(dataSource, settlement.id);
    const elements = await queryOverpassApi(httpService, polyCoords);
    const { nodeCoordinates, ways } = parseOverpassResponse(elements);
    const streetGroups = groupWaysByStreetName(ways);

    const streets: StreetData[] = [];
    let errors = 0;

    for (const [streetName, streetWays] of streetGroups.entries()) {
      try {
        const streetData = processStreetGroup(settlement.id, streetName, streetWays, nodeCoordinates);
        if (streetData) {
          streets.push(streetData);
        }
      } catch {
        errors++;
      }
    }

    // Bulk insert
    await bulkUpsertStreets(dataSource, streets);

    return { streets: streets.length, errors };
  } catch (error) {
    logger.error(`Failed ${settlement.name}: ${(error as Error).message}`);
    return { streets: 0, errors: 1 };
  }
}

async function processWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  delayMs: number,
  processor: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  const queue = [...items];
  let active = 0;
  let completed = 0;

  return new Promise((resolve) => {
    const processNext = async () => {
      if (queue.length === 0 && active === 0) {
        resolve(results);
        return;
      }

      while (active < concurrency && queue.length > 0) {
        const item = queue.shift()!;
        active++;

        processor(item)
          .then((result) => {
            results.push(result);
            completed++;
            active--;

            // Delay before next request
            setTimeout(processNext, delayMs);
          })
          .catch(() => {
            active--;
            setTimeout(processNext, delayMs);
          });
      }
    };

    processNext();
  });
}

async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  const app = await NestFactory.createApplicationContext(ParseStreetsOsmModule, {
    logger: ['log', 'error', 'warn'],
  });

  const dataSource = app.get(DataSource);
  const httpService = app.get(HttpService);

  try {
    if (command === 'region' && args[1]) {
      const alias = args[1].toLowerCase();

      // Find region
      const region = await dataSource.query(
        `SELECT id, name->>'uk' as name FROM geo WHERE type = $1 AND alias = $2`,
        [GeoType.Region, alias],
      );

      if (!region.length) {
        logger.error(`Region not found: ${alias}`);
        process.exit(1);
      }

      logger.log(`\n${'='.repeat(60)}`);
      logger.log(`Parsing streets for: ${region[0].name}`);
      logger.log(`Concurrency: ${CONCURRENCY}, Delay: ${DELAY_BETWEEN_REQUESTS}ms`);
      logger.log('='.repeat(60));

      // Get settlements
      const settlements = await dataSource.query(`
        SELECT child.id, child.name->>'uk' as name, child.type
        FROM geo child
        INNER JOIN geo parent ON child.lft > parent.lft AND child.rgt < parent.rgt
        WHERE parent.id = $1
          AND child.type IN ($2, $3)
          AND child.polygon IS NOT NULL
        ORDER BY child.type DESC, child.name->>'uk'
      `, [region[0].id, GeoType.City, GeoType.Village]);

      logger.log(`Found ${settlements.length} settlements`);

      const startTime = Date.now();
      let totalStreets = 0;
      let totalErrors = 0;
      let processed = 0;

      // Process with concurrency
      for (let i = 0; i < settlements.length; i += CONCURRENCY) {
        const batch = settlements.slice(i, i + CONCURRENCY);

        const promises = batch.map(async (s: SettlementInfo) => {
          const result = await parseSettlement(dataSource, httpService, s);
          processed++;
          logger.log(`[${processed}/${settlements.length}] ${s.name}: ${result.streets} streets`);
          return result;
        });

        const results = await Promise.all(promises);
        results.forEach((r) => {
          totalStreets += r.streets;
          totalErrors += r.errors;
        });

        // Delay between batches
        if (i + CONCURRENCY < settlements.length) {
          await new Promise((r) => setTimeout(r, DELAY_BETWEEN_REQUESTS));
        }
      }

      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      logger.log(`\n${'='.repeat(60)}`);
      logger.log(`Completed in ${elapsed}s`);
      logger.log(`Settlements: ${processed}`);
      logger.log(`Streets: ${totalStreets}`);
      logger.log(`Errors: ${totalErrors}`);
      logger.log('='.repeat(60));

    } else if (command === 'all') {
      // Get all regions
      const regions = await dataSource.query(
        `SELECT id, name->>'uk' as name, alias FROM geo WHERE type = $1 ORDER BY name->>'uk'`,
        [GeoType.Region],
      );

      logger.log(`Found ${regions.length} regions`);

      let grandTotalStreets = 0;
      let grandTotalErrors = 0;
      const startTime = Date.now();

      for (const region of regions) {
        logger.log(`\n${'='.repeat(60)}`);
        logger.log(`Region: ${region.name}`);

        const settlements = await dataSource.query(`
          SELECT child.id, child.name->>'uk' as name, child.type
          FROM geo child
          INNER JOIN geo parent ON child.lft > parent.lft AND child.rgt < parent.rgt
          WHERE parent.id = $1
            AND child.type IN ($2, $3)
            AND child.polygon IS NOT NULL
          ORDER BY child.type DESC, child.name->>'uk'
        `, [region.id, GeoType.City, GeoType.Village]);

        logger.log(`${settlements.length} settlements`);

        let regionStreets = 0;
        let processed = 0;

        for (let i = 0; i < settlements.length; i += CONCURRENCY) {
          const batch = settlements.slice(i, i + CONCURRENCY);

          const promises = batch.map(async (s: SettlementInfo) => {
            const result = await parseSettlement(dataSource, httpService, s);
            processed++;
            if (result.streets > 0) {
              logger.log(`  [${processed}/${settlements.length}] ${s.name}: ${result.streets} streets`);
            }
            return result;
          });

          const results = await Promise.all(promises);
          results.forEach((r) => {
            regionStreets += r.streets;
            grandTotalErrors += r.errors;
          });

          if (i + CONCURRENCY < settlements.length) {
            await new Promise((r) => setTimeout(r, DELAY_BETWEEN_REQUESTS));
          }
        }

        grandTotalStreets += regionStreets;
        logger.log(`Region total: ${regionStreets} streets`);
      }

      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      logger.log(`\n${'='.repeat(60)}`);
      logger.log(`ALL REGIONS COMPLETED in ${elapsed} minutes`);
      logger.log(`Total streets: ${grandTotalStreets}`);
      logger.log(`Total errors: ${grandTotalErrors}`);
      logger.log('='.repeat(60));

    } else if (command === 'missing') {
      // Parse only settlements WITHOUT any streets
      logger.log(`\n${'='.repeat(60)}`);
      logger.log('Parsing settlements WITHOUT streets');
      logger.log(`Concurrency: ${CONCURRENCY}, Delay: ${DELAY_BETWEEN_REQUESTS}ms`);
      logger.log('='.repeat(60));

      const settlements = await dataSource.query(`
        SELECT g.id, g.name->>'uk' as name, g.type
        FROM geo g
        WHERE g.type IN ($1, $2)
          AND g.polygon IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM streets s WHERE s.geo_id = g.id)
        ORDER BY g.type DESC, g.name->>'uk'
      `, [GeoType.City, GeoType.Village]);

      logger.log(`Found ${settlements.length} settlements without streets`);

      const startTime = Date.now();
      let totalStreets = 0;
      let totalErrors = 0;
      let processed = 0;

      for (const settlement of settlements) {
        const result = await parseSettlement(dataSource, httpService, settlement);
        processed++;
        totalStreets += result.streets;
        totalErrors += result.errors;

        if (result.streets > 0) {
          logger.log(`[${processed}/${settlements.length}] ${settlement.name}: ${result.streets} streets`);
        } else {
          logger.log(`[${processed}/${settlements.length}] ${settlement.name}: 0 streets`);
        }

        // Delay between requests
        await sleep(DELAY_BETWEEN_REQUESTS);
      }

      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      logger.log(`\n${'='.repeat(60)}`);
      logger.log(`Completed in ${elapsed} minutes`);
      logger.log(`Settlements: ${processed}`);
      logger.log(`Streets: ${totalStreets}`);
      logger.log(`Errors: ${totalErrors}`);
      logger.log('='.repeat(60));

    } else if (command === 'list') {
      const regions = await dataSource.query(
        `SELECT id, name->>'uk' as name, alias FROM geo WHERE type = $1 ORDER BY name->>'uk'`,
        [GeoType.Region],
      );

      console.log('Available regions:');
      regions.forEach((r: { alias: string; name: string; id: number }) => {
        console.log(`  ${r.alias.padEnd(25)} - ${r.name} (ID: ${r.id})`);
      });

      // Show missing count
      const missing = await dataSource.query(`
        SELECT COUNT(*) as cnt FROM geo
        WHERE type IN ($1, $2) AND polygon IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM streets s WHERE s.geo_id = geo.id)
      `, [GeoType.City, GeoType.Village]);

      console.log(`\nSettlements without streets: ${missing[0].cnt}`);

    } else {
      console.log('Usage:');
      console.log('  yarn parse-streets-fast list              - List regions');
      console.log('  yarn parse-streets-fast region <alias>    - Parse one region');
      console.log('  yarn parse-streets-fast all               - Parse ALL regions');
      console.log('  yarn parse-streets-fast missing           - Parse only settlements WITHOUT streets');
    }
  } finally {
    await app.close();
  }
}

main().catch((err) => {
  logger.error('Fatal error:', err);
  process.exit(1);
});
