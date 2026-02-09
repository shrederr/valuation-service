import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';

const logger = new Logger('OlxStreetsOptimized');

interface StreetData {
  id: number;
  geoId: number | null;
  nameUk: string | null;
  nameRu: string | null;
  normalizedUk: string;
  normalizedRu: string;
}

interface ListingData {
  id: string;
  geoId: number | null;
  lng: number;
  lat: number;
  title: string;
  descriptionUk: string;
}

/**
 * Normalize street name for comparison
 */
function normalizeStreetName(name: string | null): string {
  if (!name) return '';
  return name
    .toLowerCase()
    .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр\.|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.)\s*/gi, '')
    .replace(/[«»""''`']/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Check if street name is found in text
 */
function findStreetInText(normalizedStreet: string, text: string): boolean {
  if (!normalizedStreet || normalizedStreet.length < 3) return false;
  if (!text) return false;

  const normalizedText = text.toLowerCase().replace(/[«»""''`']/g, '').replace(/\s+/g, ' ');

  // Direct inclusion
  if (normalizedText.includes(normalizedStreet)) {
    return true;
  }

  return false;
}

async function main() {
  const args = process.argv.slice(2);
  const batchSize = parseInt(args[0] || '5000', 10);

  logger.log('='.repeat(60));
  logger.log('OLX Optimized Street Resolution');
  logger.log('='.repeat(60));
  logger.log(`Batch size: ${batchSize}`);
  logger.log('='.repeat(60));

  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const dataSource = app.get(DataSource);

  try {
    // Step 1: Load all streets into memory with their normalized names
    logger.log('\nStep 1: Loading all streets into memory...');
    const streetsRaw = await dataSource.query(`
      SELECT id, geo_id as "geoId", name->>'uk' as "nameUk", name->>'ru' as "nameRu"
      FROM streets
      WHERE name->>'uk' IS NOT NULL OR name->>'ru' IS NOT NULL
    `);

    const streets: StreetData[] = streetsRaw.map((s: any) => ({
      id: s.id,
      geoId: s.geoId,
      nameUk: s.nameUk,
      nameRu: s.nameRu,
      normalizedUk: normalizeStreetName(s.nameUk),
      normalizedRu: normalizeStreetName(s.nameRu),
    }));

    logger.log(`Loaded ${streets.length} streets`);

    // Build geo -> streets index
    const streetsByGeo = new Map<number, StreetData[]>();
    for (const street of streets) {
      if (street.geoId) {
        if (!streetsByGeo.has(street.geoId)) {
          streetsByGeo.set(street.geoId, []);
        }
        streetsByGeo.get(street.geoId)!.push(street);
      }
    }
    logger.log(`Indexed streets for ${streetsByGeo.size} geo areas`);

    // Step 2: Count OLX listings to process
    const countResult = await dataSource.query(`
      SELECT COUNT(*) as cnt FROM unified_listings
      WHERE realty_platform = 'olx' AND street_id IS NULL AND geo_id IS NOT NULL
    `);
    const totalCount = parseInt(countResult[0].cnt, 10);
    logger.log(`\nStep 2: Processing ${totalCount} OLX listings`);

    let processed = 0;
    let matchedByTitle = 0;
    let matchedByDescription = 0;
    let matchedByNearest = 0;
    let noStreetInGeo = 0;
    let offset = 0;

    const startTime = Date.now();

    while (processed < totalCount) {
      // Fetch batch of listings
      const listings = await dataSource.query<ListingData[]>(`
        SELECT
          id,
          geo_id as "geoId",
          lng, lat,
          COALESCE(primary_data->>'title', '') as title,
          COALESCE(description->>'uk', '') as "descriptionUk"
        FROM unified_listings
        WHERE realty_platform = 'olx' AND street_id IS NULL AND geo_id IS NOT NULL
        ORDER BY id
        LIMIT $1 OFFSET $2
      `, [batchSize, offset]);

      if (listings.length === 0) break;

      const updates: { id: string; streetId: number }[] = [];

      for (const listing of listings) {
        const geoStreets = streetsByGeo.get(listing.geoId!) || [];

        if (geoStreets.length === 0) {
          noStreetInGeo++;
          processed++;
          continue;
        }

        let matchedStreet: StreetData | null = null;
        let matchMethod: 'title' | 'description' | 'nearest' = 'nearest';

        // Try title match
        for (const street of geoStreets) {
          if (street.normalizedUk && findStreetInText(street.normalizedUk, listing.title)) {
            matchedStreet = street;
            matchMethod = 'title';
            break;
          }
          if (street.normalizedRu && findStreetInText(street.normalizedRu, listing.title)) {
            matchedStreet = street;
            matchMethod = 'title';
            break;
          }
        }

        // Try description match
        if (!matchedStreet && listing.descriptionUk) {
          for (const street of geoStreets) {
            if (street.normalizedUk && findStreetInText(street.normalizedUk, listing.descriptionUk)) {
              matchedStreet = street;
              matchMethod = 'description';
              break;
            }
            if (street.normalizedRu && findStreetInText(street.normalizedRu, listing.descriptionUk)) {
              matchedStreet = street;
              matchMethod = 'description';
              break;
            }
          }
        }

        // Fallback: use first street in geo (we'll update with nearest in SQL later)
        if (!matchedStreet) {
          matchedStreet = geoStreets[0];
          matchMethod = 'nearest';
        }

        updates.push({ id: listing.id, streetId: matchedStreet.id });

        if (matchMethod === 'title') matchedByTitle++;
        else if (matchMethod === 'description') matchedByDescription++;
        else matchedByNearest++;

        processed++;
      }

      // Bulk update
      if (updates.length > 0) {
        const values = updates.map(u => `('${u.id}'::uuid, ${u.streetId})`).join(',');
        await dataSource.query(`
          UPDATE unified_listings ul
          SET street_id = v.street_id
          FROM (VALUES ${values}) AS v(id, street_id)
          WHERE ul.id = v.id
        `);
      }

      offset += batchSize;

      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      const rate = Math.round(processed / parseFloat(elapsed));
      logger.log(
        `Progress: ${processed}/${totalCount} (${((processed/totalCount)*100).toFixed(1)}%) | ` +
        `Title: ${matchedByTitle} | Desc: ${matchedByDescription} | Fallback: ${matchedByNearest} | ` +
        `NoStreets: ${noStreetInGeo} | ${elapsed}min (${rate}/min)`
      );
    }

    // Step 3: Update fallback matches with actual nearest street
    logger.log('\nStep 3: Updating fallback matches with nearest street...');
    const nearestResult = await dataSource.query(`
      UPDATE unified_listings ul
      SET street_id = nearest.street_id
      FROM (
        SELECT DISTINCT ON (ul2.id) ul2.id as listing_id, s.id as street_id
        FROM unified_listings ul2
        CROSS JOIN LATERAL (
          SELECT s.id
          FROM streets s
          WHERE s.geo_id = ul2.geo_id
            AND ST_DWithin(
              s.line::geography,
              ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography,
              5000
            )
          ORDER BY ST_Distance(
            s.line::geography,
            ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography
          )
          LIMIT 1
        ) s
        WHERE ul2.realty_platform = 'olx'
          AND ul2.geo_id IS NOT NULL
          AND ul2.lat IS NOT NULL
          AND ul2.lng IS NOT NULL
      ) nearest
      WHERE ul.id = nearest.listing_id
        AND ul.street_id != nearest.street_id
    `);
    logger.log(`Updated ${nearestResult[1] || 0} with nearest street`);

    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

    logger.log('\n' + '='.repeat(60));
    logger.log('COMPLETED');
    logger.log('='.repeat(60));
    logger.log(`Total time: ${totalTime} minutes`);
    logger.log(`Matched by title: ${matchedByTitle} (${((matchedByTitle/processed)*100).toFixed(1)}%)`);
    logger.log(`Matched by description: ${matchedByDescription} (${((matchedByDescription/processed)*100).toFixed(1)}%)`);
    logger.log(`Fallback/nearest: ${matchedByNearest} (${((matchedByNearest/processed)*100).toFixed(1)}%)`);
    logger.log(`No streets in geo: ${noStreetInGeo}`);
    logger.log('='.repeat(60));

  } catch (error) {
    logger.error(`Fatal error: ${(error as Error).message}`);
    console.error(error);
  } finally {
    await app.close();
  }
}

main().catch(console.error);
