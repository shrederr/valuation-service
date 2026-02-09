import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';

const logger = new Logger('OlxStreetsTextV2');

interface StreetData {
  id: number;
  nameUk: string | null;
  nameRu: string | null;
  normalizedUk: string;
  normalizedRu: string;
}

/**
 * Normalize street name for comparison
 */
function normalizeStreetName(name: string | null): string {
  if (!name) return '';
  return name
    .toLowerCase()
    // Remove street type prefixes (Ukrainian and Russian)
    .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр\.|пр-т|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.|шосе|шоссе|алея|алея\.|проїзд|проезд|узвіз|спуск|тупик)\s*/gi, '')
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
  const radiusMeters = parseInt(args[0] || '5000', 10);

  logger.log('='.repeat(60));
  logger.log('OLX Street Resolution v2 - Spatial Text Match');
  logger.log('='.repeat(60));
  logger.log(`Search radius: ${radiusMeters}m`);
  logger.log('Strategy: 5km radius → text match title → text match desc → nearest');
  logger.log('='.repeat(60));

  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const dataSource = app.get(DataSource);

  try {
    // Step 1: Load all streets with normalized names
    logger.log('\nStep 1: Loading all streets into memory...');
    const streetsRaw = await dataSource.query(`
      SELECT id, name->>'uk' as "nameUk", name->>'ru' as "nameRu"
      FROM streets
      WHERE name->>'uk' IS NOT NULL OR name->>'ru' IS NOT NULL
    `);

    const allStreets: StreetData[] = streetsRaw.map((s: any) => ({
      id: s.id,
      nameUk: s.nameUk,
      nameRu: s.nameRu,
      normalizedUk: normalizeStreetName(s.nameUk),
      normalizedRu: normalizeStreetName(s.nameRu),
    }));

    // Create normalized name → street map for fast lookup
    const streetsByNormalizedName = new Map<string, StreetData[]>();
    for (const street of allStreets) {
      if (street.normalizedUk && street.normalizedUk.length >= 3) {
        if (!streetsByNormalizedName.has(street.normalizedUk)) {
          streetsByNormalizedName.set(street.normalizedUk, []);
        }
        streetsByNormalizedName.get(street.normalizedUk)!.push(street);
      }
      if (street.normalizedRu && street.normalizedRu.length >= 3) {
        if (!streetsByNormalizedName.has(street.normalizedRu)) {
          streetsByNormalizedName.set(street.normalizedRu, []);
        }
        streetsByNormalizedName.get(street.normalizedRu)!.push(street);
      }
    }

    logger.log(`Loaded ${allStreets.length} streets, ${streetsByNormalizedName.size} unique normalized names`);

    // Step 2: Get unique normalized names sorted by length (longest first for better matching)
    const normalizedNames = Array.from(streetsByNormalizedName.keys()).sort((a, b) => b.length - a.length);
    logger.log(`Searching for ${normalizedNames.length} unique street names`);

    // Step 3: Count OLX listings to process
    const countResult = await dataSource.query(`
      SELECT COUNT(*) as cnt FROM unified_listings
      WHERE realty_platform = 'olx' AND street_id IS NULL
        AND lat IS NOT NULL AND lng IS NOT NULL
    `);
    const totalCount = parseInt(countResult[0].cnt, 10);
    logger.log(`\nStep 2: Processing ${totalCount} OLX listings without street_id`);

    let processed = 0;
    let matchedByTitle = 0;
    let matchedByDescription = 0;
    let offset = 0;
    const batchSize = 1000;
    const startTime = Date.now();

    while (processed < totalCount) {
      // Fetch batch of listings
      const listings = await dataSource.query(`
        SELECT
          id,
          lng, lat,
          COALESCE(primary_data->>'title', '') as title,
          COALESCE(description->>'uk', '') as "descriptionUk"
        FROM unified_listings
        WHERE realty_platform = 'olx' AND street_id IS NULL
          AND lat IS NOT NULL AND lng IS NOT NULL
        ORDER BY id
        LIMIT $1 OFFSET $2
      `, [batchSize, offset]);

      if (listings.length === 0) break;

      const updates: { id: string; streetId: number }[] = [];

      for (const listing of listings) {
        let matchedStreetId: number | null = null;
        let matchMethod: 'title' | 'description' | null = null;

        // Try to find street name in title
        for (const normalizedName of normalizedNames) {
          if (findStreetInText(normalizedName, listing.title)) {
            // Found! Now get streets with this name that are within radius
            const streets = streetsByNormalizedName.get(normalizedName)!;

            // Check which of these streets is within radius
            const nearbyStreets = await dataSource.query(`
              SELECT s.id, ST_Distance(
                s.line::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
              ) as distance
              FROM streets s
              WHERE s.id = ANY($3::int[])
                AND ST_DWithin(
                  s.line::geography,
                  ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                  $4
                )
              ORDER BY distance
              LIMIT 1
            `, [listing.lng, listing.lat, streets.map(s => s.id), radiusMeters]);

            if (nearbyStreets.length > 0) {
              matchedStreetId = nearbyStreets[0].id;
              matchMethod = 'title';
              break;
            }
          }
        }

        // Try description if no title match
        if (!matchedStreetId && listing.descriptionUk) {
          for (const normalizedName of normalizedNames) {
            if (findStreetInText(normalizedName, listing.descriptionUk)) {
              const streets = streetsByNormalizedName.get(normalizedName)!;

              const nearbyStreets = await dataSource.query(`
                SELECT s.id, ST_Distance(
                  s.line::geography,
                  ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                ) as distance
                FROM streets s
                WHERE s.id = ANY($3::int[])
                  AND ST_DWithin(
                    s.line::geography,
                    ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                    $4
                  )
                ORDER BY distance
                LIMIT 1
              `, [listing.lng, listing.lat, streets.map(s => s.id), radiusMeters]);

              if (nearbyStreets.length > 0) {
                matchedStreetId = nearbyStreets[0].id;
                matchMethod = 'description';
                break;
              }
            }
          }
        }

        if (matchedStreetId) {
          updates.push({ id: listing.id, streetId: matchedStreetId });
          if (matchMethod === 'title') matchedByTitle++;
          else if (matchMethod === 'description') matchedByDescription++;
        }

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
        `Title: ${matchedByTitle} | Desc: ${matchedByDescription} | ` +
        `${elapsed}min (${rate}/min)`
      );
    }

    // Step 3: For remaining listings without street, use nearest fallback
    logger.log('\nStep 3: Updating remaining with nearest street (5km radius)...');
    const nearestResult = await dataSource.query(`
      UPDATE unified_listings ul
      SET street_id = nearest.street_id
      FROM (
        SELECT DISTINCT ON (ul2.id) ul2.id as listing_id, s.id as street_id
        FROM unified_listings ul2
        CROSS JOIN LATERAL (
          SELECT s.id
          FROM streets s
          WHERE s.line IS NOT NULL
            AND ST_DWithin(
              s.line::geography,
              ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography,
              $1
            )
          ORDER BY ST_Distance(
            s.line::geography,
            ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography
          )
          LIMIT 1
        ) s
        WHERE ul2.realty_platform = 'olx'
          AND ul2.street_id IS NULL
          AND ul2.lat IS NOT NULL
          AND ul2.lng IS NOT NULL
      ) nearest
      WHERE ul.id = nearest.listing_id
    `, [radiusMeters]);
    logger.log(`Updated ${nearestResult[1] || 0} with nearest street`);

    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

    logger.log('\n' + '='.repeat(60));
    logger.log('COMPLETED');
    logger.log('='.repeat(60));
    logger.log(`Total time: ${totalTime} minutes`);
    logger.log(`Matched by title: ${matchedByTitle}`);
    logger.log(`Matched by description: ${matchedByDescription}`);
    logger.log(`Nearest fallback: ${nearestResult[1] || 0}`);
    logger.log('='.repeat(60));

  } catch (error) {
    logger.error(`Fatal error: ${(error as Error).message}`);
    console.error(error);
  } finally {
    await app.close();
  }
}

main().catch(console.error);
