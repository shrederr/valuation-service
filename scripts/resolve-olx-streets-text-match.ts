import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';

const logger = new Logger('OlxStreetsTextMatch');

interface StreetCandidate {
  listingId: string;
  streetId: number;
  streetGeoId: number | null;
  nameUk: string | null;
  nameRu: string | null;
  distance: number;
}

interface ListingText {
  id: string;
  title: string;
  descriptionUk: string;
}

/**
 * Normalize street name for comparison
 * Removes common prefixes, quotes, extra spaces
 */
function normalizeStreetName(name: string | null): string {
  if (!name) return '';
  return name
    .toLowerCase()
    // Remove street type prefixes (Ukrainian and Russian)
    .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр-т|пр\.|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)\s*/gi, '')
    // Remove quotes and special chars
    .replace(/[«»""''`']/g, '')
    // Normalize dashes
    .replace(/[–—−]/g, '-')
    // Normalize spaces
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Normalize text for searching
 */
function normalizeText(text: string | null): string {
  if (!text) return '';
  return text
    .toLowerCase()
    .replace(/[«»""''`']/g, '')
    .replace(/[–—−]/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Check if normalized street name is found in normalized text
 */
function findStreetInText(normalizedStreet: string, normalizedText: string): boolean {
  if (!normalizedStreet || normalizedStreet.length < 3) return false;
  if (!normalizedText) return false;

  // Direct substring match
  if (normalizedText.includes(normalizedStreet)) {
    return true;
  }

  // Try word boundary match (street name followed by number, comma, etc.)
  const escaped = normalizedStreet.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(^|\\s|,|\\.|:)${escaped}($|\\s|,|\\.|:|\\d)`, 'i');
  return regex.test(normalizedText);
}

async function main() {
  const args = process.argv.slice(2);
  const batchSize = parseInt(args[0] || '500', 10);
  const radiusMeters = parseInt(args[1] || '5000', 10);
  const maxStreetsPerListing = parseInt(args[2] || '100', 10);

  logger.log('='.repeat(60));
  logger.log('OLX Street Resolution with Text Matching');
  logger.log('='.repeat(60));
  logger.log(`Batch size: ${batchSize}`);
  logger.log(`Search radius: ${radiusMeters}m`);
  logger.log(`Max streets per listing: ${maxStreetsPerListing}`);
  logger.log('Priority: title match → description match → nearest');
  logger.log('='.repeat(60));

  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const dataSource = app.get(DataSource);

  try {
    // Count OLX listings to process
    const countResult = await dataSource.query(`
      SELECT COUNT(*) as cnt FROM unified_listings
      WHERE realty_platform = 'olx' AND street_id IS NULL
        AND lat IS NOT NULL AND lng IS NOT NULL
    `);
    const totalCount = parseInt(countResult[0].cnt, 10);
    logger.log(`\nTotal OLX listings to process: ${totalCount}`);

    if (totalCount === 0) {
      logger.log('No listings to process. Exiting.');
      await app.close();
      return;
    }

    let processed = 0;
    let matchedByTitle = 0;
    let matchedByDescription = 0;
    let matchedByNearest = 0;
    let noStreetFound = 0;
    let offset = 0;

    const startTime = Date.now();

    while (processed < totalCount) {
      // Step 1: Get batch of listings with their text
      const listings = await dataSource.query<ListingText[]>(`
        SELECT
          id,
          COALESCE(primary_data->>'title', '') as title,
          COALESCE(description->>'uk', '') as "descriptionUk"
        FROM unified_listings
        WHERE realty_platform = 'olx' AND street_id IS NULL
          AND lat IS NOT NULL AND lng IS NOT NULL
        ORDER BY id
        LIMIT $1 OFFSET $2
      `, [batchSize, offset]);

      if (listings.length === 0) break;

      const listingIds = listings.map(l => l.id);

      // Step 2: Get all street candidates for this batch (within radius)
      const candidates = await dataSource.query<StreetCandidate[]>(`
        SELECT
          ul.id as "listingId",
          s.id as "streetId",
          s.geo_id as "streetGeoId",
          s.name->>'uk' as "nameUk",
          s.name->>'ru' as "nameRu",
          ST_Distance(
            s.line::geography,
            ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326)::geography
          ) as distance
        FROM unified_listings ul
        CROSS JOIN LATERAL (
          SELECT s.id, s.geo_id, s.name, s.line
          FROM streets s
          WHERE s.line IS NOT NULL
            AND ST_DWithin(
              s.line::geography,
              ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326)::geography,
              $2
            )
          ORDER BY ST_Distance(
            s.line::geography,
            ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326)::geography
          )
          LIMIT $3
        ) s
        WHERE ul.id = ANY($1::uuid[])
      `, [listingIds, radiusMeters, maxStreetsPerListing]);

      // Group candidates by listing
      const candidatesByListing = new Map<string, StreetCandidate[]>();
      for (const c of candidates) {
        if (!candidatesByListing.has(c.listingId)) {
          candidatesByListing.set(c.listingId, []);
        }
        candidatesByListing.get(c.listingId)!.push(c);
      }

      // Step 3: Do text matching for each listing
      const updates: { id: string; streetId: number; geoId: number | null }[] = [];

      for (const listing of listings) {
        const streetCandidates = candidatesByListing.get(listing.id) || [];

        if (streetCandidates.length === 0) {
          noStreetFound++;
          processed++;
          continue;
        }

        const normalizedTitle = normalizeText(listing.title);
        const normalizedDesc = normalizeText(listing.descriptionUk);

        let matchedStreet: StreetCandidate | null = null;
        let matchMethod: 'title' | 'description' | 'nearest' = 'nearest';

        // Try to find street name in title
        for (const street of streetCandidates) {
          const normalizedUk = normalizeStreetName(street.nameUk);
          const normalizedRu = normalizeStreetName(street.nameRu);

          if (normalizedUk && findStreetInText(normalizedUk, normalizedTitle)) {
            matchedStreet = street;
            matchMethod = 'title';
            break;
          }
          if (normalizedRu && findStreetInText(normalizedRu, normalizedTitle)) {
            matchedStreet = street;
            matchMethod = 'title';
            break;
          }
        }

        // Try to find street name in description
        if (!matchedStreet && normalizedDesc) {
          for (const street of streetCandidates) {
            const normalizedUk = normalizeStreetName(street.nameUk);
            const normalizedRu = normalizeStreetName(street.nameRu);

            if (normalizedUk && findStreetInText(normalizedUk, normalizedDesc)) {
              matchedStreet = street;
              matchMethod = 'description';
              break;
            }
            if (normalizedRu && findStreetInText(normalizedRu, normalizedDesc)) {
              matchedStreet = street;
              matchMethod = 'description';
              break;
            }
          }
        }

        // Fallback to nearest street
        if (!matchedStreet) {
          matchedStreet = streetCandidates[0]; // Already sorted by distance
          matchMethod = 'nearest';
        }

        updates.push({
          id: listing.id,
          streetId: matchedStreet.streetId,
          geoId: matchedStreet.streetGeoId,
        });

        if (matchMethod === 'title') matchedByTitle++;
        else if (matchMethod === 'description') matchedByDescription++;
        else matchedByNearest++;

        processed++;
      }

      // Step 4: Bulk update
      if (updates.length > 0) {
        // Update street_id and geo_id from street
        const values = updates.map(u =>
          `('${u.id}'::uuid, ${u.streetId}, ${u.geoId !== null ? u.geoId : 'NULL'})`
        ).join(',');

        await dataSource.query(`
          UPDATE unified_listings ul
          SET street_id = v.street_id,
              geo_id = COALESCE(v.geo_id, ul.geo_id)
          FROM (VALUES ${values}) AS v(id, street_id, geo_id)
          WHERE ul.id = v.id
        `);
      }

      offset += batchSize;

      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      const rate = Math.round(processed / Math.max(0.1, parseFloat(elapsed)));
      const titlePct = processed > 0 ? ((matchedByTitle / processed) * 100).toFixed(1) : '0';
      const descPct = processed > 0 ? ((matchedByDescription / processed) * 100).toFixed(1) : '0';
      const nearestPct = processed > 0 ? ((matchedByNearest / processed) * 100).toFixed(1) : '0';

      logger.log(
        `Progress: ${processed}/${totalCount} (${((processed/totalCount)*100).toFixed(1)}%) | ` +
        `Title: ${matchedByTitle} (${titlePct}%) | Desc: ${matchedByDescription} (${descPct}%) | ` +
        `Nearest: ${matchedByNearest} (${nearestPct}%) | NoStreet: ${noStreetFound} | ` +
        `${elapsed}min (${rate}/min)`
      );
    }

    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

    logger.log('\n' + '='.repeat(60));
    logger.log('COMPLETED');
    logger.log('='.repeat(60));
    logger.log(`Total time: ${totalTime} minutes`);
    logger.log(`Total processed: ${processed}`);
    logger.log(`Matched by title: ${matchedByTitle} (${((matchedByTitle/processed)*100).toFixed(1)}%)`);
    logger.log(`Matched by description: ${matchedByDescription} (${((matchedByDescription/processed)*100).toFixed(1)}%)`);
    logger.log(`Matched by nearest: ${matchedByNearest} (${((matchedByNearest/processed)*100).toFixed(1)}%)`);
    logger.log(`No street found (outside ${radiusMeters}m): ${noStreetFound}`);
    logger.log('='.repeat(60));

  } catch (error) {
    logger.error(`Fatal error: ${(error as Error).message}`);
    console.error(error);
  } finally {
    await app.close();
  }
}

main().catch(console.error);
