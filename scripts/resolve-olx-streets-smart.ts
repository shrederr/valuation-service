import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';
import { UnifiedListing } from '@libs/database';

const logger = new Logger('OlxStreetsSmart');

interface StreetCandidate {
  id: number;
  nameUk: string | null;
  nameRu: string | null;
  distanceMeters: number;
}

interface MatchResult {
  streetId: number;
  matchMethod: 'title' | 'description' | 'nearest';
  matchedName: string | null;
}

/**
 * Normalize street name for comparison
 * Removes common prefixes, lowercase, trim
 */
function normalizeStreetName(name: string | null): string {
  if (!name) return '';

  return name
    .toLowerCase()
    // Remove street type prefixes
    .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр\.|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.)\s*/gi, '')
    // Remove quotes and special chars
    .replace(/[«»""''`]/g, '')
    // Normalize spaces
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Normalize text for searching (title/description)
 */
function normalizeText(text: string | null): string {
  if (!text) return '';

  return text
    .toLowerCase()
    // Normalize quotes
    .replace(/[«»""''`]/g, '"')
    // Normalize dashes
    .replace(/[–—−]/g, '-')
    // Normalize spaces
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Check if street name is found in text
 * Returns true if found with word boundaries
 */
function findStreetInText(streetName: string, text: string): boolean {
  if (!streetName || streetName.length < 3) return false;
  if (!text) return false;

  const normalizedStreet = normalizeStreetName(streetName);
  const normalizedText = normalizeText(text);

  if (normalizedStreet.length < 3) return false;

  // Try exact match first
  if (normalizedText.includes(normalizedStreet)) {
    return true;
  }

  // Try with word boundaries (street name followed by number, comma, space, etc.)
  const escapedStreet = normalizedStreet.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`\\b${escapedStreet}\\b|${escapedStreet}[\\s,\\.\\d]`, 'i');

  return regex.test(normalizedText);
}

/**
 * Try to match any street candidate in text
 */
function matchStreetInText(candidates: StreetCandidate[], text: string): StreetCandidate | null {
  if (!text) return null;

  for (const candidate of candidates) {
    // Try Ukrainian name
    if (candidate.nameUk && findStreetInText(candidate.nameUk, text)) {
      return candidate;
    }
    // Try Russian name
    if (candidate.nameRu && findStreetInText(candidate.nameRu, text)) {
      return candidate;
    }
  }

  return null;
}

async function main() {
  const args = process.argv.slice(2);
  const batchSize = parseInt(args[0] || '1000', 10);
  const radiusMeters = parseInt(args[1] || '5000', 10);

  logger.log('='.repeat(60));
  logger.log('OLX Smart Street Resolution');
  logger.log('='.repeat(60));
  logger.log(`Batch size: ${batchSize}`);
  logger.log(`Search radius: ${radiusMeters}m`);
  logger.log('Priority: title match → description match → nearest');
  logger.log('='.repeat(60));

  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const dataSource = app.get(DataSource);
  const listingRepository = dataSource.getRepository(UnifiedListing);

  try {
    // Count OLX listings without street_id
    const totalCount = await listingRepository.createQueryBuilder('ul')
      .where('ul.realty_platform = :platform', { platform: 'olx' })
      .andWhere('ul.street_id IS NULL')
      .andWhere('ul.lat IS NOT NULL')
      .andWhere('ul.lng IS NOT NULL')
      .getCount();

    logger.log(`Total OLX listings to process: ${totalCount}`);

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
    let errors = 0;

    const startTime = Date.now();

    while (processed < totalCount) {
      // Fetch batch
      const listings = await listingRepository.createQueryBuilder('ul')
        .where('ul.realty_platform = :platform', { platform: 'olx' })
        .andWhere('ul.street_id IS NULL')
        .andWhere('ul.lat IS NOT NULL')
        .andWhere('ul.lng IS NOT NULL')
        .orderBy('ul.id', 'ASC')
        .take(batchSize)
        .getMany();

      if (listings.length === 0) break;

      for (const listing of listings) {
        try {
          // Get all streets within radius
          const candidates = await dataSource.query<StreetCandidate[]>(`
            SELECT
              s.id,
              s.name->>'uk' as "nameUk",
              s.name->>'ru' as "nameRu",
              ST_Distance(
                s.line::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
              ) as "distanceMeters"
            FROM streets s
            WHERE s.line IS NOT NULL
              AND ST_DWithin(
                s.line::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                $3
              )
            ORDER BY "distanceMeters" ASC
            LIMIT 100
          `, [listing.lng, listing.lat, radiusMeters]);

          if (candidates.length === 0) {
            noStreetFound++;
            processed++;
            continue;
          }

          // Extract title and description
          const title = (listing.primaryData as any)?.title || '';
          const descriptionUk = (listing.description as any)?.uk || '';

          let result: MatchResult | null = null;

          // 1. Try to match in title
          const titleMatch = matchStreetInText(candidates, title);
          if (titleMatch) {
            result = {
              streetId: titleMatch.id,
              matchMethod: 'title',
              matchedName: titleMatch.nameUk || titleMatch.nameRu,
            };
            matchedByTitle++;
          }

          // 2. Try to match in description
          if (!result) {
            const descMatch = matchStreetInText(candidates, descriptionUk);
            if (descMatch) {
              result = {
                streetId: descMatch.id,
                matchMethod: 'description',
                matchedName: descMatch.nameUk || descMatch.nameRu,
              };
              matchedByDescription++;
            }
          }

          // 3. Fallback to nearest
          if (!result) {
            result = {
              streetId: candidates[0].id,
              matchMethod: 'nearest',
              matchedName: null,
            };
            matchedByNearest++;
          }

          // Update listing
          listing.streetId = result.streetId;
          await listingRepository.save(listing);

          processed++;

          if (processed % 500 === 0) {
            const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
            const rate = (processed / parseFloat(elapsed)).toFixed(0);
            logger.log(
              `Progress: ${processed}/${totalCount} (${((processed/totalCount)*100).toFixed(1)}%) | ` +
              `Title: ${matchedByTitle} | Desc: ${matchedByDescription} | Nearest: ${matchedByNearest} | ` +
              `NoStreet: ${noStreetFound} | ${elapsed}min (${rate}/min)`
            );
          }
        } catch (error) {
          errors++;
          if (errors <= 5) {
            logger.error(`Error processing ${listing.id}: ${(error as Error).message}`);
          }
        }
      }
    }

    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

    logger.log('='.repeat(60));
    logger.log('COMPLETED');
    logger.log('='.repeat(60));
    logger.log(`Total time: ${totalTime} minutes`);
    logger.log(`Total processed: ${processed}`);
    logger.log(`Matched by title: ${matchedByTitle} (${((matchedByTitle/processed)*100).toFixed(1)}%)`);
    logger.log(`Matched by description: ${matchedByDescription} (${((matchedByDescription/processed)*100).toFixed(1)}%)`);
    logger.log(`Matched by nearest: ${matchedByNearest} (${((matchedByNearest/processed)*100).toFixed(1)}%)`);
    logger.log(`No street found: ${noStreetFound}`);
    logger.log(`Errors: ${errors}`);
    logger.log('='.repeat(60));

  } catch (error) {
    logger.error(`Fatal error: ${(error as Error).message}`);
    console.error(error);
  } finally {
    await app.close();
  }
}

main().catch(console.error);
