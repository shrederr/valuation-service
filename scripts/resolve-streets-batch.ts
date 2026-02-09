import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';
import { UnifiedListing } from '@libs/database';
import { StreetMatcherService } from '../apps/valuation/src/modules/osm/street-matcher.service';

const logger = new Logger('ResolveStreetsBatch');

function extractDescriptionText(description: unknown): string {
  if (!description) return '';

  if (typeof description === 'object') {
    const desc = description as Record<string, string>;
    const parts: string[] = [];
    if (desc.uk) parts.push(desc.uk);
    if (desc.ru) parts.push(desc.ru);
    if (desc.en) parts.push(desc.en);
    return parts.join(' ').substring(0, 2000);
  }

  if (typeof description === 'string') {
    try {
      const parsed = JSON.parse(description);
      return extractDescriptionText(parsed);
    } catch {
      return description.substring(0, 2000);
    }
  }

  return '';
}

async function main() {
  const args = process.argv.slice(2);
  const batchSize = parseInt(args[0] || '500', 10);
  const excludePlatforms = (args[1] || 'olx').split(',');

  logger.log('='.repeat(60));
  logger.log('Batch Street Resolution for Unified Listings');
  logger.log('='.repeat(60));
  logger.log(`Batch size: ${batchSize}`);
  logger.log(`Excluding platforms: ${excludePlatforms.join(', ')}`);
  logger.log('Processing listings WITH geo_id but WITHOUT street_id');
  logger.log('='.repeat(60));

  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const dataSource = app.get(DataSource);
  const streetMatcherService = app.get(StreetMatcherService);
  const listingRepository = dataSource.getRepository(UnifiedListing);

  try {
    // Count total listings to process (has geo_id, no street_id, has coordinates)
    const totalCount = await listingRepository.createQueryBuilder('ul')
      .where('ul.geo_id IS NOT NULL')
      .andWhere('ul.street_id IS NULL')
      .andWhere('ul.lat IS NOT NULL')
      .andWhere('ul.lng IS NOT NULL')
      .andWhere('ul.realty_platform NOT IN (:...excludePlatforms)', { excludePlatforms })
      .getCount();

    logger.log(`Total listings to process: ${totalCount}`);

    if (totalCount === 0) {
      logger.log('No listings to process. Exiting.');
      await app.close();
      return;
    }

    let processed = 0;
    let streetResolved = 0;
    let streetByText = 0;
    let streetByNearest = 0;
    let noStreetFound = 0;
    let errors = 0;

    while (processed < totalCount) {
      // Fetch batch - always get first N without street_id
      const listings = await listingRepository.createQueryBuilder('ul')
        .where('ul.geo_id IS NOT NULL')
        .andWhere('ul.street_id IS NULL')
        .andWhere('ul.lat IS NOT NULL')
        .andWhere('ul.lng IS NOT NULL')
        .andWhere('ul.realty_platform NOT IN (:...excludePlatforms)', { excludePlatforms })
        .orderBy('ul.id', 'ASC')
        .take(batchSize)
        .getMany();

      if (listings.length === 0) break;

      for (const listing of listings) {
        try {
          if (!listing.lat || !listing.lng) continue;

          // Get description text for street matching
          const descText = extractDescriptionText(listing.description);
          const urlText = listing.externalUrl || '';
          const textForMatching = `${descText} ${urlText}`;

          // Use street matcher with text
          const result = await streetMatcherService.resolveStreet(
            listing.lng,
            listing.lat,
            textForMatching,
            listing.geoId || undefined,
          );

          if (result.streetId) {
            listing.streetId = result.streetId;
            await listingRepository.save(listing);
            streetResolved++;

            if (result.matchMethod === 'text_parsed' || result.matchMethod === 'text_found') {
              streetByText++;
            } else if (result.matchMethod === 'nearest') {
              streetByNearest++;
            }
          } else {
            noStreetFound++;
          }

          processed++;

          if (processed % 100 === 0) {
            const progress = ((processed / totalCount) * 100).toFixed(2);
            logger.log(
              `Progress: ${processed}/${totalCount} (${progress}%) | ` +
              `Resolved: ${streetResolved} (text: ${streetByText}, nearest: ${streetByNearest}) | ` +
              `NoStreet: ${noStreetFound}`
            );
          }
        } catch (error) {
          errors++;
          if (errors <= 10) {
            logger.error(`Error processing listing ${listing.id}: ${(error as Error).message}`);
          }
        }
      }
    }

    logger.log('='.repeat(60));
    logger.log('COMPLETED');
    logger.log('='.repeat(60));
    logger.log(`Total processed: ${processed}`);
    logger.log(`Streets resolved: ${streetResolved}`);
    logger.log(`  - By text parsing/matching: ${streetByText}`);
    logger.log(`  - By nearest: ${streetByNearest}`);
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
