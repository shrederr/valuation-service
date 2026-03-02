/**
 * Resync missing aggregator objects.
 *
 * Finds objects that exist in aggregator DB but are missing from valuation DB,
 * then publishes them as RabbitMQ messages for the existing consumer to process.
 *
 * Usage: node scripts/resync-missing-aggregator.js [--dry-run] [--limit N] [--batch-size N]
 *
 * Run on server: cd /var/www/liquidity-define && node scripts/resync-missing-aggregator.js
 */

const { Client } = require('pg');
const amqplib = require('amqplib');

// Config
const AGGREGATOR_DB = {
  host: '127.0.0.1',
  port: 54325,
  database: 'vector',
  user: 'vector',
  password: 'vector',
};

const VALUATION_DB = {
  host: '127.0.0.1',
  port: 5433,
  database: 'valuation',
  user: 'postgres',
  password: 'postgres',
};

const RABBITMQ_URL = 'amqp://vector:vector@127.0.0.1:5672';
const EXCHANGE = 'valuation_exchange';
const ROUTING_KEY = 'valuation.aggregator.property.created';

// Parse args
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const LIMIT = parseInt(args.find((a, i) => args[i - 1] === '--limit') || '0', 10);
const BATCH_SIZE = parseInt(args.find((a, i) => args[i - 1] === '--batch-size') || '100', 10);
const DELAY_MS = parseInt(args.find((a, i) => args[i - 1] === '--delay') || '50', 10);

async function main() {
  console.log('=== Resync Missing Aggregator Objects ===');
  console.log(`DRY_RUN: ${DRY_RUN}, LIMIT: ${LIMIT || 'ALL'}, BATCH_SIZE: ${BATCH_SIZE}, DELAY: ${DELAY_MS}ms`);

  // Connect to both DBs
  const aggClient = new Client(AGGREGATOR_DB);
  const valClient = new Client(VALUATION_DB);

  await aggClient.connect();
  console.log('Connected to aggregator DB');
  await valClient.connect();
  console.log('Connected to valuation DB');

  // Step 1: Find missing IDs
  console.log('\nStep 1: Finding missing IDs...');

  // Get active aggregator IDs (recent)
  const aggResult = await aggClient.query(
    'SELECT id FROM exported_properties WHERE is_active = true AND id >= 1200000 ORDER BY id',
  );
  const aggIds = new Set(aggResult.rows.map((r) => r.id));
  console.log(`  Aggregator active IDs (>= 1200000): ${aggIds.size}`);

  // Get our IDs
  const valResult = await valClient.query(
    "SELECT source_id FROM unified_listings WHERE source_type = 'aggregator' AND source_id >= 1200000",
  );
  const valIds = new Set(valResult.rows.map((r) => r.source_id));
  console.log(`  Valuation IDs (>= 1200000): ${valIds.size}`);

  // Find missing
  let missingIds = [...aggIds].filter((id) => !valIds.has(id)).sort((a, b) => a - b);
  console.log(`  Missing: ${missingIds.length}`);

  if (LIMIT > 0) {
    missingIds = missingIds.slice(0, LIMIT);
    console.log(`  Limited to: ${missingIds.length}`);
  }

  if (missingIds.length === 0) {
    console.log('\nNo missing objects. Done!');
    await aggClient.end();
    await valClient.end();
    return;
  }

  // Step 2: Connect to RabbitMQ
  let channel = null;
  if (!DRY_RUN) {
    console.log('\nStep 2: Connecting to RabbitMQ...');
    const conn = await amqplib.connect(RABBITMQ_URL);
    channel = await conn.createChannel();
    await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
    console.log('  Connected to RabbitMQ');
  }

  // Step 3: Process in batches
  console.log(`\nStep 3: Processing ${missingIds.length} objects...`);
  let processed = 0;
  let errors = 0;

  for (let i = 0; i < missingIds.length; i += BATCH_SIZE) {
    const batchIds = missingIds.slice(i, i + BATCH_SIZE);

    // Fetch full data from aggregator
    const placeholders = batchIds.map((_, idx) => `$${idx + 1}`).join(',');
    const batchData = await aggClient.query(
      `SELECT
        id, deal_type, realty_type, realty_platform,
        geo_id, street_id, complex_id, topzone_id,
        house_number, lat, lng,
        price, attributes, primary_data, description,
        images, external_url, external_id,
        is_active, is_exported, corps, apartment_number,
        created_at, updated_at, deleted_at, hash
      FROM exported_properties
      WHERE id IN (${placeholders})`,
      batchIds,
    );

    for (const row of batchData.rows) {
      try {
        // Transform to AggregatorPropertyEventDto format
        const dto = {
          id: row.id,
          externalId: row.external_id,
          dealType: row.deal_type,
          realtyType: row.realty_type,
          realtyPlatform: row.realty_platform,
          geoId: row.geo_id,
          streetId: row.street_id,
          topzoneId: row.topzone_id,
          complexId: row.complex_id,
          houseNumber: row.house_number,
          lat: row.lat ? parseFloat(row.lat) : null,
          lng: row.lng ? parseFloat(row.lng) : null,
          price: row.price ? parseFloat(row.price) : null,
          currency: 'USD',
          attributes: row.attributes || {},
          primaryData: row.primary_data || null,
          description: row.description || {},
          images: row.images || [],
          url: row.external_url,
          external_url: row.external_url,
          hash: row.hash,
          isActive: row.is_active,
          isExported: row.is_exported,
          corps: row.corps,
          apartmentNumber: row.apartment_number,
          createdAt: row.created_at,
          updatedAt: row.updated_at,
          deletedAt: row.deleted_at,
        };

        if (DRY_RUN) {
          console.log(`  [DRY] Would publish ID=${dto.id} (${dto.realtyPlatform}, ${dto.realtyType})`);
        } else {
          channel.publish(
            EXCHANGE,
            ROUTING_KEY,
            Buffer.from(JSON.stringify(dto)),
            { persistent: true, contentType: 'application/json' },
          );
        }

        processed++;
      } catch (err) {
        errors++;
        console.error(`  Error processing ID=${row.id}: ${err.message}`);
      }
    }

    // Progress
    const pct = Math.round(((i + batchIds.length) / missingIds.length) * 100);
    process.stdout.write(`\r  Progress: ${i + batchIds.length}/${missingIds.length} (${pct}%) - published: ${processed}, errors: ${errors}`);

    // Delay between batches to avoid overwhelming the consumer
    if (!DRY_RUN && i + BATCH_SIZE < missingIds.length) {
      await new Promise((r) => setTimeout(r, DELAY_MS));
    }
  }

  console.log('\n');
  console.log(`\n=== Done ===`);
  console.log(`  Total missing: ${missingIds.length}`);
  console.log(`  Published: ${processed}`);
  console.log(`  Errors: ${errors}`);

  // Cleanup
  if (channel) {
    await channel.close();
  }
  await aggClient.end();
  await valClient.end();
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
