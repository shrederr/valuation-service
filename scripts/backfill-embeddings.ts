/**
 * Backfill script: generate embeddings for existing vector/vector_crm listings.
 *
 * Usage:
 *   npx ts-node -r tsconfig-paths/register scripts/backfill-embeddings.ts
 *
 * Env vars:
 *   DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE
 *   TRANSFORMERS_CACHE — path to cached ONNX model (optional)
 *   BATCH_SIZE — number of listings per batch (default: 50)
 */

import { Client } from 'pg';

async function main() {
  const batchSize = parseInt(process.env.BATCH_SIZE || '50', 10);

  const client = new Client({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5433', 10),
    user: process.env.DB_USERNAME || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
    database: process.env.DB_DATABASE || 'valuation',
  });

  await client.connect();
  console.log('Connected to database');

  const { pipeline, env } = await import('@huggingface/transformers');
  if (process.env.TRANSFORMERS_CACHE) {
    env.cacheDir = process.env.TRANSFORMERS_CACHE;
  }
  env.allowLocalModels = true;

  console.log('Loading embedding model...');
  const embedder = await pipeline(
    'feature-extraction',
    'Xenova/paraphrase-multilingual-mpnet-base-v2',
    { dtype: 'fp32' },
  );
  console.log('Model loaded');

  const countResult = await client.query(
    `SELECT COUNT(*) as cnt FROM unified_listings
     WHERE source_type IN ('vector', 'vector_crm') AND is_active = true
       AND embedding IS NULL AND description IS NOT NULL`,
  );
  const total = parseInt(countResult.rows[0].cnt, 10);
  console.log(`Total listings to process: ${total}`);

  let processed = 0;
  let errors = 0;

  while (true) {
    const batch = await client.query(
      `SELECT id, description FROM unified_listings
       WHERE source_type IN ('vector', 'vector_crm') AND is_active = true
         AND embedding IS NULL AND description IS NOT NULL
       ORDER BY updated_at ASC
       LIMIT $1`,
      [batchSize],
    );

    if (batch.rows.length === 0) break;

    for (const row of batch.rows) {
      try {
        const desc = row.description;
        const text = desc?.uk || desc?.ru || desc?.en || '';
        if (!text || text.length < 10) {
          processed++;
          continue;
        }

        const result = await embedder(text, {
          pooling: 'mean',
          normalize: true,
        });
        const embedding = Array.from(result.data as Float32Array);
        const vectorStr = `[${embedding.join(',')}]`;

        await client.query(
          `UPDATE unified_listings SET embedding = $1::vector WHERE id = $2`,
          [vectorStr, row.id],
        );
        processed++;
      } catch (err) {
        errors++;
        console.error(`Error processing ${row.id}: ${err}`);
      }
    }

    const pct = ((processed / total) * 100).toFixed(1);
    console.log(`Processed: ${processed}/${total} (${pct}%) | Errors: ${errors}`);
  }

  console.log(`\nDone! Processed: ${processed}, Errors: ${errors}`);
  await client.end();
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
