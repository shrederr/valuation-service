import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddExportTracking1741100000000 implements MigrationInterface {
  name = 'AddExportTracking1741100000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS postgis;`);
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS vector;`);

    // Export tracking columns
    await queryRunner.query(`
      ALTER TABLE unified_listings
        ADD COLUMN IF NOT EXISTS export_status VARCHAR(20),
        ADD COLUMN IF NOT EXISTS crm_external_id VARCHAR(100),
        ADD COLUMN IF NOT EXISTS last_exported_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS export_error TEXT;
    `);

    // Phone for dedup
    await queryRunner.query(`
      ALTER TABLE unified_listings
        ADD COLUMN IF NOT EXISTS normalized_phone VARCHAR(20);
    `);

    // Embedding for semantic dedup (pgvector)
    await queryRunner.query(`
      ALTER TABLE unified_listings
        ADD COLUMN IF NOT EXISTS embedding vector(768);
    `);

    // Index for export cron
    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS idx_unified_listings_export
        ON unified_listings (source_type, is_active, export_status, updated_at)
        WHERE source_type = 'aggregator';
    `);

    // Dedup indexes
    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS idx_dedup_address
        ON unified_listings (street_id, house_number, rooms)
        WHERE source_type IN ('vector', 'vector_crm') AND is_active = true;
    `);

    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS idx_dedup_phone
        ON unified_listings (normalized_phone)
        WHERE source_type IN ('vector', 'vector_crm') AND is_active = true
          AND normalized_phone IS NOT NULL;
    `);

    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS idx_dedup_geo
        ON unified_listings USING GIST (
          geography(ST_SetSRID(ST_MakePoint(lng::float8, lat::float8), 4326))
        )
        WHERE source_type IN ('vector', 'vector_crm') AND is_active = true
          AND lat IS NOT NULL AND lng IS NOT NULL;
    `);

    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS idx_dedup_embedding_hnsw
        ON unified_listings USING hnsw (embedding vector_cosine_ops)
        WHERE source_type IN ('vector', 'vector_crm') AND is_active = true
          AND embedding IS NOT NULL;
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX IF EXISTS idx_dedup_embedding_hnsw;`);
    await queryRunner.query(`DROP INDEX IF EXISTS idx_dedup_geo;`);
    await queryRunner.query(`DROP INDEX IF EXISTS idx_dedup_phone;`);
    await queryRunner.query(`DROP INDEX IF EXISTS idx_dedup_address;`);
    await queryRunner.query(`DROP INDEX IF EXISTS idx_unified_listings_export;`);

    await queryRunner.query(`
      ALTER TABLE unified_listings
        DROP COLUMN IF EXISTS embedding,
        DROP COLUMN IF EXISTS normalized_phone,
        DROP COLUMN IF EXISTS export_error,
        DROP COLUMN IF EXISTS last_exported_at,
        DROP COLUMN IF EXISTS crm_external_id,
        DROP COLUMN IF EXISTS export_status;
    `);
  }
}
