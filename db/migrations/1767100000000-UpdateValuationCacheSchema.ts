import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateValuationCacheSchema1767100000000 implements MigrationInterface {
  name = 'UpdateValuationCacheSchema1767100000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Drop the old valuation_cache table and recreate with correct schema
    await queryRunner.query(`DROP TABLE IF EXISTS "valuation_cache" CASCADE`);

    // Create new valuation_cache table with JSONB columns
    await queryRunner.query(`
      CREATE TABLE "valuation_cache" (
        "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
        "listing_id" uuid NOT NULL,
        "analogs_data" jsonb NOT NULL,
        "fair_price" jsonb NOT NULL,
        "liquidity" jsonb NOT NULL,
        "calculated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        "expires_at" TIMESTAMP WITH TIME ZONE NOT NULL,
        CONSTRAINT "PK_valuation_cache_id" PRIMARY KEY ("id")
      )
    `);

    // Create indexes
    await queryRunner.query(`CREATE INDEX "IDX_valuation_cache_listing_id" ON "valuation_cache" ("listing_id")`);
    await queryRunner.query(`CREATE INDEX "IDX_valuation_cache_calculated_at" ON "valuation_cache" ("calculated_at")`);
    await queryRunner.query(`CREATE INDEX "IDX_valuation_cache_expires_at" ON "valuation_cache" ("expires_at")`);

    // Add foreign key
    await queryRunner.query(`
      ALTER TABLE "valuation_cache" ADD CONSTRAINT "FK_valuation_cache_listing"
      FOREIGN KEY ("listing_id") REFERENCES "unified_listings"("id") ON DELETE CASCADE
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Drop new table
    await queryRunner.query(`DROP TABLE IF EXISTS "valuation_cache" CASCADE`);

    // Recreate old table structure
    await queryRunner.query(`
      CREATE TABLE "valuation_cache" (
        "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
        "listing_id" uuid NOT NULL,
        "fair_price_median" numeric(15,2) NOT NULL,
        "fair_price_min" numeric(15,2) NOT NULL,
        "fair_price_max" numeric(15,2) NOT NULL,
        "price_range_low" numeric(15,2) NOT NULL,
        "price_range_high" numeric(15,2) NOT NULL,
        "liquidity_score" numeric(3,1) NOT NULL,
        "analogs_count" integer NOT NULL,
        "search_radius" varchar(50) NOT NULL,
        "calculated_at" TIMESTAMP NOT NULL DEFAULT now(),
        "expires_at" TIMESTAMP NOT NULL,
        "raw_result" jsonb,
        CONSTRAINT "PK_valuation_cache_id" PRIMARY KEY ("id")
      )
    `);
    await queryRunner.query(`CREATE INDEX "IDX_vc_listing_id" ON "valuation_cache" ("listing_id")`);
    await queryRunner.query(`CREATE INDEX "IDX_vc_expires_at" ON "valuation_cache" ("expires_at")`);
    await queryRunner.query(`
      ALTER TABLE "valuation_cache" ADD CONSTRAINT "FK_vc_listing"
      FOREIGN KEY ("listing_id") REFERENCES "unified_listings"("id") ON DELETE CASCADE
    `);
  }
}
