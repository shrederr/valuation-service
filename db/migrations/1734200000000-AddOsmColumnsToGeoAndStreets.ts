import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddOsmColumnsToGeoAndStreets1734200000000 implements MigrationInterface {
  name = 'AddOsmColumnsToGeoAndStreets1734200000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Add OSM-related columns to geo table
    await queryRunner.query(`ALTER TABLE "geo" ADD COLUMN IF NOT EXISTS "osm_id" BIGINT`);
    await queryRunner.query(`ALTER TABLE "geo" ADD COLUMN IF NOT EXISTS "parent_id" INTEGER`);
    await queryRunner.query(`ALTER TABLE "geo" ADD COLUMN IF NOT EXISTS "polygon" geometry(MultiPolygon, 4326)`);
    await queryRunner.query(`ALTER TABLE "geo" ADD COLUMN IF NOT EXISTS "population" INTEGER`);

    // Create indexes for geo
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "idx_geo_osm_id" ON "geo" ("osm_id") WHERE osm_id IS NOT NULL`);
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "idx_geo_parent_id" ON "geo" ("parent_id") WHERE parent_id IS NOT NULL`);
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "idx_geo_polygon" ON "geo" USING GIST ("polygon")`);

    // Add OSM-related columns to streets table
    await queryRunner.query(`ALTER TABLE "streets" ADD COLUMN IF NOT EXISTS "osm_id" BIGINT`);
    await queryRunner.query(`ALTER TABLE "streets" ADD COLUMN IF NOT EXISTS "line" geometry(MultiLineString, 4326)`);
    await queryRunner.query(`ALTER TABLE "streets" ADD COLUMN IF NOT EXISTS "names" JSONB`);

    // Create indexes for streets
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "idx_streets_osm_id" ON "streets" ("osm_id") WHERE osm_id IS NOT NULL`);
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "idx_streets_line" ON "streets" USING GIST ("line")`);

    // Enable pg_trgm extension for text search
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS pg_trgm`);

    // Create trigram index for street name search
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "idx_streets_name_gin" ON "streets" USING GIN ((name->>'uk') gin_trgm_ops)`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Remove indexes from streets
    await queryRunner.query(`DROP INDEX IF EXISTS "idx_streets_name_gin"`);
    await queryRunner.query(`DROP INDEX IF EXISTS "idx_streets_line"`);
    await queryRunner.query(`DROP INDEX IF EXISTS "idx_streets_osm_id"`);

    // Remove columns from streets
    await queryRunner.query(`ALTER TABLE "streets" DROP COLUMN IF EXISTS "names"`);
    await queryRunner.query(`ALTER TABLE "streets" DROP COLUMN IF EXISTS "line"`);
    await queryRunner.query(`ALTER TABLE "streets" DROP COLUMN IF EXISTS "osm_id"`);

    // Remove indexes from geo
    await queryRunner.query(`DROP INDEX IF EXISTS "idx_geo_polygon"`);
    await queryRunner.query(`DROP INDEX IF EXISTS "idx_geo_parent_id"`);
    await queryRunner.query(`DROP INDEX IF EXISTS "idx_geo_osm_id"`);

    // Remove columns from geo
    await queryRunner.query(`ALTER TABLE "geo" DROP COLUMN IF EXISTS "population"`);
    await queryRunner.query(`ALTER TABLE "geo" DROP COLUMN IF EXISTS "polygon"`);
    await queryRunner.query(`ALTER TABLE "geo" DROP COLUMN IF EXISTS "parent_id"`);
    await queryRunner.query(`ALTER TABLE "geo" DROP COLUMN IF EXISTS "osm_id"`);
  }
}
