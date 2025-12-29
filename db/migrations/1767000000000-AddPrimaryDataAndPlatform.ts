import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddPrimaryDataAndPlatform1767000000000 implements MigrationInterface {
  name = 'AddPrimaryDataAndPlatform1767000000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      ALTER TABLE "unified_listings"
      ADD COLUMN IF NOT EXISTS "primary_data" jsonb;
    `);

    await queryRunner.query(`
      ALTER TABLE "unified_listings"
      ADD COLUMN IF NOT EXISTS "realty_platform" text;
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      ALTER TABLE "unified_listings"
      DROP COLUMN IF EXISTS "realty_platform";
    `);

    await queryRunner.query(`
      ALTER TABLE "unified_listings"
      DROP COLUMN IF EXISTS "primary_data";
    `);
  }
}
