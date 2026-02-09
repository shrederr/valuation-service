import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddInfrastructureColumns1767300000000 implements MigrationInterface {
  name = 'AddInfrastructureColumns1767300000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Infrastructure distance columns (in meters)
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS nearest_school integer');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS nearest_hospital integer');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS nearest_supermarket integer');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS nearest_parking integer');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS nearest_public_transport integer');

    // Full infrastructure data as JSONB
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS infrastructure jsonb');

    // Index for infrastructure-based filtering
    await queryRunner.query('CREATE INDEX IF NOT EXISTS idx_ul_nearest_school ON unified_listings (nearest_school) WHERE nearest_school IS NOT NULL');
    await queryRunner.query('CREATE INDEX IF NOT EXISTS idx_ul_nearest_supermarket ON unified_listings (nearest_supermarket) WHERE nearest_supermarket IS NOT NULL');
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('DROP INDEX IF EXISTS idx_ul_nearest_school');
    await queryRunner.query('DROP INDEX IF EXISTS idx_ul_nearest_supermarket');
    await queryRunner.query('ALTER TABLE unified_listings DROP COLUMN IF EXISTS nearest_school');
    await queryRunner.query('ALTER TABLE unified_listings DROP COLUMN IF EXISTS nearest_hospital');
    await queryRunner.query('ALTER TABLE unified_listings DROP COLUMN IF EXISTS nearest_supermarket');
    await queryRunner.query('ALTER TABLE unified_listings DROP COLUMN IF EXISTS nearest_parking');
    await queryRunner.query('ALTER TABLE unified_listings DROP COLUMN IF EXISTS nearest_public_transport');
    await queryRunner.query('ALTER TABLE unified_listings DROP COLUMN IF EXISTS infrastructure');
  }
}
