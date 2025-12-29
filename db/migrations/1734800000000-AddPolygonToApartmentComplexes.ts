import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddPolygonToApartmentComplexes1734800000000 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    // Add polygon column for PostGIS geometry
    await queryRunner.query(`
      ALTER TABLE apartment_complexes
      ADD COLUMN IF NOT EXISTS polygon geometry(Polygon, 4326)
    `);

    // Create spatial index for polygon queries
    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS idx_ac_polygon
      ON apartment_complexes USING GIST(polygon)
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX IF EXISTS idx_ac_polygon`);
    await queryRunner.query(`ALTER TABLE apartment_complexes DROP COLUMN IF EXISTS polygon`);
  }
}
