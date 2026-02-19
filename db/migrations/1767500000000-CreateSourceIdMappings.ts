import { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateSourceIdMappings1767500000000 implements MigrationInterface {
  name = 'CreateSourceIdMappings1767500000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE IF NOT EXISTS "source_id_mappings" (
        "id" SERIAL PRIMARY KEY,
        "source" VARCHAR(30) NOT NULL,
        "entity_type" VARCHAR(20) NOT NULL,
        "source_id" INTEGER NOT NULL,
        "local_id" INTEGER NOT NULL,
        "confidence" DECIMAL(3,2) NOT NULL DEFAULT 1.0,
        "match_method" VARCHAR(30),
        "created_at" TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);

    await queryRunner.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS "IDX_source_mapping_unique"
      ON "source_id_mappings" ("source", "entity_type", "source_id")
    `);

    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS "IDX_source_mapping_local"
      ON "source_id_mappings" ("source", "entity_type", "local_id")
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE IF EXISTS "source_id_mappings"`);
  }
}
