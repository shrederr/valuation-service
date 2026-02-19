import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddVectorCrmSourceType1767400000000 implements MigrationInterface {
  name = 'AddVectorCrmSourceType1767400000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      ALTER TYPE "unified_listings_source_type_enum" ADD VALUE IF NOT EXISTS 'vector_crm'
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // PostgreSQL doesn't support removing enum values directly
    // To reverse: recreate enum without 'vector_crm' and update all references
    // This is intentionally left as a no-op for safety
  }
}
