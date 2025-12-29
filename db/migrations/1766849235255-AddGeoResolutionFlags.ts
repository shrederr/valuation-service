import { MigrationInterface, QueryRunner } from "typeorm";

export class AddGeoResolutionFlags1766849235255 implements MigrationInterface {
    name = 'AddGeoResolutionFlags1766849235255'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "unified_listings" ADD "geo_resolution_flags" text array`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "unified_listings" DROP COLUMN "geo_resolution_flags"`);
    }

}
