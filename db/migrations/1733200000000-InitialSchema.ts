import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialSchema1733200000000 implements MigrationInterface {
  name = 'InitialSchema1733200000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Create enums
    await queryRunner.query(`
      CREATE TYPE "source_type_enum" AS ENUM ('vector', 'aggregator')
    `);
    await queryRunner.query(`
      CREATE TYPE "deal_type_enum" AS ENUM ('sell', 'rent')
    `);
    await queryRunner.query(`
      CREATE TYPE "realty_type_enum" AS ENUM ('apartment', 'house', 'commercial', 'land', 'garage', 'room')
    `);
    await queryRunner.query(`
      CREATE TYPE "geo_type_enum" AS ENUM ('country', 'region', 'city', 'city_district', 'region_district', 'village')
    `);

    // Create geo table with nested set model
    await queryRunner.query(`
      CREATE TABLE "geo" (
        "id" SERIAL NOT NULL,
        "name" jsonb NOT NULL,
        "type" "geo_type_enum" NOT NULL,
        "parent_id" integer,
        "lft" integer NOT NULL DEFAULT 0,
        "rgt" integer NOT NULL DEFAULT 0,
        "depth" integer NOT NULL DEFAULT 0,
        "vector_id" integer,
        "aggregator_id" integer,
        "created_at" TIMESTAMP NOT NULL DEFAULT now(),
        "updated_at" TIMESTAMP NOT NULL DEFAULT now(),
        CONSTRAINT "PK_geo_id" PRIMARY KEY ("id")
      )
    `);
    await queryRunner.query(`CREATE INDEX "IDX_geo_type" ON "geo" ("type")`);
    await queryRunner.query(`CREATE INDEX "IDX_geo_lft_rgt" ON "geo" ("lft", "rgt")`);
    await queryRunner.query(`CREATE INDEX "IDX_geo_vector_id" ON "geo" ("vector_id")`);
    await queryRunner.query(`CREATE INDEX "IDX_geo_aggregator_id" ON "geo" ("aggregator_id")`);

    // Create streets table
    await queryRunner.query(`
      CREATE TABLE "streets" (
        "id" SERIAL NOT NULL,
        "name" jsonb NOT NULL,
        "geo_id" integer NOT NULL,
        "vector_id" integer,
        "aggregator_id" integer,
        "created_at" TIMESTAMP NOT NULL DEFAULT now(),
        "updated_at" TIMESTAMP NOT NULL DEFAULT now(),
        CONSTRAINT "PK_streets_id" PRIMARY KEY ("id")
      )
    `);
    await queryRunner.query(`CREATE INDEX "IDX_streets_geo_id" ON "streets" ("geo_id")`);
    await queryRunner.query(`CREATE INDEX "IDX_streets_vector_id" ON "streets" ("vector_id")`);

    // Create topzones table
    await queryRunner.query(`
      CREATE TABLE "topzones" (
        "id" SERIAL NOT NULL,
        "name" jsonb NOT NULL,
        "vector_id" integer,
        "aggregator_id" integer,
        "created_at" TIMESTAMP NOT NULL DEFAULT now(),
        "updated_at" TIMESTAMP NOT NULL DEFAULT now(),
        CONSTRAINT "PK_topzones_id" PRIMARY KEY ("id")
      )
    `);
    await queryRunner.query(`CREATE INDEX "IDX_topzones_vector_id" ON "topzones" ("vector_id")`);

    // Create geo_topzones pivot table
    await queryRunner.query(`
      CREATE TABLE "geo_topzones" (
        "id" SERIAL NOT NULL,
        "geo_id" integer NOT NULL,
        "topzone_id" integer NOT NULL,
        CONSTRAINT "PK_geo_topzones_id" PRIMARY KEY ("id"),
        CONSTRAINT "UQ_geo_topzones" UNIQUE ("geo_id", "topzone_id")
      )
    `);

    // Create apartment_complexes table
    await queryRunner.query(`
      CREATE TABLE "apartment_complexes" (
        "id" SERIAL NOT NULL,
        "name" jsonb NOT NULL,
        "geo_id" integer,
        "street_id" integer,
        "topzone_id" integer,
        "vector_id" integer,
        "aggregator_id" integer,
        "created_at" TIMESTAMP NOT NULL DEFAULT now(),
        "updated_at" TIMESTAMP NOT NULL DEFAULT now(),
        CONSTRAINT "PK_apartment_complexes_id" PRIMARY KEY ("id")
      )
    `);
    await queryRunner.query(`CREATE INDEX "IDX_ac_geo_id" ON "apartment_complexes" ("geo_id")`);
    await queryRunner.query(`CREATE INDEX "IDX_ac_vector_id" ON "apartment_complexes" ("vector_id")`);

    // Create unified_listings table
    await queryRunner.query(`
      CREATE TABLE "unified_listings" (
        "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
        "source_type" "source_type_enum" NOT NULL,
        "source_id" integer NOT NULL,
        "deal_type" "deal_type_enum" NOT NULL,
        "realty_type" "realty_type_enum" NOT NULL,
        "geo_id" integer,
        "street_id" integer,
        "topzone_id" integer,
        "apartment_complex_id" integer,
        "address" varchar(500),
        "building_number" varchar(50),
        "price" numeric(15,2) NOT NULL,
        "price_per_meter" numeric(15,2),
        "currency" varchar(3) NOT NULL DEFAULT 'UAH',
        "area" numeric(10,2) NOT NULL,
        "rooms" integer,
        "floor" integer,
        "total_floors" integer,
        "condition" varchar(100),
        "house_type" varchar(100),
        "walls_type" varchar(100),
        "heating_type" varchar(100),
        "is_active" boolean NOT NULL DEFAULT true,
        "external_url" varchar(1000),
        "raw_data" jsonb,
        "created_at" TIMESTAMP NOT NULL DEFAULT now(),
        "updated_at" TIMESTAMP NOT NULL DEFAULT now(),
        CONSTRAINT "PK_unified_listings_id" PRIMARY KEY ("id")
      )
    `);
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);
    await queryRunner.query(`
      CREATE UNIQUE INDEX "IDX_ul_source" ON "unified_listings" ("source_type", "source_id")
    `);
    await queryRunner.query(`
      CREATE INDEX "IDX_ul_geo" ON "unified_listings" ("geo_id", "street_id", "topzone_id")
    `);
    await queryRunner.query(`CREATE INDEX "IDX_ul_price" ON "unified_listings" ("price")`);
    await queryRunner.query(`CREATE INDEX "IDX_ul_area" ON "unified_listings" ("area")`);
    await queryRunner.query(`CREATE INDEX "IDX_ul_rooms" ON "unified_listings" ("rooms")`);
    await queryRunner.query(`CREATE INDEX "IDX_ul_realty_type" ON "unified_listings" ("realty_type")`);
    await queryRunner.query(`CREATE INDEX "IDX_ul_deal_type" ON "unified_listings" ("deal_type")`);
    await queryRunner.query(`CREATE INDEX "IDX_ul_is_active" ON "unified_listings" ("is_active")`);

    // Create valuation_cache table
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
    await queryRunner.query(`
      CREATE INDEX "IDX_vc_listing_id" ON "valuation_cache" ("listing_id")
    `);
    await queryRunner.query(`
      CREATE INDEX "IDX_vc_expires_at" ON "valuation_cache" ("expires_at")
    `);

    // Add foreign keys
    await queryRunner.query(`
      ALTER TABLE "geo" ADD CONSTRAINT "FK_geo_parent"
      FOREIGN KEY ("parent_id") REFERENCES "geo"("id") ON DELETE SET NULL
    `);
    await queryRunner.query(`
      ALTER TABLE "streets" ADD CONSTRAINT "FK_streets_geo"
      FOREIGN KEY ("geo_id") REFERENCES "geo"("id") ON DELETE CASCADE
    `);
    await queryRunner.query(`
      ALTER TABLE "geo_topzones" ADD CONSTRAINT "FK_gt_geo"
      FOREIGN KEY ("geo_id") REFERENCES "geo"("id") ON DELETE CASCADE
    `);
    await queryRunner.query(`
      ALTER TABLE "geo_topzones" ADD CONSTRAINT "FK_gt_topzone"
      FOREIGN KEY ("topzone_id") REFERENCES "topzones"("id") ON DELETE CASCADE
    `);
    await queryRunner.query(`
      ALTER TABLE "apartment_complexes" ADD CONSTRAINT "FK_ac_geo"
      FOREIGN KEY ("geo_id") REFERENCES "geo"("id") ON DELETE SET NULL
    `);
    await queryRunner.query(`
      ALTER TABLE "apartment_complexes" ADD CONSTRAINT "FK_ac_street"
      FOREIGN KEY ("street_id") REFERENCES "streets"("id") ON DELETE SET NULL
    `);
    await queryRunner.query(`
      ALTER TABLE "apartment_complexes" ADD CONSTRAINT "FK_ac_topzone"
      FOREIGN KEY ("topzone_id") REFERENCES "topzones"("id") ON DELETE SET NULL
    `);
    await queryRunner.query(`
      ALTER TABLE "unified_listings" ADD CONSTRAINT "FK_ul_geo"
      FOREIGN KEY ("geo_id") REFERENCES "geo"("id") ON DELETE SET NULL
    `);
    await queryRunner.query(`
      ALTER TABLE "unified_listings" ADD CONSTRAINT "FK_ul_street"
      FOREIGN KEY ("street_id") REFERENCES "streets"("id") ON DELETE SET NULL
    `);
    await queryRunner.query(`
      ALTER TABLE "unified_listings" ADD CONSTRAINT "FK_ul_topzone"
      FOREIGN KEY ("topzone_id") REFERENCES "topzones"("id") ON DELETE SET NULL
    `);
    await queryRunner.query(`
      ALTER TABLE "unified_listings" ADD CONSTRAINT "FK_ul_ac"
      FOREIGN KEY ("apartment_complex_id") REFERENCES "apartment_complexes"("id") ON DELETE SET NULL
    `);
    await queryRunner.query(`
      ALTER TABLE "valuation_cache" ADD CONSTRAINT "FK_vc_listing"
      FOREIGN KEY ("listing_id") REFERENCES "unified_listings"("id") ON DELETE CASCADE
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Drop foreign keys
    await queryRunner.query(`ALTER TABLE "valuation_cache" DROP CONSTRAINT "FK_vc_listing"`);
    await queryRunner.query(`ALTER TABLE "unified_listings" DROP CONSTRAINT "FK_ul_ac"`);
    await queryRunner.query(`ALTER TABLE "unified_listings" DROP CONSTRAINT "FK_ul_topzone"`);
    await queryRunner.query(`ALTER TABLE "unified_listings" DROP CONSTRAINT "FK_ul_street"`);
    await queryRunner.query(`ALTER TABLE "unified_listings" DROP CONSTRAINT "FK_ul_geo"`);
    await queryRunner.query(`ALTER TABLE "apartment_complexes" DROP CONSTRAINT "FK_ac_topzone"`);
    await queryRunner.query(`ALTER TABLE "apartment_complexes" DROP CONSTRAINT "FK_ac_street"`);
    await queryRunner.query(`ALTER TABLE "apartment_complexes" DROP CONSTRAINT "FK_ac_geo"`);
    await queryRunner.query(`ALTER TABLE "geo_topzones" DROP CONSTRAINT "FK_gt_topzone"`);
    await queryRunner.query(`ALTER TABLE "geo_topzones" DROP CONSTRAINT "FK_gt_geo"`);
    await queryRunner.query(`ALTER TABLE "streets" DROP CONSTRAINT "FK_streets_geo"`);
    await queryRunner.query(`ALTER TABLE "geo" DROP CONSTRAINT "FK_geo_parent"`);

    // Drop tables
    await queryRunner.query(`DROP TABLE "valuation_cache"`);
    await queryRunner.query(`DROP TABLE "unified_listings"`);
    await queryRunner.query(`DROP TABLE "apartment_complexes"`);
    await queryRunner.query(`DROP TABLE "geo_topzones"`);
    await queryRunner.query(`DROP TABLE "topzones"`);
    await queryRunner.query(`DROP TABLE "streets"`);
    await queryRunner.query(`DROP TABLE "geo"`);

    // Drop enums
    await queryRunner.query(`DROP TYPE "geo_type_enum"`);
    await queryRunner.query(`DROP TYPE "realty_type_enum"`);
    await queryRunner.query(`DROP TYPE "deal_type_enum"`);
    await queryRunner.query(`DROP TYPE "source_type_enum"`);
  }
}
