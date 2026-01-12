import { MigrationInterface, QueryRunner } from 'typeorm';

export class SyncSchemaWithEntities1767200000000 implements MigrationInterface {
  name = 'SyncSchemaWithEntities1767200000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // UNIFIED_LISTINGS
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS source_global_id uuid');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS realty_subtype text');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS apartment_number integer');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS corps text');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS lat decimal(10,7)');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS lng decimal(10,7)');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS living_area decimal(10,2)');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS kitchen_area decimal(10,2)');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS land_area decimal(12,2)');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS planning_type text');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS attributes jsonb');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS description jsonb');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS cadastral_number jsonb');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS is_exclusive boolean DEFAULT false');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS published_at timestamptz');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS deleted_at timestamptz');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS synced_at timestamptz DEFAULT CURRENT_TIMESTAMP');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS total_area decimal(10,2)');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS house_number text');
    await queryRunner.query('ALTER TABLE unified_listings ADD COLUMN IF NOT EXISTS complex_id integer');
    await queryRunner.query('CREATE INDEX IF NOT EXISTS idx_ul_lat_lng ON unified_listings (lat, lng)');
    await queryRunner.query('CREATE INDEX IF NOT EXISTS idx_ul_complex_id ON unified_listings (complex_id)');

    // GEO
    await queryRunner.query("ALTER TABLE geo ADD COLUMN IF NOT EXISTS alias text DEFAULT ''");
    await queryRunner.query('ALTER TABLE geo ADD COLUMN IF NOT EXISTS lvl integer DEFAULT 0');
    await queryRunner.query('ALTER TABLE geo ADD COLUMN IF NOT EXISTS lat decimal(10,7)');
    await queryRunner.query('ALTER TABLE geo ADD COLUMN IF NOT EXISTS lng decimal(10,7)');
    await queryRunner.query('ALTER TABLE geo ADD COLUMN IF NOT EXISTS bounds jsonb');
    await queryRunner.query('ALTER TABLE geo ADD COLUMN IF NOT EXISTS declension jsonb');
    await queryRunner.query('ALTER TABLE geo ADD COLUMN IF NOT EXISTS synced_at timestamptz DEFAULT CURRENT_TIMESTAMP');

    // STREETS
    await queryRunner.query("ALTER TABLE streets ADD COLUMN IF NOT EXISTS alias text DEFAULT ''");
    await queryRunner.query('ALTER TABLE streets ADD COLUMN IF NOT EXISTS bounds jsonb');
    await queryRunner.query('ALTER TABLE streets ADD COLUMN IF NOT EXISTS coordinates jsonb');
    await queryRunner.query('ALTER TABLE streets ADD COLUMN IF NOT EXISTS synced_at timestamptz DEFAULT CURRENT_TIMESTAMP');

    // TOPZONES
    await queryRunner.query("ALTER TABLE topzones ADD COLUMN IF NOT EXISTS alias text DEFAULT ''");
    await queryRunner.query('ALTER TABLE topzones ADD COLUMN IF NOT EXISTS lat decimal(10,7)');
    await queryRunner.query('ALTER TABLE topzones ADD COLUMN IF NOT EXISTS lng decimal(10,7)');
    await queryRunner.query('ALTER TABLE topzones ADD COLUMN IF NOT EXISTS bounds jsonb');
    await queryRunner.query('ALTER TABLE topzones ADD COLUMN IF NOT EXISTS declension jsonb');
    await queryRunner.query('ALTER TABLE topzones ADD COLUMN IF NOT EXISTS coordinates jsonb');
    await queryRunner.query('ALTER TABLE topzones ADD COLUMN IF NOT EXISTS synced_at timestamptz DEFAULT CURRENT_TIMESTAMP');

    // APARTMENT_COMPLEXES
    await queryRunner.query('ALTER TABLE apartment_complexes ADD COLUMN IF NOT EXISTS osm_id bigint');
    await queryRunner.query('ALTER TABLE apartment_complexes ADD COLUMN IF NOT EXISTS osm_type varchar(20)');
    await queryRunner.query('ALTER TABLE apartment_complexes ADD COLUMN IF NOT EXISTS name_ru varchar(255)');
    await queryRunner.query('ALTER TABLE apartment_complexes ADD COLUMN IF NOT EXISTS name_uk varchar(255)');
    await queryRunner.query('ALTER TABLE apartment_complexes ADD COLUMN IF NOT EXISTS name_en varchar(255)');
    await queryRunner.query('ALTER TABLE apartment_complexes ADD COLUMN IF NOT EXISTS name_normalized varchar(255)');
    await queryRunner.query('ALTER TABLE apartment_complexes ADD COLUMN IF NOT EXISTS lat decimal(10,8)');
    await queryRunner.query('ALTER TABLE apartment_complexes ADD COLUMN IF NOT EXISTS lng decimal(11,8)');
    await queryRunner.query("ALTER TABLE apartment_complexes ADD COLUMN IF NOT EXISTS source varchar(20) DEFAULT 'geovector'");
    await queryRunner.query('CREATE INDEX IF NOT EXISTS idx_ac_name_normalized ON apartment_complexes (name_normalized)');
    await queryRunner.query('CREATE INDEX IF NOT EXISTS idx_ac_coords ON apartment_complexes (lat, lng)');
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('ALTER TABLE unified_listings DROP COLUMN IF EXISTS source_global_id');
    await queryRunner.query('ALTER TABLE geo DROP COLUMN IF EXISTS alias');
    await queryRunner.query('ALTER TABLE streets DROP COLUMN IF EXISTS alias');
    await queryRunner.query('ALTER TABLE topzones DROP COLUMN IF EXISTS alias');
  }
}
