import { Entity, Column, PrimaryGeneratedColumn, ManyToOne, OneToMany, JoinColumn, Index } from 'typeorm';

import { Geo } from './geo.entity';
import { Street } from './street.entity';
import { Topzone } from './topzone.entity';
import { UnifiedListing } from './unified-listing.entity';

@Entity('apartment_complexes')
@Index('idx_ac_name_normalized', ['nameNormalized'])
@Index('idx_ac_coords', ['lat', 'lng'])
export class ApartmentComplex {
  @PrimaryGeneratedColumn()
  public id: number;

  @Column({ name: 'osm_id', type: 'bigint', nullable: true })
  public osmId?: number;

  @Column({ name: 'osm_type', type: 'varchar', length: 20, nullable: true })
  public osmType?: string;

  @Column({ name: 'name_ru', type: 'varchar', length: 255 })
  public nameRu: string;

  @Column({ name: 'name_uk', type: 'varchar', length: 255 })
  public nameUk: string;

  @Column({ name: 'name_en', type: 'varchar', length: 255, nullable: true })
  public nameEn?: string;

  @Column({ name: 'name_normalized', type: 'varchar', length: 255 })
  public nameNormalized: string;

  @Column({ type: 'decimal', precision: 10, scale: 8 })
  public lat: number;

  @Column({ type: 'decimal', precision: 11, scale: 8 })
  public lng: number;

  // Polygon stored as PostGIS geometry - accessed via raw queries
  // TypeORM manages this column, but we use raw SQL for spatial operations
  @Column({
    name: 'polygon',
    type: 'geometry',
    spatialFeatureType: 'Polygon',
    srid: 4326,
    nullable: true,
  })
  public polygon?: string; // WKT or GeoJSON - access via raw queries

  @Column({ name: 'geo_id', type: 'integer', nullable: true })
  public geoId?: number;

  @ManyToOne(() => Geo)
  @JoinColumn({ name: 'geo_id' })
  public geo?: Geo;

  @Column({ name: 'street_id', type: 'integer', nullable: true })
  public streetId?: number;

  @ManyToOne(() => Street)
  @JoinColumn({ name: 'street_id' })
  public street?: Street;

  @Column({ name: 'topzone_id', type: 'integer', nullable: true })
  public topzoneId?: number;

  @ManyToOne(() => Topzone)
  @JoinColumn({ name: 'topzone_id' })
  public topzone?: Topzone;

  @Column({ type: 'varchar', length: 20 })
  public source: 'geovector' | 'osm' | 'merged';

  @OneToMany(() => UnifiedListing, (listing) => listing.complex)
  public listings?: UnifiedListing[];

  @Column({ name: 'created_at', type: 'timestamptz', default: () => 'CURRENT_TIMESTAMP' })
  public createdAt: Date;

  @Column({ name: 'updated_at', type: 'timestamptz', default: () => 'CURRENT_TIMESTAMP' })
  public updatedAt: Date;
}

// Alias for backward compatibility
export { ApartmentComplex as ApartmentComplexEntity };
