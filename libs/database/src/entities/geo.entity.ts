import { Entity, Column, PrimaryColumn, OneToMany, Index, ManyToOne, JoinColumn } from 'typeorm';
import { GeoType, MultiLanguageDto, WGS84_SRID } from '@libs/common';

import { Street } from './street.entity';
import { UnifiedListing } from './unified-listing.entity';

@Entity('geo')
@Index('idx_geo_nested_set', ['lft', 'rgt', 'lvl', 'type'])
@Index(['type'])
@Index('idx_geo_osm_id', ['osmId'], { where: 'osm_id IS NOT NULL' })
@Index('idx_geo_parent_id', ['parentId'], { where: 'parent_id IS NOT NULL' })
@Index('idx_geo_polygon', { synchronize: false })
export class Geo {
  @PrimaryColumn({ type: 'integer' })
  public id: number;

  @Column({ name: 'osm_id', type: 'bigint', nullable: true })
  public osmId?: string;

  @Column({ name: 'parent_id', type: 'integer', nullable: true })
  public parentId?: number;

  @ManyToOne(() => Geo, { nullable: true, onDelete: 'SET NULL' })
  @JoinColumn({ name: 'parent_id' })
  public parent?: Geo;

  @Column({ type: 'jsonb' })
  public name: MultiLanguageDto;

  @Column({ type: 'text' })
  public alias: string;

  @Column({ type: 'enum', enum: GeoType })
  public type: GeoType;

  @Column({ type: 'integer' })
  public lvl: number;

  @Column({ type: 'integer' })
  public lft: number;

  @Column({ type: 'integer' })
  public rgt: number;

  @Column({ type: 'decimal', precision: 10, scale: 7, nullable: true })
  public lat?: number;

  @Column({ type: 'decimal', precision: 10, scale: 7, nullable: true })
  public lng?: number;

  @Column('geometry', {
    spatialFeatureType: 'MultiPolygon',
    srid: WGS84_SRID,
    nullable: true,
    select: false,
  })
  public polygon?: string;

  @Column({ type: 'integer', nullable: true })
  public population?: number;

  @Column({ type: 'jsonb', nullable: true })
  public bounds?: Record<string, number>;

  @Column({ type: 'jsonb', nullable: true })
  public declension?: MultiLanguageDto;

  @OneToMany(() => Street, (street) => street.geo)
  public streets?: Street[];

  @OneToMany(() => UnifiedListing, (listing) => listing.geo)
  public listings?: UnifiedListing[];

  @Column({ name: 'synced_at', type: 'timestamptz', default: () => 'CURRENT_TIMESTAMP' })
  public syncedAt: Date;
}

// Alias for backward compatibility
export { Geo as GeoEntity };
