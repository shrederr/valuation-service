import { Entity, Column, PrimaryColumn, ManyToOne, OneToMany, JoinColumn, Index } from 'typeorm';
import { MultiLanguageDto, WGS84_SRID } from '@libs/common';

import { Geo } from './geo.entity';
import { UnifiedListing } from './unified-listing.entity';

@Entity('streets')
@Index(['geoId'])
@Index('idx_streets_osm_id', ['osmId'], { where: 'osm_id IS NOT NULL' })
@Index('idx_streets_line', { synchronize: false })
export class Street {
  @PrimaryColumn({ type: 'integer' })
  public id: number;

  @Column({ name: 'osm_id', type: 'bigint', nullable: true })
  public osmId?: string;

  @Column({ type: 'jsonb' })
  public name: MultiLanguageDto;

  @Column({ type: 'jsonb', nullable: true })
  public names?: Record<string, string>;

  @Column('geometry', {
    spatialFeatureType: 'MultiLineString',
    srid: WGS84_SRID,
    nullable: true,
    select: false,
  })
  public line?: string;

  @Column({ type: 'text' })
  public alias: string;

  @Column({ name: 'geo_id', type: 'integer' })
  public geoId: number;

  @ManyToOne(() => Geo, (geo) => geo.streets)
  @JoinColumn({ name: 'geo_id' })
  public geo?: Geo;

  @Column({ type: 'jsonb', nullable: true })
  public bounds?: Record<string, number>;

  @Column({ type: 'jsonb', nullable: true })
  public coordinates?: number[][];

  @OneToMany(() => UnifiedListing, (listing) => listing.street)
  public listings?: UnifiedListing[];

  @Column({ name: 'synced_at', type: 'timestamptz', default: () => 'CURRENT_TIMESTAMP' })
  public syncedAt: Date;
}

// Alias for backward compatibility
export { Street as StreetEntity };
