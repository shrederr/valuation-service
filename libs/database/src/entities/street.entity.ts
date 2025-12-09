import { Entity, Column, PrimaryColumn, ManyToOne, OneToMany, JoinColumn, Index } from 'typeorm';
import { MultiLanguageDto } from '@libs/common';

import { Geo } from './geo.entity';
import { UnifiedListing } from './unified-listing.entity';

@Entity('streets')
@Index(['geoId'])
export class Street {
  @PrimaryColumn({ type: 'integer' })
  public id: number;

  @Column({ type: 'jsonb' })
  public name: MultiLanguageDto;

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
