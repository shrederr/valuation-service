import { Entity, Column, PrimaryColumn, OneToMany, Index } from 'typeorm';
import { GeoType } from '@libs/common';

import { Street } from './street.entity';
import { UnifiedListing } from './unified-listing.entity';

export interface MultiLanguageField {
  uk: string;
  ru?: string;
  en?: string;
}

@Entity('geo')
@Index('idx_geo_nested_set', ['lft', 'rgt', 'lvl', 'type'])
@Index(['type'])
export class Geo {
  @PrimaryColumn({ type: 'integer' })
  public id: number;

  @Column({ type: 'jsonb' })
  public name: MultiLanguageField;

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

  @Column({ type: 'jsonb', nullable: true })
  public bounds?: Record<string, number>;

  @Column({ type: 'jsonb', nullable: true })
  public declension?: MultiLanguageField;

  @OneToMany(() => Street, (street) => street.geo)
  public streets?: Street[];

  @OneToMany(() => UnifiedListing, (listing) => listing.geo)
  public listings?: UnifiedListing[];

  @Column({ name: 'synced_at', type: 'timestamptz', default: () => 'CURRENT_TIMESTAMP' })
  public syncedAt: Date;
}

// Alias for backward compatibility
export { Geo as GeoEntity };
