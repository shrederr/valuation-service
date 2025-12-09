import { Entity, Column, PrimaryColumn, ManyToOne, OneToMany, JoinColumn } from 'typeorm';

import { Geo, MultiLanguageField } from './geo.entity';
import { UnifiedListing } from './unified-listing.entity';

@Entity('apartment_complexes')
export class ApartmentComplex {
  @PrimaryColumn({ type: 'integer' })
  public id: number;

  @Column({ type: 'jsonb' })
  public name: MultiLanguageField;

  @Column({ name: 'geo_id', type: 'integer', nullable: true })
  public geoId?: number;

  @ManyToOne(() => Geo)
  @JoinColumn({ name: 'geo_id' })
  public geo?: Geo;

  @Column({ name: 'topzone_id', type: 'integer', nullable: true })
  public topzoneId?: number;

  @Column({ type: 'decimal', precision: 10, scale: 7, nullable: true })
  public lat?: number;

  @Column({ type: 'decimal', precision: 10, scale: 7, nullable: true })
  public lng?: number;

  @Column({ type: 'integer', nullable: true })
  public type?: number;

  @OneToMany(() => UnifiedListing, (listing) => listing.complex)
  public listings?: UnifiedListing[];

  @Column({ name: 'synced_at', type: 'timestamptz', default: () => 'CURRENT_TIMESTAMP' })
  public syncedAt: Date;
}

// Alias for backward compatibility
export { ApartmentComplex as ApartmentComplexEntity };
