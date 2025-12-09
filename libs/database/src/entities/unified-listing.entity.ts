import { Entity, Column, ManyToOne, JoinColumn, Index, PrimaryGeneratedColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';
import { SourceType, DealType, RealtyType } from '@libs/common';

import { Geo, MultiLanguageField } from './geo.entity';
import { Street } from './street.entity';
import { Topzone } from './topzone.entity';
import { ApartmentComplex } from './apartment-complex.entity';

@Entity('unified_listings')
@Index(['sourceType', 'sourceId'], { unique: true })
@Index(['geoId', 'streetId', 'topzoneId'])
@Index(['realtyType', 'dealType'])
@Index(['price'])
@Index(['totalArea'])
@Index(['rooms'])
@Index(['isActive', 'deletedAt'])
@Index(['complexId'])
export class UnifiedListing {
  @PrimaryGeneratedColumn('uuid')
  public id: string;

  // === Source ===
  @Column({ name: 'source_type', type: 'enum', enum: SourceType })
  public sourceType: SourceType;

  @Column({ name: 'source_id', type: 'integer' })
  public sourceId: number;

  @Column({ name: 'source_global_id', type: 'uuid', nullable: true })
  public sourceGlobalId?: string;

  // === Deal & Realty Type ===
  @Column({ name: 'deal_type', type: 'enum', enum: DealType })
  public dealType: DealType;

  @Column({ name: 'realty_type', type: 'enum', enum: RealtyType })
  public realtyType: RealtyType;

  @Column({ name: 'realty_subtype', type: 'text', nullable: true })
  public realtySubtype?: string;

  // === Geography ===
  @Column({ name: 'geo_id', type: 'integer', nullable: true })
  public geoId?: number;

  @ManyToOne(() => Geo, (geo) => geo.listings)
  @JoinColumn({ name: 'geo_id' })
  public geo?: Geo;

  @Column({ name: 'street_id', type: 'integer', nullable: true })
  public streetId?: number;

  @ManyToOne(() => Street, (street) => street.listings)
  @JoinColumn({ name: 'street_id' })
  public street?: Street;

  @Column({ name: 'topzone_id', type: 'integer', nullable: true })
  public topzoneId?: number;

  @ManyToOne(() => Topzone, (topzone) => topzone.listings)
  @JoinColumn({ name: 'topzone_id' })
  public topzone?: Topzone;

  @Column({ name: 'complex_id', type: 'integer', nullable: true })
  public complexId?: number;

  @ManyToOne(() => ApartmentComplex, (complex) => complex.listings)
  @JoinColumn({ name: 'complex_id' })
  public complex?: ApartmentComplex;

  @Column({ name: 'house_number', type: 'text', nullable: true })
  public houseNumber?: string;

  @Column({ name: 'apartment_number', type: 'integer', nullable: true })
  public apartmentNumber?: number;

  @Column({ type: 'text', nullable: true })
  public corps?: string;

  // === Coordinates ===
  @Column({ type: 'decimal', precision: 10, scale: 7, nullable: true })
  public lat?: number;

  @Column({ type: 'decimal', precision: 10, scale: 7, nullable: true })
  public lng?: number;

  // === Price ===
  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  public price?: number;

  @Column({ type: 'text', default: 'USD' })
  public currency: string;

  @Column({ name: 'price_per_meter', type: 'decimal', precision: 12, scale: 2, nullable: true })
  public pricePerMeter?: number;

  // === Characteristics ===
  @Column({ name: 'total_area', type: 'decimal', precision: 10, scale: 2, nullable: true })
  public totalArea?: number;

  @Column({ name: 'living_area', type: 'decimal', precision: 10, scale: 2, nullable: true })
  public livingArea?: number;

  @Column({ name: 'kitchen_area', type: 'decimal', precision: 10, scale: 2, nullable: true })
  public kitchenArea?: number;

  @Column({ name: 'land_area', type: 'decimal', precision: 12, scale: 2, nullable: true })
  public landArea?: number;

  @Column({ type: 'integer', nullable: true })
  public rooms?: number;

  @Column({ type: 'integer', nullable: true })
  public floor?: number;

  @Column({ name: 'total_floors', type: 'integer', nullable: true })
  public totalFloors?: number;

  @Column({ type: 'text', nullable: true })
  public condition?: string;

  @Column({ name: 'house_type', type: 'text', nullable: true })
  public houseType?: string;

  @Column({ name: 'planning_type', type: 'text', nullable: true })
  public planningType?: string;

  @Column({ name: 'heating_type', type: 'text', nullable: true })
  public heatingType?: string;

  // === Additional Attributes ===
  @Column({ type: 'jsonb', nullable: true })
  public attributes?: Record<string, unknown>;

  @Column({ type: 'jsonb', nullable: true })
  public description?: MultiLanguageField;

  @Column({ name: 'cadastral_number', type: 'jsonb', nullable: true })
  public cadastralNumber?: Record<string, string>;

  // === Status ===
  @Column({ name: 'is_active', type: 'boolean', default: true })
  public isActive: boolean;

  @Column({ name: 'is_exclusive', type: 'boolean', default: false })
  public isExclusive: boolean;

  @Column({ name: 'published_at', type: 'timestamptz', nullable: true })
  public publishedAt?: Date;

  @Column({ name: 'external_url', type: 'text', nullable: true })
  public externalUrl?: string;

  // === Service Fields ===
  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  public createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  public updatedAt: Date;

  @Column({ name: 'deleted_at', type: 'timestamptz', nullable: true })
  public deletedAt?: Date;

  @Column({ name: 'synced_at', type: 'timestamptz', default: () => 'CURRENT_TIMESTAMP' })
  public syncedAt: Date;
}

// Alias for backward compatibility
export { UnifiedListing as UnifiedListingEntity };
