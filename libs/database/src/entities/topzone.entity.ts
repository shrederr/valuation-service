import { Entity, Column, PrimaryColumn, OneToMany } from 'typeorm';

import { MultiLanguageField } from './geo.entity';
import { UnifiedListing } from './unified-listing.entity';

@Entity('topzones')
export class Topzone {
  @PrimaryColumn({ type: 'integer' })
  public id: number;

  @Column({ type: 'jsonb' })
  public name: MultiLanguageField;

  @Column({ type: 'text' })
  public alias: string;

  @Column({ type: 'decimal', precision: 10, scale: 7, nullable: true })
  public lat?: number;

  @Column({ type: 'decimal', precision: 10, scale: 7, nullable: true })
  public lng?: number;

  @Column({ type: 'jsonb', nullable: true })
  public bounds?: Record<string, number>;

  @Column({ type: 'jsonb', nullable: true })
  public declension?: MultiLanguageField;

  @Column({ type: 'jsonb', nullable: true })
  public coordinates?: number[][][];

  @OneToMany(() => UnifiedListing, (listing) => listing.topzone)
  public listings?: UnifiedListing[];

  @Column({ name: 'synced_at', type: 'timestamptz', default: () => 'CURRENT_TIMESTAMP' })
  public syncedAt: Date;
}

// Alias for backward compatibility
export { Topzone as TopzoneEntity };
