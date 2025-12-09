import { Entity, Column, ManyToOne, JoinColumn, Index, PrimaryGeneratedColumn, CreateDateColumn } from 'typeorm';

import { UnifiedListing } from './unified-listing.entity';

export interface AnalogsData {
  count: number;
  analogIds: string[];
  searchRadius: 'building' | 'street' | 'topzone' | 'district' | 'neighbor_districts' | 'city';
}

export interface FairPriceData {
  median: number;
  average: number;
  min: number;
  max: number;
  q1: number;
  q3: number;
  pricePerMeter: {
    median: number;
    average: number;
  };
  verdict: 'cheap' | 'in_market' | 'expensive';
}

export interface LiquidityData {
  score: number;
  level: 'low' | 'medium' | 'high';
  breakdown: Record<string, { score: number; weight: number }>;
}

@Entity('valuation_cache')
@Index(['listingId'])
@Index(['calculatedAt'])
@Index(['expiresAt'])
export class ValuationCache {
  @PrimaryGeneratedColumn('uuid')
  public id: string;

  @Column({ name: 'listing_id', type: 'uuid' })
  public listingId: string;

  @ManyToOne(() => UnifiedListing)
  @JoinColumn({ name: 'listing_id' })
  public listing?: UnifiedListing;

  @Column({ name: 'analogs_data', type: 'jsonb' })
  public analogsData: AnalogsData;

  @Column({ name: 'fair_price', type: 'jsonb' })
  public fairPrice: FairPriceData;

  @Column({ type: 'jsonb' })
  public liquidity: LiquidityData;

  @CreateDateColumn({ name: 'calculated_at', type: 'timestamptz' })
  public calculatedAt: Date;

  @Column({ name: 'expires_at', type: 'timestamptz' })
  public expiresAt: Date;
}

// Alias for backward compatibility
export { ValuationCache as ValuationCacheEntity };
