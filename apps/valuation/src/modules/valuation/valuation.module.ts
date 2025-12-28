import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { UnifiedListing, ValuationCache } from '@libs/database';

import { AnalogsModule } from '../analogs';
import { FairPriceModule } from '../fair-price';
import { LiquidityModule } from '../liquidity';

import { ValuationService } from './valuation.service';
import { ValuationController } from './valuation.controller';
import { ValuationCacheService } from './services/valuation-cache.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([UnifiedListing, ValuationCache]),
    AnalogsModule,
    FairPriceModule,
    LiquidityModule,
  ],
  controllers: [ValuationController],
  providers: [ValuationService, ValuationCacheService],
  exports: [ValuationService],
})
export class ValuationModule {}
