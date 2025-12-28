import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { UnifiedListing } from '@libs/database';

import { AnalogsModule } from '../analogs';

import { FairPriceService } from './fair-price.service';
import { FairPriceController } from './fair-price.controller';
import { StatisticsCalculator } from './calculators/statistics.calculator';
import { OutlierFilter } from './calculators/outlier-filter';
import { PriceVerdictService } from './services/price-verdict.service';

@Module({
  imports: [TypeOrmModule.forFeature([UnifiedListing]), AnalogsModule],
  controllers: [FairPriceController],
  providers: [FairPriceService, StatisticsCalculator, OutlierFilter, PriceVerdictService],
  exports: [FairPriceService],
})
export class FairPriceModule {}
