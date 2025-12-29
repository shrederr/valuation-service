import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { UnifiedListing } from '@libs/database';

import { AnalogsModule } from '../analogs';
import { FairPriceModule } from '../fair-price';

import { LiquidityService } from './liquidity.service';
import { LiquidityController } from './liquidity.controller';
import { PriceCriterion } from './criteria/price.criterion';
import { PricePerMeterCriterion } from './criteria/price-per-meter.criterion';
import { CompetitionCriterion } from './criteria/competition.criterion';
import { LocationCriterion } from './criteria/location.criterion';
import { ConditionCriterion } from './criteria/condition.criterion';
import { FormatCriterion } from './criteria/format.criterion';
import { FloorCriterion } from './criteria/floor.criterion';
import { HouseTypeCriterion } from './criteria/house-type.criterion';
import { ExposureTimeCriterion } from './criteria/exposure-time.criterion';

@Module({
  imports: [TypeOrmModule.forFeature([UnifiedListing]), AnalogsModule, FairPriceModule],
  controllers: [LiquidityController],
  providers: [
    LiquidityService,
    PriceCriterion,
    PricePerMeterCriterion,
    CompetitionCriterion,
    LocationCriterion,
    ConditionCriterion,
    FormatCriterion,
    FloorCriterion,
    HouseTypeCriterion,
    ExposureTimeCriterion,
  ],
  exports: [LiquidityService],
})
export class LiquidityModule {}
