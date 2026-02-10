import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { UnifiedListing } from '@libs/database';

import { AnalogsModule } from '../analogs';
import { FairPriceModule } from '../fair-price';

import { LiquidityService } from './liquidity.service';
import { LiquidityController } from './liquidity.controller';
import { PriceCriterion } from './criteria/price.criterion';
import { LivingAreaCriterion } from './criteria/living-area.criterion';
import { CompetitionCriterion } from './criteria/competition.criterion';
import { LocationCriterion } from './criteria/location.criterion';
import { ConditionCriterion } from './criteria/condition.criterion';
import { FormatCriterion } from './criteria/format.criterion';
import { FloorCriterion } from './criteria/floor.criterion';
import { HouseTypeCriterion } from './criteria/house-type.criterion';
import { ExposureTimeCriterion } from './criteria/exposure-time.criterion';
import { InfrastructureCriterion } from './criteria/infrastructure.criterion';
import { FurnitureCriterion } from './criteria/furniture.criterion';
import { CommunicationsCriterion } from './criteria/communications.criterion';
import { UniqueFeaturesCriterion } from './criteria/unique-features.criterion';
import { BuyConditionsCriterion } from './criteria/buy-conditions.criterion';
import { PrimaryDataExtractor } from './services/primary-data-extractor';

@Module({
  imports: [TypeOrmModule.forFeature([UnifiedListing]), AnalogsModule, FairPriceModule],
  controllers: [LiquidityController],
  providers: [
    LiquidityService,
    PrimaryDataExtractor,
    PriceCriterion,
    LivingAreaCriterion,
    CompetitionCriterion,
    LocationCriterion,
    ConditionCriterion,
    FormatCriterion,
    FloorCriterion,
    HouseTypeCriterion,
    ExposureTimeCriterion,
    InfrastructureCriterion,
    FurnitureCriterion,
    CommunicationsCriterion,
    UniqueFeaturesCriterion,
    BuyConditionsCriterion,
  ],
  exports: [LiquidityService],
})
export class LiquidityModule {}
