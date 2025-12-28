import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { UnifiedListing, Geo, Street, Topzone, ApartmentComplex } from '@libs/database';

import { AnalogsService } from './analogs.service';
import { AnalogsController } from './analogs.controller';
import { GeoFallbackStrategy } from './strategies/geo-fallback.strategy';
import { AnalogFilterService } from './services/analog-filter.service';
import { AnalogScorerService } from './services/analog-scorer.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([UnifiedListing, Geo, Street, Topzone, ApartmentComplex]),
  ],
  controllers: [AnalogsController],
  providers: [AnalogsService, GeoFallbackStrategy, AnalogFilterService, AnalogScorerService],
  exports: [AnalogsService],
})
export class AnalogsModule {}
