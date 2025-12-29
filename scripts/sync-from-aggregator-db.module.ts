import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { HttpModule } from '@nestjs/axios';

import { DatabaseModule } from '@libs/database';
import { GeoLookupService } from '../apps/valuation/src/modules/osm/geo-lookup.service';
import { StreetMatcherService } from '../apps/valuation/src/modules/osm/street-matcher.service';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true, envFilePath: ['.env.local', '.env'] }), DatabaseModule, HttpModule],
  providers: [GeoLookupService, StreetMatcherService],
  exports: [GeoLookupService, StreetMatcherService],
})
export class SyncFromAggregatorDbModule {}
