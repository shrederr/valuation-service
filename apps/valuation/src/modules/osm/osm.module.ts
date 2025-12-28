import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { DatabaseModule } from '@libs/database';

import { GeoOsmParserService } from './geo-osm-parser.service';
import { StreetOsmParserService } from './street-osm-parser.service';
import { GeoLookupService } from './geo-lookup.service';
import { StreetMatcherService } from './street-matcher.service';

@Module({
  imports: [DatabaseModule, HttpModule],
  providers: [GeoOsmParserService, StreetOsmParserService, GeoLookupService, StreetMatcherService],
  exports: [GeoOsmParserService, StreetOsmParserService, GeoLookupService, StreetMatcherService],
})
export class OsmModule {}
