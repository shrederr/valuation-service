import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';

import { DatabaseModule } from '@libs/database';

import { StreetOsmParserService } from '../apps/valuation/src/modules/osm/street-osm-parser.service';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true, envFilePath: ['.env.local', '.env'] }), DatabaseModule, HttpModule],
  providers: [StreetOsmParserService],
})
export class ParseStreetsOsmModule {}
