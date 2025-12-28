import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ServeStaticModule } from '@nestjs/serve-static';
import { join } from 'path';
import { DatabaseModule } from '@libs/database';
import { RabbitMQModule } from '@libs/rabbitmq';
import { SyncModule } from './modules/sync/sync.module';
import { OsmModule } from './modules/osm/osm.module';
import { AnalogsModule } from './modules/analogs';
import { FairPriceModule } from './modules/fair-price';
import { LiquidityModule } from './modules/liquidity';
import { ValuationModule } from './modules/valuation';
import { ListingsModule } from './modules/listings/listings.module';
import { AppController } from './app.controller';
import { AppService } from './app.service';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['.env.local', '.env'],
    }),
    ServeStaticModule.forRoot({
      rootPath: join(process.cwd(), 'public'),
      exclude: ['/api/(.*)'],
    }),
    DatabaseModule,
    RabbitMQModule,
    SyncModule,
    OsmModule,
    AnalogsModule,
    FairPriceModule,
    LiquidityModule,
    ValuationModule,
    ListingsModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
