import { Module, DynamicModule } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { DatabaseModule } from '@libs/database';
import { RabbitMQModule } from '@libs/rabbitmq';
import { AttributeMapperService } from '@libs/common';
import { OsmModule } from '../osm/osm.module';
import { InfrastructureModule } from '../infrastructure';
import { ValuationModule } from '../valuation';
import { GeoSyncService } from './services/geo-sync.service';
import { PropertySyncService } from './services/property-sync.service';
import { GeoSyncConsumer } from './consumers/geo-sync.consumer';
import { PropertySyncConsumer } from './consumers/property-sync.consumer';
import { VectorPropertyMapper } from './mappers/vector-property.mapper';
import { AggregatorPropertyMapper } from './mappers/aggregator-property.mapper';
import { Vector2PropertyMapper } from './mappers/vector2-property.mapper';
import { ComplexMatcherService } from './services/complex-matcher.service';
import { SourceIdMappingService } from './services/source-id-mapping.service';
import { CurrencyService } from './services/currency.service';
import { WebhookController } from './controllers/webhook.controller';

const isRabbitMqEnabled = () => {
  const url = process.env.RABBITMQ_URL;
  return url && url.startsWith('amqp');
};

@Module({})
export class SyncModule {
  static forRoot(): DynamicModule {
    const baseProviders = [
      // Services
      GeoSyncService,
      PropertySyncService,
      ComplexMatcherService,
      SourceIdMappingService,
      CurrencyService,
      // Mappers
      VectorPropertyMapper,
      AggregatorPropertyMapper,
      Vector2PropertyMapper,
      // Attribute mapping
      AttributeMapperService,
    ];

    const consumerProviders = isRabbitMqEnabled() ? [
      GeoSyncConsumer,
      PropertySyncConsumer,
    ] : [];

    return {
      module: SyncModule,
      imports: [ConfigModule, DatabaseModule, RabbitMQModule.forRoot(), OsmModule, InfrastructureModule, ValuationModule],
      controllers: [WebhookController],
      providers: [...baseProviders, ...consumerProviders],
      exports: [GeoSyncService, PropertySyncService, ComplexMatcherService],
    };
  }
}
