import { Module, DynamicModule } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { DatabaseModule } from '@libs/database';
import { RabbitMQModule } from '@libs/rabbitmq';
import { AttributeMapperService } from '@libs/common';
import { OsmModule } from '../osm/osm.module';
import { GeoSyncService } from './services/geo-sync.service';
import { PropertySyncService } from './services/property-sync.service';
import { InitialSyncService } from './services/initial-sync.service';
import { ConsumerControlService } from './services/consumer-control.service';
import { GeoSyncConsumer } from './consumers/geo-sync.consumer';
import { PropertySyncConsumer } from './consumers/property-sync.consumer';
import { VectorPropertyMapper } from './mappers/vector-property.mapper';
import { AggregatorPropertyMapper } from './mappers/aggregator-property.mapper';
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
      InitialSyncService,
      ConsumerControlService,
      // Mappers
      VectorPropertyMapper,
      AggregatorPropertyMapper,
      // Attribute mapping
      AttributeMapperService,
    ];

    const consumerProviders = isRabbitMqEnabled() ? [
      GeoSyncConsumer,
      PropertySyncConsumer,
    ] : [];

    return {
      module: SyncModule,
      imports: [ConfigModule, DatabaseModule, RabbitMQModule.forRoot(), OsmModule],
      controllers: [WebhookController],
      providers: [...baseProviders, ...consumerProviders],
      exports: [GeoSyncService, PropertySyncService, InitialSyncService, ConsumerControlService],
    };
  }
}
