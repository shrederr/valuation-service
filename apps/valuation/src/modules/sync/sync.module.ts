import { Module } from '@nestjs/common';
import { DatabaseModule } from '@libs/database';
import { RabbitMQModule } from '@libs/rabbitmq';
import { GeoSyncService } from './services/geo-sync.service';
import { PropertySyncService } from './services/property-sync.service';
import { GeoSyncConsumer } from './consumers/geo-sync.consumer';
import { PropertySyncConsumer } from './consumers/property-sync.consumer';
import { VectorPropertyMapper } from './mappers/vector-property.mapper';
import { AggregatorPropertyMapper } from './mappers/aggregator-property.mapper';

@Module({
  imports: [DatabaseModule, RabbitMQModule],
  providers: [
    // Services
    GeoSyncService,
    PropertySyncService,
    // Consumers
    GeoSyncConsumer,
    PropertySyncConsumer,
    // Mappers
    VectorPropertyMapper,
    AggregatorPropertyMapper,
  ],
  exports: [GeoSyncService, PropertySyncService],
})
export class SyncModule {}
