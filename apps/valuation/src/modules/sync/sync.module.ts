import { Module } from '@nestjs/common';
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

@Module({
  imports: [DatabaseModule, RabbitMQModule, OsmModule],
  providers: [
    // Services
    GeoSyncService,
    PropertySyncService,
    InitialSyncService,
    ConsumerControlService,
    // Consumers
    GeoSyncConsumer,
    PropertySyncConsumer,
    // Mappers
    VectorPropertyMapper,
    AggregatorPropertyMapper,
    // Attribute mapping
    AttributeMapperService,
  ],
  exports: [GeoSyncService, PropertySyncService, InitialSyncService, ConsumerControlService],
})
export class SyncModule {}
