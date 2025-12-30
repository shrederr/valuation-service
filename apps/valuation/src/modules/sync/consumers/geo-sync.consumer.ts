import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RabbitSubscribe, Nack } from '@golevelup/nestjs-rabbitmq';
import { VECTOR_EVENTS_EXCHANGE, QUEUES, ROUTING_KEYS } from '@libs/rabbitmq';
import { GeoSyncService } from '../services/geo-sync.service';
import {
  GeoEventDto,
  GeoDeletedEventDto,
  StreetEventDto,
  StreetDeletedEventDto,
  TopzoneEventDto,
  TopzoneDeletedEventDto,
  ComplexEventDto,
  ComplexDeletedEventDto,
} from '../dto';

@Injectable()
export class GeoSyncConsumer {
  private readonly logger = new Logger(GeoSyncConsumer.name);
  private readonly geoSyncDisabled: boolean;

  constructor(
    private readonly geoSyncService: GeoSyncService,
    private readonly configService: ConfigService,
  ) {
    this.geoSyncDisabled = this.configService.get<string>('DISABLE_GEO_SYNC') === 'true';

    if (this.geoSyncDisabled) {
      this.logger.warn('Geo sync from vector-api is DISABLED (DISABLE_GEO_SYNC=true). Using OSM data instead.');
    }
  }

  private isGeoSyncDisabled(): boolean {
    if (this.geoSyncDisabled) {
      this.logger.debug('Geo sync event ignored (DISABLE_GEO_SYNC=true)');
      return true;
    }
    return false;
  }

  // === Geo Events ===

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.GEO_CREATED,
    queue: QUEUES.GEO_SYNC,
    queueOptions: { durable: true },
  })
  async handleGeoCreated(data: GeoEventDto): Promise<void | Nack> {
    if (this.isGeoSyncDisabled()) return;
    this.logger.log(`Received geo.created event: ${data.id}`);
    try {
      await this.geoSyncService.handleGeoCreated(data);
    } catch (error) {
      this.logger.error(`Error processing geo.created: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.GEO_UPDATED,
    queue: QUEUES.GEO_SYNC,
    queueOptions: { durable: true },
  })
  async handleGeoUpdated(data: GeoEventDto): Promise<void | Nack> {
    if (this.isGeoSyncDisabled()) return;
    this.logger.log(`Received geo.updated event: ${data.id}`);
    try {
      await this.geoSyncService.handleGeoUpdated(data);
    } catch (error) {
      this.logger.error(`Error processing geo.updated: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.GEO_DELETED,
    queue: QUEUES.GEO_SYNC,
    queueOptions: { durable: true },
  })
  async handleGeoDeleted(data: GeoDeletedEventDto): Promise<void | Nack> {
    if (this.isGeoSyncDisabled()) return;
    this.logger.log(`Received geo.deleted event: ${data.id}`);
    try {
      await this.geoSyncService.handleGeoDeleted(data.id);
    } catch (error) {
      this.logger.error(`Error processing geo.deleted: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  // === Street Events ===

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.STREET_CREATED,
    queue: QUEUES.STREET_SYNC,
    queueOptions: { durable: true },
  })
  async handleStreetCreated(data: StreetEventDto): Promise<void | Nack> {
    if (this.isGeoSyncDisabled()) return;
    this.logger.log(`Received street.created event: ${data.id}`);
    try {
      await this.geoSyncService.handleStreetCreated(data);
    } catch (error) {
      this.logger.error(`Error processing street.created: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.STREET_UPDATED,
    queue: QUEUES.STREET_SYNC,
    queueOptions: { durable: true },
  })
  async handleStreetUpdated(data: StreetEventDto): Promise<void | Nack> {
    if (this.isGeoSyncDisabled()) return;
    this.logger.log(`Received street.updated event: ${data.id}`);
    try {
      await this.geoSyncService.handleStreetUpdated(data);
    } catch (error) {
      this.logger.error(`Error processing street.updated: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.STREET_DELETED,
    queue: QUEUES.STREET_SYNC,
    queueOptions: { durable: true },
  })
  async handleStreetDeleted(data: StreetDeletedEventDto): Promise<void | Nack> {
    if (this.isGeoSyncDisabled()) return;
    this.logger.log(`Received street.deleted event: ${data.id}`);
    try {
      await this.geoSyncService.handleStreetDeleted(data.id);
    } catch (error) {
      this.logger.error(`Error processing street.deleted: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  // === Topzone Events ===

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.TOPZONE_CREATED,
    queue: QUEUES.TOPZONE_SYNC,
    queueOptions: { durable: true },
  })
  async handleTopzoneCreated(data: TopzoneEventDto): Promise<void | Nack> {
    this.logger.log(`Received topzone.created event: ${data.id}`);
    try {
      await this.geoSyncService.handleTopzoneCreated(data);
    } catch (error) {
      this.logger.error(`Error processing topzone.created: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.TOPZONE_UPDATED,
    queue: QUEUES.TOPZONE_SYNC,
    queueOptions: { durable: true },
  })
  async handleTopzoneUpdated(data: TopzoneEventDto): Promise<void | Nack> {
    this.logger.log(`Received topzone.updated event: ${data.id}`);
    try {
      await this.geoSyncService.handleTopzoneUpdated(data);
    } catch (error) {
      this.logger.error(`Error processing topzone.updated: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.TOPZONE_DELETED,
    queue: QUEUES.TOPZONE_SYNC,
    queueOptions: { durable: true },
  })
  async handleTopzoneDeleted(data: TopzoneDeletedEventDto): Promise<void | Nack> {
    this.logger.log(`Received topzone.deleted event: ${data.id}`);
    try {
      await this.geoSyncService.handleTopzoneDeleted(data.id);
    } catch (error) {
      this.logger.error(`Error processing topzone.deleted: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  // === Complex Events ===

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.COMPLEX_CREATED,
    queue: QUEUES.COMPLEX_SYNC,
    queueOptions: { durable: true },
  })
  async handleComplexCreated(data: ComplexEventDto): Promise<void | Nack> {
    this.logger.log(`Received complex.created event: ${data.id}`);
    try {
      await this.geoSyncService.handleComplexCreated(data);
    } catch (error) {
      this.logger.error(`Error processing complex.created: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.COMPLEX_UPDATED,
    queue: QUEUES.COMPLEX_SYNC,
    queueOptions: { durable: true },
  })
  async handleComplexUpdated(data: ComplexEventDto): Promise<void | Nack> {
    this.logger.log(`Received complex.updated event: ${data.id}`);
    try {
      await this.geoSyncService.handleComplexUpdated(data);
    } catch (error) {
      this.logger.error(`Error processing complex.updated: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VECTOR_EVENTS_EXCHANGE,
    routingKey: ROUTING_KEYS.COMPLEX_DELETED,
    queue: QUEUES.COMPLEX_SYNC,
    queueOptions: { durable: true },
  })
  async handleComplexDeleted(data: ComplexDeletedEventDto): Promise<void | Nack> {
    this.logger.log(`Received complex.deleted event: ${data.id}`);
    try {
      await this.geoSyncService.handleComplexDeleted(data.id);
    } catch (error) {
      this.logger.error(`Error processing complex.deleted: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }
}
