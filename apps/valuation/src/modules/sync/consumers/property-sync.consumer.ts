import { Injectable, Logger } from '@nestjs/common';
import { RabbitSubscribe, Nack } from '@golevelup/nestjs-rabbitmq';
import { VALUATION_EXCHANGE, QUEUES, ROUTING_KEYS } from '@libs/rabbitmq';
import { PropertySyncService } from '../services/property-sync.service';
import {
  VectorPropertyEventDto,
  VectorPropertyArchivedEventDto,
  VectorPropertyUnarchivedEventDto,
  AggregatorPropertyEventDto,
  AggregatorPropertyDeletedEventDto,
} from '../dto';

@Injectable()
export class PropertySyncConsumer {
  private readonly logger = new Logger(PropertySyncConsumer.name);

  constructor(private readonly propertySyncService: PropertySyncService) {}

  // === Vector Property Events ===

  @RabbitSubscribe({
    exchange: VALUATION_EXCHANGE,
    routingKey: ROUTING_KEYS.VECTOR_PROPERTY_CREATED,
    queue: QUEUES.VECTOR_PROPERTY_SYNC,
    queueOptions: { durable: true },
  })
  async handleVectorPropertyCreated(data: VectorPropertyEventDto): Promise<void | Nack> {
    this.logger.log(`Received vector.property.created event: ${data.id}`);
    try {
      await this.propertySyncService.handleVectorPropertyCreated(data);
    } catch (error) {
      this.logger.error(`Error processing vector.property.created: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VALUATION_EXCHANGE,
    routingKey: ROUTING_KEYS.VECTOR_PROPERTY_UPDATED,
    queue: QUEUES.VECTOR_PROPERTY_SYNC,
    queueOptions: { durable: true },
  })
  async handleVectorPropertyUpdated(data: VectorPropertyEventDto): Promise<void | Nack> {
    this.logger.log(`Received vector.property.updated event: ${data.id}`);
    try {
      await this.propertySyncService.handleVectorPropertyUpdated(data);
    } catch (error) {
      this.logger.error(`Error processing vector.property.updated: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VALUATION_EXCHANGE,
    routingKey: ROUTING_KEYS.VECTOR_PROPERTY_ARCHIVED,
    queue: QUEUES.VECTOR_PROPERTY_SYNC,
    queueOptions: { durable: true },
  })
  async handleVectorPropertyArchived(data: VectorPropertyArchivedEventDto): Promise<void | Nack> {
    this.logger.log(`Received vector.property.archived event: ${data.id}`);
    try {
      await this.propertySyncService.handleVectorPropertyArchived(data);
    } catch (error) {
      this.logger.error(`Error processing vector.property.archived: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VALUATION_EXCHANGE,
    routingKey: ROUTING_KEYS.VECTOR_PROPERTY_UNARCHIVED,
    queue: QUEUES.VECTOR_PROPERTY_SYNC,
    queueOptions: { durable: true },
  })
  async handleVectorPropertyUnarchived(data: VectorPropertyUnarchivedEventDto): Promise<void | Nack> {
    this.logger.log(`Received vector.property.unarchived event: ${data.id}`);
    try {
      await this.propertySyncService.handleVectorPropertyUnarchived(data);
    } catch (error) {
      this.logger.error(`Error processing vector.property.unarchived: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  // === Aggregator Property Events ===

  @RabbitSubscribe({
    exchange: VALUATION_EXCHANGE,
    routingKey: ROUTING_KEYS.AGGREGATOR_PROPERTY_CREATED,
    queue: QUEUES.AGGREGATOR_PROPERTY_SYNC,
    queueOptions: { durable: true },
  })
  async handleAggregatorPropertyCreated(data: AggregatorPropertyEventDto): Promise<void | Nack> {
    this.logger.log(`Received aggregator.property.created event: ${data.id}`);
    try {
      await this.propertySyncService.handleAggregatorPropertyCreated(data);
    } catch (error) {
      this.logger.error(`Error processing aggregator.property.created: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VALUATION_EXCHANGE,
    routingKey: ROUTING_KEYS.AGGREGATOR_PROPERTY_UPDATED,
    queue: QUEUES.AGGREGATOR_PROPERTY_SYNC,
    queueOptions: { durable: true },
  })
  async handleAggregatorPropertyUpdated(data: AggregatorPropertyEventDto): Promise<void | Nack> {
    this.logger.log(`Received aggregator.property.updated event: ${data.id}`);
    try {
      await this.propertySyncService.handleAggregatorPropertyUpdated(data);
    } catch (error) {
      this.logger.error(`Error processing aggregator.property.updated: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }

  @RabbitSubscribe({
    exchange: VALUATION_EXCHANGE,
    routingKey: ROUTING_KEYS.AGGREGATOR_PROPERTY_DELETED,
    queue: QUEUES.AGGREGATOR_PROPERTY_SYNC,
    queueOptions: { durable: true },
  })
  async handleAggregatorPropertyDeleted(data: AggregatorPropertyDeletedEventDto): Promise<void | Nack> {
    this.logger.log(`Received aggregator.property.deleted event: ${data.id}`);
    try {
      await this.propertySyncService.handleAggregatorPropertyDeleted(data);
    } catch (error) {
      this.logger.error(`Error processing aggregator.property.deleted: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return new Nack(false);
    }
  }
}
