import { Module, DynamicModule } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { RabbitMQModule as GolevelupRabbitMQModule } from '@golevelup/nestjs-rabbitmq';

// Exchange from vector-api (for customer property events)
export const VECTOR_EVENTS_EXCHANGE = 'vector-events';
// Exchange from api-property-aggregator (valuation_exchange, NOT aggregator-events!)
export const AGGREGATOR_EVENTS_EXCHANGE = 'valuation_exchange';

export const QUEUES = {
  // Vector property sync queues
  VECTOR_PROPERTY_SYNC: 'valuation.vector.property.sync',
  // Aggregator property sync queues
  AGGREGATOR_PROPERTY_SYNC: 'valuation.aggregator.property.sync',
  // Geo sync queues (not yet implemented in vector-api)
  GEO_SYNC: 'valuation.geo.sync',
  STREET_SYNC: 'valuation.street.sync',
  TOPZONE_SYNC: 'valuation.topzone.sync',
  COMPLEX_SYNC: 'valuation.complex.sync',
} as const;

export const ROUTING_KEYS = {
  // From vector-api: Customer Property events (exchange: vector-events)
  VECTOR_PROPERTY_CREATED: 'customer-property.created',
  VECTOR_PROPERTY_UPDATED: 'customer-property.updated',
  VECTOR_PROPERTY_ARCHIVED: 'customer-property.archived',
  VECTOR_PROPERTY_UNARCHIVED: 'customer-property.unarchived',
  // From api-property-aggregator: Exported Property events (exchange: valuation_exchange)
  AGGREGATOR_PROPERTY_CREATED: 'valuation.aggregator.property.created',
  AGGREGATOR_PROPERTY_UPDATED: 'valuation.aggregator.property.updated',
  AGGREGATOR_PROPERTY_DELETED: 'valuation.aggregator.property.deleted',
  // Geo events (not yet implemented in vector-api, kept for compatibility)
  GEO_CREATED: 'geo.created',
  GEO_UPDATED: 'geo.updated',
  GEO_DELETED: 'geo.deleted',
  STREET_CREATED: 'street.created',
  STREET_UPDATED: 'street.updated',
  STREET_DELETED: 'street.deleted',
  TOPZONE_CREATED: 'topzone.created',
  TOPZONE_UPDATED: 'topzone.updated',
  TOPZONE_DELETED: 'topzone.deleted',
  COMPLEX_CREATED: 'complex.created',
  COMPLEX_UPDATED: 'complex.updated',
  COMPLEX_DELETED: 'complex.deleted',
} as const;

@Module({})
export class RabbitMQModule {
  static forRoot(): DynamicModule {
    const rabbitMqUrl = process.env.RABBITMQ_URL;
    const isRabbitMqEnabled = rabbitMqUrl && rabbitMqUrl.startsWith('amqp');

    if (!isRabbitMqEnabled) {
      console.log('[RabbitMQModule] RabbitMQ is disabled (RABBITMQ_URL not set or invalid)');
      return {
        module: RabbitMQModule,
        imports: [],
        exports: [],
      };
    }

    return {
      module: RabbitMQModule,
      imports: [
        GolevelupRabbitMQModule.forRootAsync({
          imports: [ConfigModule],
          inject: [ConfigService],
          useFactory: (configService: ConfigService) => ({
            uri: configService.get<string>('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672'),
            deserializer: (message: Buffer) => {
              try {
                return JSON.parse(message.toString());
              } catch {
                return message.toString();
              }
            },
            exchanges: [
              {
                name: VECTOR_EVENTS_EXCHANGE,
                type: 'topic',
                options: { durable: true },
              },
              {
                name: AGGREGATOR_EVENTS_EXCHANGE,
                type: 'topic',
                options: { durable: true },
              },
            ],
            connectionInitOptions: { wait: true },
            enableControllerDiscovery: true,
          }),
        }),
      ],
      exports: [GolevelupRabbitMQModule],
    };
  }
}
