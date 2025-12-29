import { Module, DynamicModule } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { RabbitMQModule as GolevelupRabbitMQModule } from '@golevelup/nestjs-rabbitmq';

export const VALUATION_EXCHANGE = 'valuation_exchange';

export const QUEUES = {
  // Geo sync queues
  GEO_SYNC: 'valuation.geo.sync',
  STREET_SYNC: 'valuation.street.sync',
  TOPZONE_SYNC: 'valuation.topzone.sync',
  COMPLEX_SYNC: 'valuation.complex.sync',
  // Vector property sync queues
  VECTOR_PROPERTY_SYNC: 'valuation.vector.property.sync',
  // Aggregator property sync queues
  AGGREGATOR_PROPERTY_SYNC: 'valuation.aggregator.property.sync',
} as const;

export const ROUTING_KEYS = {
  // From vector-api: Geo events
  GEO_CREATED: 'valuation.geo.created',
  GEO_UPDATED: 'valuation.geo.updated',
  GEO_DELETED: 'valuation.geo.deleted',
  // From vector-api: Street events
  STREET_CREATED: 'valuation.street.created',
  STREET_UPDATED: 'valuation.street.updated',
  STREET_DELETED: 'valuation.street.deleted',
  // From vector-api: Topzone events
  TOPZONE_CREATED: 'valuation.topzone.created',
  TOPZONE_UPDATED: 'valuation.topzone.updated',
  TOPZONE_DELETED: 'valuation.topzone.deleted',
  // From vector-api: Apartment Complex events
  COMPLEX_CREATED: 'valuation.complex.created',
  COMPLEX_UPDATED: 'valuation.complex.updated',
  COMPLEX_DELETED: 'valuation.complex.deleted',
  // From vector-api: Customer Property events
  VECTOR_PROPERTY_CREATED: 'valuation.property.created',
  VECTOR_PROPERTY_UPDATED: 'valuation.property.updated',
  VECTOR_PROPERTY_ARCHIVED: 'valuation.property.archived',
  VECTOR_PROPERTY_UNARCHIVED: 'valuation.property.unarchived',
  // From api-property-aggregator: Exported Property events
  AGGREGATOR_PROPERTY_CREATED: 'valuation.aggregator.property.created',
  AGGREGATOR_PROPERTY_UPDATED: 'valuation.aggregator.property.updated',
  AGGREGATOR_PROPERTY_DELETED: 'valuation.aggregator.property.deleted',
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
            exchanges: [
              {
                name: VALUATION_EXCHANGE,
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
