import { Injectable, Logger, Optional } from '@nestjs/common';
import { AmqpConnection } from '@golevelup/nestjs-rabbitmq';

/**
 * Service to control RabbitMQ consumers.
 * Allows pausing/resuming message consumption during initial sync.
 */
@Injectable()
export class ConsumerControlService {
  private readonly logger = new Logger(ConsumerControlService.name);
  private isPaused = false;

  constructor(@Optional() private readonly amqpConnection?: AmqpConnection) {}

  /**
   * Pause all consumers by setting prefetch to 0.
   * Messages will accumulate in RabbitMQ until resumed.
   */
  async pauseConsumers(): Promise<void> {
    if (!this.amqpConnection) {
      this.logger.log('RabbitMQ not configured - skip pause');
      return;
    }
    if (this.isPaused) {
      this.logger.warn('Consumers already paused');
      return;
    }

    try {
      const channel = this.amqpConnection.channel;
      await channel.prefetch(0);
      this.isPaused = true;
      this.logger.log('Consumers paused - messages will queue in RabbitMQ');
    } catch (error) {
      this.logger.error('Failed to pause consumers', error instanceof Error ? error.message : undefined);
      throw error;
    }
  }

  /**
   * Resume consumers by restoring prefetch count.
   */
  async resumeConsumers(prefetchCount = 10): Promise<void> {
    if (!this.amqpConnection) {
      this.logger.log('RabbitMQ not configured - skip resume');
      return;
    }
    if (!this.isPaused) {
      this.logger.warn('Consumers not paused');
      return;
    }

    try {
      const channel = this.amqpConnection.channel;
      await channel.prefetch(prefetchCount);
      this.isPaused = false;
      this.logger.log(`Consumers resumed with prefetch=${prefetchCount}`);
    } catch (error) {
      this.logger.error('Failed to resume consumers', error instanceof Error ? error.message : undefined);
      throw error;
    }
  }

  /**
   * Check if consumers are currently paused.
   */
  arePaused(): boolean {
    return this.isPaused;
  }
}
