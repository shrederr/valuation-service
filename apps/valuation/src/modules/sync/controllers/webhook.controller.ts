import {
  Controller,
  Post,
  Body,
  Headers,
  HttpCode,
  HttpStatus,
  UnauthorizedException,
  Logger,
  BadRequestException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ApiTags, ApiOperation, ApiHeader, ApiBody, ApiResponse } from '@nestjs/swagger';
import { PropertySyncService } from '../services/property-sync.service';
import { VectorPropertyEventDto, VectorPropertyArchivedEventDto } from '../dto';

type PropertyEventType = 'created' | 'updated' | 'archived' | 'unarchived';

interface WebhookPayload {
  event: PropertyEventType;
  data: VectorPropertyEventDto | VectorPropertyArchivedEventDto;
}

interface WebhookResponse {
  success: boolean;
  message?: string;
}

@ApiTags('Webhooks')
@Controller('api/v1/webhooks')
export class WebhookController {
  private readonly logger = new Logger(WebhookController.name);
  private readonly webhookSecret: string | undefined;

  constructor(
    private readonly propertySyncService: PropertySyncService,
    private readonly configService: ConfigService,
  ) {
    this.webhookSecret = this.configService.get<string>('WEBHOOK_SECRET');
    if (this.webhookSecret) {
      this.logger.log('Webhook secret configured - authentication enabled');
    } else {
      this.logger.warn('WEBHOOK_SECRET not configured - webhook authentication disabled');
    }
  }

  @Post('vector/property')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Receive property events from vector-api' })
  @ApiHeader({ name: 'x-webhook-secret', required: false, description: 'Webhook authentication secret' })
  @ApiBody({ description: 'Property event payload' })
  @ApiResponse({ status: 200, description: 'Event processed successfully' })
  @ApiResponse({ status: 401, description: 'Invalid webhook secret' })
  @ApiResponse({ status: 400, description: 'Invalid payload' })
  async handleVectorPropertyWebhook(
    @Headers('x-webhook-secret') secret: string,
    @Body() payload: WebhookPayload,
  ): Promise<WebhookResponse> {
    // Validate webhook secret if configured
    if (this.webhookSecret && secret !== this.webhookSecret) {
      this.logger.warn('Webhook request rejected: invalid secret');
      throw new UnauthorizedException('Invalid webhook secret');
    }

    // Validate payload
    if (!payload || !payload.event || !payload.data) {
      throw new BadRequestException('Invalid payload: missing event or data');
    }

    if (!payload.data.id) {
      throw new BadRequestException('Invalid payload: missing property id');
    }

    this.logger.log(`Received webhook: ${payload.event} for property ${payload.data.id}`);

    try {
      switch (payload.event) {
        case 'created':
          await this.propertySyncService.handleVectorPropertyCreated(payload.data as VectorPropertyEventDto);
          break;
        case 'updated':
          await this.propertySyncService.handleVectorPropertyUpdated(payload.data as VectorPropertyEventDto);
          break;
        case 'archived':
          await this.propertySyncService.handleVectorPropertyArchived(payload.data as VectorPropertyArchivedEventDto);
          break;
        case 'unarchived':
          await this.propertySyncService.handleVectorPropertyUnarchived(payload.data as VectorPropertyEventDto);
          break;
        default:
          throw new BadRequestException(`Unknown event type: ${payload.event}`);
      }

      this.logger.log(`Webhook processed successfully: ${payload.event} for property ${payload.data.id}`);
      return { success: true };
    } catch (error) {
      this.logger.error(
        `Webhook processing failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }
}
