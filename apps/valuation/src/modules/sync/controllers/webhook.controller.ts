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
import { VectorPropertyEventDto, VectorPropertyArchivedEventDto, Vector2ObjectRow } from '../dto';
import { ValuationService } from '../../valuation/valuation.service';
import { SourceType } from '@libs/common';

type PropertyEventType = 'created' | 'updated' | 'archived' | 'unarchived';

interface WebhookPayload {
  event: PropertyEventType;
  data: VectorPropertyEventDto | VectorPropertyArchivedEventDto;
}

interface Vector2WebhookPayload {
  event: 'created' | 'updated' | 'archived';
  data: Vector2ObjectRow;
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
    private readonly valuationService: ValuationService,
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

  @Post('vector2/property')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Receive property events from vector2 CRM (vec.atlanta.ua)' })
  @ApiHeader({ name: 'x-webhook-secret', required: false, description: 'Webhook authentication secret' })
  @ApiBody({ description: 'Vector2 property event payload' })
  @ApiResponse({ status: 200, description: 'Event processed (check success field)' })
  @ApiResponse({ status: 401, description: 'Invalid webhook secret' })
  @ApiResponse({ status: 400, description: 'Invalid payload' })
  async handleVector2PropertyWebhook(
    @Headers('x-webhook-secret') secret: string,
    @Body() payload: Vector2WebhookPayload,
  ): Promise<Record<string, unknown>> {
    if (this.webhookSecret && secret !== this.webhookSecret) {
      this.logger.warn('Vector2 webhook request rejected: invalid secret');
      throw new UnauthorizedException('Invalid webhook secret');
    }

    if (!payload || !payload.event || !payload.data) {
      throw new BadRequestException('Invalid payload: missing event or data');
    }

    if (!payload.data.id) {
      throw new BadRequestException('Invalid payload: missing property id');
    }

    this.logger.log(`Received vector2 webhook: ${payload.event} for property ${payload.data.id}`);

    // Archive — just deactivate, no valuation needed
    if (payload.event === 'archived') {
      try {
        await this.propertySyncService.handleVector2PropertyArchived(payload.data.id);
        return { success: true, event: 'archived', sourceId: payload.data.id, syncedAt: new Date() };
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        this.logger.error(`Vector2 archive failed for ${payload.data.id}: ${message}`);
        return { success: false, message };
      }
    }

    // Created / Updated — upsert + valuation
    if (payload.event !== 'created' && payload.event !== 'updated') {
      throw new BadRequestException(`Unknown event type: ${payload.event}`);
    }

    // Step 1: Upsert
    let listingId: string;
    try {
      listingId = await this.propertySyncService.handleVector2PropertyUpsert(payload.data);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      this.logger.error(`Vector2 upsert failed for ${payload.data.id}: ${message}`);
      return { success: false, message };
    }

    // Step 2: Valuation
    let valuation: Record<string, unknown> | undefined;
    try {
      const report = await this.valuationService.getFullReport({
        sourceType: SourceType.VECTOR_CRM,
        sourceId: payload.data.id,
        forceRefresh: true,
      });
      valuation = report as unknown as Record<string, unknown>;
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Unknown error';
      this.logger.warn(`Valuation failed for vector2 ${payload.data.id}: ${msg}`);
    }

    this.logger.log(`Vector2 webhook processed: ${payload.event} for property ${payload.data.id}`);

    return {
      success: true,
      event: payload.event,
      sourceId: payload.data.id,
      listingId,
      syncedAt: new Date(),
      valuation: valuation || null,
    };
  }
}
