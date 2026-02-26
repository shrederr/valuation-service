import {
  Controller,
  Get,
  Post,
  Param,
  Body,
  Headers,
  HttpCode,
  HttpStatus,
  UnauthorizedException,
  NotFoundException,
  Logger,
  BadRequestException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ApiTags, ApiOperation, ApiHeader, ApiBody, ApiResponse } from '@nestjs/swagger';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UnifiedListing } from '@libs/database';
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
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
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
    this.logger.log(`Vector2 incoming payload: ${JSON.stringify(payload.data)}`);

    // Archive — deactivate + return liquidity score
    if (payload.event === 'archived') {
      try {
        await this.propertySyncService.handleVector2PropertyArchived(payload.data.id);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        this.logger.error(`Vector2 archive failed for ${payload.data.id}: ${message}`);
      }

      let liquidityScore: number | null = 0;
      try {
        const report = await this.valuationService.getFullReport({
          sourceType: SourceType.VECTOR_CRM,
          sourceId: payload.data.id,
        });
        liquidityScore = report.liquidity?.score ?? 0;
      } catch {
        this.logger.warn(`Valuation failed for archived vector2 ${payload.data.id}`);
      }

      const response = { success: true, event: 'archived', sourceId: payload.data.id, syncedAt: new Date(), liquidityScore };
      this.logger.log(`Vector2 webhook response for ${payload.data.id}: ${JSON.stringify(response)}`);
      return response;
    }

    // Created / Updated — upsert + valuation
    if (payload.event !== 'created' && payload.event !== 'updated') {
      throw new BadRequestException(`Unknown event type: ${payload.event}`);
    }

    // Step 1: Check if exists, upsert only if new
    let listingId: string;
    const existing = await this.listingRepository.findOne({
      where: { sourceType: SourceType.VECTOR_CRM, sourceId: payload.data.id },
    });

    if (existing) {
      listingId = existing.id;
      this.logger.log(`Vector2 property ${payload.data.id} already exists (${listingId}), skipping upsert`);
    } else {
      try {
        listingId = await this.propertySyncService.handleVector2PropertyUpsert(payload.data);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown error';
        this.logger.error(`Vector2 upsert failed for ${payload.data.id}: ${message}`);
        return { success: true, event: payload.event, sourceId: payload.data.id, listingId: null, syncedAt: new Date(), liquidityScore: 0 };
      }
    }

    // Step 2: Liquidity score
    let liquidityScore: number | null = null;
    try {
      const report = await this.valuationService.getFullReport({
        sourceType: SourceType.VECTOR_CRM,
        sourceId: payload.data.id,
        forceRefresh: true,
      });
      liquidityScore = report.liquidity?.score ?? null;
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Unknown error';
      this.logger.warn(`Valuation failed for vector2 ${payload.data.id}: ${msg}`);
    }

    const response = {
      success: true,
      event: payload.event,
      sourceId: payload.data.id,
      listingId,
      syncedAt: new Date(),
      liquidityScore,
    };
    this.logger.log(`Vector2 webhook response for ${payload.data.id}: ${JSON.stringify(response)}`);

    return response;
  }

  @Get('vector2/property/:sourceId')
  @ApiOperation({ summary: 'Get vector2 property by sourceId with liquidity score' })
  @ApiResponse({ status: 200, description: 'Property found and evaluated' })
  @ApiResponse({ status: 404, description: 'Property not found' })
  async getVector2Property(
    @Param('sourceId') sourceIdParam: string,
  ): Promise<Record<string, unknown>> {
    const sourceId = parseInt(sourceIdParam, 10);
    if (isNaN(sourceId)) {
      throw new BadRequestException('sourceId must be a number');
    }

    const listing = await this.listingRepository.findOne({
      where: { sourceType: SourceType.VECTOR_CRM, sourceId },
    });

    if (!listing) {
      this.logger.warn(`Vector2 GET lookup: property ${sourceId} not found in DB`);
      return { success: true, event: 'lookup', sourceId, listingId: null, syncedAt: null, liquidityScore: 0 };
    }

    let liquidityScore: number | null = null;
    try {
      const report = await this.valuationService.getFullReport({
        sourceType: SourceType.VECTOR_CRM,
        sourceId,
        forceRefresh: true,
      });
      liquidityScore = report.liquidity?.score ?? null;
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Unknown error';
      this.logger.warn(`Valuation failed for vector2 ${sourceId}: ${msg}`);
    }

    return {
      success: true,
      event: 'lookup',
      sourceId,
      listingId: listing.id,
      syncedAt: listing.syncedAt,
      liquidityScore,
    };
  }
}
