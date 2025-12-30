import { Controller, Get, Param, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiParam, ApiQuery, ApiResponse } from '@nestjs/swagger';
import { SourceType } from '@libs/common';
import { ValuationReportDto } from '@libs/models';

import { ValuationService } from './valuation.service';

@ApiTags('Valuation')
@Controller('api/v1/valuation')
export class ValuationController {
  public constructor(private readonly valuationService: ValuationService) {}

  @Get(':id/full')
  @ApiOperation({ summary: 'Get full valuation report for a listing' })
  @ApiParam({ name: 'id', description: 'Listing UUID or sourceId' })
  @ApiQuery({ name: 'source', enum: ['vector', 'aggregator'], required: false })
  @ApiQuery({ name: 'refresh', type: Boolean, required: false, description: 'Force refresh cache' })
  @ApiResponse({ status: 200, description: 'Full valuation report', type: ValuationReportDto })
  @ApiResponse({ status: 404, description: 'Listing not found' })
  public async getFullReport(
    @Param('id') id: string,
    @Query('source') source?: string,
    @Query('refresh') refresh?: string,
  ): Promise<ValuationReportDto> {
    const isUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);
    const forceRefresh = refresh === 'true';

    if (isUuid) {
      return this.valuationService.getFullReport({ listingId: id, forceRefresh });
    }

    const sourceId = parseInt(id, 10);

    // If source is specified, use it directly
    if (source) {
      return this.valuationService.getFullReport({
        sourceType: source as SourceType,
        sourceId,
        forceRefresh,
      });
    }

    // Auto mode: try aggregator first (most objects), then vector
    try {
      return await this.valuationService.getFullReport({
        sourceType: SourceType.AGGREGATOR,
        sourceId,
        forceRefresh,
      });
    } catch {
      // If not found in aggregator, try vector
      return this.valuationService.getFullReport({
        sourceType: SourceType.VECTOR,
        sourceId,
        forceRefresh,
      });
    }
  }
}
