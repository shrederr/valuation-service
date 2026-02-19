import { Controller, Get, Param, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiParam, ApiQuery, ApiResponse } from '@nestjs/swagger';
import { SourceType } from '@libs/common';
import { FairPriceDto } from '@libs/models';

import { FairPriceService } from './fair-price.service';

@ApiTags('Fair Price')
@Controller('api/v1/valuation')
export class FairPriceController {
  public constructor(private readonly fairPriceService: FairPriceService) {}

  @Get(':id/fair-price')
  @ApiOperation({ summary: 'Calculate fair price for a listing' })
  @ApiParam({ name: 'id', description: 'Listing UUID or sourceId' })
  @ApiQuery({ name: 'source', enum: ['vector', 'aggregator', 'vector_crm'], required: false })
  @ApiResponse({ status: 200, description: 'Fair price calculated', type: FairPriceDto })
  @ApiResponse({ status: 404, description: 'Listing not found' })
  public async calculateFairPrice(
    @Param('id') id: string,
    @Query('source') source?: string,
  ): Promise<FairPriceDto> {
    const isUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);

    if (isUuid) {
      return this.fairPriceService.calculateFairPrice({ listingId: id });
    }

    const sourceId = parseInt(id, 10);
    const sourceType = (source as SourceType) || SourceType.VECTOR;

    return this.fairPriceService.calculateFairPrice({ sourceType, sourceId });
  }
}
