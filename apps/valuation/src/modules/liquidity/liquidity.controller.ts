import { Controller, Get, Param, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiParam, ApiQuery, ApiResponse } from '@nestjs/swagger';
import { SourceType } from '@libs/common';
import { LiquidityDto } from '@libs/models';

import { LiquidityService } from './liquidity.service';

@ApiTags('Liquidity')
@Controller('api/v1/valuation')
export class LiquidityController {
  public constructor(private readonly liquidityService: LiquidityService) {}

  @Get(':id/liquidity')
  @ApiOperation({ summary: 'Calculate liquidity score for a listing' })
  @ApiParam({ name: 'id', description: 'Listing UUID or sourceId' })
  @ApiQuery({ name: 'source', enum: ['vector', 'aggregator'], required: false })
  @ApiResponse({ status: 200, description: 'Liquidity calculated', type: LiquidityDto })
  @ApiResponse({ status: 404, description: 'Listing not found' })
  public async calculateLiquidity(
    @Param('id') id: string,
    @Query('source') source?: string,
  ): Promise<LiquidityDto> {
    const isUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);

    if (isUuid) {
      return this.liquidityService.calculateLiquidity({ listingId: id });
    }

    const sourceId = parseInt(id, 10);
    const sourceType = (source as SourceType) || SourceType.VECTOR;

    return this.liquidityService.calculateLiquidity({ sourceType, sourceId });
  }
}
