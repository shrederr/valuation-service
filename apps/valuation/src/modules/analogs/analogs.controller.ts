import { Controller, Get, Param, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiParam, ApiQuery, ApiResponse } from '@nestjs/swagger';
import { SourceType } from '@libs/common';
import { AnalogSearchResultDto } from '@libs/models';

import { AnalogsService } from './analogs.service';

@ApiTags('Analogs')
@Controller('api/v1/valuation')
export class AnalogsController {
  public constructor(private readonly analogsService: AnalogsService) {}

  @Get(':id/analogs')
  @ApiOperation({ summary: 'Find analogs for a listing' })
  @ApiParam({ name: 'id', description: 'Listing UUID or sourceId' })
  @ApiQuery({ name: 'source', enum: ['vector', 'aggregator'], required: false })
  @ApiQuery({ name: 'limit', type: Number, required: false, description: 'Max analogs to return' })
  @ApiResponse({ status: 200, description: 'Analogs found', type: AnalogSearchResultDto })
  @ApiResponse({ status: 404, description: 'Listing not found' })
  public async findAnalogs(
    @Param('id') id: string,
    @Query('source') source?: string,
    @Query('limit') limit?: string,
  ): Promise<AnalogSearchResultDto> {
    const isUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);

    if (isUuid) {
      return this.analogsService.findAnalogs({
        listingId: id,
        maxAnalogs: limit ? parseInt(limit, 10) : undefined,
      });
    }

    const sourceId = parseInt(id, 10);
    const sourceType = (source as SourceType) || SourceType.VECTOR;

    return this.analogsService.findAnalogs({
      sourceType,
      sourceId,
      maxAnalogs: limit ? parseInt(limit, 10) : undefined,
    });
  }
}
