import { Controller, Get, Query, NotFoundException } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiQuery, ApiResponse } from '@nestjs/swagger';
import { ListingsService } from './listings.service';

@ApiTags('Listings')
@Controller('api/v1/listings')
export class ListingsController {
  constructor(private readonly listingsService: ListingsService) {}

  @Get('search')
  @ApiOperation({ summary: 'Search for a listing by external URL or source ID' })
  @ApiQuery({ name: 'external_url', required: false, description: 'External URL of the listing' })
  @ApiQuery({ name: 'source_id', required: false, description: 'Source ID of the listing' })
  @ApiQuery({ name: 'source_type', required: false, enum: ['vector', 'aggregator'] })
  @ApiResponse({ status: 200, description: 'Listing found' })
  @ApiResponse({ status: 404, description: 'Listing not found' })
  async search(
    @Query('external_url') externalUrl?: string,
    @Query('source_id') sourceId?: string,
    @Query('source_type') sourceType?: string,
  ) {
    const listing = await this.listingsService.search({
      external_url: externalUrl,
      source_id: sourceId ? parseInt(sourceId, 10) : undefined,
      source_type: sourceType,
    });

    if (!listing) {
      throw new NotFoundException('Listing not found');
    }

    return {
      id: listing.id,
      sourceType: listing.sourceType,
      sourceId: listing.sourceId,
      externalUrl: listing.externalUrl,
    };
  }
}
