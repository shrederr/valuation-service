import { Controller, Get, Post, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiQuery } from '@nestjs/swagger';
import { InfrastructureService } from './infrastructure.service';

@ApiTags('Infrastructure')
@Controller('api/v1/infrastructure')
export class InfrastructureController {
  constructor(private readonly infrastructureService: InfrastructureService) {}

  @Get('status')
  @ApiOperation({ summary: 'Count listings without infrastructure data' })
  async getStatus() {
    const missing = await this.infrastructureService.getListingsWithoutInfrastructureCount();
    return { missingInfrastructure: missing };
  }

  @Post('batch')
  @ApiOperation({ summary: 'Trigger batch infrastructure processing' })
  @ApiQuery({ name: 'batchSize', required: false, type: Number })
  @ApiQuery({ name: 'delayMs', required: false, type: Number })
  async runBatch(
    @Query('batchSize') batchSize?: string,
    @Query('delayMs') delayMs?: string,
  ) {
    const result = await this.infrastructureService.processListingsBatch(
      batchSize ? Number(batchSize) : 500,
      delayMs ? Number(delayMs) : 200,
    );
    return result;
  }
}
