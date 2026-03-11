import { Controller, Get, Post, Param, Body, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiQuery } from '@nestjs/swagger';
import { ExportService } from './export.service';
import { ExportRunDto } from './dto';

@ApiTags('Export')
@Controller('api/v1/export')
export class ExportController {
  constructor(private readonly exportService: ExportService) {}

  @Post('run')
  @ApiOperation({ summary: 'Trigger export manually' })
  async run(@Body() body: ExportRunDto) {
    return this.exportService.runExport({ batchSize: body.batchSize, geoId: body.geoId, realtyType: body.realtyType });
  }

  @Get('status')
  @ApiOperation({ summary: 'Get export status' })
  getStatus() {
    return this.exportService.getStatus();
  }

  @Get('stats')
  @ApiOperation({ summary: 'Get export statistics' })
  @ApiResponse({ status: 200 })
  async getStats() {
    return this.exportService.getStats();
  }

  @Get('preview/:id')
  @ApiOperation({ summary: 'Preview export mapping for a listing' })
  async preview(@Param('id') id: string) {
    const result = await this.exportService.previewExport(id);
    if (!result) {
      return { error: 'Listing not found' };
    }
    return result;
  }

  @Post('single/:id')
  @ApiOperation({ summary: 'Export a single listing to CRM' })
  async exportSingle(@Param('id') id: string) {
    return this.exportService.exportSingle(id);
  }

  @Post('translate-batch')
  @ApiOperation({ summary: 'Pre-translate descriptions (UK↔RU) for listings missing a language' })
  @ApiQuery({ name: 'batchSize', required: false, type: Number })
  async translateBatch(@Query('batchSize') batchSize?: string) {
    return this.exportService.translateBatch(batchSize ? Number(batchSize) : 100);
  }
}
