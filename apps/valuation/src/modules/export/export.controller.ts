import { Controller, Get, Post, Param, Body, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse } from '@nestjs/swagger';
import { ExportService } from './export.service';
import { ExportRunDto } from './dto';

@ApiTags('Export')
@Controller('api/v1/export')
export class ExportController {
  constructor(private readonly exportService: ExportService) {}

  @Post('run')
  @ApiOperation({ summary: 'Trigger export manually' })
  async run(@Body() body: ExportRunDto) {
    return this.exportService.runExport(body.batchSize);
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
  @ApiOperation({ summary: 'Export a single listing' })
  async exportSingle(@Param('id') id: string) {
    const preview = await this.exportService.previewExport(id);
    if (!preview) {
      return { error: 'Listing not found' };
    }
    if (preview.dedup.isDuplicate) {
      return { error: 'Listing is a duplicate', dedup: preview.dedup };
    }
    // For now, just return preview (actual send when CRM endpoint is ready)
    return { status: 'preview_only', ...preview };
  }
}
