import { Controller, Get, Post, Res } from '@nestjs/common';
import { Response } from 'express';
import { join } from 'path';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(
    private readonly appService: AppService,
    private readonly dataSource: DataSource,
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
  ) {}

  @Get()
  serveIndex(@Res() res: Response): void {
    res.sendFile(join(process.cwd(), 'public', 'index.html'));
  }

  @Get('health')
  getHealth(): { status: string } {
    return { status: 'ok' };
  }

  @Get('debug/sync-status')
  async getSyncStatus(): Promise<{
    count: number;
    lastSynced: Date | null;
    sample: unknown;
  }> {
    const count = await this.listingRepository.count();
    const lastRecord = await this.listingRepository.findOne({
      order: { syncedAt: 'DESC' },
      where: {},
    });
    return {
      count,
      lastSynced: lastRecord?.syncedAt || null,
      sample: lastRecord
        ? { id: lastRecord.id, sourceType: lastRecord.sourceType, price: lastRecord.price }
        : null,
    };
  }

  @Post('admin/truncate-streets')
  async truncateStreets(): Promise<{ success: boolean; message: string }> {
    await this.dataSource.query('TRUNCATE streets CASCADE');
    return { success: true, message: 'Streets table truncated' };
  }

  @Get('admin/streets-count')
  async getStreetsCount(): Promise<{ count: number }> {
    const result = await this.dataSource.query('SELECT COUNT(*) as count FROM streets');
    return { count: parseInt(result[0].count, 10) };
  }

  @Post('admin/truncate-listings')
  async truncateListings(): Promise<{ success: boolean; message: string }> {
    await this.dataSource.query('TRUNCATE unified_listings CASCADE');
    return { success: true, message: 'Unified listings table truncated' };
  }

  @Get('admin/listings-count')
  async getListingsCount(): Promise<{ count: number }> {
    const count = await this.listingRepository.count();
    return { count };
  }
}
