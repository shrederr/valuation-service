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

  @Get('admin/listings-stats')
  async getListingsStats(): Promise<{ total: number; active: number; inactive: number }> {
    const total = await this.listingRepository.count();
    const active = await this.listingRepository.count({ where: { isActive: true } });
    return { total, active, inactive: total - active };
  }

  @Post('admin/recalculate-price-per-meter')
  async recalculatePricePerMeter(): Promise<{ success: boolean; updated: number }> {
    // Update all records where price and totalArea exist but pricePerMeter is null or 0
    const result = await this.dataSource.query(`
      UPDATE unified_listings
      SET price_per_meter = price / total_area
      WHERE price IS NOT NULL
        AND price > 0
        AND total_area IS NOT NULL
        AND total_area > 0
        AND (price_per_meter IS NULL OR price_per_meter = 0)
    `);
    return { success: true, updated: result[1] || 0 };
  }

  @Get('admin/price-per-meter-stats')
  async getPricePerMeterStats(): Promise<{ withPPM: number; withoutPPM: number; canCalculate: number }> {
    const withPPM = await this.dataSource.query(`
      SELECT COUNT(*) as count FROM unified_listings
      WHERE price_per_meter IS NOT NULL AND price_per_meter > 0
    `);
    const withoutPPM = await this.dataSource.query(`
      SELECT COUNT(*) as count FROM unified_listings
      WHERE price_per_meter IS NULL OR price_per_meter = 0
    `);
    const canCalculate = await this.dataSource.query(`
      SELECT COUNT(*) as count FROM unified_listings
      WHERE (price_per_meter IS NULL OR price_per_meter = 0)
        AND price IS NOT NULL AND price > 0
        AND total_area IS NOT NULL AND total_area > 0
    `);
    return {
      withPPM: parseInt(withPPM[0].count, 10),
      withoutPPM: parseInt(withoutPPM[0].count, 10),
      canCalculate: parseInt(canCalculate[0].count, 10),
    };
  }
}
