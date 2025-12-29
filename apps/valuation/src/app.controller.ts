import { Controller, Get, Res } from '@nestjs/common';
import { Response } from 'express';
import { join } from 'path';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(
    private readonly appService: AppService,
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
}
