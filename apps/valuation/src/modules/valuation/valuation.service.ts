import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { UnifiedListing } from '@libs/database';
import { SourceType } from '@libs/common';
import { ValuationReportDto, PropertyInfoDto } from '@libs/models';

import { AnalogsService } from '../analogs';
import { FairPriceService } from '../fair-price';
import { LiquidityService } from '../liquidity';

import { ValuationCacheService } from './services/valuation-cache.service';

export interface ValuationOptions {
  sourceType?: SourceType;
  sourceId?: number;
  listingId?: string;
  forceRefresh?: boolean;
}

@Injectable()
export class ValuationService {
  private readonly logger = new Logger(ValuationService.name);

  public constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
    private readonly analogsService: AnalogsService,
    private readonly fairPriceService: FairPriceService,
    private readonly liquidityService: LiquidityService,
    private readonly cacheService: ValuationCacheService,
  ) {}

  public async getFullReport(options: ValuationOptions): Promise<ValuationReportDto> {
    const subject = await this.findSubjectListing(options);

    if (!subject) {
      throw new NotFoundException('Subject listing not found');
    }

    if (!options.forceRefresh) {
      const cached = await this.cacheService.get(subject.id);

      if (cached) {
        this.logger.debug(`Returning cached report for listing ${subject.id}`);

        return cached;
      }
    }

    const report = await this.generateReport(subject);

    await this.cacheService.set(subject.id, report);

    return report;
  }

  private async generateReport(subject: UnifiedListing): Promise<ValuationReportDto> {
    const [analogsResult, fairPrice, liquidity] = await Promise.all([
      this.analogsService.findAnalogs({ listingId: subject.id }),
      this.fairPriceService.calculateFairPrice({ listingId: subject.id }),
      this.liquidityService.calculateLiquidity({ listingId: subject.id }),
    ]);

    const confidence = this.calculateConfidence(analogsResult.totalCount, fairPrice.analogsCount);
    const notes = this.generateNotes(subject, analogsResult, fairPrice);

    return {
      reportId: uuidv4(),
      generatedAt: new Date(),
      property: this.buildPropertyInfo(subject),
      fairPrice,
      liquidity,
      analogs: analogsResult,
      confidence,
      notes: notes.length > 0 ? notes : undefined,
    };
  }

  private buildPropertyInfo(listing: UnifiedListing): PropertyInfoDto {
    const address = this.buildAddress(listing);

    // Get complex name (supports both old name object and new nameRu/nameUk fields)
    let complexName: string | undefined;
    if (listing.complex) {
      if ((listing.complex as any).nameRu) {
        complexName = (listing.complex as any).nameRu;
      } else if ((listing.complex as any).name) {
        const name = (listing.complex as any).name;
        complexName = typeof name === 'object' ? name.uk || name.ru : name;
      }
    }

    return {
      sourceId: listing.sourceId,
      sourceType: listing.sourceType,
      address,
      complexName,
      area: Number(listing.totalArea) || 0,
      rooms: listing.rooms,
      floor: listing.floor,
      totalFloors: listing.totalFloors,
      askingPrice: listing.price ? Number(listing.price) : undefined,
    };
  }

  private buildAddress(listing: UnifiedListing): string {
    const parts: string[] = [];

    if (listing.street?.name) {
      const streetName =
        typeof listing.street.name === 'object' ? (listing.street.name as { uk?: string }).uk : listing.street.name;
      parts.push(streetName || '');
    }

    if (listing.houseNumber) {
      parts.push(listing.houseNumber);
    }

    if (listing.geo?.name) {
      const geoName =
        typeof listing.geo.name === 'object' ? (listing.geo.name as { uk?: string }).uk : listing.geo.name;
      parts.push(geoName || '');
    }

    return parts.filter(Boolean).join(', ');
  }

  private calculateConfidence(analogsCount: number, filteredAnalogsCount: number): number {
    if (filteredAnalogsCount >= 15) {
      return 0.95;
    }

    if (filteredAnalogsCount >= 10) {
      return 0.85;
    }

    if (filteredAnalogsCount >= 7) {
      return 0.75;
    }

    if (filteredAnalogsCount >= 5) {
      return 0.6;
    }

    if (filteredAnalogsCount >= 3) {
      return 0.4;
    }

    return 0.2;
  }

  private generateNotes(
    subject: UnifiedListing,
    analogsResult: { totalCount: number; warning?: string },
    fairPrice: { analogsCount: number },
  ): string[] {
    const notes: string[] = [];

    if (analogsResult.warning) {
      notes.push(analogsResult.warning);
    }

    if (fairPrice.analogsCount < 7) {
      notes.push('Низька кількість аналогів може впливати на точність оцінки');
    }

    if (!subject.totalArea) {
      notes.push('Площа об\'єкта не вказана - оцінка може бути неточною');
    }

    if (!subject.price) {
      notes.push('Ціна об\'єкта не вказана');
    }

    return notes;
  }

  private async findSubjectListing(options: ValuationOptions): Promise<UnifiedListing | null> {
    if (options.listingId) {
      return this.listingRepository.findOne({
        where: { id: options.listingId },
        relations: ['geo', 'street', 'complex'],
      });
    }

    if (options.sourceType && options.sourceId) {
      return this.listingRepository.findOne({
        where: { sourceType: options.sourceType, sourceId: options.sourceId },
        relations: ['geo', 'street', 'complex'],
      });
    }

    return null;
  }
}
