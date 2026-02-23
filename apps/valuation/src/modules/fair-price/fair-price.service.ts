import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { SourceType } from '@libs/common';
import { FairPriceDto, AnalogDto } from '@libs/models';

import { AnalogsService } from '../analogs';

import { StatisticsCalculator } from './calculators/statistics.calculator';
import { OutlierFilter } from './calculators/outlier-filter';
import { PriceVerdictService } from './services/price-verdict.service';

export interface FairPriceOptions {
  sourceType?: SourceType;
  sourceId?: number;
  listingId?: string;
}

@Injectable()
export class FairPriceService {
  private readonly logger = new Logger(FairPriceService.name);

  public constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
    private readonly analogsService: AnalogsService,
    private readonly statisticsCalculator: StatisticsCalculator,
    private readonly outlierFilter: OutlierFilter,
    private readonly priceVerdictService: PriceVerdictService,
  ) {}

  public async calculateFairPrice(options: FairPriceOptions): Promise<FairPriceDto> {
    const subject = await this.findSubjectListing(options);

    if (!subject) {
      throw new NotFoundException('Subject listing not found');
    }

    const analogsResult = await this.analogsService.findAnalogs({
      listingId: subject.id,
    });

    return this.calculateFromAnalogs(subject, analogsResult.analogs);
  }

  public calculateFromAnalogs(subject: UnifiedListing, analogs: AnalogDto[]): FairPriceDto {
    const prices = analogs.map((a) => a.price).filter((p) => p > 0);

    // Рассчитываем цену за м² из цены и площади, если pricePerMeter не указана
    // Фильтруем магическое значение 9999999999 (заглушка для участков без площади)
    const MAX_REASONABLE_PRICE_PER_METER = 500000; // $500k/м² — разумный верхний предел
    const pricesPerMeter = analogs
      .map((a) => {
        if (a.pricePerMeter && a.pricePerMeter > 0 && a.pricePerMeter < MAX_REASONABLE_PRICE_PER_METER) {
          return a.pricePerMeter;
        }
        // Рассчитываем из цены и площади
        if (a.price > 0 && a.area && a.area > 0) {
          const calculated = Math.round(a.price / a.area);
          return calculated < MAX_REASONABLE_PRICE_PER_METER ? calculated : 0;
        }
        return 0;
      })
      .filter((p) => p > 0);

    const { filtered: filteredPrices } = this.outlierFilter.filterOutliers(prices);
    const { filtered: filteredPricesPerMeter } = this.outlierFilter.filterOutliers(pricesPerMeter);

    const priceStats = this.statisticsCalculator.calculate(filteredPrices);
    const pricePerMeterStats = this.statisticsCalculator.calculate(filteredPricesPerMeter);

    const subjectPrice = Number(subject.price) || 0;
    const verdict = this.priceVerdictService.determineVerdict(subjectPrice, priceStats);

    return {
      median: priceStats.median,
      average: priceStats.average,
      min: priceStats.min,
      max: priceStats.max,
      range: {
        low: priceStats.q1,
        high: priceStats.q3,
      },
      pricePerMeter: {
        median: pricePerMeterStats.median,
        average: pricePerMeterStats.average,
      },
      verdict,
      analogsCount: filteredPrices.length,
    };
  }

  private async findSubjectListing(options: FairPriceOptions): Promise<UnifiedListing | null> {
    if (options.listingId) {
      return this.listingRepository.findOne({
        where: { id: options.listingId },
      });
    }

    if (options.sourceType && options.sourceId) {
      return this.listingRepository.findOne({
        where: { sourceType: options.sourceType, sourceId: options.sourceId },
      });
    }

    return null;
  }
}
