import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, LessThan } from 'typeorm';
import { ValuationCache, AnalogsData, FairPriceData, LiquidityData } from '@libs/database';
import { ValuationReportDto } from '@libs/models';

const CACHE_TTL_HOURS = 24;

@Injectable()
export class ValuationCacheService {
  private readonly logger = new Logger(ValuationCacheService.name);

  public constructor(
    @InjectRepository(ValuationCache)
    private readonly cacheRepository: Repository<ValuationCache>,
  ) {}

  public async get(listingId: string): Promise<ValuationReportDto | null> {
    const cached = await this.cacheRepository.findOne({
      where: { listingId },
    });

    if (!cached) {
      return null;
    }

    if (new Date() > cached.expiresAt) {
      await this.cacheRepository.delete({ id: cached.id });
      return null;
    }

    return this.cacheToReport(cached, listingId);
  }

  public async set(listingId: string, report: ValuationReportDto): Promise<void> {
    const expiresAt = new Date();
    expiresAt.setHours(expiresAt.getHours() + CACHE_TTL_HOURS);

    const analogsData: AnalogsData = {
      count: report.analogs?.totalCount || 0,
      analogIds: report.analogs?.analogs?.map((a) => a.id) || [],
      searchRadius: (report.analogs?.searchRadius as AnalogsData['searchRadius']) || 'city',
    };

    const fairPrice: FairPriceData = {
      median: report.fairPrice?.median || 0,
      average: report.fairPrice?.average || 0,
      min: report.fairPrice?.min || 0,
      max: report.fairPrice?.max || 0,
      q1: report.fairPrice?.range?.low || 0,
      q3: report.fairPrice?.range?.high || 0,
      pricePerMeter: {
        median: report.fairPrice?.pricePerMeter?.median || 0,
        average: report.fairPrice?.pricePerMeter?.average || 0,
      },
      verdict: (report.fairPrice?.verdict as FairPriceData['verdict']) || 'in_market',
    };

    const breakdownFromCriteria: Record<string, { score: number; weight: number }> = {};
    if (report.liquidity?.criteria) {
      for (const criterion of report.liquidity.criteria) {
        breakdownFromCriteria[criterion.name] = {
          score: criterion.score,
          weight: criterion.weight,
        };
      }
    }

    const liquidity: LiquidityData = {
      score: report.liquidity?.score || 0,
      level: (report.liquidity?.level as LiquidityData['level']) || 'medium',
      breakdown: breakdownFromCriteria,
    };

    const existing = await this.cacheRepository.findOne({ where: { listingId } });

    if (existing) {
      existing.analogsData = analogsData;
      existing.fairPrice = fairPrice;
      existing.liquidity = liquidity;
      existing.expiresAt = expiresAt;
      await this.cacheRepository.save(existing);
    } else {
      const cache = this.cacheRepository.create({
        listingId,
        analogsData,
        fairPrice,
        liquidity,
        expiresAt,
      });
      await this.cacheRepository.save(cache);
    }
  }

  public async invalidate(listingId: string): Promise<void> {
    await this.cacheRepository.delete({ listingId });
  }

  public async cleanupExpired(): Promise<number> {
    const result = await this.cacheRepository.delete({
      expiresAt: LessThan(new Date()),
    });

    return result.affected || 0;
  }

  private cacheToReport(cached: ValuationCache, listingId: string): ValuationReportDto {
    const criteria = Object.entries(cached.liquidity.breakdown).map(([name, data]) => ({
      name,
      weight: data.weight,
      score: data.score,
      weightedScore: data.weight * data.score,
    }));

    return {
      reportId: cached.id,
      generatedAt: cached.calculatedAt,
      property: {
        sourceId: 0,
        sourceType: '',
        area: 0,
      },
      fairPrice: {
        median: cached.fairPrice.median,
        average: cached.fairPrice.average,
        min: cached.fairPrice.min,
        max: cached.fairPrice.max,
        range: {
          low: cached.fairPrice.q1,
          high: cached.fairPrice.q3,
        },
        pricePerMeter: cached.fairPrice.pricePerMeter,
        verdict: cached.fairPrice.verdict,
        analogsCount: cached.analogsData.count,
      },
      liquidity: {
        score: cached.liquidity.score,
        level: cached.liquidity.level,
        criteria,
        estimatedDaysToSell: this.estimateDaysToSell(cached.liquidity.level),
      },
      analogs: {
        analogs: [],
        totalCount: cached.analogsData.count,
        searchRadius: cached.analogsData.searchRadius,
      },
    };
  }

  private estimateDaysToSell(level: 'low' | 'medium' | 'high'): number {
    switch (level) {
      case 'high':
        return 30;
      case 'medium':
        return 60;
      case 'low':
        return 120;
      default:
        return 90;
    }
  }
}
