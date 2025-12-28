import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull, Not, MoreThan } from 'typeorm';
import { UnifiedListing } from '@libs/database';

import { BaseCriterion, CriterionResult, CriterionContext, LIQUIDITY_WEIGHTS, MEDIAN_DAYS_TO_SELL } from './base.criterion';

@Injectable()
export class ExposureTimeCriterion extends BaseCriterion {
  public readonly name = 'exposureTime';
  public readonly weight = LIQUIDITY_WEIGHTS.exposureTime;

  constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
  ) {
    super();
  }

  public evaluate(context: CriterionContext): CriterionResult {
    // Базовая оценка без async запроса
    // Реальное среднее время будет использоваться в estimateDaysToSell
    const { subject, fairPrice } = context;

    if (!fairPrice || fairPrice.analogsCount === undefined || fairPrice.analogsCount === 0) {
      return this.createNullResult('Немає аналогів для оцінки часу експозиції');
    }

    // Используем медианное время для типа недвижимости как базу
    const realtyType = subject.realtyType as keyof typeof MEDIAN_DAYS_TO_SELL;
    const medianDays = MEDIAN_DAYS_TO_SELL[realtyType] || MEDIAN_DAYS_TO_SELL.default;

    // Если цена ниже рынка - время экспозиции будет меньше
    // Если цена выше рынка - время экспозиции будет больше
    let score: number;
    let explanation: string;

    if (fairPrice.verdict === 'cheap') {
      score = 9;
      explanation = `Об'єкт продасться швидше за медіанний час (${medianDays} днів) через низьку ціну`;
    } else if (fairPrice.verdict === 'in_market') {
      score = 6;
      explanation = `Орієнтовний час продажу близький до медіанного (${medianDays} днів)`;
    } else {
      score = 3;
      explanation = `Час продажу може перевищити медіанний (${medianDays} днів) через завищену ціну`;
    }

    return this.createResult(score, explanation);
  }

  /**
   * Рассчитывает реальное среднее время экспозиции для аналогичных объектов
   */
  public async calculateAverageExposureTime(
    geoId: number | null,
    realtyType: string,
  ): Promise<{ medianDays: number; avgDays: number; count: number } | null> {
    const query = this.listingRepository
      .createQueryBuilder('listing')
      .select('listing.deleted_at - listing.published_at', 'exposure')
      .where('listing.deletedAt IS NOT NULL')
      .andWhere('listing.publishedAt IS NOT NULL')
      .andWhere('listing.deletedAt > listing.publishedAt')
      .andWhere('listing.realtyType = :realtyType', { realtyType });

    if (geoId) {
      query.andWhere('listing.geoId = :geoId', { geoId });
    }

    const results = await query.getRawMany();

    if (results.length === 0) {
      return null;
    }

    const exposureDays = results
      .map((r) => {
        const exposure = r.exposure;
        if (!exposure) return null;
        // exposure is interval, convert to days
        const days = exposure.days || 0;
        const hours = exposure.hours || 0;
        return days + hours / 24;
      })
      .filter((d): d is number => d !== null && d > 0)
      .sort((a, b) => a - b);

    if (exposureDays.length === 0) {
      return null;
    }

    const sum = exposureDays.reduce((a, b) => a + b, 0);
    const avgDays = Math.round(sum / exposureDays.length);
    const medianIndex = Math.floor(exposureDays.length / 2);
    const medianDays = Math.round(
      exposureDays.length % 2 === 0
        ? (exposureDays[medianIndex - 1] + exposureDays[medianIndex]) / 2
        : exposureDays[medianIndex],
    );

    return {
      medianDays,
      avgDays,
      count: exposureDays.length,
    };
  }
}
