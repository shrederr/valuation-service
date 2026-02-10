import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
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
    const { subject, fairPrice, exposureStats } = context;

    if (!fairPrice || fairPrice.analogsCount === undefined || fairPrice.analogsCount === 0) {
      return this.createNullResult('Немає аналогів для оцінки часу експозиції');
    }

    const realtyType = subject.realtyType as keyof typeof MEDIAN_DAYS_TO_SELL;
    const baseDays = MEDIAN_DAYS_TO_SELL[realtyType] || MEDIAN_DAYS_TO_SELL.default;

    // Если есть реальные данные из БД — используем их
    if (exposureStats && exposureStats.count >= 10 && exposureStats.medianDays > 0) {
      return this.evaluateWithRealData(subject, fairPrice, exposureStats.medianDays, baseDays);
    }

    // Fallback: оценка по вердикту fair price
    return this.evaluateByVerdict(fairPrice, baseDays);
  }

  /**
   * Оценка на основе реальных данных экспозиции из БД.
   * Сравниваем предполагаемое время продажи объекта с реальной медианой рынка.
   */
  private evaluateWithRealData(
    subject: UnifiedListing,
    fairPrice: { verdict: string },
    realMedianDays: number,
    baseDays: number,
  ): CriterionResult {
    // Множитель на основе цены: дешевые продаются быстрее
    let priceMultiplier = 1.0;
    if (fairPrice.verdict === 'cheap') {
      priceMultiplier = 0.6;
    } else if (fairPrice.verdict === 'expensive') {
      priceMultiplier = 1.5;
    }

    const subjectEstimate = baseDays * priceMultiplier;
    const ratio = subjectEstimate / realMedianDays;

    let score: number;
    if (ratio <= 0.5) {
      score = 10;
    } else if (ratio <= 0.8) {
      score = 8;
    } else if (ratio <= 1.0) {
      score = 7;
    } else if (ratio <= 1.2) {
      score = 6;
    } else if (ratio <= 1.5) {
      score = 5;
    } else if (ratio <= 2.0) {
      score = 3;
    } else {
      score = 2;
    }

    const estimatedDays = Math.round(subjectEstimate);
    const explanation = `Оцінка ${estimatedDays} днів vs медіана ринку ${realMedianDays} днів`;

    return this.createResult(score, explanation);
  }

  /**
   * Fallback: оценка по вердикту fair price (текущая логика).
   */
  private evaluateByVerdict(
    fairPrice: { verdict: string },
    medianDays: number,
  ): CriterionResult {
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
   * Рассчитывает реальное среднее время экспозиции для аналогичных объектов.
   * Вызывается из LiquidityService перед evaluate().
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
