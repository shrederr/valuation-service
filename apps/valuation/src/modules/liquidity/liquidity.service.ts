import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { SourceType } from '@libs/common';
import { LiquidityDto, LiquidityCriterionDto, FairPriceDto } from '@libs/models';

import { FairPriceService } from '../fair-price';
import { AnalogsService } from '../analogs';

import { PriceCriterion } from './criteria/price.criterion';
import { LivingAreaCriterion } from './criteria/living-area.criterion';
import { CompetitionCriterion } from './criteria/competition.criterion';
import { LocationCriterion } from './criteria/location.criterion';
import { ConditionCriterion } from './criteria/condition.criterion';
import { FormatCriterion } from './criteria/format.criterion';
import { FloorCriterion } from './criteria/floor.criterion';
import { HouseTypeCriterion } from './criteria/house-type.criterion';
import { ExposureTimeCriterion } from './criteria/exposure-time.criterion';
import { InfrastructureCriterion } from './criteria/infrastructure.criterion';
import { FurnitureCriterion } from './criteria/furniture.criterion';
import { CommunicationsCriterion } from './criteria/communications.criterion';
import { UniqueFeaturesCriterion } from './criteria/unique-features.criterion';
import { BuyConditionsCriterion } from './criteria/buy-conditions.criterion';
import { CriterionResult, CriterionContext, ExposureStats, MEDIAN_DAYS_TO_SELL } from './criteria/base.criterion';

export interface LiquidityOptions {
  sourceType?: SourceType;
  sourceId?: number;
  listingId?: string;
}

@Injectable()
export class LiquidityService {
  private readonly logger = new Logger(LiquidityService.name);

  public constructor(
    @InjectRepository(UnifiedListing)
    private readonly listingRepository: Repository<UnifiedListing>,
    private readonly fairPriceService: FairPriceService,
    private readonly analogsService: AnalogsService,
    private readonly priceCriterion: PriceCriterion,
    private readonly livingAreaCriterion: LivingAreaCriterion,
    private readonly competitionCriterion: CompetitionCriterion,
    private readonly locationCriterion: LocationCriterion,
    private readonly conditionCriterion: ConditionCriterion,
    private readonly formatCriterion: FormatCriterion,
    private readonly floorCriterion: FloorCriterion,
    private readonly houseTypeCriterion: HouseTypeCriterion,
    private readonly exposureTimeCriterion: ExposureTimeCriterion,
    private readonly infrastructureCriterion: InfrastructureCriterion,
    private readonly furnitureCriterion: FurnitureCriterion,
    private readonly communicationsCriterion: CommunicationsCriterion,
    private readonly uniqueFeaturesCriterion: UniqueFeaturesCriterion,
    private readonly buyConditionsCriterion: BuyConditionsCriterion,
  ) {}

  public async calculateLiquidity(options: LiquidityOptions): Promise<LiquidityDto> {
    const subject = await this.findSubjectListing(options);

    if (!subject) {
      throw new NotFoundException('Subject listing not found');
    }

    let fairPrice: FairPriceDto | undefined;
    let analogs: UnifiedListing[] | undefined;

    try {
      const analogsResult = await this.analogsService.findAnalogs({ listingId: subject.id });
      // Получаем raw UnifiedListing объекты для min-max нормализации
      if (analogsResult.analogs.length > 0) {
        analogs = await this.listingRepository
          .createQueryBuilder('listing')
          .where('listing.id IN (:...ids)', { ids: analogsResult.analogs.map((a) => a.id) })
          .getMany();
      }
      fairPrice = this.fairPriceService.calculateFromAnalogs(subject, analogsResult.analogs);
    } catch {
      this.logger.warn(`Could not calculate fair price for listing ${subject.id}`);
    }

    // Получаем реальные данные по экспозиции
    let exposureStats: ExposureStats | null = null;
    try {
      exposureStats = await this.exposureTimeCriterion.calculateAverageExposureTime(
        subject.geoId ?? null,
        subject.realtyType,
      );
    } catch {
      this.logger.warn(`Could not calculate exposure stats for listing ${subject.id}`);
    }

    return this.calculateFromSubject(subject, fairPrice, analogs, exposureStats);
  }

  public calculateFromSubject(
    subject: UnifiedListing,
    fairPrice?: FairPriceDto,
    analogs?: UnifiedListing[],
    exposureStats?: ExposureStats | null,
  ): LiquidityDto {
    const context: CriterionContext = { subject, fairPrice, analogs, exposureStats };

    const criteriaResults: CriterionResult[] = [
      this.priceCriterion.evaluate(context),
      this.livingAreaCriterion.evaluate(context),
      this.exposureTimeCriterion.evaluate(context),
      this.competitionCriterion.evaluate(context),
      this.locationCriterion.evaluate(context),
      this.conditionCriterion.evaluate(context),
      this.formatCriterion.evaluate(context),
      this.floorCriterion.evaluate(context),
      this.houseTypeCriterion.evaluate(context),
      this.infrastructureCriterion.evaluate(context),
      this.furnitureCriterion.evaluate(context),
      this.communicationsCriterion.evaluate(context),
      this.uniqueFeaturesCriterion.evaluate(context),
      this.buyConditionsCriterion.evaluate(context),
    ];

    const totalWeight = criteriaResults.reduce((sum, c) => sum + c.weight, 0);
    const weightedSum = criteriaResults.reduce((sum, c) => sum + c.weightedScore, 0);
    const score = totalWeight > 0 ? Math.round((weightedSum / totalWeight) * 10) / 10 : 5;

    const level = this.determineLevel(score);
    const estimatedDaysToSell = this.estimateDaysToSell(score, subject.realtyType);
    const recommendations = this.generateRecommendations(criteriaResults);
    const confidence = this.determineConfidence(totalWeight);

    return {
      score,
      level,
      criteria: criteriaResults.map((c) => this.mapToCriterionDto(c)),
      estimatedDaysToSell,
      recommendations: recommendations.length > 0 ? recommendations : undefined,
      confidence,
    };
  }

  private determineLevel(score: number): 'high' | 'medium' | 'low' {
    if (score >= 7) {
      return 'high';
    }

    if (score >= 5) {
      return 'medium';
    }

    return 'low';
  }

  /**
   * Определяет достоверность оценки на основе суммы весов оценённых критериев.
   * Если суммарный вес < 0.6, данных недостаточно.
   */
  private determineConfidence(totalWeight: number): 'high' | 'medium' | 'low' {
    if (totalWeight >= 0.7) {
      return 'high';
    }
    if (totalWeight >= 0.5) {
      return 'medium';
    }
    return 'low';
  }

  /**
   * Расчет ориентировочного времени продажи на основе:
   * 1. Реальных данных по медианному времени продажи для типа недвижимости
   * 2. Корректировки на основе скоринга ликвидности
   */
  private estimateDaysToSell(score: number, realtyType?: string): number {
    const type = (realtyType || 'default') as keyof typeof MEDIAN_DAYS_TO_SELL;
    const baseDays = MEDIAN_DAYS_TO_SELL[type] || MEDIAN_DAYS_TO_SELL.default;

    // score 10 -> multiplier 0.5 (в 2 раза быстрее)
    // score 5 -> multiplier 1.0 (медианное время)
    // score 0 -> multiplier 2.0 (в 2 раза дольше)
    const multiplier = 2 - (score / 10) * 1.5;

    const estimatedDays = Math.round(baseDays * multiplier);
    return Math.max(7, Math.min(180, estimatedDays));
  }

  private generateRecommendations(criteria: CriterionResult[]): string[] {
    const recommendations: string[] = [];

    for (const criterion of criteria) {
      if (criterion.score < 5 && criterion.weight > 0) {
        switch (criterion.name) {
          case 'price':
            recommendations.push('Розгляньте можливість зниження ціни для швидшого продажу');
            break;
          case 'livingArea':
            recommendations.push('Площа менша за аналоги — підкресліть інші переваги');
            break;
          case 'competition':
            recommendations.push('Висока конкуренція на ринку — потрібна конкурентна ціна або унікальна пропозиція');
            break;
          case 'condition':
            recommendations.push('Косметичний ремонт може підвищити ліквідність');
            break;
          case 'floor':
            recommendations.push('Зверніть увагу на особливості поверху в описі');
            break;
          case 'infrastructure':
            recommendations.push('Об\'єкт далеко від інфраструктури — підкресліть інші переваги локації');
            break;
          case 'furniture':
            recommendations.push('Додавання меблів та техніки може підвищити ліквідність');
            break;
          case 'communications':
            recommendations.push('Недостатньо комунікацій — це може ускладнити продаж');
            break;
          case 'exposureTime':
            recommendations.push('Час експозиції вище ринку — розгляньте коригування ціни');
            break;
        }
      }
    }

    return recommendations;
  }

  private mapToCriterionDto(result: CriterionResult): LiquidityCriterionDto {
    return {
      name: result.name,
      weight: result.weight,
      score: result.score,
      weightedScore: Math.round(result.weightedScore * 100) / 100,
      explanation: result.explanation,
    };
  }

  private async findSubjectListing(options: LiquidityOptions): Promise<UnifiedListing | null> {
    if (options.listingId) {
      return this.listingRepository.findOne({ where: { id: options.listingId } });
    }

    if (options.sourceType && options.sourceId) {
      return this.listingRepository.findOne({
        where: { sourceType: options.sourceType, sourceId: options.sourceId },
      });
    }

    return null;
  }
}
